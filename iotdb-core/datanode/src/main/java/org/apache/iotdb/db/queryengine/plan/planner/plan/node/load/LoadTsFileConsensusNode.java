/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.load;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.IPlanVisitor;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.SearchNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntryType;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntryValue;
import org.apache.iotdb.db.storageengine.load.splitter.TsFileData;

import org.apache.tsfile.exception.NotImplementedException;
import org.apache.tsfile.exception.write.PageException;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** One command of a consensus-backed LOAD transaction, see {@link LoadTsFileConsensusOp}. */
public class LoadTsFileConsensusNode extends SearchNode implements WALEntryValue {

  private final LoadTsFileConsensusOp op;

  /** The staged directory every phase applies to: the identity of the transaction. */
  private final String loadId;

  /** The source TsFile of a PIECE, kept for diagnostics. */
  private final String tsFileId;

  /** The position of a PIECE in the sequence of pieces of one staged file. */
  private final long pieceIndex;

  /** The staged files of this piece, and - once staged - the range of every payload in them. */
  private List<PieceRef> pieceRefs = new ArrayList<>();

  /** The chunks and deletions of this piece, with payloads unless it only references them. */
  private List<TsFileData> tsFileDataList = new ArrayList<>();

  /** The payload bytes this piece carries, used to account for it while it is buffered. */
  private long dataSize;

  /** The checksum the coordinator computed for this piece; part of the identity of the piece. */
  private long checksum;

  /** How many pieces the coordinator sent to the region, as accounted per region. */
  private int pieceCount = -1;

  /** How many payload bytes those pieces carried, as accounted per region. */
  private long totalBytes;

  /** The progress index to adopt per time partition this transaction wrote. */
  private Map<TTimePartitionSlot, byte[]> timePartition2ProgressIndex = new HashMap<>();

  /** Whether the staged file was produced by a pipe, which selects the import path of the data. */
  private boolean isGeneratedByPipe;

  private TRegionReplicaSet regionReplicaSet;
  private ProgressIndex progressIndex;
  private boolean isGeneratedByRemoteConsensusLeader;

  private LoadTsFileConsensusNode(
      final PlanNodeId id,
      final LoadTsFileConsensusOp op,
      final String loadId,
      final String tsFileId,
      final long pieceIndex) {
    super(id);
    this.op = op;
    this.loadId = loadId;
    this.tsFileId = tsFileId;
    this.pieceIndex = pieceIndex;
  }

  /** The PIECE the coordinator sends: the chunks of the piece, payloads included. */
  public static LoadTsFileConsensusNode piece(
      final PlanNodeId id,
      final String loadId,
      final String tsFileId,
      final long pieceIndex,
      final List<TsFileData> tsFileDataList) {
    final LoadTsFileConsensusNode node =
        new LoadTsFileConsensusNode(id, LoadTsFileConsensusOp.PIECE, loadId, tsFileId, pieceIndex);
    node.tsFileDataList =
        tsFileDataList == null ? new ArrayList<>() : new ArrayList<>(tsFileDataList);
    node.dataSize = node.tsFileDataList.stream().mapToLong(TsFileData::getDataSize).sum();
    return node;
  }

  public static LoadTsFileConsensusNode pieceRef(
      final PlanNodeId id,
      final String loadId,
      final String tsFileId,
      final long pieceIndex,
      final String relativePath,
      final long offset,
      final long size,
      final long checksum) {
    return pieceRefs(
        id,
        loadId,
        tsFileId,
        pieceIndex,
        Collections.singletonList(new PieceRef(relativePath, offset, size)),
        checksum,
        size);
  }

  public static LoadTsFileConsensusNode pieceRefs(
      final PlanNodeId id,
      final String loadId,
      final String tsFileId,
      final long pieceIndex,
      final List<PieceRef> refs,
      final long checksum,
      final long dataSize) {
    return pieceRefs(id, loadId, tsFileId, pieceIndex, refs, checksum, dataSize, null);
  }

  /** The PIECE the write node re-logs once staged: references instead of payload bytes. */
  public static LoadTsFileConsensusNode pieceRefs(
      final PlanNodeId id,
      final String loadId,
      final String tsFileId,
      final long pieceIndex,
      final List<PieceRef> refs,
      final long checksum,
      final long dataSize,
      final List<TsFileData> deletions) {
    final LoadTsFileConsensusNode node =
        new LoadTsFileConsensusNode(id, LoadTsFileConsensusOp.PIECE, loadId, tsFileId, pieceIndex);
    node.pieceRefs = refs == null ? new ArrayList<>() : new ArrayList<>(refs);
    node.dataSize = dataSize;
    node.checksum = checksum;
    if (deletions != null) {
      node.tsFileDataList = new ArrayList<>(deletions);
    }
    return node;
  }

  /** A PIECE with metadata but nothing to apply; only tests build it. */
  public static LoadTsFileConsensusNode pieceMarker(
      final PlanNodeId id,
      final String loadId,
      final String tsFileId,
      final long pieceIndex,
      final long checksum,
      final long dataSize) {
    final LoadTsFileConsensusNode node =
        new LoadTsFileConsensusNode(id, LoadTsFileConsensusOp.PIECE, loadId, tsFileId, pieceIndex);
    node.checksum = checksum;
    node.dataSize = dataSize;
    return node;
  }

  public static LoadTsFileConsensusNode pieceRef(
      final PlanNodeId id,
      final String loadId,
      final String tsFileId,
      final long pieceIndex,
      final String relativePath,
      final long offset,
      final long size,
      final long checksum,
      final long dataSize) {
    final LoadTsFileConsensusNode node =
        pieceRef(id, loadId, tsFileId, pieceIndex, relativePath, offset, size, checksum);
    node.dataSize = dataSize;
    return node;
  }

  /** PREPARE: writes out the staged resources with the progress indexes, imports nothing. */
  public static LoadTsFileConsensusNode prepare(
      final PlanNodeId id,
      final String loadId,
      final int pieceCount,
      final long totalBytes,
      final boolean isGeneratedByPipe,
      final Map<TTimePartitionSlot, byte[]> timePartition2ProgressIndex) {
    final LoadTsFileConsensusNode node =
        new LoadTsFileConsensusNode(id, LoadTsFileConsensusOp.PREPARE, loadId, null, -1L);
    node.pieceCount = pieceCount;
    node.totalBytes = totalBytes;
    node.isGeneratedByPipe = isGeneratedByPipe;
    node.timePartition2ProgressIndex = copyProgressIndexes(timePartition2ProgressIndex);
    return node;
  }

  /** COMMIT: imports the staged data. Deleting the source TsFile is the coordinator's business. */
  public static LoadTsFileConsensusNode commit(
      final PlanNodeId id,
      final String loadId,
      final boolean isGeneratedByPipe,
      final Map<TTimePartitionSlot, byte[]> timePartition2ProgressIndex) {
    final LoadTsFileConsensusNode node =
        new LoadTsFileConsensusNode(id, LoadTsFileConsensusOp.COMMIT, loadId, null, -1L);
    node.isGeneratedByPipe = isGeneratedByPipe;
    node.timePartition2ProgressIndex = copyProgressIndexes(timePartition2ProgressIndex);
    return node;
  }

  /** ABORT: discards the staged data. Idempotent, so it settles a COMMIT answer that was lost. */
  public static LoadTsFileConsensusNode abort(
      final PlanNodeId id, final String loadId, final boolean isGeneratedByPipe) {
    final LoadTsFileConsensusNode node =
        new LoadTsFileConsensusNode(id, LoadTsFileConsensusOp.ABORT, loadId, null, -1L);
    node.isGeneratedByPipe = isGeneratedByPipe;
    return node;
  }

  private static Map<TTimePartitionSlot, byte[]> copyProgressIndexes(
      final Map<TTimePartitionSlot, byte[]> timePartition2ProgressIndex) {
    return timePartition2ProgressIndex == null
        ? new HashMap<>()
        : new HashMap<>(timePartition2ProgressIndex);
  }

  public LoadTsFileConsensusOp getOp() {
    return op;
  }

  public String getLoadId() {
    return loadId;
  }

  public String getTsFileId() {
    return tsFileId;
  }

  public long getPieceIndex() {
    return pieceIndex;
  }

  public String getRelativePath() {
    return pieceRefs.isEmpty() ? null : pieceRefs.get(0).relativePath;
  }

  public long getSize() {
    return pieceRefs.stream().mapToLong(ref -> ref.size).sum();
  }

  public List<PieceRef> getPieceRefs() {
    return pieceRefs;
  }

  public void setPieceRefs(final List<PieceRef> pieceRefs) {
    this.pieceRefs = new ArrayList<>(pieceRefs);
  }

  public long getDataSize() {
    return dataSize;
  }

  public long getChecksum() {
    return checksum;
  }

  public boolean isGeneratedByRemoteConsensusLeader() {
    return isGeneratedByRemoteConsensusLeader;
  }

  public void markAsGeneratedByRemoteConsensusLeader() {
    this.isGeneratedByRemoteConsensusLeader = true;
  }

  public int getPieceCount() {
    return pieceCount;
  }

  public long getTotalBytes() {
    return totalBytes;
  }

  public boolean isGeneratedByPipe() {
    return isGeneratedByPipe;
  }

  public List<TsFileData> getTsFileDataList() {
    return tsFileDataList;
  }

  /** Whether this PIECE carries actual chunk/deletion data (write-node path) or is a marker. */
  public boolean hasChunkData() {
    return !tsFileDataList.isEmpty();
  }

  public Map<TTimePartitionSlot, byte[]> getTimePartition2ProgressIndex() {
    return timePartition2ProgressIndex;
  }

  public void setRegionReplicaSet(TRegionReplicaSet regionReplicaSet) {
    this.regionReplicaSet = regionReplicaSet;
  }

  @Override
  public TRegionReplicaSet getRegionReplicaSet() {
    return regionReplicaSet;
  }

  @Override
  public ProgressIndex getProgressIndex() {
    return progressIndex;
  }

  @Override
  public void setProgressIndex(ProgressIndex progressIndex) {
    this.progressIndex = progressIndex;
  }

  @Override
  public SearchNode merge(List<SearchNode> searchNodes) {
    if (searchNodes.size() == 1) {
      return searchNodes.get(0);
    }
    throw new UnsupportedOperationException(DataNodeQueryMessages.MERGE_IS_NOT_SUPPORTED);
  }

  @Override
  public List<WritePlanNode> splitByPartition(IAnalysis analysis) {
    return Collections.singletonList(this);
  }

  @Override
  public List<PlanNode> getChildren() {
    return Collections.emptyList();
  }

  @Override
  public void addChild(PlanNode child) {
    // no children
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.LOAD_TSFILE_CONSENSUS;
  }

  @Override
  public PlanNode clone() {
    throw new NotImplementedException(
        DataNodeQueryMessages.CLONE_OF_LOAD_PIECE_TSFILE_IS_NOT_IMPLEMENTED);
  }

  @Override
  public int allowedChildCount() {
    return NO_CHILD_ALLOWED;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return Collections.emptyList();
  }

  @Override
  public <R, C> R accept(IPlanVisitor<R, C> visitor, C context) {
    return ((PlanVisitor<R, C>) visitor).visitLoadTsFileConsensus(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.LOAD_TSFILE_CONSENSUS.serialize(byteBuffer);
    try {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      final DataOutputStream stream = new DataOutputStream(baos);
      serializeBody(stream, true);
      byteBuffer.put(baos.toByteArray());
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.LOAD_TSFILE_CONSENSUS.serialize(stream);
    serializeBody(stream, true);
  }

  private void serializeBody(DataOutputStream stream) throws IOException {
    serializeBody(stream, false);
  }

  /** Writes the phase code, then only the fields of that phase; inlines payloads when asked. */
  private void serializeBody(DataOutputStream stream, boolean includeContent) throws IOException {
    ReadWriteIOUtils.write(op.getCode(), stream);
    ReadWriteIOUtils.write(loadId, stream);
    switch (op) {
      case PIECE:
        serializePieceBody(stream, includeContent);
        return;
      case PREPARE:
        serializePrepareBody(stream);
        return;
      case COMMIT:
        serializeCommitBody(stream);
        return;
      case ABORT:
        serializeAbortBody(stream);
        return;
    }
  }

  private void serializePieceBody(final DataOutputStream stream, final boolean includeContent)
      throws IOException {
    ReadWriteIOUtils.write(tsFileId, stream);
    ReadWriteIOUtils.write(pieceIndex, stream);
    ReadWriteIOUtils.write(checksum, stream);
    ReadWriteIOUtils.write(dataSize, stream);
    ReadWriteIOUtils.write(pieceRefs.size(), stream);
    for (final PieceRef ref : pieceRefs) {
      ReadWriteIOUtils.write(ref.relativePath, stream);
      ReadWriteIOUtils.write(ref.offset, stream);
      ReadWriteIOUtils.write(ref.size, stream);
    }
    ReadWriteIOUtils.write(tsFileDataList.size(), stream);
    for (final TsFileData data : tsFileDataList) {
      data.serialize(stream, includeContent);
    }
  }

  private void serializePrepareBody(final DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(pieceCount, stream);
    ReadWriteIOUtils.write(totalBytes, stream);
    ReadWriteIOUtils.write(isGeneratedByPipe, stream);
    writeProgressIndexes(stream);
  }

  private void serializeCommitBody(final DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(isGeneratedByPipe, stream);
    writeProgressIndexes(stream);
  }

  private void serializeAbortBody(final DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(isGeneratedByPipe, stream);
  }

  private void writeProgressIndexes(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(timePartition2ProgressIndex.size(), stream);
    // Sorted: a HashMap's iteration order is not reproducible and a retry must log the same bytes.
    final List<Map.Entry<TTimePartitionSlot, byte[]>> sortedProgressIndexes =
        new ArrayList<>(timePartition2ProgressIndex.entrySet());
    sortedProgressIndexes.sort(Comparator.comparingLong(entry -> entry.getKey().getStartTime()));
    for (final Map.Entry<TTimePartitionSlot, byte[]> entry : sortedProgressIndexes) {
      ReadWriteIOUtils.write(entry.getKey().getStartTime(), stream);
      ReadWriteIOUtils.write(entry.getValue().length, stream);
      stream.write(entry.getValue());
    }
  }

  public static LoadTsFileConsensusNode deserialize(ByteBuffer buffer) {
    try {
      final LoadTsFilePieceNode.ByteBufferInputStream stream =
          new LoadTsFilePieceNode.ByteBufferInputStream(buffer);
      final LoadTsFileConsensusNode node = deserializeBody(stream);
      final PlanNodeId planNodeId = PlanNodeId.deserialize(buffer);
      node.setPlanNodeId(planNodeId);
      // A serialized plan node ends with its child count; a LOAD command never has one, so it is 0.
      ReadWriteIOUtils.readInt(buffer);
      return node;
    } catch (IOException | PageException | IllegalPathException e) {
      throw new IllegalStateException(e);
    }
  }

  private static LoadTsFileConsensusNode deserializeBody(final InputStream stream)
      throws IOException, PageException, IllegalPathException {
    final LoadTsFileConsensusOp op =
        LoadTsFileConsensusOp.fromCode(ReadWriteIOUtils.readInt(stream));
    final String loadId = ReadWriteIOUtils.readString(stream);
    return switch (op) {
      case PIECE -> deserializePieceBody(stream, loadId);
      case PREPARE -> deserializePrepareBody(stream, loadId);
      case COMMIT -> deserializeCommitBody(stream, loadId);
      case ABORT -> deserializeAbortBody(stream, loadId);
    };
  }

  private static LoadTsFileConsensusNode deserializePieceBody(
      final InputStream stream, final String loadId)
      throws IOException, PageException, IllegalPathException {
    final String tsFileId = ReadWriteIOUtils.readString(stream);
    final long pieceIndex = ReadWriteIOUtils.readLong(stream);
    final long checksum = ReadWriteIOUtils.readLong(stream);
    final long dataSize = ReadWriteIOUtils.readLong(stream);
    final int refCount = readCount(stream, "piece reference");
    final List<PieceRef> pieceRefs = new ArrayList<>(refCount);
    for (int i = 0; i < refCount; i++) {
      final String refPath = ReadWriteIOUtils.readString(stream);
      final long refOffset = ReadWriteIOUtils.readLong(stream);
      final long refSize = ReadWriteIOUtils.readLong(stream);
      pieceRefs.add(new PieceRef(refPath, refOffset, refSize));
    }
    final int dataCount = readCount(stream, "staged data");
    final List<TsFileData> tsFileDataList = new ArrayList<>(dataCount);
    for (int i = 0; i < dataCount; i++) {
      tsFileDataList.add(TsFileData.deserialize(stream));
    }
    final LoadTsFileConsensusNode node =
        new LoadTsFileConsensusNode(
            new PlanNodeId(""), LoadTsFileConsensusOp.PIECE, loadId, tsFileId, pieceIndex);
    node.checksum = checksum;
    node.dataSize = dataSize;
    node.pieceRefs = pieceRefs;
    node.tsFileDataList = tsFileDataList;
    return node;
  }

  private static LoadTsFileConsensusNode deserializePrepareBody(
      final InputStream stream, final String loadId) throws IOException {
    final int pieceCount = ReadWriteIOUtils.readInt(stream);
    final long totalBytes = ReadWriteIOUtils.readLong(stream);
    final boolean isGeneratedByPipe = ReadWriteIOUtils.readBool(stream);
    final Map<TTimePartitionSlot, byte[]> progressIndexes = readProgressIndexes(stream);
    final LoadTsFileConsensusNode node =
        new LoadTsFileConsensusNode(
            new PlanNodeId(""), LoadTsFileConsensusOp.PREPARE, loadId, null, -1L);
    node.pieceCount = pieceCount;
    node.totalBytes = totalBytes;
    node.isGeneratedByPipe = isGeneratedByPipe;
    node.timePartition2ProgressIndex = progressIndexes;
    return node;
  }

  private static LoadTsFileConsensusNode deserializeCommitBody(
      final InputStream stream, final String loadId) throws IOException {
    final boolean isGeneratedByPipe = ReadWriteIOUtils.readBool(stream);
    final Map<TTimePartitionSlot, byte[]> progressIndexes = readProgressIndexes(stream);
    final LoadTsFileConsensusNode node =
        new LoadTsFileConsensusNode(
            new PlanNodeId(""), LoadTsFileConsensusOp.COMMIT, loadId, null, -1L);
    node.isGeneratedByPipe = isGeneratedByPipe;
    node.timePartition2ProgressIndex = progressIndexes;
    return node;
  }

  private static LoadTsFileConsensusNode deserializeAbortBody(
      final InputStream stream, final String loadId) throws IOException {
    final boolean isGeneratedByPipe = ReadWriteIOUtils.readBool(stream);
    final LoadTsFileConsensusNode node =
        new LoadTsFileConsensusNode(
            new PlanNodeId(""), LoadTsFileConsensusOp.ABORT, loadId, null, -1L);
    node.isGeneratedByPipe = isGeneratedByPipe;
    return node;
  }

  private static Map<TTimePartitionSlot, byte[]> readProgressIndexes(final InputStream stream)
      throws IOException {
    final int progressCount = readCount(stream, "progress index");
    final Map<TTimePartitionSlot, byte[]> timePartition2ProgressIndex =
        new HashMap<>(progressCount);
    for (int i = 0; i < progressCount; i++) {
      final long startTime = ReadWriteIOUtils.readLong(stream);
      final int len = readCount(stream, "progress index payload length");
      final byte[] bytes = new byte[len];
      int offset = 0;
      while (offset < len) {
        final int read = stream.read(bytes, offset, len - offset);
        if (read < 0) {
          throw new IOException(
              String.format(
                  DataNodeQueryMessages
                      .EXCEPTION_LOAD_CONSENSUS_ENTRY_ENDED_WHILE_READING_ARG_ARG_OF_ARG_BYTES_WERE_AVAILABLE_D927147B,
                  "progress index payload",
                  offset,
                  len));
        }
        offset += read;
      }
      timePartition2ProgressIndex.put(new TTimePartitionSlot(startTime), bytes);
    }
    return timePartition2ProgressIndex;
  }

  /** Reads a length-prefixed element count, rejecting one that cannot describe a body. */
  private static int readCount(final InputStream stream, final String what) throws IOException {
    final int count = ReadWriteIOUtils.readInt(stream);
    if (count < 0) {
      throw new IOException(
          String.format(
              DataNodeQueryMessages
                  .EXCEPTION_LOAD_CONSENSUS_ENTRY_DECLARES_A_NEGATIVE_ARG_COUNT_ARG_2DEADCCC,
              what,
              count));
    }
    return count;
  }

  @Override
  public void serializeToWAL(IWALByteBufferView buffer) {
    serializeToWAL(buffer, getEncodedSearchIndex());
  }

  public void serializeToWAL(IWALByteBufferView buffer, long encodedSearchIndex) {
    try {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      final DataOutputStream stream = new DataOutputStream(baos);
      stream.writeShort(getType().getNodeType());
      stream.writeLong(encodedSearchIndex);
      serializeBody(stream);
      buffer.put(baos.toByteArray());
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  /** Serialize this node into an IoTConsensusRequest-compatible buffer with content expanded. */
  public ByteBuffer serialize() {
    return serialize(true);
  }

  /**
   * Serialize this node into an IoTConsensusRequest-compatible buffer, that is, the entry type, a
   * sentinel mem-table id, the node type, the search index of the piece and then the node itself.
   *
   * @param includeContent whether the payload of every chunk is inlined, reading it back from the
   *     staged file when this node was restored from the WAL. Passing false keeps the payload
   *     references instead, which makes the receiver fail its PREPARE loudly instead of failing
   *     this node while it is being dispatched.
   */
  public ByteBuffer serialize(final boolean includeContent) {
    try (PublicBAOS byteArrayOutputStream =
            new PublicBAOS(initialSerializationCapacity(includeContent));
        DataOutputStream stream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(WALEntryType.LOAD_TSFILE_CONSENSUS_NODE.getCode(), stream);
      ReadWriteIOUtils.write(-1L, stream);
      ReadWriteIOUtils.write(getType().getNodeType(), stream);
      // The replica reads this buffer back as a consensus entry, so it has to carry the search
      // index the piece was written with: without it every entry the replica derives from this
      // piece would look as if it never took part in consensus.
      ReadWriteIOUtils.write(getEncodedSearchIndex(), stream);
      serializeBody(stream, includeContent);
      getPlanNodeId().serialize(stream);
      // A serialized plan node ends with its child count; a LOAD command never has one, so it is 0.
      ReadWriteIOUtils.write(0, stream);
      return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  /** Small default start for a buffer whose final size is not known upfront. */
  private static final int DEFAULT_SERIALIZATION_BUFFER_SIZE = 64;

  /** Room for everything a PIECE writes next to its payloads. */
  private static final int PIECE_METADATA_SIZE = 4 * 1024;

  /** Sizes the buffer from {@link #dataSize} upfront instead of growing it. */
  private int initialSerializationCapacity(final boolean includeContent) {
    if (!includeContent) {
      return DEFAULT_SERIALIZATION_BUFFER_SIZE;
    }
    // Clamped so that the cast cannot overflow even for a piece close to 2 GiB of payloads.
    return (int)
        Math.min(
            Integer.MAX_VALUE - Long.BYTES,
            Math.max(DEFAULT_SERIALIZATION_BUFFER_SIZE, dataSize + PIECE_METADATA_SIZE));
  }

  @Override
  public int serializedSize() {
    // Measured with a byte counter: exact, and no buffer as large as the body.
    final SizeCountingOutputStream counter = new SizeCountingOutputStream();
    try {
      serializeBody(new DataOutputStream(counter));
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    return Short.BYTES + Long.BYTES + Math.toIntExact(counter.getSize());
  }

  /** An {@link OutputStream} that measures what is written to it and keeps none of it. */
  private static final class SizeCountingOutputStream extends OutputStream {

    private long size;

    @Override
    public void write(final int b) {
      size++;
    }

    @Override
    public void write(final byte[] b, final int off, final int len) {
      size += len;
    }

    long getSize() {
      return size;
    }
  }

  public static LoadTsFileConsensusNode deserializeFromWAL(DataInputStream stream)
      throws IOException {
    final long searchIndex = stream.readLong();
    try {
      final LoadTsFileConsensusNode node = deserializeBody(stream);
      node.setSearchIndexFromWAL(searchIndex);
      return node;
    } catch (PageException | IllegalPathException e) {
      throw new IOException(e);
    }
  }

  public static LoadTsFileConsensusNode deserializeFromWAL(ByteBuffer buffer) {
    final long encodedSearchIndex = buffer.getLong();
    try {
      final LoadTsFileConsensusNode node =
          deserializeBody(new LoadTsFilePieceNode.ByteBufferInputStream(buffer));
      node.setSearchIndexFromWAL(encodedSearchIndex);
      // A serialized plan node ends with its child count; a LOAD command never has one, so it is 0.
      node.setPlanNodeId(PlanNodeId.deserialize(buffer));
      ReadWriteIOUtils.readInt(buffer);
      return node;
    } catch (IOException | PageException | IllegalPathException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof LoadTsFileConsensusNode)) {
      return false;
    }
    final LoadTsFileConsensusNode that = (LoadTsFileConsensusNode) o;
    return Objects.equals(loadId, that.loadId)
        && Objects.equals(tsFileId, that.tsFileId)
        && op == that.op
        && pieceIndex == that.pieceIndex
        && checksum == that.checksum;
  }

  @Override
  public int hashCode() {
    return Objects.hash(op, loadId, tsFileId, pieceIndex, checksum);
  }

  @Override
  public String toString() {
    return "LoadTsFileConsensusNode{op="
        + op
        + ", loadId="
        + loadId
        + ", tsFileId="
        + tsFileId
        + ", pieceIndex="
        + pieceIndex
        + '}';
  }

  /**
   * One staged file of a piece, so that a replica can be told which files the piece consists of.
   *
   * <p>This is a summary only: the payload of every chunk is referenced by {@link
   * org.apache.iotdb.db.storageengine.load.splitter.ChunkPayloadRef}.
   */
  public static class PieceRef {
    private final String relativePath;
    private final long offset;
    private final long size;

    public PieceRef(String relativePath, long size) {
      this(relativePath, 0L, size);
    }

    public PieceRef(String relativePath, long offset, long size) {
      // Bounds/overflow guard before the reference is used to seek the staged file: a malformed or
      // hostile ref must fail here instead of truncating, overflowing or escaping into an
      // arbitrary byte array.
      //
      // The offset is deliberately not bounded: this reference names a staged TsFile (offset 0, the
      // whole file) or a payload inside it, and a staged TsFile larger than 2 GiB is ordinary. Only
      // the end of the referenced range has to stay representable, which the subtraction-free form
      // below checks for an offset of any magnitude.
      if (offset < 0 || size < 0 || size > Integer.MAX_VALUE || Long.MAX_VALUE - offset < size) {
        throw new IllegalArgumentException(
            String.format(
                DataNodeQueryMessages.EXCEPTION_LOAD_CONSENSUS_INVALID_PIECE_REF_F3498507,
                relativePath,
                offset,
                size));
      }
      this.relativePath = relativePath;
      this.offset = offset;
      this.size = size;
    }

    public String getRelativePath() {
      return relativePath;
    }

    public long getOffset() {
      return offset;
    }

    public long getSize() {
      return size;
    }
  }
}
