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
import org.apache.iotdb.db.storageengine.load.LoadTsFileManager;
import org.apache.iotdb.db.storageengine.load.splitter.TsFileData;

import org.apache.tsfile.exception.NotImplementedException;
import org.apache.tsfile.exception.write.PageException;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Consensus-backed LOAD request carrying Begin / Piece / Seal / Commit phases. Submitted once to
 * the DataRegion write peer (or Ratis leader); replicas apply via the consensus state machine.
 */
public class LoadTsFileConsensusNode extends SearchNode implements WALEntryValue {

  private LoadTsFileConsensusOp op;
  private String loadId;
  private String tsFileId;
  private boolean isTableModel;
  private String database;
  private int expectedPieceCount = -1;
  private long pieceIndex = -1;
  private long pieceOffset = -1;
  private List<PieceRef> pieceRefs = new ArrayList<>();
  private long dataSize;
  private long checksum;
  private int pieceCount = -1;
  private long totalBytes;
  private boolean isGeneratedByPipe;
  private boolean deleteAfterLoad;
  private List<TsFileData> tsFileDataList = new ArrayList<>();
  private Map<TTimePartitionSlot, byte[]> timePartition2ProgressIndex = new HashMap<>();
  private TRegionReplicaSet regionReplicaSet;
  private ProgressIndex progressIndex;
  private boolean isGeneratedByRemoteConsensusLeader;

  /** Endpoint ("ip:port") of the follower that issued a PULL, used to push the piece back. */
  private String pullSourceEndPoint;

  public LoadTsFileConsensusNode(PlanNodeId id) {
    super(id);
  }

  public static LoadTsFileConsensusNode begin(
      PlanNodeId id,
      String loadId,
      String tsFileId,
      boolean isTableModel,
      String database,
      int expectedPieceCount) {
    final LoadTsFileConsensusNode node = new LoadTsFileConsensusNode(id);
    node.op = LoadTsFileConsensusOp.BEGIN;
    node.loadId = loadId;
    node.tsFileId = tsFileId;
    node.isTableModel = isTableModel;
    node.database = database == null ? "" : database;
    node.expectedPieceCount = expectedPieceCount;
    return node;
  }

  public static LoadTsFileConsensusNode piece(
      PlanNodeId id,
      String loadId,
      String tsFileId,
      long pieceIndex,
      long pieceOffset,
      List<TsFileData> tsFileDataList,
      long checksum) {
    final LoadTsFileConsensusNode node = new LoadTsFileConsensusNode(id);
    node.op = LoadTsFileConsensusOp.PIECE;
    node.loadId = loadId;
    node.tsFileId = tsFileId;
    node.pieceIndex = pieceIndex;
    node.pieceOffset = pieceOffset;
    node.tsFileDataList =
        tsFileDataList == null ? new ArrayList<>() : new ArrayList<>(tsFileDataList);
    node.dataSize = node.tsFileDataList.stream().mapToLong(TsFileData::getDataSize).sum();
    // The checksum is part of the consensus contract and must be preserved exactly as supplied by
    // the caller. Recomputing it here silently changes the value for callers that use a staged
    // piece checksum (and made marker/piece validation disagree).
    node.checksum = checksum;
    return node;
  }

  public static LoadTsFileConsensusNode pieceRef(
      PlanNodeId id,
      String loadId,
      String tsFileId,
      long pieceIndex,
      String relativePath,
      long offset,
      long size,
      long checksum) {
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
      PlanNodeId id,
      String loadId,
      String tsFileId,
      long pieceIndex,
      List<PieceRef> refs,
      long checksum,
      long dataSize) {
    return pieceRefs(id, loadId, tsFileId, pieceIndex, refs, checksum, dataSize, null);
  }

  public static LoadTsFileConsensusNode pieceRefs(
      PlanNodeId id,
      String loadId,
      String tsFileId,
      long pieceIndex,
      List<PieceRef> refs,
      long checksum,
      long dataSize,
      List<TsFileData> deletions) {
    final LoadTsFileConsensusNode node = new LoadTsFileConsensusNode(id);
    node.op = LoadTsFileConsensusOp.PIECE;
    node.loadId = loadId;
    node.tsFileId = tsFileId;
    node.pieceIndex = pieceIndex;
    node.pieceRefs = refs == null ? new ArrayList<>() : new ArrayList<>(refs);
    node.dataSize = dataSize;
    node.checksum = checksum;
    if (deletions != null) {
      node.tsFileDataList = new ArrayList<>(deletions);
    }
    return node;
  }

  /**
   * Marker-only PIECE: carries the piece metadata (index, checksum, byte size) but neither chunk
   * data nor piece refs. This is what the write node logs to the WAL after applying a chunk-data
   * piece. The write node keeps the actual chunk bytes in its retained-piece store; a follower
   * applies them only when this marker arrives (pulling the retained bytes back on demand), so the
   * WAL stays at dozens of bytes per piece instead of the full LOAD bytes.
   */
  public static LoadTsFileConsensusNode pieceMarker(
      PlanNodeId id,
      String loadId,
      String tsFileId,
      long pieceIndex,
      long checksum,
      long dataSize) {
    final LoadTsFileConsensusNode node = new LoadTsFileConsensusNode(id);
    node.op = LoadTsFileConsensusOp.PIECE;
    node.loadId = loadId;
    node.tsFileId = tsFileId;
    node.pieceIndex = pieceIndex;
    node.checksum = checksum;
    node.dataSize = dataSize;
    return node;
  }

  /** PULL request: asks the current write node to re-deliver one already-applied piece. */
  public static LoadTsFileConsensusNode pull(
      PlanNodeId id,
      String loadId,
      String tsFileId,
      long pieceIndex,
      long checksum,
      String pullSourceEndPoint) {
    final LoadTsFileConsensusNode node = new LoadTsFileConsensusNode(id);
    node.op = LoadTsFileConsensusOp.PULL;
    node.loadId = loadId;
    node.tsFileId = tsFileId;
    node.pieceIndex = pieceIndex;
    node.checksum = checksum;
    node.pullSourceEndPoint = pullSourceEndPoint;
    return node;
  }

  public static LoadTsFileConsensusNode pieceRef(
      PlanNodeId id,
      String loadId,
      String tsFileId,
      long pieceIndex,
      String relativePath,
      long offset,
      long size,
      long checksum,
      long dataSize) {
    final LoadTsFileConsensusNode node =
        pieceRef(id, loadId, tsFileId, pieceIndex, relativePath, offset, size, checksum);
    node.dataSize = dataSize;
    return node;
  }

  public static LoadTsFileConsensusNode prepare(
      PlanNodeId id,
      String loadId,
      String tsFileId,
      int pieceCount,
      long totalBytes,
      long checksum,
      Map<TTimePartitionSlot, byte[]> timePartition2ProgressIndex) {
    final LoadTsFileConsensusNode node = new LoadTsFileConsensusNode(id);
    node.op = LoadTsFileConsensusOp.PREPARE;
    node.loadId = loadId;
    node.tsFileId = tsFileId;
    node.pieceCount = pieceCount;
    node.totalBytes = totalBytes;
    node.checksum = checksum;
    node.timePartition2ProgressIndex =
        timePartition2ProgressIndex == null
            ? new HashMap<>()
            : new HashMap<>(timePartition2ProgressIndex);
    return node;
  }

  public static LoadTsFileConsensusNode abort(
      PlanNodeId id, String loadId, String tsFileId, boolean isGeneratedByPipe) {
    final LoadTsFileConsensusNode node = new LoadTsFileConsensusNode(id);
    node.op = LoadTsFileConsensusOp.ABORT;
    node.loadId = loadId;
    node.tsFileId = tsFileId;
    node.isGeneratedByPipe = isGeneratedByPipe;
    return node;
  }

  public static LoadTsFileConsensusNode commit(
      PlanNodeId id,
      String loadId,
      String tsFileId,
      boolean isGeneratedByPipe,
      boolean deleteAfterLoad,
      Map<TTimePartitionSlot, byte[]> timePartition2ProgressIndex) {
    final LoadTsFileConsensusNode node = new LoadTsFileConsensusNode(id);
    node.op = LoadTsFileConsensusOp.COMMIT;
    node.loadId = loadId;
    node.tsFileId = tsFileId;
    node.isGeneratedByPipe = isGeneratedByPipe;
    node.deleteAfterLoad = deleteAfterLoad;
    node.timePartition2ProgressIndex =
        timePartition2ProgressIndex == null
            ? new HashMap<>()
            : new HashMap<>(timePartition2ProgressIndex);
    return node;
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

  public boolean isTableModel() {
    return isTableModel;
  }

  public String getDatabase() {
    return database;
  }

  public int getExpectedPieceCount() {
    return expectedPieceCount;
  }

  public long getPieceIndex() {
    return pieceIndex;
  }

  public long getPieceOffset() {
    return pieceOffset;
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

  public boolean isDeleteAfterLoad() {
    return deleteAfterLoad;
  }

  public List<TsFileData> getTsFileDataList() {
    return tsFileDataList;
  }

  /** Whether this PIECE carries actual chunk/deletion data (write-node path) or is a marker. */
  public boolean hasChunkData() {
    return !tsFileDataList.isEmpty();
  }

  public String getPullSourceEndPoint() {
    return pullSourceEndPoint;
  }

  public void setPullSourceEndPoint(String pullSourceEndPoint) {
    this.pullSourceEndPoint = pullSourceEndPoint;
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

  private void serializeBody(DataOutputStream stream, boolean includeContent) throws IOException {
    ReadWriteIOUtils.write(op.ordinal(), stream);
    ReadWriteIOUtils.write(loadId, stream);
    ReadWriteIOUtils.write(tsFileId, stream);
    ReadWriteIOUtils.write(isTableModel, stream);
    ReadWriteIOUtils.write(database == null ? "" : database, stream);
    ReadWriteIOUtils.write(expectedPieceCount, stream);
    ReadWriteIOUtils.write(pieceIndex, stream);
    ReadWriteIOUtils.write(pieceOffset, stream);
    ReadWriteIOUtils.write(dataSize, stream);
    ReadWriteIOUtils.write(checksum, stream);
    ReadWriteIOUtils.write(pieceCount, stream);
    ReadWriteIOUtils.write(totalBytes, stream);
    ReadWriteIOUtils.write(isGeneratedByPipe, stream);
    ReadWriteIOUtils.write(deleteAfterLoad, stream);
    ReadWriteIOUtils.write(pieceRefs.size(), stream);
    for (PieceRef ref : pieceRefs) {
      ReadWriteIOUtils.write(ref.relativePath, stream);
      ReadWriteIOUtils.write(ref.offset, stream);
      ReadWriteIOUtils.write(ref.size, stream);
      if (includeContent) {
        final byte[] content = readPieceContent(ref);
        ReadWriteIOUtils.write(content.length, stream);
        stream.write(content);
      }
    }
    ReadWriteIOUtils.write(tsFileDataList.size(), stream);
    for (TsFileData data : tsFileDataList) {
      data.serialize(stream);
    }
    ReadWriteIOUtils.write(timePartition2ProgressIndex.size(), stream);
    for (Map.Entry<TTimePartitionSlot, byte[]> entry : timePartition2ProgressIndex.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey().getStartTime(), stream);
      ReadWriteIOUtils.write(entry.getValue().length, stream);
      stream.write(entry.getValue());
    }
    ReadWriteIOUtils.write(pullSourceEndPoint == null ? "" : pullSourceEndPoint, stream);
  }

  public static LoadTsFileConsensusNode deserialize(ByteBuffer buffer) {
    try {
      final LoadTsFilePieceNode.ByteBufferInputStream stream =
          new LoadTsFilePieceNode.ByteBufferInputStream(buffer);
      final LoadTsFileConsensusNode node = deserializeBody(stream, true);
      final PlanNodeId planNodeId = PlanNodeId.deserialize(buffer);
      node.setPlanNodeId(planNodeId);
      ReadWriteIOUtils.readInt(buffer);
      return node;
    } catch (IOException | PageException | IllegalPathException e) {
      throw new IllegalStateException(e);
    }
  }

  private static LoadTsFileConsensusNode deserializeBody(InputStream stream)
      throws IOException, PageException, IllegalPathException {
    return deserializeBody(stream, false);
  }

  private static LoadTsFileConsensusNode deserializeBody(InputStream stream, boolean readContent)
      throws IOException, PageException, IllegalPathException {
    final LoadTsFileConsensusNode node = new LoadTsFileConsensusNode(new PlanNodeId(""));
    node.op = LoadTsFileConsensusOp.fromOrdinal(ReadWriteIOUtils.readInt(stream));
    node.loadId = ReadWriteIOUtils.readString(stream);
    node.tsFileId = ReadWriteIOUtils.readString(stream);
    node.isTableModel = ReadWriteIOUtils.readBool(stream);
    node.database = ReadWriteIOUtils.readString(stream);
    node.expectedPieceCount = ReadWriteIOUtils.readInt(stream);
    node.pieceIndex = ReadWriteIOUtils.readLong(stream);
    node.pieceOffset = ReadWriteIOUtils.readLong(stream);
    node.dataSize = ReadWriteIOUtils.readLong(stream);
    node.checksum = ReadWriteIOUtils.readLong(stream);
    node.pieceCount = ReadWriteIOUtils.readInt(stream);
    node.totalBytes = ReadWriteIOUtils.readLong(stream);
    node.isGeneratedByPipe = ReadWriteIOUtils.readBool(stream);
    node.deleteAfterLoad = ReadWriteIOUtils.readBool(stream);
    final int refCount = ReadWriteIOUtils.readInt(stream);
    node.pieceRefs = new ArrayList<>(refCount);
    for (int i = 0; i < refCount; i++) {
      final String refPath = ReadWriteIOUtils.readString(stream);
      final long refOffset = ReadWriteIOUtils.readLong(stream);
      final long refSize = ReadWriteIOUtils.readLong(stream);
      byte[] content = null;
      if (readContent) {
        final int contentLength = ReadWriteIOUtils.readInt(stream);
        if (contentLength > 0) {
          content = new byte[contentLength];
          int offset = 0;
          while (offset < contentLength) {
            final int read = stream.read(content, offset, contentLength - offset);
            if (read < 0) {
              throw new IOException(
                  DataNodeQueryMessages.EXCEPTION_UNKNOWN_LOADTSFILECONSENSUSOP_ORDINAL_ARG_62848FC2
                      + "pieceContent");
            }
            offset += read;
          }
        }
      }
      node.pieceRefs.add(new PieceRef(refPath, refOffset, refSize, content));
    }
    final int dataCount = ReadWriteIOUtils.readInt(stream);
    node.tsFileDataList = new ArrayList<>(dataCount);
    for (int i = 0; i < dataCount; i++) {
      node.tsFileDataList.add(TsFileData.deserialize(stream));
    }
    final int progressCount = ReadWriteIOUtils.readInt(stream);
    node.timePartition2ProgressIndex = new HashMap<>(progressCount);
    for (int i = 0; i < progressCount; i++) {
      final long startTime = ReadWriteIOUtils.readLong(stream);
      final int len = ReadWriteIOUtils.readInt(stream);
      final byte[] bytes = new byte[len];
      int offset = 0;
      while (offset < len) {
        final int read = stream.read(bytes, offset, len - offset);
        if (read < 0) {
          throw new IOException(
              DataNodeQueryMessages.EXCEPTION_UNKNOWN_LOADTSFILECONSENSUSOP_ORDINAL_ARG_62848FC2
                  + "progressIndex");
        }
        offset += read;
      }
      node.timePartition2ProgressIndex.put(new TTimePartitionSlot(startTime), bytes);
    }
    node.pullSourceEndPoint = ReadWriteIOUtils.readString(stream);
    return node;
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
    try (PublicBAOS byteArrayOutputStream = new PublicBAOS();
        DataOutputStream stream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(WALEntryType.LOAD_TSFILE_CONSENSUS_NODE.getCode(), stream);
      ReadWriteIOUtils.write(-1L, stream);
      ReadWriteIOUtils.write(getType().getNodeType(), stream);
      serializeBody(stream, true);
      getPlanNodeId().serialize(stream);
      ReadWriteIOUtils.write(0, stream);
      return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  private byte[] readPieceContent(PieceRef ref) {
    if (ref.content != null) {
      return ref.content;
    }
    // PieceRef construction already validated offset/size bounds (non-negative, no overflow,
    // within int range), so the narrowing cast below is safe.
    final byte[] content = new byte[(int) ref.size];
    if (content.length == 0) {
      return content;
    }
    final File file =
        LoadTsFileManager.findLoadTsFile(ref.relativePath)
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        DataNodeQueryMessages
                                .EXCEPTION_UNKNOWN_LOADTSFILECONSENSUSOP_ORDINAL_ARG_62848FC2
                            + "piece file not found: "
                            + ref.relativePath));
    try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
      raf.seek(ref.offset);
      raf.readFully(content);
      return content;
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public int serializedSize() {
    try {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      final DataOutputStream stream = new DataOutputStream(baos);
      serializeBody(stream);
      return Short.BYTES + Long.BYTES + baos.size();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  public static LoadTsFileConsensusNode deserializeFromWAL(DataInputStream stream)
      throws IOException {
    final long searchIndex = stream.readLong();
    try {
      final LoadTsFileConsensusNode node = deserializeBody(stream, false);
      node.setSearchIndexFromWAL(searchIndex);
      return node;
    } catch (PageException | IllegalPathException e) {
      throw new IOException(e);
    }
  }

  public static LoadTsFileConsensusNode deserializeFromWAL(ByteBuffer buffer) {
    final long searchIndex = buffer.getLong();
    try {
      final LoadTsFileConsensusNode node =
          deserializeBody(new LoadTsFilePieceNode.ByteBufferInputStream(buffer), false);
      node.setSearchIndexFromWAL(searchIndex);
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

  /** A staging-file reference recorded by the consensus/WAL entry instead of the raw content. */
  public static class PieceRef {
    private final String relativePath;
    private final long offset;
    private final long size;
    private final byte[] content;

    public PieceRef(String relativePath, long size) {
      this(relativePath, 0L, size, null);
    }

    public PieceRef(String relativePath, long offset, long size) {
      this(relativePath, offset, size, null);
    }

    public PieceRef(String relativePath, long offset, long size, byte[] content) {
      // Bounds/overflow guard before the legacy raw-ref path casts size to int and seeks the file:
      // a malformed or hostile ref must fail here instead of truncating, overflowing or escaping
      // into an arbitrary byte array.
      if (offset < 0
          || size < 0
          || size > Integer.MAX_VALUE
          || offset + size < 0
          || offset + size > Integer.MAX_VALUE) {
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
      this.content = content;
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

    public byte[] getContent() {
      return content;
    }
  }
}
