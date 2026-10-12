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

package org.apache.iotdb.db.storageengine.load;

import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.i18n.StorageEngineMessages;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFileConsensusOp;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Tracks persistently placed physical chunk ranges for a staged TsFile during distributed LOAD.
 * Guarantees idempotency, crash-recovery, and hole-detection before file sealing.
 */
public class LoadTsFileProgress {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadTsFileProgress.class);

  public static final String PROGRESS_SUFFIX = ".progress";
  private static final String MAGIC = "LTPROG1";

  // Unified Version 1 entry markers
  private static final byte ENTRY_VERSION_CHUNK = 1;
  private static final byte ENTRY_VERSION_TERMINAL = 2;

  private static final long TS_FILE_HEADER_SIZE = 7L;

  private final File progressFile;
  private final String uuid;

  private final List<ChunkRangeRecord> records = new ArrayList<>();
  private final Set<Long> recordedChunkOffsets = new HashSet<>();
  private final Map<Long, Long> chunkOffset2SearchIndex = new HashMap<>();

  /** Records built by {@link #queueChunk} and not yet appended by {@link #flushQueuedRecords}. */
  private final List<PendingChunkRecord> queuedChunkRecords = new ArrayList<>();

  private boolean recordsLoaded;
  private long maxPhysicalEnd = -1L;
  private long lastRecordedSearchIndex = -1L;

  public LoadTsFileProgress(final File tsFile) {
    this.progressFile = progressFileFor(tsFile);
    this.uuid = tsFile.getParentFile() == null ? "" : tsFile.getParentFile().getName();
  }

  public static File progressFileFor(final File tsFile) {
    return new File(tsFile.getAbsolutePath() + PROGRESS_SUFFIX);
  }

  public boolean exists() {
    return progressFile.isFile();
  }

  public File getProgressFile() {
    return progressFile;
  }

  // -------------------------------------------------------------------------
  // Record Appending Operations
  // -------------------------------------------------------------------------

  private void ensureActivated() throws IOException {
    if (!progressFile.exists()) {
      final File parent = progressFile.getParentFile();
      if (parent != null && !parent.exists()) {
        parent.mkdirs();
      }
      try (final DataOutputStream out =
          new DataOutputStream(
              new BufferedOutputStream(new FileOutputStream(progressFile, false)))) {
        ReadWriteIOUtils.write(MAGIC, out);
        final byte[] uuidBytes = uuid.getBytes(StandardCharsets.UTF_8);
        ReadWriteIOUtils.write(uuidBytes.length, out);
        out.write(uuidBytes);
      }
    }
  }

  public void recordChunk(
      final IDeviceID device,
      final boolean aligned,
      final long chunkGroupHeaderOffset,
      final long chunkOffset,
      final boolean firstChunkOfGroup,
      final Chunk chunk,
      final long dataEnd,
      final long searchIndex)
      throws IOException {
    final long physicalStart = firstChunkOfGroup ? chunkGroupHeaderOffset : chunkOffset;
    recordChunk(
        device,
        aligned,
        chunkGroupHeaderOffset,
        chunkOffset,
        firstChunkOfGroup,
        chunk,
        dataEnd,
        physicalStart,
        searchIndex);
  }

  /**
   * Builds the record of a chunk whose bytes are already written and queues it for {@link
   * #flushQueuedRecords()}. Nothing is appended here: the caller stages the whole piece and forces
   * its bytes first, so a record never reaches the file before the bytes it describes.
   */
  public void queueChunk(
      final IDeviceID device,
      final boolean aligned,
      final long chunkGroupHeaderOffset,
      final long chunkOffset,
      final boolean firstChunkOfGroup,
      final Chunk chunk,
      final long dataEnd,
      final long actualPhysicalStart,
      final long searchIndex)
      throws IOException {
    ensureActivated();

    final ByteBuffer chunkHeaderBody = serializeChunkHeader(chunk);
    final ByteBuffer statisticsBody = serializeStatistics(chunk);
    final IDeviceID normalizedDevice =
        device instanceof StringArrayDeviceID ? device : new StringArrayDeviceID(device.toString());

    final ChunkRangeRecord record =
        new ChunkRangeRecord(
            normalizedDevice,
            aligned,
            chunkGroupHeaderOffset,
            chunkOffset,
            firstChunkOfGroup,
            chunk.getHeader().getDataType(),
            chunk.getHeader().getChunkType(),
            chunkHeaderBody,
            statisticsBody,
            actualPhysicalStart,
            dataEnd);

    queuedChunkRecords.add(new PendingChunkRecord(record, searchIndex));
    // The in-memory state follows the queue, so hasChunkAt() also sees the chunks being staged;
    // only the file is written later, see flushQueuedRecords().
    records.add(record);
    recordedChunkOffsets.add(chunkOffset);
    chunkOffset2SearchIndex.put(chunkOffset, searchIndex);
    maxPhysicalEnd = Math.max(maxPhysicalEnd, record.physicalEnd());
    lastRecordedSearchIndex = Math.max(lastRecordedSearchIndex, searchIndex);
  }

  /** Appends the queued records in one append, once the caller has forced the staged bytes. */
  public void flushQueuedRecords() throws IOException {
    if (queuedChunkRecords.isEmpty()) {
      return;
    }
    ensureActivated();
    int written = 0;
    try (final DataOutputStream output =
        new DataOutputStream(new BufferedOutputStream(new FileOutputStream(progressFile, true)))) {
      for (final PendingChunkRecord pending : queuedChunkRecords) {
        writeChunkEntry(pending.record(), pending.searchIndex(), output);
        written++;
      }
    } finally {
      queuedChunkRecords.subList(0, written).clear();
    }
  }

  /** Queues and appends the record of one chunk: for callers that do not stage a whole piece. */
  public void recordChunk(
      final IDeviceID device,
      final boolean aligned,
      final long chunkGroupHeaderOffset,
      final long chunkOffset,
      final boolean firstChunkOfGroup,
      final Chunk chunk,
      final long dataEnd,
      final long actualPhysicalStart,
      final long searchIndex)
      throws IOException {
    queueChunk(
        device,
        aligned,
        chunkGroupHeaderOffset,
        chunkOffset,
        firstChunkOfGroup,
        chunk,
        dataEnd,
        actualPhysicalStart,
        searchIndex);
    flushQueuedRecords();
  }

  public void recordTerminal(final LoadTsFileConsensusOp op, final long searchIndex)
      throws IOException {
    ensureActivated();
    try (final DataOutputStream out =
        new DataOutputStream(new BufferedOutputStream(new FileOutputStream(progressFile, true)))) {
      // Entry layout: [Total Length (4B)] [Version (1B)] [Op (4B)] [SearchIndex (8B)]
      final int payloadSize = 1 + Integer.BYTES + Long.BYTES;
      ReadWriteIOUtils.write(payloadSize, out);
      out.writeByte(ENTRY_VERSION_TERMINAL);
      // The code of the command, not the ordinal: codes are never renumbered.
      ReadWriteIOUtils.write(op.getCode(), out);
      ReadWriteIOUtils.write(searchIndex, out);
    }
  }

  // -------------------------------------------------------------------------
  // State Querying & Progress Inspection
  // -------------------------------------------------------------------------

  public boolean hasChunkAt(final long chunkOffset) throws IOException {
    ensureRecordsLoaded();
    return recordedChunkOffsets.contains(chunkOffset);
  }

  /**
   * Seeds the in-memory state from the progress file, once. The flag is set before reading: a file
   * that does not exist yet (the first piece of a task) has nothing to seed, and reading it again
   * later would rebuild the state from the file alone and drop the records queued by {@link
   * #queueChunk}.
   */
  private void ensureRecordsLoaded() throws IOException {
    if (recordsLoaded) {
      return;
    }
    recordsLoaded = true;
    readAllRecords();
  }

  public long getTotalLength() {
    return maxPhysicalEnd;
  }

  public long getChunkSearchIndex(final long chunkOffset) {
    return chunkOffset2SearchIndex.getOrDefault(chunkOffset, -1L);
  }

  public long getLastRecordedSearchIndex() {
    return lastRecordedSearchIndex;
  }

  /**
   * Checks whether the staged TsFile has no gaps from header to current end and matches records.
   */
  public boolean isReady(final long currentFileLength) throws IOException {
    if (currentFileLength <= TS_FILE_HEADER_SIZE) {
      return false;
    }
    if (readAllRecords().isEmpty()) {
      return false;
    }
    return getFirstHoleOffset() < 0 && currentFileLength >= getTotalLength();
  }

  public List<ChunkRangeRecord> getContiguousPrefix() {
    final List<ChunkRangeRecord> sorted = getRecordsByPhysicalStart();
    final List<ChunkRangeRecord> prefix = new ArrayList<>(sorted.size());
    long expectedOffset = TS_FILE_HEADER_SIZE;
    for (final ChunkRangeRecord record : sorted) {
      if (record.physicalStart() > expectedOffset) {
        break;
      }
      prefix.add(record);
      expectedOffset = Math.max(expectedOffset, record.physicalEnd());
    }
    return prefix;
  }

  public long getResumableLength() {
    long resumableLength = -1L;
    for (final ChunkRangeRecord record : getContiguousPrefix()) {
      resumableLength = Math.max(resumableLength, record.physicalEnd());
    }
    return resumableLength;
  }

  public long getFirstHoleOffset() {
    long expectedOffset = TS_FILE_HEADER_SIZE;
    for (final ChunkRangeRecord record : getRecordsByPhysicalStart()) {
      if (record.physicalStart() > expectedOffset) {
        return expectedOffset;
      }
      expectedOffset = Math.max(expectedOffset, record.physicalEnd());
    }
    return -1L;
  }

  public List<ChunkRangeRecord> getRecordsByPhysicalStart() {
    final List<ChunkRangeRecord> sorted = new ArrayList<>(records);
    sorted.sort(Comparator.comparingLong(ChunkRangeRecord::physicalStart));
    return sorted;
  }

  // -------------------------------------------------------------------------
  // Deserialization & Recovery
  // -------------------------------------------------------------------------

  public List<ChunkRangeRecord> readAllRecordsRepairingTornTail() throws IOException {
    if (!progressFile.isFile()) {
      return Collections.emptyList();
    }
    final long tornTailOffset = findTornTailOffset();
    if (tornTailOffset >= 0L) {
      LOGGER.warn(
          StorageEngineMessages
              .LOG_DROPPED_THE_TRAILING_ENTRY_OF_THE_LOAD_PROGRESS_FILE_ARG_FROM_OFFSET_ARG_ON_WHICH_WAS_NOT_FULLY_APPENDED_05C8341B,
          progressFile.getAbsolutePath(),
          tornTailOffset);
      try (final FileChannel channel =
          FileChannel.open(progressFile.toPath(), StandardOpenOption.WRITE)) {
        channel.truncate(tornTailOffset);
      }
    }
    return readAllRecords();
  }

  public List<ChunkRangeRecord> readAllRecords() throws IOException {
    if (!progressFile.isFile()) {
      return Collections.emptyList();
    }

    final List<ChunkRangeRecord> loadedRecords = new ArrayList<>();
    try (final DataInputStream in =
        new DataInputStream(new BufferedInputStream(new FileInputStream(progressFile)))) {
      final String magic = ReadWriteIOUtils.readString(in);
      if (!MAGIC.equals(magic)) {
        throw new IOException(
            String.format(
                StorageEngineMessages.EXCEPTION_INVALID_LOAD_PROGRESS_FILE_ARG_15643A3E,
                progressFile));
      }
      skipUuid(in);

      while (in.available() > 0) {
        final int entryLength = ReadWriteIOUtils.readInt(in);
        if (entryLength <= 0) {
          throw new IOException(
              String.format(
                  StorageEngineMessages.EXCEPTION_INVALID_PROGRESS_ENTRY_LENGTH_ARG_E9A96035,
                  entryLength));
        }

        final byte version = in.readByte();
        if (version == ENTRY_VERSION_CHUNK) {
          final ChunkRangeRecord record = ChunkRangeRecord.deserializePayload(in);
          loadedRecords.add(record);
          final long entrySearchIndex = ReadWriteIOUtils.readLong(in);
          chunkOffset2SearchIndex.put(record.chunkOffset(), entrySearchIndex);
          lastRecordedSearchIndex = Math.max(lastRecordedSearchIndex, entrySearchIndex);
        } else if (version == ENTRY_VERSION_TERMINAL) {
          in.skipBytes(entryLength - 1);
        } else {
          throw new IOException(
              String.format(
                  StorageEngineMessages.EXCEPTION_INVALID_LOAD_PROGRESS_FILE_ARG_15643A3E,
                  progressFile));
        }
      }
    }

    records.clear();
    records.addAll(loadedRecords);
    recordedChunkOffsets.clear();
    maxPhysicalEnd = -1L;

    for (final ChunkRangeRecord record : loadedRecords) {
      recordedChunkOffsets.add(record.chunkOffset());
      maxPhysicalEnd = Math.max(maxPhysicalEnd, record.physicalEnd());
    }
    // The records queued for the piece being staged are not in the file yet: the state is what is
    // staged, so they are kept.
    for (final PendingChunkRecord pending : queuedChunkRecords) {
      records.add(pending.record());
      recordedChunkOffsets.add(pending.record().chunkOffset());
      maxPhysicalEnd = Math.max(maxPhysicalEnd, pending.record().physicalEnd());
    }
    recordsLoaded = true;
    return loadedRecords;
  }

  /**
   * Reads the last terminal record of a progress file, or null when it holds none. Forward scan,
   * not a tail read: entry boundaries cannot be located from the tail, and a wrong guess is
   * destructive.
   */
  public static TerminalRecord readTerminal(final File progressFile) throws IOException {
    if (progressFile == null || !progressFile.isFile()) {
      return null;
    }
    TerminalRecord terminal = null;
    try (final DataInputStream in =
        new DataInputStream(new BufferedInputStream(new FileInputStream(progressFile)))) {
      final String magic = ReadWriteIOUtils.readString(in);
      if (!MAGIC.equals(magic)) {
        return null;
      }
      skipUuid(in);

      while (in.available() > 0) {
        final int entryLength = ReadWriteIOUtils.readInt(in);
        if (entryLength <= 0) {
          return terminal;
        }
        final byte version = in.readByte();
        if (version == ENTRY_VERSION_TERMINAL) {
          final int opCode = ReadWriteIOUtils.readInt(in);
          final long searchIndex = ReadWriteIOUtils.readLong(in);
          terminal = new TerminalRecord(LoadTsFileConsensusOp.fromCode(opCode), searchIndex);
        } else {
          in.skipBytes(entryLength - 1);
        }
      }
    }
    return terminal;
  }

  /** Locates the offset of an incomplete trailing append entry, or -1 when the file is intact. */
  private long findTornTailOffset() throws IOException {
    try (final CountingInputStream countingIn =
            new CountingInputStream(new BufferedInputStream(new FileInputStream(progressFile)));
        final DataInputStream in = new DataInputStream(countingIn)) {
      final String magic = ReadWriteIOUtils.readString(in);
      if (!MAGIC.equals(magic)) {
        return -1L;
      }
      skipUuid(in);

      // The length prefix is read into the same four bytes for every entry.
      final byte[] lengthBuf = new byte[Integer.BYTES];
      final ByteBuffer lengthView = ByteBuffer.wrap(lengthBuf);

      while (true) {
        final long entryStart = countingIn.getBytesRead();
        in.mark(Integer.BYTES);
        final int read = in.read(lengthBuf);
        if (read < 0) {
          return -1L; // Clean EOF
        }
        if (read < Integer.BYTES) {
          return entryStart; // Partially written length header
        }

        lengthView.clear();
        final int entryLength = lengthView.getInt();
        if (entryLength <= 0) {
          return -1L; // Corrupted marker, not a simple append truncation
        }

        long skipped = 0;
        while (skipped < entryLength) {
          final long step = in.skip(entryLength - skipped);
          if (step <= 0) {
            return entryStart; // Incomplete entry payload
          }
          skipped += step;
        }
      }
    }
  }

  private static void skipUuid(final DataInputStream in) throws IOException {
    final int uuidLength = ReadWriteIOUtils.readInt(in);
    if (uuidLength > 0) {
      in.skipBytes(uuidLength);
    }
  }

  // -------------------------------------------------------------------------
  // Chunk Serialization Utilities
  // -------------------------------------------------------------------------

  /** Serializes the chunk header body, that is the header without its leading chunk-type byte. */
  private static ByteBuffer serializeChunkHeader(final Chunk chunk) throws IOException {
    try (final PublicBAOS out = new PublicBAOS()) {
      chunk.getHeader().serializeTo(out);
      // Drop the leading chunk-type byte: deserializeFrom takes it separately.
      final int bodySize = chunk.getHeader().getSerializedSize() - 1;
      if (bodySize <= 0 || bodySize > out.size()) {
        throw new IllegalStateException(
            String.format(
                StorageEngineMessages
                    .EXCEPTION_UNEXPECTED_CHUNK_HEADER_SIZE_OF_ARG_SERIALIZED_ARG_BYTE_S_BODY_ARG_0A3F7B17,
                chunk.getHeader().getMeasurementID(),
                out.size(),
                bodySize));
      }
      return ByteBuffer.wrap(out.getBuf(), out.size() - bodySize, bodySize);
    }
  }

  private static ByteBuffer serializeStatistics(final Chunk chunk) throws IOException {
    try (final PublicBAOS out = new PublicBAOS()) {
      chunk.getChunkStatistic().serialize(out);
      return ByteBuffer.wrap(out.getBuf(), 0, out.size());
    }
  }

  private static void writeChunkEntry(
      final ChunkRangeRecord record, final long searchIndex, final DataOutputStream out)
      throws IOException {
    final PublicBAOS payloadOut = new PublicBAOS();
    try (final DataOutputStream payloadStream = new DataOutputStream(payloadOut)) {
      record.serializePayload(payloadStream);
      ReadWriteIOUtils.write(searchIndex, payloadStream);
    }
    ReadWriteIOUtils.write(payloadOut.size() + 1, out); // +1 for version byte
    out.writeByte(ENTRY_VERSION_CHUNK);
    // The bytes of the payload are written straight out of the buffer they were built in.
    out.write(payloadOut.getBuf(), 0, payloadOut.size());
  }

  /** Deserializes a chunk header; the input is read through a duplicate, never consumed. */
  public static ChunkHeader deserializeChunkHeader(final byte chunkType, final ByteBuffer body)
      throws IOException {
    try (final InputStream input = bodyStream(body)) {
      return ChunkHeader.deserializeFrom(input, chunkType);
    }
  }

  /** Deserializes statistics; the input is read through a duplicate, never consumed. */
  public static Statistics<?> deserializeStatistics(
      final TSDataType dataType, final ByteBuffer body) throws IOException {
    try (final InputStream input = bodyStream(body)) {
      return Statistics.deserialize(input, dataType);
    }
  }

  /** Opens a stream over the bytes of {@code body}, without copying the array-backed views. */
  private static InputStream bodyStream(final ByteBuffer body) {
    final ByteBuffer view = body.duplicate();
    if (view.hasArray()) {
      return new ByteArrayInputStream(
          view.array(), view.arrayOffset() + view.position(), view.remaining());
    }
    final byte[] bytes = new byte[view.remaining()];
    view.get(bytes);
    return new ByteArrayInputStream(bytes);
  }

  public static Chunk deserializeChunk(final ChunkRangeRecord record, final byte[] chunkDataBytes)
      throws IOException {
    final ChunkHeader chunkHeader =
        deserializeChunkHeader(record.chunkType(), record.chunkHeaderBody());
    final Statistics<?> statistics =
        deserializeStatistics(record.dataType(), record.statisticsBody());
    return new Chunk(chunkHeader, ByteBuffer.wrap(chunkDataBytes), null, statistics);
  }

  // -------------------------------------------------------------------------
  // Records & Stream Helpers
  // -------------------------------------------------------------------------

  /** Represents a physically placed chunk interval and the metadata needed to rebuild it. */
  public static final class ChunkRangeRecord {
    private final IDeviceID device;
    private final boolean aligned;
    private final long chunkGroupHeaderOffset;
    private final long chunkOffset;
    private final boolean firstChunkOfGroup;
    private final TSDataType dataType;
    private final byte chunkType;

    /** Chunk header body, see {@link #serializeChunkHeader(Chunk)}; must not be modified. */
    private final ByteBuffer chunkHeaderBody;

    /** Statistics of the chunk, with the ownership rule of {@link #chunkHeaderBody}. */
    private final ByteBuffer statisticsBody;

    private final long physicalStart;
    private final long physicalEnd;

    /** Cached size of {@link #chunkHeaderBody}; negative means not computed yet. */
    private transient int cachedChunkHeaderSize = -1;

    public ChunkRangeRecord(
        final IDeviceID device,
        final boolean aligned,
        final long chunkGroupHeaderOffset,
        final long chunkOffset,
        final boolean firstChunkOfGroup,
        final TSDataType dataType,
        final byte chunkType,
        final ByteBuffer chunkHeaderBody,
        final ByteBuffer statisticsBody,
        final long physicalStart,
        final long physicalEnd) {
      this.device =
          Objects.requireNonNull(
              device, DataNodeQueryMessages.EXCEPTION_DEVICE_CANNOT_BE_NULL_F1EB20B6);
      this.aligned = aligned;
      this.chunkGroupHeaderOffset = chunkGroupHeaderOffset;
      this.chunkOffset = chunkOffset;
      this.firstChunkOfGroup = firstChunkOfGroup;
      this.dataType = dataType;
      this.chunkType = chunkType;
      this.chunkHeaderBody = chunkHeaderBody;
      this.statisticsBody = statisticsBody;
      this.physicalStart = physicalStart;
      this.physicalEnd = physicalEnd;
    }

    public String device() {
      return device.toString();
    }

    public IDeviceID deviceId() {
      return device;
    }

    public boolean aligned() {
      return aligned;
    }

    public long chunkGroupHeaderOffset() {
      return chunkGroupHeaderOffset;
    }

    public long chunkOffset() {
      return chunkOffset;
    }

    public boolean firstChunkOfGroup() {
      return firstChunkOfGroup;
    }

    public TSDataType dataType() {
      return dataType;
    }

    public byte chunkType() {
      return chunkType;
    }

    /** {@link #chunkHeaderBody}, as a view so that reading it cannot consume the stored buffer. */
    public ByteBuffer chunkHeaderBody() {
      return chunkHeaderBody.duplicate();
    }

    /** {@link #statisticsBody}, as a view for the reason given at {@link #chunkHeaderBody()}. */
    public ByteBuffer statisticsBody() {
      return statisticsBody.duplicate();
    }

    public long physicalStart() {
      return physicalStart;
    }

    public long physicalEnd() {
      return physicalEnd;
    }

    public long dataLength() {
      return physicalEnd - chunkOffset - chunkHeaderSize();
    }

    /** Size of the header body, cached on the first {@link #dataLength()} call. */
    private int chunkHeaderSize() {
      int size = cachedChunkHeaderSize;
      if (size < 0) {
        size = deserializeHeaderSizeSafe();
        cachedChunkHeaderSize = size;
      }
      return size;
    }

    private int deserializeHeaderSizeSafe() {
      try {
        return deserializeChunkHeader(chunkType, chunkHeaderBody).getSerializedSize() - 1;
      } catch (final IOException e) {
        throw new IllegalStateException(e);
      }
    }

    public void serializePayload(final DataOutputStream output) throws IOException {
      device.serialize(output);
      ReadWriteIOUtils.write(aligned, output);
      ReadWriteIOUtils.write(chunkGroupHeaderOffset, output);
      ReadWriteIOUtils.write(chunkOffset, output);
      ReadWriteIOUtils.write(firstChunkOfGroup, output);
      ReadWriteIOUtils.write(dataType.ordinal(), output);
      output.writeByte(chunkType);
      writeBody(chunkHeaderBody, output);
      writeBody(statisticsBody, output);
      ReadWriteIOUtils.write(physicalStart, output);
      ReadWriteIOUtils.write(physicalEnd, output);
    }

    /** Writes the length of {@code body} and its bytes, on the format of {@link #readByteArray}. */
    private static void writeBody(final ByteBuffer body, final DataOutputStream output)
        throws IOException {
      final ByteBuffer view = body.duplicate();
      ReadWriteIOUtils.write(view.remaining(), output);
      if (view.hasArray()) {
        output.write(view.array(), view.arrayOffset() + view.position(), view.remaining());
      } else {
        final byte[] bytes = new byte[view.remaining()];
        view.get(bytes);
        output.write(bytes);
      }
    }

    public static ChunkRangeRecord deserializePayload(final DataInputStream input)
        throws IOException {
      final IDeviceID device = StringArrayDeviceID.deserialize(input);
      final boolean aligned = ReadWriteIOUtils.readBool(input);
      final long chunkGroupHeaderOffset = ReadWriteIOUtils.readLong(input);
      final long chunkOffset = ReadWriteIOUtils.readLong(input);
      final boolean firstChunkOfGroup = ReadWriteIOUtils.readBool(input);
      final TSDataType dataType = TSDataType.values()[ReadWriteIOUtils.readInt(input)];
      final byte chunkType = input.readByte();
      final ByteBuffer chunkHeaderBody = readByteArray(input);
      final ByteBuffer statisticsBody = readByteArray(input);
      final long physicalStart = ReadWriteIOUtils.readLong(input);
      final long physicalEnd = ReadWriteIOUtils.readLong(input);
      return new ChunkRangeRecord(
          device,
          aligned,
          chunkGroupHeaderOffset,
          chunkOffset,
          firstChunkOfGroup,
          dataType,
          chunkType,
          chunkHeaderBody,
          statisticsBody,
          physicalStart,
          physicalEnd);
    }

    private static ByteBuffer readByteArray(final DataInputStream input) throws IOException {
      final int length = ReadWriteIOUtils.readInt(input);
      if (length < 0) {
        throw new IOException(
            StorageEngineMessages.EXCEPTION_NEGATIVE_BYTE_ARRAY_LENGTH_IN_PROGRESS_FILE_C397A4DC);
      }
      final byte[] bytes = new byte[length];
      input.readFully(bytes);
      return ByteBuffer.wrap(bytes);
    }
  }

  public record TerminalRecord(LoadTsFileConsensusOp op, long searchIndex) {}

  /** One chunk record waiting to be appended, with the search index it is stamped with. */
  private record PendingChunkRecord(ChunkRangeRecord record, long searchIndex) {}

  /** Stream decorator tracking total read byte count without loading file into memory. */
  private static final class CountingInputStream extends FilterInputStream {
    private long bytesRead;

    private CountingInputStream(final InputStream in) {
      super(in);
    }

    @Override
    public int read() throws IOException {
      final int r = super.read();
      if (r >= 0) {
        bytesRead++;
      }
      return r;
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
      final int r = super.read(b, off, len);
      if (r > 0) {
        bytesRead += r;
      }
      return r;
    }

    @Override
    public long skip(final long n) throws IOException {
      final long s = super.skip(n);
      if (s > 0) {
        bytesRead += s;
      }
      return s;
    }

    public long getBytesRead() {
      return bytesRead;
    }
  }
}
