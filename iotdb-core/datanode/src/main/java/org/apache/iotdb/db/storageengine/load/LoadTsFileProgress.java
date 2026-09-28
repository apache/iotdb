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
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
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
    ensureActivated();

    final byte[] chunkHeaderBytes = serializeChunkHeader(chunk);
    final byte[] statisticsBytes = serializeStatistics(chunk);
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
            chunkHeaderBytes,
            statisticsBytes,
            actualPhysicalStart,
            dataEnd);

    // Append to file first to ensure durability before updating in-memory state
    try (final DataOutputStream output =
        new DataOutputStream(new BufferedOutputStream(new FileOutputStream(progressFile, true)))) {
      writeChunkEntry(record, searchIndex, output);
    }

    records.add(record);
    recordedChunkOffsets.add(chunkOffset);
    chunkOffset2SearchIndex.put(chunkOffset, searchIndex);
    maxPhysicalEnd = Math.max(maxPhysicalEnd, record.physicalEnd());
    lastRecordedSearchIndex = Math.max(lastRecordedSearchIndex, searchIndex);
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
      ReadWriteIOUtils.write(op.ordinal(), out);
      ReadWriteIOUtils.write(searchIndex, out);
    }
  }

  // -------------------------------------------------------------------------
  // State Querying & Progress Inspection
  // -------------------------------------------------------------------------

  public boolean hasChunkAt(final long chunkOffset) throws IOException {
    if (!recordsLoaded) {
      readAllRecords();
    }
    return recordedChunkOffsets.contains(chunkOffset);
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
    recordsLoaded = true;
    return loadedRecords;
  }

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
          final int opOrdinal = ReadWriteIOUtils.readInt(in);
          final long searchIndex = ReadWriteIOUtils.readLong(in);
          terminal = new TerminalRecord(LoadTsFileConsensusOp.fromOrdinal(opOrdinal), searchIndex);
        } else {
          in.skipBytes(entryLength - 1);
        }
      }
    }
    return terminal;
  }

  /**
   * Scans the progress file to locate the offset of an incomplete trailing append entry. Returns -1
   * if the file structure is completely valid.
   */
  private long findTornTailOffset() throws IOException {
    try (final CountingInputStream countingIn =
            new CountingInputStream(new BufferedInputStream(new FileInputStream(progressFile)));
        final DataInputStream in = new DataInputStream(countingIn)) {
      final String magic = ReadWriteIOUtils.readString(in);
      if (!MAGIC.equals(magic)) {
        return -1L;
      }
      skipUuid(in);

      while (true) {
        final long entryStart = countingIn.getBytesRead();
        in.mark(Integer.BYTES);
        final byte[] lengthBuf = new byte[Integer.BYTES];
        final int read = in.read(lengthBuf);
        if (read < 0) {
          return -1L; // Clean EOF
        }
        if (read < Integer.BYTES) {
          return entryStart; // Partially written length header
        }

        final int entryLength = ByteBuffer.wrap(lengthBuf).getInt();
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

  private byte[] serializeChunkHeader(final Chunk chunk) throws IOException {
    final byte[] serialized;
    try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      chunk.getHeader().serializeTo(out);
      serialized = out.toByteArray();
    }
    // Drop leading chunk-type byte to allow deserialization via deserializeFrom(InputStream, byte)
    final int bodySize = chunk.getHeader().getSerializedSize() - 1;
    if (bodySize <= 0 || bodySize > serialized.length) {
      throw new IllegalStateException(
          String.format(
              StorageEngineMessages
                  .EXCEPTION_UNEXPECTED_CHUNK_HEADER_SIZE_OF_ARG_SERIALIZED_ARG_BYTE_S_BODY_ARG_0A3F7B17,
              chunk.getHeader().getMeasurementID(),
              serialized.length,
              bodySize));
    }
    return Arrays.copyOfRange(serialized, serialized.length - bodySize, serialized.length);
  }

  private byte[] serializeStatistics(final Chunk chunk) throws IOException {
    try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      chunk.getChunkStatistic().serialize(out);
      return out.toByteArray();
    }
  }

  private static void writeChunkEntry(
      final ChunkRangeRecord record, final long searchIndex, final DataOutputStream out)
      throws IOException {
    final ByteArrayOutputStream payloadOut = new ByteArrayOutputStream();
    try (final DataOutputStream payloadStream = new DataOutputStream(payloadOut)) {
      record.serializePayload(payloadStream);
      ReadWriteIOUtils.write(searchIndex, payloadStream);
    }
    final byte[] payload = payloadOut.toByteArray();
    ReadWriteIOUtils.write(payload.length + 1, out); // +1 for version byte
    out.writeByte(ENTRY_VERSION_CHUNK);
    out.write(payload);
  }

  public static ChunkHeader deserializeChunkHeader(
      final byte chunkType, final byte[] chunkHeaderBytes) throws IOException {
    try (final ByteArrayInputStream input = new ByteArrayInputStream(chunkHeaderBytes)) {
      return ChunkHeader.deserializeFrom(input, chunkType);
    }
  }

  public static Statistics<?> deserializeStatistics(
      final TSDataType dataType, final byte[] statisticsBytes) throws IOException {
    try (final ByteArrayInputStream input = new ByteArrayInputStream(statisticsBytes)) {
      return Statistics.deserialize(input, dataType);
    }
  }

  public static Chunk deserializeChunk(final ChunkRangeRecord record, final byte[] chunkDataBytes)
      throws IOException {
    final ChunkHeader chunkHeader =
        deserializeChunkHeader(record.chunkType(), record.chunkHeaderBytes());
    final Statistics<?> statistics =
        deserializeStatistics(record.dataType(), record.statisticsBytes());
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
    private final byte[] chunkHeaderBytes;
    private final byte[] statisticsBytes;
    private final long physicalStart;
    private final long physicalEnd;

    public ChunkRangeRecord(
        final IDeviceID device,
        final boolean aligned,
        final long chunkGroupHeaderOffset,
        final long chunkOffset,
        final boolean firstChunkOfGroup,
        final TSDataType dataType,
        final byte chunkType,
        final byte[] chunkHeaderBytes,
        final byte[] statisticsBytes,
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
      this.chunkHeaderBytes = chunkHeaderBytes;
      this.statisticsBytes = statisticsBytes;
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

    public byte[] chunkHeaderBytes() {
      return chunkHeaderBytes;
    }

    public byte[] statisticsBytes() {
      return statisticsBytes;
    }

    public long physicalStart() {
      return physicalStart;
    }

    public long physicalEnd() {
      return physicalEnd;
    }

    public long dataLength() {
      return physicalEnd - chunkOffset - deserializeHeaderSizeSafe();
    }

    private int deserializeHeaderSizeSafe() {
      try {
        return deserializeChunkHeader(chunkType, chunkHeaderBytes).getSerializedSize() - 1;
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
      ReadWriteIOUtils.write(chunkHeaderBytes.length, output);
      output.write(chunkHeaderBytes);
      ReadWriteIOUtils.write(statisticsBytes.length, output);
      output.write(statisticsBytes);
      ReadWriteIOUtils.write(physicalStart, output);
      ReadWriteIOUtils.write(physicalEnd, output);
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
      final byte[] chunkHeaderBytes = readByteArray(input);
      final byte[] statisticsBytes = readByteArray(input);
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
          chunkHeaderBytes,
          statisticsBytes,
          physicalStart,
          physicalEnd);
    }

    private static byte[] readByteArray(final DataInputStream input) throws IOException {
      final int length = ReadWriteIOUtils.readInt(input);
      if (length < 0) {
        throw new IOException(
            StorageEngineMessages.EXCEPTION_NEGATIVE_BYTE_ARRAY_LENGTH_IN_PROGRESS_FILE_C397A4DC);
      }
      final byte[] bytes = new byte[length];
      input.readFully(bytes);
      return bytes;
    }
  }

  public record TerminalRecord(LoadTsFileConsensusOp op, long searchIndex) {}

  /** Stream decorator tracking total read byte count without loading file into memory. */
  private static final class CountingInputStream extends java.io.FilterInputStream {
    private long bytesRead;

    private CountingInputStream(final java.io.InputStream in) {
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
