/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.storageengine.load;

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
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Persistent physical-range tracker for a staged TsFile whose data zone may be written with holes
 * during IoTConsensus V1 leader handoff.
 *
 * <p>The file is append-only. Every successfully placed physical chunk is recorded with enough
 * information to rebuild the {@link org.apache.tsfile.write.TsFilePrecalculatedChunkWriter}
 * metadata later: device, aligned flag, chunk-group-header offset, chunk offset, first-chunk
 * marker, serialized chunk header, serialized chunk statistics and the exact data range in the
 * staged file.
 */
public class LoadTsFileProgress {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadTsFileProgress.class);

  public static final String PROGRESS_SUFFIX = ".progress";
  private static final String MAGIC = "LTPROG1";

  private static final byte PROGRESS_ENTRY_VERSION = 3;

  /**
   * The tail of the file: the COMMIT or ABORT command that finished the task and the consensus
   * index it was applied with. A reader that does not know it skips it, a reader that scans the
   * directory reads the whole history of the task from one file.
   */
  private static final byte TERMINAL_ENTRY_VERSION = 4;

  /** TsFile header size written by {@code TsFilePrecalculatedChunkWriter}. */
  private static final long TS_FILE_HEADER_SIZE = 7L;

  private final File progressFile;
  private final String uuid;
  private final List<ChunkRangeRecord> records = new ArrayList<>();

  /**
   * The offsets the recorded chunks start at. A chunk occupies its offset alone, so an offset that
   * is recorded here means the bytes of that chunk are in the staged file.
   */
  private final Set<Long> recordedChunkOffsets = new HashSet<>();

  /**
   * Whether {@link #records} reflects the progress file, which it does not right after a restart.
   */
  private boolean recordsLoaded;

  /** The consensus index of every chunk entry that carries one, by chunk offset. */
  private final Map<Long, Long> chunkOffset2SearchIndex = new HashMap<>();

  /** The highest consensus index any entry of this file was written with, or -1. */
  private long lastRecordedSearchIndex = -1L;

  public LoadTsFileProgress(final File tsFile) {
    this.progressFile = new File(tsFile.getAbsolutePath() + PROGRESS_SUFFIX);
    this.uuid = tsFile.getParentFile() == null ? "" : tsFile.getParentFile().getName();
  }

  public static File progressFileFor(final File tsFile) {
    return new File(tsFile.getAbsolutePath() + PROGRESS_SUFFIX);
  }

  public boolean exists() {
    return progressFile.isFile();
  }

  private void activate() throws IOException {
    if (!progressFile.exists()) {
      try (final DataOutputStream output =
          new DataOutputStream(new FileOutputStream(progressFile, false))) {
        ReadWriteIOUtils.write(MAGIC, output);
        writeUuid(output);
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
    activate();

    final byte[] chunkHeaderBytes = serializeChunkHeader(chunk);
    final byte[] statisticsBytes = serializeStatistics(chunk);

    final ChunkRangeRecord record =
        new ChunkRangeRecord(
            // A device that is not segmented is normalized the way the piece pipeline normalizes
            // it,
            // and a segmented one is kept exactly as it is: rebuilding it from the string it prints
            // as would split it into a different number of segments, and the metadata restored from
            // this record would then be filed under a device that no reader ever asks for.
            device instanceof StringArrayDeviceID
                ? device
                : new StringArrayDeviceID(device.toString()),
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
    records.add(record);
    recordedChunkOffsets.add(chunkOffset);

    try (final DataOutputStream output =
        new DataOutputStream(new FileOutputStream(progressFile, true))) {
      writeEntry(record, searchIndex, output);
      chunkOffset2SearchIndex.put(record.chunkOffset(), searchIndex);
      lastRecordedSearchIndex = Math.max(lastRecordedSearchIndex, searchIndex);
    }
  }

  /**
   * Whether the chunk that would be written at this offset is already staged.
   *
   * <p>The offsets of a piece are derived from its content, so the same piece always names the same
   * offsets. A chunk of a LOAD is written once and then recorded here, which is what lets a piece
   * that arrives a second time - a retried request, the replay of its WAL entry, a replica that got
   * it twice - be recognized without keeping any state of its own.
   */
  public boolean hasChunkAt(final long chunkOffset) throws IOException {
    if (!recordsLoaded) {
      readAllRecords();
    }
    return recordedChunkOffsets.contains(chunkOffset);
  }

  /**
   * Whether the staged file is complete: its records cover it from the TsFile header on without a
   * hole, and the file holds at least the bytes they claim.
   *
   * <p>A hole means a piece never arrived. The file can be longer than the end of the ranges the
   * records describe - a piece that arrived out of order is written at the absolute offset its own
   * content defines, past the hole an earlier piece left - so the length alone cannot tell a
   * complete file from one that is missing a piece in the middle. This is the check that gates
   * sealing a staged file (PREPARE) and reclaiming its directory (the cleaner): both would turn a
   * zero-filled hole into data, or delete bytes a replica still has to read back.
   *
   * <p>The preceding stages are more tolerant on purpose: a writer that resumes an interrupted task
   * continues it after {@link #getTotalLength()}, keeping the hole, because the missing piece lands
   * where its own offsets say as soon as it arrives.
   */
  public boolean isReady(final long currentFileLength) throws IOException {
    if (currentFileLength <= TS_FILE_HEADER_SIZE) {
      return false;
    }

    if (readAllRecords().isEmpty()) {
      return false;
    }
    // Every recorded range has to be covered without a hole up to the end of the record set, and
    // the file has to hold at least those bytes: a file that ends earlier lost a piece, a file with
    // a hole in the middle is missing one.
    return getFirstHoleOffset() < 0 && currentFileLength >= getTotalLength();
  }

  /**
   * Reads the records of this file, and drops a trailing entry that was not appended completely.
   *
   * <p>An entry is appended with several writes, so a copy of this file taken while a piece was
   * being appended ends in the middle of one. That is what a snapshot of the staging directory can
   * hold: the snapshot is taken on the thread that migrates the region, while the pieces of the
   * region keep being applied on the state machine thread, and the two are not serialized by the
   * region lock. The entries before the fragment are intact and describe bytes that are on disk, so
   * the fragment is cut off here and the file is read as the complete entries it holds.
   *
   * <p>Without that, the reader fails on the fragment, the staged file looks like it holds no
   * attributable bytes at all, and the pieces that arrive after it are then written nowhere (see
   * {@link TsFileWriterManager#write}) - the replica would import a file that silently misses them.
   *
   * @return the records of the complete entries
   */
  public List<ChunkRangeRecord> readAllRecordsRepairingTornTail() throws IOException {
    if (!progressFile.isFile()) {
      return new ArrayList<>();
    }
    final long tornTailOffset = tornTailOffset();
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

  /**
   * @return the offset the trailing entry that was not appended completely starts at, or -1 when
   *     every entry is complete or when the file is unreadable for any other reason
   */
  private long tornTailOffset() throws IOException {
    final byte[] bytes = Files.readAllBytes(progressFile.toPath());
    try (final DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes))) {
      if (!MAGIC.equals(ReadWriteIOUtils.readString(input))) {
        // Not a progress file of this version: nothing here says which bytes a task may keep.
        return -1L;
      }
      readUuid(input);
      while (input.available() > 0) {
        final long entryStart = bytes.length - input.available();
        if (input.available() < Integer.BYTES) {
          // The length of the next entry is only partly there.
          return entryStart;
        }
        final int entryLength = ReadWriteIOUtils.readInt(input);
        if (entryLength <= 0) {
          // An entry the writer never announced: a corrupt file, not an append that was cut off.
          return -1L;
        }
        if (input.available() < entryLength) {
          // The body of the next entry was not appended completely.
          return entryStart;
        }
        input.skipBytes(entryLength);
      }
      return -1L;
    }
  }

  public List<ChunkRangeRecord> readAllRecords() throws IOException {
    if (!progressFile.isFile()) {
      return new ArrayList<>();
    }

    final List<ChunkRangeRecord> result = new ArrayList<>();
    try (final DataInputStream input = new DataInputStream(new FileInputStream(progressFile))) {
      final String magic = ReadWriteIOUtils.readString(input);
      if (!MAGIC.equals(magic)) {
        throw new IOException(
            String.format(
                StorageEngineMessages.EXCEPTION_INVALID_LOAD_PROGRESS_FILE_ARG_15643A3E,
                progressFile));
      }
      readUuid(input);
      while (input.available() > 0) {
        final int entryLength = ReadWriteIOUtils.readInt(input);
        if (entryLength <= 0) {
          throw new IOException(
              String.format(
                  StorageEngineMessages.EXCEPTION_INVALID_PROGRESS_ENTRY_LENGTH_ARG_E9A96035,
                  entryLength));
        }
        final byte version = input.readByte();
        final byte[] payload = new byte[entryLength - 1];
        input.readFully(payload);
        if (version == PROGRESS_ENTRY_VERSION) {
          try (final DataInputStream payloadInput =
              new DataInputStream(new ByteArrayInputStream(payload))) {
            final ChunkRangeRecord record = ChunkRangeRecord.deserializePayload(payloadInput);
            result.add(record);
            final long entrySearchIndex = ReadWriteIOUtils.readLong(payloadInput);
            chunkOffset2SearchIndex.put(record.chunkOffset(), entrySearchIndex);
            lastRecordedSearchIndex = Math.max(lastRecordedSearchIndex, entrySearchIndex);
          }
        } else if (version != TERMINAL_ENTRY_VERSION) {
          throw new IOException(
              String.format(
                  StorageEngineMessages.EXCEPTION_INVALID_LOAD_PROGRESS_FILE_ARG_15643A3E,
                  progressFile));
        }
      }
    }
    long maxPhysicalEnd = -1L;
    for (final ChunkRangeRecord record : result) {
      maxPhysicalEnd = Math.max(maxPhysicalEnd, record.physicalEnd());
    }
    records.clear();
    records.addAll(result);
    recordedChunkOffsets.clear();
    for (final ChunkRangeRecord record : result) {
      recordedChunkOffsets.add(record.chunkOffset());
    }
    recordsLoaded = true;
    return result;
  }

  public File getProgressFile() {
    return progressFile;
  }

  public long getTotalLength() {
    long maxPhysicalEnd = -1L;
    for (final ChunkRangeRecord record : records) {
      maxPhysicalEnd = Math.max(maxPhysicalEnd, record.physicalEnd());
    }
    return maxPhysicalEnd;
  }

  /**
   * Returns the records whose ranges form the gap-free run starting right after the TsFile header,
   * in physical order.
   *
   * <p>A piece can reach a replica after a piece that follows it, so the file is written with a
   * hole that a later piece fills in: while that is happening the ranges are not contiguous yet,
   * but once every piece has arrived they merge into a single run. Only that run may be sealed,
   * because anything behind a still-open gap would be placed past bytes that are missing.
   */
  public List<ChunkRangeRecord> getContiguousPrefix() {
    final List<ChunkRangeRecord> sorted = getRecordsByPhysicalStart();
    final List<ChunkRangeRecord> prefix = new ArrayList<>(sorted.size());
    long expectedOffset = TS_FILE_HEADER_SIZE;
    for (final ChunkRangeRecord record : sorted) {
      if (record.physicalStart() > expectedOffset) {
        // Everything from here on sits behind a gap, so it is outside the prefix.
        break;
      }
      prefix.add(record);
      expectedOffset = Math.max(expectedOffset, record.physicalEnd());
    }
    return prefix;
  }

  /**
   * Returns the offset up to which the staged file of this task is known to be completely written,
   * i.e. the end of the gap-free run of recorded chunk ranges that starts right after the TsFile
   * header.
   *
   * <p>A missing link in the middle means the piece owning that range has not reached this node
   * yet. Such a hole does not stop the task from being continued - that piece may still arrive and
   * land on its own precomputed offset - it only stops the file from being sealed, which the caller
   * detects by comparing the file length with {@link #getTotalLength()}.
   *
   * @return the end of the contiguous prefix, or {@code -1} when even the very first range does not
   *     start right after the header
   */
  public long getResumableLength() {
    long resumableLength = -1L;
    for (final ChunkRangeRecord record : getContiguousPrefix()) {
      resumableLength = Math.max(resumableLength, record.physicalEnd());
    }
    return resumableLength;
  }

  /**
   * Returns the offset at which the recorded chunk ranges stop being contiguous, or {@code -1} when
   * they cover the file from the header on without any hole.
   */
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

  /**
   * Returns the recorded chunk ranges ordered by their position in the staged file. The file itself
   * is written by absolute offset, so the order in which the records were appended is only the same
   * as the physical order while no hole had to be filled.
   */
  public List<ChunkRangeRecord> getRecordsByPhysicalStart() {
    final List<ChunkRangeRecord> sorted = new ArrayList<>(records);
    sorted.sort(Comparator.comparingLong(ChunkRangeRecord::physicalStart));
    return sorted;
  }

  private void writeUuid(final DataOutputStream output) throws IOException {
    final byte[] uuidBytes = uuid.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    ReadWriteIOUtils.write(uuidBytes.length, output);
    output.write(uuidBytes);
  }

  private void readUuid(final DataInputStream input) throws IOException {
    final int uuidLength = ReadWriteIOUtils.readInt(input);
    final byte[] uuidBytes = new byte[uuidLength];
    input.readFully(uuidBytes);
  }

  /**
   * Serializes a chunk header without its leading chunk-type byte, which is the form a header can
   * be read back from: {@code ChunkHeader#serializeTo(OutputStream)} writes that byte first, while
   * {@code ChunkHeader#deserializeFrom(InputStream, byte)} takes it as a separate argument and
   * expects a stream that already starts after it. The byte is not lost, because the chunk type is
   * kept next to these bytes in {@link ChunkRangeRecord#chunkType()} and is passed back in when the
   * header is restored.
   *
   * <p>The result is one byte shorter than {@link ChunkHeader#getSerializedSize()}, which counts
   * the leading chunk-type byte as part of the header.
   */
  /** The consensus index of the entry that staged a chunk, or -1 when it carries none. */
  public long getChunkSearchIndex(final long chunkOffset) {
    return chunkOffset2SearchIndex.getOrDefault(chunkOffset, -1L);
  }

  /** The highest consensus index an entry of this file was written with, or -1. */
  public long getLastRecordedSearchIndex() {
    return lastRecordedSearchIndex;
  }

  private byte[] serializeChunkHeader(final Chunk chunk) throws IOException {
    final byte[] serialized;
    try (final ByteArrayOutputStream output = new ByteArrayOutputStream()) {
      chunk.getHeader().serializeTo(output);
      serialized = output.toByteArray();
    }
    // The body is one chunk-type byte shorter than the header that was written out.
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
    try (final ByteArrayOutputStream output = new ByteArrayOutputStream()) {
      chunk.getChunkStatistic().serialize(output);
      return output.toByteArray();
    }
  }

  private static void writeEntry(
      final ChunkRangeRecord record, final long searchIndex, final DataOutputStream output)
      throws IOException {
    final ByteArrayOutputStream payloadOutput = new ByteArrayOutputStream();
    try (final DataOutputStream payloadStream = new DataOutputStream(payloadOutput)) {
      record.serializePayload(payloadStream);
    }
    final byte[] payload = payloadOutput.toByteArray();
    ReadWriteIOUtils.write(payload.length + 1 + Long.BYTES, output);
    output.writeByte(PROGRESS_ENTRY_VERSION);
    output.write(payload);
    ReadWriteIOUtils.write(searchIndex, output);
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

  /** A physical interval plus the metadata required to recover the writer entry. */
  public static final class ChunkRangeRecord {
    /**
     * The device as it identifies itself on the wire. It is kept as an {@link IDeviceID} instead of
     * as its string form because a device ID is not interchangeable with the string it prints as:
     * {@code new StringArrayDeviceID("root.sg.d1.temperature")} splits into two segments while the
     * four-segment device of the same name stays four segments, and the two are not equal.
     * Restoring metadata under the wrong device would split one measurement into several entries in
     * the sealed file.
     */
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

    /**
     * Takes the device as it was read back from the progress file, so that a device ID surviving a
     * restart keeps the very shape it was written with.
     */
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
      this.device = device;
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
        // One byte shorter than getSerializedSize(), which also counts the chunk-type byte that
        // precedes the body.
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
      writeBytes(chunkHeaderBytes, output);
      writeBytes(statisticsBytes, output);
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
      final byte[] chunkHeaderBytes = readBytes(input);
      final byte[] statisticsBytes = readBytes(input);
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

    private static void writeBytes(final byte[] bytes, final DataOutputStream output)
        throws IOException {
      ReadWriteIOUtils.write(bytes.length, output);
      output.write(bytes);
    }

    private static byte[] readBytes(final DataInputStream input) throws IOException {
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

  /**
   * Appends the tail of the task to the progress file: the COMMIT or ABORT command that finished it
   * and the consensus index the command carries. Once every replica has applied that index the
   * staged bytes can no longer be read back, so the directory that holds them may be deleted.
   */
  public void recordTerminal(final LoadTsFileConsensusOp op, final long searchIndex)
      throws IOException {
    activate();
    final byte[] payload;
    try (final java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        final DataOutputStream payloadStream = new DataOutputStream(baos)) {
      ReadWriteIOUtils.write(op.ordinal(), payloadStream);
      ReadWriteIOUtils.write(searchIndex, payloadStream);
      payload = baos.toByteArray();
    }
    try (final DataOutputStream output =
        new DataOutputStream(new FileOutputStream(progressFile, true))) {
      ReadWriteIOUtils.write(payload.length + 1, output);
      output.writeByte(TERMINAL_ENTRY_VERSION);
      output.write(payload);
    }
  }

  /**
   * Reads the tail of a progress file without loading its chunk records.
   *
   * @return the command and the consensus index the task finished with, or null when the task never
   *     reached COMMIT or ABORT
   */
  public static TerminalRecord readTerminal(final File progressFile) throws IOException {
    if (progressFile == null || !progressFile.isFile()) {
      return null;
    }
    TerminalRecord terminal = null;
    try (final DataInputStream input = new DataInputStream(new FileInputStream(progressFile))) {
      final String magic = ReadWriteIOUtils.readString(input);
      if (!MAGIC.equals(magic)) {
        return null;
      }
      final int uuidLength = ReadWriteIOUtils.readInt(input);
      input.skipBytes(uuidLength);
      while (input.available() > 0) {
        final int entryLength = ReadWriteIOUtils.readInt(input);
        if (entryLength <= 0) {
          return terminal;
        }
        final byte version = input.readByte();
        final byte[] payload = new byte[entryLength - 1];
        input.readFully(payload);
        if (version == TERMINAL_ENTRY_VERSION) {
          try (final DataInputStream payloadInput =
              new DataInputStream(new ByteArrayInputStream(payload))) {
            terminal =
                new TerminalRecord(
                    LoadTsFileConsensusOp.fromOrdinal(ReadWriteIOUtils.readInt(payloadInput)),
                    ReadWriteIOUtils.readLong(payloadInput));
          }
        }
      }
    }
    return terminal;
  }

  /** The command a task finished with and the consensus index it was applied at. */
  public record TerminalRecord(LoadTsFileConsensusOp op, long searchIndex) {}
}
