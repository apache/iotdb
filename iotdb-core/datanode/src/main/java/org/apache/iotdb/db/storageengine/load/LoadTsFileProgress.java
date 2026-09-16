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

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.header.ChunkHeader;
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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

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
  private static final byte PROGRESS_ENTRY_VERSION = 1;

  /** TsFile header size written by {@code TsFilePrecalculatedChunkWriter}. */
  private static final long TS_FILE_HEADER_SIZE = 7L;

  private final File progressFile;
  private long totalLength = -1L;
  private final List<ChunkRangeRecord> records = new ArrayList<>();

  public LoadTsFileProgress(final File tsFile) {
    this.progressFile = new File(tsFile.getAbsolutePath() + PROGRESS_SUFFIX);
  }

  public static File progressFileFor(final File tsFile) {
    return new File(tsFile.getAbsolutePath() + PROGRESS_SUFFIX);
  }

  public boolean exists() {
    return progressFile.isFile();
  }

  public void activate(final long totalLength) throws IOException {
    this.totalLength = Math.max(this.totalLength, totalLength);
    if (!progressFile.exists()) {
      try (final DataOutputStream output =
          new DataOutputStream(new FileOutputStream(progressFile, false))) {
        ReadWriteIOUtils.write(MAGIC, output);
        ReadWriteIOUtils.write(this.totalLength, output);
      }
    }
  }

  public void recordChunk(
      final String device,
      final boolean aligned,
      final long chunkGroupHeaderOffset,
      final long chunkOffset,
      final boolean firstChunkOfGroup,
      final Chunk chunk,
      final long dataEnd)
      throws IOException {
    activate(dataEnd);

    final byte[] chunkHeaderBytes = serializeChunkHeader(chunk);
    final byte[] statisticsBytes = serializeStatistics(chunk);
    final long physicalStart = firstChunkOfGroup ? chunkGroupHeaderOffset : chunkOffset;

    final ChunkRangeRecord record =
        new ChunkRangeRecord(
            device,
            aligned,
            chunkGroupHeaderOffset,
            chunkOffset,
            firstChunkOfGroup,
            chunk.getHeader().getDataType(),
            chunk.getHeader().getChunkType(),
            chunkHeaderBytes,
            statisticsBytes,
            physicalStart,
            dataEnd);
    records.add(record);

    try (final DataOutputStream output =
        new DataOutputStream(new FileOutputStream(progressFile, true))) {
      writeEntry(record, output);
    }
  }

  public boolean isReady(final long currentFileLength) throws IOException {
    if (currentFileLength <= TS_FILE_HEADER_SIZE) {
      return false;
    }

    final List<ChunkRangeRecord> sorted = readAllRecords();
    if (sorted.isEmpty()) {
      return false;
    }
    sorted.sort(Comparator.comparingLong(ChunkRangeRecord::physicalStart));

    long coveredStart = sorted.get(0).physicalStart();
    long coveredEnd = sorted.get(0).physicalEnd();
    if (coveredStart != TS_FILE_HEADER_SIZE) {
      return false;
    }
    for (int i = 1; i < sorted.size(); i++) {
      final ChunkRangeRecord record = sorted.get(i);
      if (record.physicalStart() > coveredEnd) {
        return false;
      }
      coveredEnd = Math.max(coveredEnd, record.physicalEnd());
    }
    return coveredEnd == currentFileLength;
  }

  public List<ChunkRangeRecord> readAllRecords() throws IOException {
    if (!progressFile.isFile()) {
      return new ArrayList<>();
    }

    final List<ChunkRangeRecord> result = new ArrayList<>();
    try (final DataInputStream input = new DataInputStream(new FileInputStream(progressFile))) {
      final String magic = ReadWriteIOUtils.readString(input);
      if (!MAGIC.equals(magic)) {
        throw new IOException("Invalid load progress file: " + progressFile);
      }
      final long persistedTotalLength = ReadWriteIOUtils.readLong(input);
      this.totalLength = persistedTotalLength;
      while (input.available() > 0) {
        final int entryLength = ReadWriteIOUtils.readInt(input);
        if (entryLength <= 0) {
          throw new IOException("Invalid progress entry length: " + entryLength);
        }
        final byte version = input.readByte();
        final byte[] payload = new byte[entryLength - 1];
        input.readFully(payload);
        if (version == PROGRESS_ENTRY_VERSION) {
          try (final DataInputStream payloadInput =
              new DataInputStream(new ByteArrayInputStream(payload))) {
            result.add(ChunkRangeRecord.deserializePayload(payloadInput));
          }
        } else {
          LOGGER.warn(
              "Skipping unsupported load progress entry version {} in {}", version, progressFile);
        }
      }
    }
    long maxPhysicalEnd = this.totalLength;
    for (final ChunkRangeRecord record : result) {
      maxPhysicalEnd = Math.max(maxPhysicalEnd, record.physicalEnd());
    }
    this.totalLength = maxPhysicalEnd;
    records.clear();
    records.addAll(result);
    return result;
  }

  public File getProgressFile() {
    return progressFile;
  }

  public long getTotalLength() {
    return totalLength;
  }

  private byte[] serializeChunkHeader(final Chunk chunk) throws IOException {
    try (final ByteArrayOutputStream output = new ByteArrayOutputStream()) {
      chunk.getHeader().serializeTo(output);
      return output.toByteArray();
    }
  }

  private byte[] serializeStatistics(final Chunk chunk) throws IOException {
    try (final ByteArrayOutputStream output = new ByteArrayOutputStream()) {
      chunk.getChunkStatistic().serialize(output);
      return output.toByteArray();
    }
  }

  private static void writeEntry(final ChunkRangeRecord record, final DataOutputStream output)
      throws IOException {
    final ByteArrayOutputStream payloadOutput = new ByteArrayOutputStream();
    try (final DataOutputStream payloadStream = new DataOutputStream(payloadOutput)) {
      record.serializePayload(payloadStream);
    }
    final byte[] payload = payloadOutput.toByteArray();
    ReadWriteIOUtils.write(payload.length + 1, output);
    output.writeByte(PROGRESS_ENTRY_VERSION);
    output.write(payload);
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
    private final String device;
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
        final String device,
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
      return device;
    }

    public StringArrayDeviceID deviceId() {
      return new StringArrayDeviceID(device);
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
        return deserializeChunkHeader(chunkType, chunkHeaderBytes).getSerializedSize();
      } catch (final IOException e) {
        throw new IllegalStateException(e);
      }
    }

    public void serializePayload(final DataOutputStream output) throws IOException {
      ReadWriteIOUtils.write(device, output);
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
      final String device = ReadWriteIOUtils.readString(input);
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
        throw new IOException("Negative byte array length in progress file");
      }
      final byte[] bytes = new byte[length];
      input.readFully(bytes);
      return bytes;
    }
  }
}
