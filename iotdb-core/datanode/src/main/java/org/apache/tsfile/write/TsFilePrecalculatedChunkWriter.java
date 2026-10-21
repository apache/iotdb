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

package org.apache.tsfile.write;

import org.apache.iotdb.db.i18n.StorageEngineMessages;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.file.MetaMarker;
import org.apache.tsfile.file.header.ChunkGroupHeader;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.MeasurementMetadataIndexEntry;
import org.apache.tsfile.file.metadata.MetadataIndexNode;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.file.metadata.TsFileMetadata;
import org.apache.tsfile.file.metadata.enums.MetadataIndexNodeType;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.utils.BloomFilter;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.writer.TsFileOutput;
import org.apache.tsfile.write.writer.tsmiterator.TSMIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.apache.tsfile.file.metadata.MetadataIndexConstructor.checkAndBuildLevelIndex;
import static org.apache.tsfile.file.metadata.MetadataIndexConstructor.splitDeviceByTable;

/**
 * High-performance low-level chunk writer that writes precalculated physical chunks directly by
 * absolute offset, allowing sparse out-of-order writes. Durability is left to the caller, which
 * syncs once per staged piece.
 */
public class TsFilePrecalculatedChunkWriter implements AutoCloseable {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(TsFilePrecalculatedChunkWriter.class);

  private static final int ZERO_BUFFER_CAPACITY = 64 * 1024;
  private static final byte[] ZERO_CHUNK = new byte[ZERO_BUFFER_CAPACITY];

  private final TsFileOutput out;

  /** Whether a chunk that would leave a hole has to be refused instead of padded up to. */
  private boolean rejectHoles;

  private final File file;
  private final Map<IDeviceID, Map<String, List<IChunkMetadata>>> device2MetadataMap =
      new TreeMap<>();
  private final Map<String, TableSchema> tableSchemaMap = new TreeMap<>();

  private long maxPhysicalFileSize;
  private boolean sealed;

  public TsFilePrecalculatedChunkWriter(final File file) throws IOException {
    this.file =
        Objects.requireNonNull(file, StorageEngineMessages.EXCEPTION_FILE_CANNOT_BE_NULL_29A83D70);
    this.out =
        new WritableFileChannelOutput(
            FileChannel.open(
                file.toPath(),
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING));
    this.sealed = false;
    startFile();
    this.maxPhysicalFileSize = out.getPosition();
    LOGGER.debug(
        StorageEngineMessages
            .LOG_OPENED_PRECALCULATED_CHUNK_WRITER_FILE_ARG_DATAOFFSET_ARG_B966A48E,
        getFileForLog(),
        maxPhysicalFileSize);
  }

  public TsFilePrecalculatedChunkWriter(final File file, final FileChannel channel)
      throws IOException {
    this(file, channel, false);
  }

  /** Resumes a staged file, preserving a completed PREPARE when it is already sealed. */
  public TsFilePrecalculatedChunkWriter(
      final File file, final FileChannel channel, final boolean sealed) throws IOException {
    this.file = file;
    this.out =
        new WritableFileChannelOutput(
            Objects.requireNonNull(
                channel, StorageEngineMessages.EXCEPTION_CHANNEL_CANNOT_BE_NULL_0F79E0FB));
    this.sealed = sealed;
    this.maxPhysicalFileSize = channel.size();
    LOGGER.info(
        StorageEngineMessages
            .LOG_RESUMED_PRECALCULATED_CHUNK_WRITER_FILE_ARG_RESUMEOFFSET_ARG_39701150,
        getFileForLog(),
        maxPhysicalFileSize);
  }

  public TsFilePrecalculatedChunkWriter(final TsFileOutput out) throws IOException {
    this.file = null;
    this.out =
        Objects.requireNonNull(out, StorageEngineMessages.EXCEPTION_OUT_CANNOT_BE_NULL_E8C2DE32);
    this.sealed = false;
    startFile();
    this.maxPhysicalFileSize = out.getPosition();
    LOGGER.debug(
        StorageEngineMessages
            .LOG_OPENED_PRECALCULATED_CHUNK_WRITER_FILE_ARG_DATAOFFSET_ARG_B966A48E,
        getFileForLog(),
        maxPhysicalFileSize);
  }

  private void startFile() throws IOException {
    out.write(BytesUtils.stringToBytes(TSFileConfig.MAGIC_STRING));
    out.write(new byte[] {TSFileConfig.VERSION_NUMBER});
  }

  // -------------------------------------------------------------------------
  // Chunk Writing & Hole Expansion
  // -------------------------------------------------------------------------

  /** Writes a chunk and its optional group header at predetermined absolute offsets. */
  public ChunkWriteResult writeChunk(
      final IDeviceID device,
      final boolean isAligned,
      final long chunkGroupHeaderOffset,
      final boolean isFirstChunkOfGroup,
      final Chunk chunk,
      long chunkOffset)
      throws IOException {
    final OutputStream stream = out.wrapAsStream();
    long actualChunkGroupHeaderOffset = -1L;

    if (isFirstChunkOfGroup) {
      actualChunkGroupHeaderOffset = alignToOffset(chunkGroupHeaderOffset, device, chunk);
      new ChunkGroupHeader(device).serializeTo(stream);
      updateMaxPhysicalFileSize(out.getPosition());
    }

    chunkOffset = alignToOffset(chunkOffset, device, chunk);
    chunk.getHeader().serializeTo(stream);
    out.write(chunk.getData().duplicate());
    final long actualChunkEndOffset = out.getPosition();
    updateMaxPhysicalFileSize(actualChunkEndOffset);

    // No fsync per chunk: the caller syncs once per staged piece, before it records what it staged.

    final ChunkMetadata metadata =
        new ChunkMetadata(
            chunk.getHeader().getMeasurementID(),
            chunk.getHeader().getDataType(),
            chunk.getHeader().getEncodingType(),
            chunk.getHeader().getCompressionType(),
            chunkOffset,
            chunk.getChunkStatistic());
    metadata.setMask(
        (byte)
            (chunk.getHeader().getChunkType()
                & (TsFileConstant.TIME_COLUMN_MASK | TsFileConstant.VALUE_COLUMN_MASK)));

    device2MetadataMap
        .computeIfAbsent(device, k -> new TreeMap<>())
        .computeIfAbsent(chunk.getHeader().getMeasurementID(), k -> new ArrayList<>())
        .add(metadata);

    LOGGER.debug(
        StorageEngineMessages
            .LOG_WROTE_CHUNK_FILE_ARG_DEVICE_ARG_MEASUREMENT_ARG_OFFSET_ARG_LENGTH_ARG_FIRSTOFGROUP_ARG_C2FEBACD,
        getFileForLog(),
        device,
        chunk.getHeader().getMeasurementID(),
        chunkOffset,
        actualChunkEndOffset - chunkOffset,
        isFirstChunkOfGroup);

    return new ChunkWriteResult(actualChunkGroupHeaderOffset, chunkOffset, actualChunkEndOffset);
  }

  /**
   * Positions writer to target offset. Pads zeros only when expected offset exceeds physical file
   * length.
   */
  private long alignToOffset(final long expectedOffset, final IDeviceID device, final Chunk chunk)
      throws IOException {
    if (out instanceof WritableFileChannelOutput channelOutput) {
      // 1. Expand file with zeros if jumping past current physical maximum bound
      if (expectedOffset > maxPhysicalFileSize) {
        final long paddingStartOffset = maxPhysicalFileSize;
        final long paddingBytes = expectedOffset - paddingStartOffset;
        if (rejectHoles) {
          throw new IOException(
              String.format(
                  StorageEngineMessages
                      .EXCEPTION_IOTCONSENSUSV2_DOES_NOT_SUPPORT_HOLES_THE_STAGED_FILE_ARG_HAS_A_GAP_OF_ARG_BYTES_BEFORE_THE_CHUNK_OF_ARG_ARG_AT_OFFSET_ARG_WHILE_THE_FILE_ENDS_AT_ARG_F8581A0D,
                  getFileForLog(),
                  paddingBytes,
                  device,
                  chunk.getHeader().getMeasurementID(),
                  expectedOffset,
                  paddingStartOffset));
        }
        channelOutput.position(paddingStartOffset);
        writeZeros(paddingBytes);
        maxPhysicalFileSize = expectedOffset;
        LOGGER.warn(
            StorageEngineMessages
                .LOG_FILLED_PHYSICAL_HOLE_BEFORE_WRITING_CHUNK_FILE_ARG_DEVICE_ARG_MEASUREMENT_ARG_EXPECTEDOFFSET_ARG_ACTUALOFFSET_ARG_FILLBYTES_ARG_9EDA3EB6,
            getFileForLog(),
            device,
            chunk.getHeader().getMeasurementID(),
            expectedOffset,
            paddingStartOffset,
            paddingBytes);
      }

      // 2. Reposition directly to expected offset within allocated file range
      channelOutput.position(expectedOffset);
      return expectedOffset;
    }

    // Fallback for sequential non-channel stream output
    final long currentOffset = out.getPosition();
    if (currentOffset < expectedOffset) {
      final long paddingBytes = expectedOffset - currentOffset;
      writeZeros(paddingBytes);
      LOGGER.warn(
          StorageEngineMessages
              .LOG_FILLED_PHYSICAL_HOLE_BEFORE_WRITING_CHUNK_FILE_ARG_DEVICE_ARG_MEASUREMENT_ARG_EXPECTEDOFFSET_ARG_ACTUALOFFSET_ARG_FILLBYTES_ARG_9EDA3EB6,
          getFileForLog(),
          device,
          chunk.getHeader().getMeasurementID(),
          expectedOffset,
          currentOffset,
          paddingBytes);
      return expectedOffset;
    }
    if (currentOffset > expectedOffset) {
      // A non-seekable output cannot place the chunk at its own offset: writing it where the stream
      // already is would produce a file that does not match the layout the pieces were built from.
      throw new IOException(
          String.format(
              StorageEngineMessages
                  .EXCEPTION_THE_OUTPUT_OF_THE_PRECALCULATED_CHUNK_WRITER_CANNOT_SEEK_SO_THE_CHUNK_OF_ARG_ARG_CANNOT_BE_WRITTEN_AT_OFFSET_ARG_WHILE_THE_STREAM_IS_ALREADY_AT_ARG_5A60F53A,
              device,
              chunk.getHeader().getMeasurementID(),
              expectedOffset,
              currentOffset));
    }
    return expectedOffset;
  }

  private void writeZeros(final long byteCount) throws IOException {
    long remaining = byteCount;
    while (remaining > 0) {
      final int step = (int) Math.min(ZERO_BUFFER_CAPACITY, remaining);
      // TsFileOutput exposes no write(byte[], int, int), and wrapping the shared buffer hands out a
      // view of it instead of copying the zeros.
      out.write(ByteBuffer.wrap(ZERO_CHUNK, 0, step));
      remaining -= step;
    }
  }

  private void updateMaxPhysicalFileSize(final long currentPosition) {
    if (currentPosition > maxPhysicalFileSize) {
      maxPhysicalFileSize = currentPosition;
    }
  }

  // -------------------------------------------------------------------------
  // Metadata Restoration & Sealing
  // -------------------------------------------------------------------------

  public void restoreChunkMetadata(
      final Map<IDeviceID, Map<String, List<IChunkMetadata>>> device2Measurement2ChunkMetadata) {
    device2MetadataMap.clear();
    device2Measurement2ChunkMetadata.forEach(
        (device, measurementMap) ->
            measurementMap.forEach(
                (measurement, chunkMetadatas) ->
                    device2MetadataMap
                        .computeIfAbsent(device, k -> new TreeMap<>())
                        .computeIfAbsent(measurement, k -> new ArrayList<>())
                        .addAll(chunkMetadatas)));
    LOGGER.info(
        StorageEngineMessages
            .LOG_RESTORED_CHUNK_METADATA_FILE_ARG_CHUNKCOUNT_ARG_SERIESCOUNT_ARG_49CF51D0,
        getFileForLog(),
        getChunkMetadataCount(),
        getSeriesCount());
  }

  public int getChunkMetadataCount() {
    int count = 0;
    for (final Map<String, List<IChunkMetadata>> measurementMap : device2MetadataMap.values()) {
      for (final List<IChunkMetadata> chunks : measurementMap.values()) {
        count += chunks.size();
      }
    }
    return count;
  }

  private int getSeriesCount() {
    return device2MetadataMap.values().stream().mapToInt(Map::size).sum();
  }

  @Override
  public void close() throws IOException {
    if (sealed) {
      LOGGER.debug(
          StorageEngineMessages
              .LOG_PRECALCULATED_CHUNK_WRITER_IS_ALREADY_SEALED_IGNORING_CLOSE_FILE_ARG_A5E3C6CE,
          getFileForLog());
      return;
    }
    sealed = true;

    try {
      // Seek past the highest written data zone byte before building indices
      if (out instanceof WritableFileChannelOutput channelOutput) {
        channelOutput.position(maxPhysicalFileSize);
      }

      final long metaOffset = out.getPosition();
      final OutputStream stream = out.wrapAsStream();
      ReadWriteIOUtils.write(MetaMarker.SEPARATOR, stream);

      final Map<IDeviceID, MetadataIndexNode> deviceMetadataIndexMap = new TreeMap<>();
      final int seriesCount = getSeriesCount();
      final BloomFilter bloomFilter =
          BloomFilter.getEmptyBloomFilter(
              TSFileDescriptor.getInstance().getConfig().getBloomFilterErrorRate(),
              Math.max(seriesCount, 1));

      for (final Map.Entry<IDeviceID, Map<String, List<IChunkMetadata>>> deviceEntry :
          device2MetadataMap.entrySet()) {
        final IDeviceID deviceId = deviceEntry.getKey();
        final MetadataIndexNode measurementNode =
            new MetadataIndexNode(MetadataIndexNodeType.LEAF_MEASUREMENT);

        for (final Map.Entry<String, List<IChunkMetadata>> measEntry :
            deviceEntry.getValue().entrySet()) {
          final String measurementId = measEntry.getKey();
          final TimeseriesMetadata tsMetadata =
              TSMIterator.constructOneTimeseriesMetadata(measurementId, measEntry.getValue());
          measurementNode.addEntry(
              new MeasurementMetadataIndexEntry(measurementId, out.getPosition()));
          tsMetadata.serializeTo(stream);
          bloomFilter.add(deviceId + "." + measurementId);
        }

        measurementNode.setEndOffset(out.getPosition());
        deviceMetadataIndexMap.put(deviceId, measurementNode);
      }

      final TsFileMetadata tsFileMetadata = new TsFileMetadata();
      final Map<String, MetadataIndexNode> tableIndexNodeMap = new TreeMap<>();
      for (final Map.Entry<String, Map<IDeviceID, MetadataIndexNode>> tableEntry :
          splitDeviceByTable(deviceMetadataIndexMap).entrySet()) {
        tableIndexNodeMap.put(
            tableEntry.getKey(), checkAndBuildLevelIndex(tableEntry.getValue(), out));
      }

      tsFileMetadata.setTableMetadataIndexNodeMap(tableIndexNodeMap);
      tsFileMetadata.setTableSchemaMap(tableSchemaMap);
      tsFileMetadata.setMetaOffset(metaOffset);
      tsFileMetadata.setBloomFilter(bloomFilter);

      final long tsFileMetadataOffset = out.getPosition();
      tsFileMetadata.serializeTo(stream);
      final int tsFileMetadataSize = (int) (out.getPosition() - tsFileMetadataOffset);

      ReadWriteIOUtils.write(tsFileMetadataSize, stream);
      out.write(BytesUtils.stringToBytes(TSFileConfig.MAGIC_STRING));
      out.force();

      LOGGER.info(
          StorageEngineMessages
              .LOG_SEALED_PRECALCULATED_CHUNK_FILE_FILE_ARG_METAOFFSET_ARG_CHUNKCOUNT_ARG_SERIESCOUNT_ARG_FILELENGTH_ARG_D837F9C1,
          getFileForLog(),
          metaOffset,
          getChunkMetadataCount(),
          seriesCount,
          out.getPosition());
    } finally {
      out.close();
    }
  }

  // -------------------------------------------------------------------------
  // Accessors & Models
  // -------------------------------------------------------------------------

  public boolean isSealed() {
    return sealed;
  }

  /** Rejects a chunk that does not follow the previous one, as IoTConsensusV2 requires. */
  public void setRejectHoles(final boolean rejectHoles) {
    this.rejectHoles = rejectHoles;
  }

  public TsFileOutput getOutput() {
    return out;
  }

  public File getFile() {
    if (file == null) {
      throw new IllegalStateException(
          StorageEngineMessages.EXCEPTION_THIS_WRITER_IS_NOT_BACKED_BY_A_FILE_7103C187);
    }
    return file;
  }

  public Map<IDeviceID, List<IChunkMetadata>> getChunkMetadataListMap() {
    final Map<IDeviceID, List<IChunkMetadata>> result = new TreeMap<>();
    device2MetadataMap.forEach(
        (device, measurementMap) ->
            result.put(
                device,
                measurementMap.values().stream()
                    .flatMap(List::stream)
                    .collect(Collectors.toList())));
    return result;
  }

  public Set<String> getTableNames() {
    return device2MetadataMap.keySet().stream()
        .map(IDeviceID::getTableName)
        .collect(Collectors.toSet());
  }

  public void registerTableSchema(final TableSchema tableSchema) {
    tableSchemaMap.put(tableSchema.getTableName(), tableSchema);
  }

  private String getFileForLog() {
    return file == null ? "<memory-output>" : file.getAbsolutePath();
  }

  public record ChunkWriteResult(
      long actualChunkGroupHeaderOffset, long actualChunkOffset, long actualChunkEndOffset) {}

  // -------------------------------------------------------------------------
  // Channel Wrapper
  // -------------------------------------------------------------------------

  private static final class WritableFileChannelOutput implements TsFileOutput {

    private final FileChannel channel;
    private final ByteBuffer singleByteBuffer = ByteBuffer.allocateDirect(1);

    private WritableFileChannelOutput(final FileChannel channel) {
      this.channel = channel;
    }

    @Override
    public void write(final byte[] bytes) throws IOException {
      write(bytes, 0, bytes.length);
    }

    public void write(final byte[] bytes, final int offset, final int length) throws IOException {
      write(ByteBuffer.wrap(bytes, offset, length));
    }

    @Override
    public synchronized void write(final byte b) throws IOException {
      singleByteBuffer.clear();
      singleByteBuffer.put(b);
      singleByteBuffer.flip();
      write(singleByteBuffer);
    }

    @Override
    public void write(final ByteBuffer buffer) throws IOException {
      while (buffer.hasRemaining()) {
        channel.write(buffer);
      }
    }

    @Override
    public long getPosition() throws IOException {
      return channel.position();
    }

    private void position(final long offset) throws IOException {
      channel.position(offset);
    }

    @Override
    public OutputStream wrapAsStream() {
      return new OutputStream() {
        @Override
        public void write(final int b) throws IOException {
          WritableFileChannelOutput.this.write((byte) b);
        }

        @Override
        public void write(final byte[] bytes, final int offset, final int length)
            throws IOException {
          WritableFileChannelOutput.this.write(bytes, offset, length);
        }
      };
    }

    @Override
    public void flush() throws IOException {
      force();
    }

    @Override
    public void force() throws IOException {
      // Pass false to fsync file content without blocking on OS metadata modifications
      channel.force(false);
    }

    @Override
    public void truncate(final long size) throws IOException {
      channel.truncate(size);
    }

    @Override
    public void close() throws IOException {
      channel.close();
    }
  }
}
