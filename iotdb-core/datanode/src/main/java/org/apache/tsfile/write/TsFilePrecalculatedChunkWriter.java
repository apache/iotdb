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
import java.util.TreeMap;

import static org.apache.tsfile.file.metadata.MetadataIndexConstructor.checkAndBuildLevelIndex;
import static org.apache.tsfile.file.metadata.MetadataIndexConstructor.splitDeviceByTable;

/** Low-level Chunk writer that writes precalculated physical Chunks directly by absolute offset. */
public class TsFilePrecalculatedChunkWriter implements AutoCloseable {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(TsFilePrecalculatedChunkWriter.class);

  private final TsFileOutput out;
  private final File file;
  private final Map<IDeviceID, Map<String, List<IChunkMetadata>>> device2MetadataMap =
      new TreeMap<>();

  /** Set once {@link #close()} has written the metadata zone, so it is never written twice. */
  private boolean sealed;

  public TsFilePrecalculatedChunkWriter(File file) throws IOException {
    this.file = file;
    // The file is opened as a channel rather than as a stream because chunks are written at the
    // absolute offsets of the layout: a piece that reaches this node after a piece the layout puts
    // behind it has to be written at its own offset, see alignToOffset.
    this.out =
        new WritableFileChannelOutput(
            FileChannel.open(
                file.toPath(),
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING));
    this.sealed = false;
    startFile();
  }

  /**
   * Resumes a staged file that was left behind by an interrupted LOAD. The file already carries its
   * header, so nothing is prepended; the data zone simply continues where the last completely
   * written chunk of that file ends.
   *
   * <p>The caller owns the file up to that point: it must have dropped whatever the interrupted
   * writer left beyond it and positioned the channel there, so that the data zone stays aligned
   * with the absolute chunk offsets that every replica computes independently.
   *
   * @param file the staged file being continued, used for logging
   * @param channel the channel of that file, already positioned where the next chunk belongs
   */
  public TsFilePrecalculatedChunkWriter(final File file, final FileChannel channel)
      throws IOException {
    this.file = file;
    this.out = new WritableFileChannelOutput(channel);
    this.sealed = false;
  }

  public TsFilePrecalculatedChunkWriter(TsFileOutput out) throws IOException {
    this.file = null;
    this.out = out;
    this.sealed = false;
    startFile();
  }

  private void startFile() throws IOException {
    out.write(BytesUtils.stringToBytes(TSFileConfig.MAGIC_STRING));
    out.write(new byte[] {TSFileConfig.VERSION_NUMBER});
  }

  /**
   * Restores the in-memory metadata of a resumed writer from the chunk headers and statistics that
   * were persisted next to the staged file while it was written.
   *
   * <p>Without this the writer would seal the file with an empty metadata zone, which would make
   * the chunks already on disk unreachable. The chunks must be supplied in the order they were
   * written, because the order of a measurement's chunk list is the order they are serialized in.
   *
   * @param device2Measurement2ChunkMetadata the restored chunks, grouped by device and measurement
   */
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
  }

  /**
   * @return the number of chunks the writer currently knows about, restored ones included. Every
   *     measurement of every device may hold several of them, so the lists are what is counted and
   *     not the measurements holding them.
   */
  public int getChunkMetadataCount() {
    int count = 0;
    for (final Map<String, List<IChunkMetadata>> measurementMap : device2MetadataMap.values()) {
      for (final List<IChunkMetadata> chunks : measurementMap.values()) {
        count += chunks.size();
      }
    }
    return count;
  }

  public boolean isSealed() {
    return sealed;
  }

  public ChunkWriteResult writeChunk(
      IDeviceID device,
      boolean isAligned,
      long chunkGroupHeaderOffset,
      boolean isFirstChunkOfGroup,
      Chunk chunk,
      long chunkOffset)
      throws IOException {
    OutputStream stream = out.wrapAsStream();
    long actualChunkGroupHeaderOffset = -1L;

    if (isFirstChunkOfGroup) {
      actualChunkGroupHeaderOffset = alignToOffset(chunkGroupHeaderOffset, device, chunk);
      new ChunkGroupHeader(device).serializeTo(stream);
    }

    chunkOffset = alignToOffset(chunkOffset, device, chunk);
    chunk.getHeader().serializeTo(stream);
    out.write(chunk.getData().duplicate());
    final long actualChunkEndOffset = out.getPosition();

    ChunkMetadata metadata =
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

    return new ChunkWriteResult(actualChunkGroupHeaderOffset, chunkOffset, actualChunkEndOffset);
  }

  private long alignToOffset(final long expectedOffset, final IDeviceID device, final Chunk chunk)
      throws IOException {
    final long currentOffset = out.getPosition();
    if (currentOffset < expectedOffset) {
      long remaining = expectedOffset - currentOffset;
      while (remaining > 0) {
        final int step = (int) Math.min(Integer.MAX_VALUE, remaining);
        out.write(new byte[step]);
        remaining -= step;
      }
      LOGGER.warn(
          StorageEngineMessages
              .LOG_FILLED_PHYSICAL_HOLE_BEFORE_WRITING_CHUNK_FILE_ARG_DEVICE_ARG_MEASUREMENT_ARG_EXPECTEDOFFSET_ARG_ACTUALOFFSET_ARG_FILLBYTES_ARG_9EDA3EB6,
          getFileForLog(),
          device,
          chunk.getHeader().getMeasurementID(),
          expectedOffset,
          currentOffset,
          expectedOffset - currentOffset);
      return expectedOffset;
    }
    if (currentOffset > expectedOffset) {
      // This piece arrived after a piece the layout puts behind it, which is what a writer resumed
      // from a snapshot that already held the later pieces sees. The offset of a chunk is part of
      // the layout every replica computes on its own, so the chunk has to be written where the
      // layout puts it: the bytes that arrived earlier are elsewhere in the file, and the region in
      // between stays the zeros this writer pads with. Appending it instead would leave the planned
      // range as a hole, which is indistinguishable from a piece that never arrived.
      if (out instanceof WritableFileChannelOutput) {
        ((WritableFileChannelOutput) out).position(expectedOffset);
        return expectedOffset;
      }
      LOGGER.warn(
          StorageEngineMessages
              .LOG_PRECALCULATED_OFFSET_IS_BEHIND_ACTUAL_FILE_POSITION_USING_ACTUAL_POSITION_FILE_ARG_DEVICE_ARG_MEASUREMENT_ARG_EXPECTEDOFFSET_ARG_ACTUALOFFSET_ARG_DELTA_ARG_F05C873F,
          getFileForLog(),
          device,
          chunk.getHeader().getMeasurementID(),
          expectedOffset,
          currentOffset,
          currentOffset - expectedOffset);
      return currentOffset;
    }
    return expectedOffset;
  }

  @Override
  public void close() throws IOException {
    if (sealed) {
      return;
    }
    sealed = true;
    // Writing a chunk behind the end of the file leaves the position there, so the metadata zone
    // has to be placed after the last byte of the data zone rather than after the last write:
    // sealing at a position inside the data zone would overwrite chunks with the metadata index.
    if (out instanceof WritableFileChannelOutput) {
      ((WritableFileChannelOutput) out).positionToEndOfFile();
    }
    final long metaOffset = out.getPosition();
    final OutputStream stream = out.wrapAsStream();
    ReadWriteIOUtils.write(MetaMarker.SEPARATOR, stream);
    final Map<IDeviceID, MetadataIndexNode> deviceMetadataIndexMap = new TreeMap<>();

    int seriesCount = device2MetadataMap.values().stream().mapToInt(Map::size).sum();
    BloomFilter bloomFilter =
        BloomFilter.getEmptyBloomFilter(
            TSFileDescriptor.getInstance().getConfig().getBloomFilterErrorRate(),
            Math.max(seriesCount, 1));

    for (Map.Entry<IDeviceID, Map<String, List<IChunkMetadata>>> deviceEntry :
        device2MetadataMap.entrySet()) {
      IDeviceID deviceId = deviceEntry.getKey();
      Map<String, List<IChunkMetadata>> measurementMap = deviceEntry.getValue();

      MetadataIndexNode measurementNode =
          new MetadataIndexNode(MetadataIndexNodeType.LEAF_MEASUREMENT);
      for (Map.Entry<String, List<IChunkMetadata>> measEntry : measurementMap.entrySet()) {
        String measurementId = measEntry.getKey();
        List<IChunkMetadata> chunkMetadatas = measEntry.getValue();

        final TimeseriesMetadata tsMetadata =
            TSMIterator.constructOneTimeseriesMetadata(measurementId, chunkMetadatas);
        final long tsMetadataOffset = out.getPosition();
        measurementNode.addEntry(
            new MeasurementMetadataIndexEntry(measurementId, tsMetadataOffset));
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
    tsFileMetadata.setMetaOffset(metaOffset);
    tsFileMetadata.setBloomFilter(bloomFilter);

    long tsFileMetadataOffset = out.getPosition();
    tsFileMetadata.serializeTo(stream);
    int tsFileMetadataSize = (int) (out.getPosition() - tsFileMetadataOffset);

    ReadWriteIOUtils.write(tsFileMetadataSize, stream);
    out.write(BytesUtils.stringToBytes(TSFileConfig.MAGIC_STRING));
    out.close();
  }

  private String getFileForLog() {
    return file == null ? "<memory-output>" : file.getAbsolutePath();
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
                    .collect(java.util.stream.Collectors.toList())));
    return result;
  }

  public record ChunkWriteResult(
      long actualChunkGroupHeaderOffset, long actualChunkOffset, long actualChunkEndOffset) {}

  /**
   * A {@link TsFileOutput} that continues an existing staged file at a given offset.
   *
   * <p>It exists because {@link LocalTsFileOutput} can only be created on a {@code
   * FileOutputStream}, which either truncates (plain constructor) or appends at the current end of
   * file — and the end of file is exactly what an interrupted LOAD cannot trust: the interrupted
   * write may have left a partially written chunk behind it. Positioning by offset keeps the data
   * zone aligned with the absolute chunk offsets that every replica computes independently.
   */
  private static final class WritableFileChannelOutput implements TsFileOutput {

    private final FileChannel channel;

    private WritableFileChannelOutput(final FileChannel channel) {
      this.channel = channel;
    }

    @Override
    public void write(final byte[] bytes) throws IOException {
      write(ByteBuffer.wrap(bytes));
    }

    @Override
    public void write(final byte b) throws IOException {
      write(new byte[] {b});
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

    /** Positions the channel at an absolute offset, so a late chunk lands where it was laid out. */
    private void position(final long offset) throws IOException {
      channel.position(offset);
    }

    /** Positions the channel after the last byte of the file. */
    private void positionToEndOfFile() throws IOException {
      channel.position(channel.size());
    }

    @Override
    public OutputStream wrapAsStream() {
      return new OutputStream() {

        @Override
        public void write(final int b) throws IOException {
          WritableFileChannelOutput.this.write(new byte[] {(byte) b});
        }

        @Override
        public void write(final byte[] bytes, final int offset, final int length)
            throws IOException {
          WritableFileChannelOutput.this.write(ByteBuffer.wrap(bytes, offset, length).slice());
        }
      };
    }

    @Override
    public void flush() {
      // Unbuffered: every write reaches the channel immediately.
    }

    @Override
    public void force() throws IOException {
      channel.force(true);
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
