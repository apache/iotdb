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
import org.apache.tsfile.write.writer.LocalTsFileOutput;
import org.apache.tsfile.write.writer.TsFileOutput;
import org.apache.tsfile.write.writer.tsmiterator.TSMIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
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

  public TsFilePrecalculatedChunkWriter(File file) throws IOException {
    this.file = file;
    this.out = new LocalTsFileOutput(new FileOutputStream(file));
    startFile();
  }

  public TsFilePrecalculatedChunkWriter(TsFileOutput out) throws IOException {
    this.file = null;
    this.out = out;
    startFile();
  }

  private void startFile() throws IOException {
    out.write(BytesUtils.stringToBytes(TSFileConfig.MAGIC_STRING));
    out.write(new byte[] {TSFileConfig.VERSION_NUMBER});
  }

  public ChunkWriteResult writeChunk(
      IDeviceID device,
      boolean isAligned,
      long chunkGroupHeaderOffset,
      boolean isFirstChunkOfGroup,
      Chunk chunk,
      long chunkOffset)
      throws IOException {
    long currentPos = out.getPosition();
    OutputStream stream = out.wrapAsStream();
    final int chunkHeaderSize = chunk.getHeader().getSerializedSize();
    final int chunkDataSize = chunk.getData().remaining();
    final long chunkLength = chunkHeaderSize + (long) chunkDataSize;
    long actualChunkGroupHeaderOffset = -1L;

    if (isFirstChunkOfGroup) {
      actualChunkGroupHeaderOffset = alignToOffset(chunkGroupHeaderOffset, device, chunk);
      new ChunkGroupHeader(device).serializeTo(stream);
      currentPos = out.getPosition();
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
          "Filled physical hole before writing chunk: file={}, device={}, measurement={}, "
              + "expectedOffset={}, actualOffset={}, fillBytes={}",
          getFileForLog(),
          device,
          chunk.getHeader().getMeasurementID(),
          expectedOffset,
          currentOffset,
          expectedOffset - currentOffset);
      return expectedOffset;
    }
    if (currentOffset > expectedOffset) {
      LOGGER.warn(
          "Precalculated offset is behind actual file position; using actual position: file={}, "
              + "device={}, measurement={}, expectedOffset={}, actualOffset={}, delta={}",
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
      throw new IllegalStateException("This writer is not backed by a file");
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
}
