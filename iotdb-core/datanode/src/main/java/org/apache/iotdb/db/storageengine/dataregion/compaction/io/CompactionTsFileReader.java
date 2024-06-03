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

package org.apache.iotdb.db.storageengine.dataregion.compaction.io;

import org.apache.iotdb.db.service.metrics.CompactionMetrics;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.constant.CompactionIoDataType;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.constant.CompactionType;

import org.apache.tsfile.file.IMetadataIndexEntry;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.MetadataIndexNode;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.file.metadata.enums.MetadataIndexNodeType;
import org.apache.tsfile.read.TsFileDeviceIterator;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.utils.Pair;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class extends the TsFileSequenceReader class to read and manage TsFile with a focus on
 * compaction-related operations. This includes functions for tracking and recording the amount of
 * data read and distinguishing between aligned and not aligned series during compaction.
 */
public class CompactionTsFileReader extends TsFileSequenceReader {
  /** Tracks the total amount of data (in bytes) that has been read. */
  private AtomicLong readDataSize = new AtomicLong(0L);

  /** The type of compaction running. */
  CompactionType compactionType;

  /** A flag that indicates if an aligned series is being read. */
  private volatile boolean readingAlignedSeries = false;

  /**
   * Constructs a new instance of CompactionTsFileReader.
   *
   * @param file The file to be read.
   * @param compactionType The type of compaction running.
   * @throws IOException If an error occurs during file operations.
   */
  public CompactionTsFileReader(String file, CompactionType compactionType) throws IOException {
    super(file);
    this.tsFileInput = new CompactionTsFileInput(tsFileInput);
    this.compactionType = compactionType;
  }

  @Override
  protected ByteBuffer readData(long position, int totalSize) throws IOException {
    acquireReadDataSizeWithCompactionReadRateLimiter(totalSize);
    ByteBuffer buffer = super.readData(position, totalSize);
    readDataSize.addAndGet(totalSize);
    return buffer;
  }

  /** Marks the start of reading an aligned series. */
  public void markStartOfAlignedSeries() {
    readingAlignedSeries = true;
  }

  /** Marks the end of reading an aligned series. */
  public void markEndOfAlignedSeries() {
    readingAlignedSeries = false;
  }

  @Override
  public Chunk readMemChunk(ChunkMetadata metaData) throws IOException {
    synchronized (this) {
      // using synchronized to avoid concurrent read that makes readDataSize not correct
      long before = readDataSize.get();
      Chunk chunk = super.readMemChunk(metaData);
      long dataSize = readDataSize.get() - before;
      CompactionMetrics.getInstance()
          .recordReadInfo(
              compactionType,
              readingAlignedSeries
                  ? CompactionIoDataType.ALIGNED
                  : CompactionIoDataType.NOT_ALIGNED,
              dataSize);
      return chunk;
    }
  }

  public ChunkHeader readChunkHeader(long position) throws IOException {
    return ChunkHeader.deserializeFrom(tsFileInput, position);
  }

  public InputStream wrapAsInputStream() throws IOException {
    return this.tsFileInput.wrapAsInputStream();
  }

  public ByteBuffer readPageWithoutUnCompressing(long startOffset, int pageSize)
      throws IOException {
    if (pageSize == 0) {
      return null;
    }
    return readData(startOffset, pageSize);
  }

  @Override
  public TsFileDeviceIterator getAllDevicesIteratorWithIsAligned() throws IOException {
    long before = readDataSize.get();
    TsFileDeviceIterator iterator = super.getAllDevicesIteratorWithIsAligned();
    long dataSize = readDataSize.get() - before;
    CompactionMetrics.getInstance()
        .recordReadInfo(compactionType, CompactionIoDataType.METADATA, dataSize);
    return iterator;
  }

  @Override
  public List<IChunkMetadata> getChunkMetadataListByTimeseriesMetadataOffset(
      long startOffset, long endOffset) throws IOException {
    long before = readDataSize.get();
    List<IChunkMetadata> chunkMetadataList =
        super.getChunkMetadataListByTimeseriesMetadataOffset(startOffset, endOffset);
    long dataSize = readDataSize.get() - before;
    CompactionMetrics.getInstance()
        .recordReadInfo(compactionType, CompactionIoDataType.METADATA, dataSize);
    return chunkMetadataList;
  }

  @Override
  public void getDevicesAndEntriesOfOneLeafNode(
      Long startOffset, Long endOffset, Queue<Pair<IDeviceID, long[]>> measurementNodeOffsetQueue)
      throws IOException {
    long before = readDataSize.get();
    super.getDevicesAndEntriesOfOneLeafNode(startOffset, endOffset, measurementNodeOffsetQueue);
    long dataSize = readDataSize.get() - before;
    CompactionMetrics.getInstance()
        .recordReadInfo(compactionType, CompactionIoDataType.METADATA, dataSize);
  }

  @Override
  public MetadataIndexNode readMetadataIndexNode(long start, long end, boolean isDeviceLevel)
      throws IOException {
    long before = readDataSize.get();
    MetadataIndexNode metadataIndexNode = super.readMetadataIndexNode(start, end, isDeviceLevel);
    long dataSize = readDataSize.get() - before;
    CompactionMetrics.getInstance()
        .recordReadInfo(compactionType, CompactionIoDataType.METADATA, dataSize);
    return metadataIndexNode;
  }

  @Override
  public Map<String, Pair<List<IChunkMetadata>, Pair<Long, Long>>>
      getTimeseriesMetadataOffsetByDevice(
          MetadataIndexNode measurementNode,
          Set<String> excludedMeasurementIds,
          boolean needChunkMetadata)
          throws IOException {
    long before = readDataSize.get();
    Map<String, Pair<List<IChunkMetadata>, Pair<Long, Long>>> result =
        super.getTimeseriesMetadataOffsetByDevice(
            measurementNode, excludedMeasurementIds, needChunkMetadata);
    long dataSize = readDataSize.get() - before;
    CompactionMetrics.getInstance()
        .recordReadInfo(compactionType, CompactionIoDataType.METADATA, dataSize);
    return result;
  }

  public Map<String, Pair<TimeseriesMetadata, Pair<Long, Long>>>
      getTimeseriesMetadataAndOffsetByDevice(
          MetadataIndexNode measurementNode,
          Set<String> excludedMeasurementIds,
          boolean needChunkMetadata)
          throws IOException {
    long before = readDataSize.get();
    Map<String, Pair<TimeseriesMetadata, Pair<Long, Long>>> timeseriesMetadataOffsetMap =
        new LinkedHashMap<>();
    List<IMetadataIndexEntry> childrenEntryList = measurementNode.getChildren();
    for (int i = 0; i < childrenEntryList.size(); i++) {
      long startOffset = childrenEntryList.get(i).getOffset();
      long endOffset =
          i == childrenEntryList.size() - 1
              ? measurementNode.getEndOffset()
              : childrenEntryList.get(i + 1).getOffset();
      ByteBuffer nextBuffer = readData(startOffset, endOffset);
      if (measurementNode.getNodeType().equals(MetadataIndexNodeType.LEAF_MEASUREMENT)) {
        // leaf measurement node
        while (nextBuffer.hasRemaining()) {
          int metadataStartOffset = nextBuffer.position();
          TimeseriesMetadata timeseriesMetadata =
              TimeseriesMetadata.deserializeFrom(
                  nextBuffer, excludedMeasurementIds, needChunkMetadata);
          timeseriesMetadataOffsetMap.put(
              timeseriesMetadata.getMeasurementId(),
              new Pair<>(
                  timeseriesMetadata,
                  new Pair<>(
                      startOffset + metadataStartOffset, startOffset + nextBuffer.position())));
        }

      } else {
        // internal measurement node
        MetadataIndexNode nextLayerMeasurementNode =
            MetadataIndexNode.deserializeFrom(nextBuffer, false);
        timeseriesMetadataOffsetMap.putAll(
            getTimeseriesMetadataAndOffsetByDevice(
                nextLayerMeasurementNode, excludedMeasurementIds, needChunkMetadata));
      }
    }

    long dataSize = readDataSize.get() - before;
    CompactionMetrics.getInstance()
        .recordReadInfo(compactionType, CompactionIoDataType.METADATA, dataSize);
    return timeseriesMetadataOffsetMap;
  }

  @Override
  public void getDeviceTimeseriesMetadata(
      List<TimeseriesMetadata> timeseriesMetadataList,
      MetadataIndexNode measurementNode,
      Set<String> excludedMeasurementIds,
      boolean needChunkMetadata)
      throws IOException {
    long before = readDataSize.get();
    super.getDeviceTimeseriesMetadata(
        timeseriesMetadataList, measurementNode, excludedMeasurementIds, needChunkMetadata);
    long dataSize = readDataSize.get() - before;
    CompactionMetrics.getInstance()
        .recordReadInfo(compactionType, CompactionIoDataType.METADATA, dataSize);
  }

  private void acquireReadDataSizeWithCompactionReadRateLimiter(int readDataSize) {
    CompactionTaskManager.getInstance().getCompactionReadOperationRateLimiter().acquire(1);
    CompactionTaskManager.getInstance().getCompactionReadRateLimiter().acquire(readDataSize);
  }

  @Override
  public boolean equals(Object o) {
    return super.equals(o);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }
}
