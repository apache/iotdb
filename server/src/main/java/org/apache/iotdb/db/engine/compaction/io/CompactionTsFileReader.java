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

package org.apache.iotdb.db.engine.compaction.io;

import org.apache.iotdb.db.engine.compaction.schedule.constant.CompactionIoDataType;
import org.apache.iotdb.db.engine.compaction.schedule.constant.CompactionType;
import org.apache.iotdb.db.service.metrics.CompactionMetrics;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.read.TsFileDeviceIterator;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
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
    this.compactionType = compactionType;
  }

  @Override
  protected ByteBuffer readData(long position, int totalSize) throws IOException {
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
}
