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
import org.apache.iotdb.tsfile.read.TsFileDeviceIterator;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;

import java.io.IOException;
import java.nio.ByteBuffer;

public class CompactionTsFileReader extends TsFileSequenceReader {
  long readDataSize = 0L;
  CompactionType compactionType;

  boolean readingAlignedSeries = false;

  public CompactionTsFileReader(String file, CompactionType compactionType) throws IOException {
    super(file);
    this.compactionType = compactionType;
  }

  @Override
  protected ByteBuffer readData(long position, int totalSize) throws IOException {
    ByteBuffer buffer = super.readData(position, totalSize);
    readDataSize += totalSize;
    return buffer;
  }

  public void markStartAlignedSeries() {
    readingAlignedSeries = true;
  }

  public void markEndAlignedSeries() {
    readingAlignedSeries = false;
  }

  @Override
  public Chunk readMemChunk(ChunkMetadata metaData) throws IOException {
    long before = readDataSize;
    Chunk chunk = super.readMemChunk(metaData);
    long dataSize = readDataSize - before;
    CompactionMetrics.getInstance()
        .recordReadInfo(
            compactionType,
            readingAlignedSeries ? CompactionIoDataType.ALIGNED : CompactionIoDataType.NOT_ALIGNED,
            dataSize);
    return chunk;
  }

  @Override
  public TsFileDeviceIterator getAllDevicesIteratorWithIsAligned() throws IOException {
    long before = readDataSize;
    TsFileDeviceIterator iterator = super.getAllDevicesIteratorWithIsAligned();
    long dataSize = readDataSize - before;
    CompactionMetrics.getInstance()
        .recordReadInfo(compactionType, CompactionIoDataType.METADATA, dataSize);
    return iterator;
  }
}
