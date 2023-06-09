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

import org.apache.iotdb.db.engine.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.engine.compaction.schedule.constant.CompactionIoDataType;
import org.apache.iotdb.db.engine.compaction.schedule.constant.CompactionType;
import org.apache.iotdb.db.service.metrics.CompactionMetrics;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;

public class CompactionTsFileWriter extends TsFileIOWriter {
  CompactionType type;

  private volatile boolean isWritingAligned = false;

  public CompactionTsFileWriter(
      File file, boolean enableMemoryControl, long maxMetadataSize, CompactionType type)
      throws IOException {
    super(file, enableMemoryControl, maxMetadataSize);
    this.type = type;
  }

  @Override
  public void writeChunk(Chunk chunk, ChunkMetadata chunkMetadata) throws IOException {
    long beforeOffset = this.getPos();
    super.writeChunk(chunk, chunkMetadata);
    long writtenDataSize = this.getPos() - beforeOffset;
    if (writtenDataSize > 0) {
      CompactionTaskManager.getInstance().getMergeWriteRateLimiter().acquire((int) writtenDataSize);
    }
    CompactionMetrics.getInstance()
        .recordWriteInfo(
            type,
            isWritingAligned ? CompactionIoDataType.ALIGNED : CompactionIoDataType.NOT_ALIGNED,
            writtenDataSize);
  }

  @Override
  public void writeEmptyValueChunk(
      String measurementId,
      CompressionType compressionType,
      TSDataType tsDataType,
      TSEncoding encodingType,
      Statistics<? extends Serializable> statistics)
      throws IOException {
    long beforeOffset = this.getPos();
    super.writeEmptyValueChunk(
        measurementId, compressionType, tsDataType, encodingType, statistics);
    long writtenDataSize = this.getPos() - beforeOffset;
    CompactionMetrics.getInstance()
        .recordWriteInfo(type, CompactionIoDataType.ALIGNED, writtenDataSize);
    if (writtenDataSize > 0) {
      CompactionTaskManager.getInstance().getMergeWriteRateLimiter().acquire((int) writtenDataSize);
    }
  }

  public void writeChunk(IChunkWriter chunkWriter) throws IOException {
    boolean isAligned = chunkWriter instanceof AlignedChunkWriterImpl;
    long beforeOffset = this.getPos();
    chunkWriter.writeToFileWriter(this);
    long writtenDataSize = this.getPos() - beforeOffset;
    if (writtenDataSize > 0) {
      CompactionTaskManager.getInstance().getMergeWriteRateLimiter().acquire((int) writtenDataSize);
    }
    CompactionMetrics.getInstance()
        .recordWriteInfo(
            type,
            isAligned ? CompactionIoDataType.ALIGNED : CompactionIoDataType.NOT_ALIGNED,
            writtenDataSize);
  }

  @Override
  public void endFile() throws IOException {
    long beforeSize = this.getPos();
    super.endFile();
    long writtenDataSize = this.getPos() - beforeSize;
    if (writtenDataSize > 0) {
      CompactionTaskManager.getInstance().getMergeWriteRateLimiter().acquire((int) writtenDataSize);
    }
    CompactionMetrics.getInstance()
        .recordWriteInfo(type, CompactionIoDataType.METADATA, writtenDataSize);
  }

  public void markStartingWritingAligned() {
    isWritingAligned = true;
  }

  public void markEndingWritingAligned() {
    isWritingAligned = false;
  }
}
