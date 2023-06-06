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
import org.apache.iotdb.db.engine.compaction.schedule.constant.CompactionType;
import org.apache.iotdb.db.engine.compaction.schedule.constant.WrittenDataType;
import org.apache.iotdb.db.service.metrics.CompactionMetrics;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import java.io.File;
import java.io.IOException;

public class CompactionTsFileWriter extends TsFileIOWriter {
  CompactionType type;

  public CompactionTsFileWriter(
      File file, boolean enableMemoryControl, long maxMetadataSize, CompactionType type)
      throws IOException {
    super(file, enableMemoryControl, maxMetadataSize);
    this.type = type;
  }

  @Override
  public void writeChunk(Chunk chunk, ChunkMetadata chunkMetadata) throws IOException {
    long beforeOffset = this.getPos();
    CompactionTaskManager.getInstance()
        .getMergeWriteRateLimiter()
        .acquire(chunk.getHeader().getDataSize() + chunk.getHeader().getSerializedSize());
    super.writeChunk(chunk, chunkMetadata);
    long writtenDataSize = this.getPos() - beforeOffset;
    CompactionMetrics.getInstance()
        .recordWriteInfo(type, WrittenDataType.NOT_ALIGNED, writtenDataSize);
  }

  public void writeChunk(IChunkWriter chunkWriter) throws IOException {
    boolean isAligned = chunkWriter instanceof AlignedChunkWriterImpl;
    long beforeOffset = this.getPos();
    chunkWriter.writeToFileWriter(this);
    long writtenDataSize = this.getPos() - beforeOffset;
    CompactionMetrics.getInstance()
        .recordWriteInfo(
            type,
            isAligned ? WrittenDataType.ALIGNED : WrittenDataType.NOT_ALIGNED,
            writtenDataSize);
  }

  @Override
  public void endFile() throws IOException {
    long beforeSize = this.getPos();
    super.endFile();
    long writtenDataSize = this.getPos() - beforeSize;
    CompactionMetrics.getInstance()
        .recordWriteInfo(type, WrittenDataType.METADATA, writtenDataSize);
  }
}
