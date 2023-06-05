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

package org.apache.iotdb.db.engine.compaction.utils;

import org.apache.iotdb.db.engine.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.engine.compaction.schedule.constant.CompactionType;
import org.apache.iotdb.db.service.metrics.CompactionMetrics;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import java.io.File;
import java.io.IOException;

public class CompactionTsFileWriter extends TsFileIOWriter {
  public CompactionTsFileWriter(File file, boolean enableMemoryControl, long maxMetadataSize)
      throws IOException {
    super(file, enableMemoryControl, maxMetadataSize);
  }

  public void writeChunk(
      CompactionType type, Chunk chunk, ChunkMetadata chunkMetadata, boolean aligned)
      throws IOException {
    int dataSize = chunk.getHeader().getSerializedSize() + chunk.getHeader().getDataSize();
    CompactionTaskManager.getInstance().getMergeWriteRateLimiter().acquire(dataSize);
    super.writeChunk(chunk, chunkMetadata);
    CompactionMetrics.getInstance().recordWriteInfo(type, aligned, dataSize);
  }
}
