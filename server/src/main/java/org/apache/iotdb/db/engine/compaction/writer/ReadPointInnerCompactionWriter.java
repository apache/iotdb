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
package org.apache.iotdb.db.engine.compaction.writer;

import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.write.chunk.AlignedChunkWriterImpl;

import java.io.IOException;

public class ReadPointInnerCompactionWriter extends AbstractInnerCompactionWriter {
  public ReadPointInnerCompactionWriter(TsFileResource targetFileResource) throws IOException {
    super(targetFileResource);
  }

  @Override
  public void write(TimeValuePair timeValuePair, int subTaskId) throws IOException {
    writeDataPoint(timeValuePair.getTimestamp(), timeValuePair.getValue(), chunkWriters[subTaskId]);
    chunkPointNumArray[subTaskId]++;
    checkChunkSizeAndMayOpenANewChunk(fileWriter, chunkWriters[subTaskId], subTaskId, false);
    isEmptyFile = false;
  }

  @Override
  public void write(TimeColumn timestamps, Column[] columns, int subTaskId, int batchSize)
      throws IOException {
    AlignedChunkWriterImpl chunkWriter = (AlignedChunkWriterImpl) this.chunkWriters[subTaskId];
    chunkWriter.write(timestamps, columns, batchSize);
    synchronized (this) {
      // we need to synchronized here to avoid multi-thread competition in sub-task
      targetResource.updateStartTime(deviceId, timestamps.getStartTime());
      targetResource.updateEndTime(deviceId, timestamps.getEndTime());
    }
    chunkPointNumArray[subTaskId] += timestamps.getTimes().length;
    checkChunkSizeAndMayOpenANewChunk(fileWriter, chunkWriter, subTaskId, false);
    isEmptyFile = false;
  }
}
