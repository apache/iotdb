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
package org.apache.iotdb.db.engine.compaction.writer;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.rescon.SystemInfo;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class InnerSpaceCompactionWriter extends AbstractCompactionWriter {
  private TsFileIOWriter fileWriter;

  private boolean isEmptyFile;
  private TsFileResource resource;

  public InnerSpaceCompactionWriter(TsFileResource targetFileResource) throws IOException {
    long sizeForFileWriter =
        (long)
            (SystemInfo.getInstance().getMemorySizeForCompaction()
                / IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread()
                * IoTDBDescriptor.getInstance()
                    .getConfig()
                    .getChunkMetadataSizeProportionInCompaction());
    this.fileWriter = new TsFileIOWriter(targetFileResource.getTsFile(), true, sizeForFileWriter);
    isEmptyFile = true;
    resource = targetFileResource;
  }

  @Override
  public void startChunkGroup(String deviceId, boolean isAlign) throws IOException {
    fileWriter.startChunkGroup(deviceId);
    this.isAlign = isAlign;
    this.deviceId = deviceId;
  }

  @Override
  public void endChunkGroup() throws IOException {
    fileWriter.endChunkGroup();
  }

  @Override
  public void endMeasurement(int subTaskId) throws IOException {
    flushChunkToFileWriter(fileWriter, subTaskId);
  }

  @Override
  public void write(long timestamp, Object value, int subTaskId) throws IOException {
    writeDataPoint(timestamp, value, subTaskId);
    if (measurementPointCountArray[subTaskId] % 10 == 0) {
      checkChunkSizeAndMayOpenANewChunk(fileWriter, subTaskId);
    }
    isEmptyFile = false;
  }

  @Override
  public void write(
      TimeColumn timestamps, Column[] columns, String device, int subTaskId, int batchSize)
      throws IOException {
    AlignedChunkWriterImpl chunkWriter = (AlignedChunkWriterImpl) this.chunkWriters[subTaskId];
    chunkWriter.write(timestamps, columns, batchSize);
    checkChunkSizeAndMayOpenANewChunk(fileWriter, subTaskId);
    synchronized (this) {
      // we need to synchronized here to avoid multi-thread competition in sub-task
      resource.updateStartTime(device, timestamps.getStartTime());
      resource.updateEndTime(device, timestamps.getEndTime());
    }
    isEmptyFile = false;
  }

  @Override
  public void endFile() throws IOException {
    fileWriter.endFile();
    if (isEmptyFile) {
      fileWriter.getFile().delete();
    }
  }

  @Override
  public void close() throws IOException {
    if (fileWriter != null && fileWriter.canWrite()) {
      fileWriter.close();
    }
    fileWriter = null;
  }

  @Override
  public void updateStartTimeAndEndTime(String device, long time, int subTaskId) {
    // we need to synchronized here to avoid multi-thread competition in sub-task
    synchronized (this) {
      resource.updateStartTime(device, time);
      resource.updateEndTime(device, time);
    }
  }

  @Override
  public List<TsFileIOWriter> getFileIOWriter() {
    return Collections.singletonList(fileWriter);
  }
}
