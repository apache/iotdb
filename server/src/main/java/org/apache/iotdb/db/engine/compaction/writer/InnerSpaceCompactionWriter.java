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
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class InnerSpaceCompactionWriter extends AbstractCompactionWriter {
  private TsFileIOWriter fileWriter;

  private boolean isEmptyFile;
  private TsFileResource resource;
  private AtomicLong[] startTimeOfCurDevice;
  private AtomicLong[] endTimeOfCurDevice;

  public InnerSpaceCompactionWriter(TsFileResource targetFileResource) throws IOException {
    long sizeForFileWriter =
        (long)
            (SystemInfo.getInstance().getMemorySizeForCompaction()
                / IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread()
                * IoTDBDescriptor.getInstance().getConfig().getChunkMetadataMemorySizeProportion());
    this.fileWriter = new TsFileIOWriter(targetFileResource.getTsFile(), true, sizeForFileWriter);
    isEmptyFile = true;
    resource = targetFileResource;
    int concurrentThreadNum =
        Math.max(1, IoTDBDescriptor.getInstance().getConfig().getSubCompactionTaskNum());
    startTimeOfCurDevice = new AtomicLong[concurrentThreadNum];
    endTimeOfCurDevice = new AtomicLong[concurrentThreadNum];
    for (int i = 0; i < concurrentThreadNum; ++i) {
      startTimeOfCurDevice[i] = new AtomicLong(Long.MAX_VALUE);
      endTimeOfCurDevice[i] = new AtomicLong(Long.MIN_VALUE);
    }
  }

  @Override
  public void startChunkGroup(String deviceId, boolean isAlign) throws IOException {
    fileWriter.startChunkGroup(deviceId);
    this.isAlign = isAlign;
    this.deviceId = deviceId;
  }

  @Override
  public void endChunkGroup() throws IOException {
    for (int i = 0; i < startTimeOfCurDevice.length; ++i) {
      resource.updateStartTime(
          fileWriter.getCurrentChunkGroupDeviceId(), startTimeOfCurDevice[i].get());
      resource.updateEndTime(
          fileWriter.getCurrentChunkGroupDeviceId(), endTimeOfCurDevice[i].get());
      startTimeOfCurDevice[i].set(Long.MAX_VALUE);
      endTimeOfCurDevice[i].set(Long.MIN_VALUE);
    }
    fileWriter.endChunkGroup();
  }

  @Override
  public void endMeasurement(int subTaskId) throws IOException {
    flushChunkToFileWriter(fileWriter, subTaskId);
  }

  @Override
  public void write(long timestamp, Object value, int subTaskId) throws IOException {
    writeDataPoint(timestamp, value, subTaskId);
    checkChunkSizeAndMayOpenANewChunk(fileWriter, subTaskId);
    isEmptyFile = false;
    startTimeOfCurDevice[subTaskId].set(Math.min(startTimeOfCurDevice[subTaskId].get(), timestamp));
    endTimeOfCurDevice[subTaskId].set(Math.max(endTimeOfCurDevice[subTaskId].get(), timestamp));
  }

  @Override
  public void write(long[] timestamps, Object values) {}

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
  public List<TsFileIOWriter> getFileIOWriter() {
    return Collections.singletonList(fileWriter);
  }
}
