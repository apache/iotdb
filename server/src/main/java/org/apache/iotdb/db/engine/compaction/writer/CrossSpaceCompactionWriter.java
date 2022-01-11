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
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CrossSpaceCompactionWriter extends AbstractCompactionWriter {
  // target fileIOWriter
  private List<TsFileIOWriter> fileWriterList = new ArrayList<>();
  // old source tsfile
  private List<TsFileResource> seqTsFileResources;

  private int seqFileIndex;

  public CrossSpaceCompactionWriter(
      List<TsFileResource> targetResources, List<TsFileResource> seqFileResources)
      throws IOException {
    for (TsFileResource resource : targetResources) {
      this.fileWriterList.add(new RestorableTsFileIOWriter(resource.getTsFile()));
    }
    this.seqTsFileResources = seqFileResources;
    seqFileIndex = 0;
  }

  @Override
  public void startChunkGroup(String deviceId, boolean isAlign) throws IOException {
    this.deviceId = deviceId;
    this.isAlign = isAlign;
    for (TsFileResource resource : seqTsFileResources) {
      // Todo:timeFileIndex cannot find device
      if (resource.isDeviceIdExist(deviceId)) {
        fileWriterList.get(seqFileIndex).startChunkGroup(deviceId);
      }
      seqFileIndex++;
    }
    seqFileIndex = 0;
  }

  @Override
  public void endChunkGroup() throws IOException {
    for (TsFileResource resource : seqTsFileResources) {
      if (resource.isDeviceIdExist(deviceId)) {
        fileWriterList.get(seqFileIndex).endChunkGroup();
      }
      seqFileIndex++;
    }
    seqFileIndex = 0;
    deviceId = null;
  }

  @Override
  public void endMeasurement() throws IOException {
    writeRateLimit(chunkWriter.estimateMaxSeriesMemSize());
    chunkWriter.writeToFileWriter(fileWriterList.get(seqFileIndex));
    chunkWriter = null;
    seqFileIndex = 0;
  }

  @Override
  public void write(long timestamp, Object value) throws IOException {
    // if timestamp is later than the current source seq tsfile, than flush chunk writer
    if (IoTDBDescriptor.getInstance().getConfig().getTimeIndexLevel().ordinal() == 1) {
      while (timestamp > seqTsFileResources.get(seqFileIndex).getEndTime(deviceId)) {
        writeRateLimit(chunkWriter.estimateMaxSeriesMemSize());
        chunkWriter.writeToFileWriter(fileWriterList.get(seqFileIndex++));
      }
    } else {
      // Todo : if is timeFileIndex
    }
    if (checkChunkSizeAndMayOpenANewChunk()) {
      writeRateLimit(chunkWriter.estimateMaxSeriesMemSize());
      chunkWriter.writeToFileWriter(fileWriterList.get(seqFileIndex));
      fileWriterList.get(seqFileIndex).endChunkGroup();
      fileWriterList.get(seqFileIndex).startChunkGroup(deviceId);
    }
    writeDataPoint(timestamp, value);
  }

  @Override
  public void write(long[] timestamps, Object values) {}

  @Override
  public void endFile() throws IOException {
    for (TsFileIOWriter targetWriter : fileWriterList) {
      targetWriter.endFile();
    }
  }

  @Override
  public void close() throws IOException {
    for (TsFileIOWriter targetWriter : fileWriterList) {
      if (targetWriter != null && targetWriter.canWrite()) {
        targetWriter.close();
      }
    }
    fileWriterList = null;
    seqTsFileResources = null;
    chunkWriter = null;
  }

  public List<TsFileIOWriter> getFileWriters() {
    return this.fileWriterList;
  }
}
