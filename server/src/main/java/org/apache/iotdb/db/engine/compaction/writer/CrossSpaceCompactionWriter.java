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

import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class CrossSpaceCompactionWriter extends AbstractCompactionWriter {
  // target fileIOWriters
  private List<TsFileIOWriter> fileWriterList = new ArrayList<>();

  // source tsfiles
  private List<TsFileResource> seqTsFileResources;

  // each sub task has its corresponding seqFileIndex
  private final AtomicInteger[] seqFileIndexArray = new AtomicInteger[subTaskNum];

  private final long[] currentDeviceEndTime;

  private final boolean[] isEmptyFile;

  // private final boolean[] hasTargetFileStartChunkGroup;
  private final AtomicBoolean[] hasTargetFileStartChunkGroup;

  public CrossSpaceCompactionWriter(
      List<TsFileResource> targetResources, List<TsFileResource> seqFileResources)
      throws IOException {
    currentDeviceEndTime = new long[seqFileResources.size()];
    isEmptyFile = new boolean[seqFileResources.size()];
    // hasTargetFileStartChunkGroup = new boolean[seqFileResources.size()];
    hasTargetFileStartChunkGroup = new AtomicBoolean[seqFileResources.size()];
    for (int i = 0; i < targetResources.size(); i++) {
      this.fileWriterList.add(new TsFileIOWriter(targetResources.get(i).getTsFile()));
      this.hasTargetFileStartChunkGroup[i] = new AtomicBoolean(false);
      isEmptyFile[i] = true;
    }
    for (int i = 0; i < seqFileIndexArray.length; i++) {
      seqFileIndexArray[i] = new AtomicInteger(0);
    }
    this.seqTsFileResources = seqFileResources;
  }

  @Override
  public void startChunkGroup(String deviceId, boolean isAlign) throws IOException {
    this.deviceId = deviceId;
    this.isAlign = isAlign;
    checkIsDeviceExistAndGetDeviceEndTime();
    for (int i = 0; i < seqTsFileResources.size(); i++) {
      // hasTargetFileStartChunkGroup[i] = false;
      hasTargetFileStartChunkGroup[i].set(false);
    }
  }

  @Override
  public void endChunkGroup() throws IOException {
    for (int i = 0; i < seqTsFileResources.size(); i++) {
      if (hasTargetFileStartChunkGroup[i].get()) {
        fileWriterList.get(i).endChunkGroup();
      }
    }
    deviceId = null;
  }

  @Override
  public void endMeasurement(int subTaskId) throws IOException {
    writeRateLimit(chunkWriterMap.get(subTaskId).estimateMaxSeriesMemSize());
    synchronized (fileWriterList.get(seqFileIndexArray[subTaskId].get())) {
      chunkWriterMap
          .get(subTaskId)
          .writeToFileWriter(fileWriterList.get(seqFileIndexArray[subTaskId].get()));
    }
    seqFileIndexArray[subTaskId].set(0);
  }

  @Override
  public void write(long timestamp, Object value, int subTaskId) throws IOException {
    checkTimeAndMayFlushChunkToCurrentFile(timestamp, subTaskId);
    checkAndMayStartChunkGroup(subTaskId);
    writeDataPoint(timestamp, value, subTaskId);
    checkChunkSizeAndMayOpenANewChunk(
        fileWriterList.get(seqFileIndexArray[subTaskId].get()), subTaskId);
    isEmptyFile[seqFileIndexArray[subTaskId].get()] = false;
  }

  @Override
  public void write(long[] timestamps, Object values) {}

  @Override
  public void endFile() throws IOException {
    for (int i = 0; i < isEmptyFile.length; i++) {
      fileWriterList.get(i).endFile();
      // delete empty target file
      if (isEmptyFile[i]) {
        fileWriterList.get(i).getFile().delete();
      }
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
    chunkWriterMap.clear();
  }

  @Override
  public List<TsFileIOWriter> getFileIOWriter() {
    return fileWriterList;
  }

  private void checkTimeAndMayFlushChunkToCurrentFile(long timestamp, int subTaskId)
      throws IOException {
    // if timestamp is later than the current source seq tsfile, than flush chunk writer
    while (timestamp > currentDeviceEndTime[seqFileIndexArray[subTaskId].get()]) {
      if (seqFileIndexArray[subTaskId].get() != seqTsFileResources.size() - 1) {
        writeRateLimit(chunkWriterMap.get(subTaskId).estimateMaxSeriesMemSize());
        synchronized (fileWriterList.get(seqFileIndexArray[subTaskId].get())) {
          chunkWriterMap
              .get(subTaskId)
              .writeToFileWriter(fileWriterList.get(seqFileIndexArray[subTaskId].get()));
        }
        seqFileIndexArray[subTaskId].getAndAdd(1);
      } else {
        // If the seq file is deleted for various reasons, the following two situations may occur
        // when selecting the source files: (1) unseq files may have some devices or measurements
        // which are not exist in seq files. (2) timestamp of one timeseries in unseq files may
        // later than any seq files. Then write these data into the last target file.
        return;
      }
    }
  }

  private void checkIsDeviceExistAndGetDeviceEndTime() throws IOException {
    int fileIndex = 0;
    while (fileIndex < seqTsFileResources.size()) {
      if (seqTsFileResources.get(fileIndex).getTimeIndexType() == 1) {
        // the timeIndexType of resource is deviceTimeIndex
        currentDeviceEndTime[fileIndex] = seqTsFileResources.get(fileIndex).getEndTime(deviceId);
      } else {
        long endTime = Long.MIN_VALUE;
        Map<String, TimeseriesMetadata> deviceMetadataMap =
            FileReaderManager.getInstance()
                .get(seqTsFileResources.get(fileIndex).getTsFilePath(), true)
                .readDeviceMetadata(deviceId);
        for (Map.Entry<String, TimeseriesMetadata> entry : deviceMetadataMap.entrySet()) {
          long tmpStartTime = entry.getValue().getStatistics().getStartTime();
          long tmpEndTime = entry.getValue().getStatistics().getEndTime();
          if (tmpEndTime >= tmpStartTime && endTime < tmpEndTime) {
            endTime = tmpEndTime;
          }
        }
        currentDeviceEndTime[fileIndex] = endTime;
      }

      fileIndex++;
    }
  }

  private void checkAndMayStartChunkGroup(int subTaskId) throws IOException {
    int fileIndex = seqFileIndexArray[subTaskId].get();
    if (hasTargetFileStartChunkGroup[fileIndex].compareAndSet(false, true)) {
      fileWriterList.get(fileIndex).startChunkGroup(deviceId);
    }
  }
}
