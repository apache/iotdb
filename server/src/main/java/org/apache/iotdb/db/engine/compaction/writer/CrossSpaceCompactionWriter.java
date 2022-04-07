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

import org.apache.iotdb.db.engine.compaction.CompactionMetricsManager;
import org.apache.iotdb.db.engine.compaction.constant.CompactionType;
import org.apache.iotdb.db.engine.compaction.constant.ProcessChunkType;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class CrossSpaceCompactionWriter extends AbstractCompactionWriter {
  // target fileIOWriters
  private List<TsFileIOWriter> fileWriterList = new ArrayList<>();

  // source tsfiles
  private List<TsFileResource> seqTsFileResources;

  // subTaskId -> seqFileIndex
  private final Map<Integer, Integer> seqFileIndexMap;

  private final long[] currentDeviceEndTime;

  private final boolean[] isEmptyFile;

  // private final boolean[] hasTargetFileStartChunkGroup;
  // it has 3 values, which is
  // 0: has not start chunk group yet
  // 1: start flushing chunk group header
  // 2: finish flushing chunk group header
  private final AtomicInteger[] hasTargetFileStartChunkGroup;

  public CrossSpaceCompactionWriter(
      List<TsFileResource> targetResources, List<TsFileResource> seqFileResources)
      throws IOException {
    currentDeviceEndTime = new long[seqFileResources.size()];
    isEmptyFile = new boolean[seqFileResources.size()];
    // hasTargetFileStartChunkGroup = new boolean[seqFileResources.size()];
    hasTargetFileStartChunkGroup = new AtomicInteger[seqFileResources.size()];
    for (int i = 0; i < targetResources.size(); i++) {
      this.fileWriterList.add(new TsFileIOWriter(targetResources.get(i).getTsFile()));
      this.hasTargetFileStartChunkGroup[i] = new AtomicInteger(0);
      isEmptyFile[i] = true;
    }
    this.seqTsFileResources = seqFileResources;
    this.seqFileIndexMap = new ConcurrentHashMap<>();
  }

  @Override
  public void startChunkGroup(String deviceId, boolean isAlign) throws IOException {
    this.deviceId = deviceId;
    this.isAlign = isAlign;
    this.seqFileIndexMap.clear();
    checkIsDeviceExistAndGetDeviceEndTime();
    for (int i = 0; i < seqTsFileResources.size(); i++) {
      // hasTargetFileStartChunkGroup[i] = false;
      hasTargetFileStartChunkGroup[i].set(0);
    }
  }

  @Override
  public void endChunkGroup() throws IOException {
    for (int i = 0; i < seqTsFileResources.size(); i++) {
      if (hasTargetFileStartChunkGroup[i].get() == 2) {
        fileWriterList.get(i).endChunkGroup();
      }
    }
    deviceId = null;
  }

  @Override
  public void endMeasurement(int subTaskId) throws IOException {
    flushChunkToFileWriter(subTaskId);
    // chunkWriterMap.get(subTaskId)=null;
    seqFileIndexMap.put(subTaskId, 0);
  }

  @Override
  public void write(long timestamp, Object value, int subTaskId) throws IOException {
    checkTimeAndMayFlushChunkToCurrentFile(timestamp, subTaskId);
    checkAndMayStartChunkGroup(subTaskId);
    writeDataPoint(timestamp, value, subTaskId);
    checkChunkSizeAndMayOpenANewChunk(
        fileWriterList.get(seqFileIndexMap.get(subTaskId)), subTaskId);
    isEmptyFile[seqFileIndexMap.get(subTaskId)] = false;
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
    int fileIndex = seqFileIndexMap.computeIfAbsent(subTaskId, id -> 0);
    // if timestamp is later than the current source seq tsfile, than flush chunk writer
    while (timestamp > currentDeviceEndTime[fileIndex]) {
      if (fileIndex != seqTsFileResources.size() - 1) {
        flushChunkToFileWriter(subTaskId);
        seqFileIndexMap.put(subTaskId, ++fileIndex);
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
    int fileIndex = seqFileIndexMap.get(subTaskId);
    if (hasTargetFileStartChunkGroup[fileIndex].compareAndSet(0, 1)) {
      fileWriterList.get(fileIndex).startChunkGroup(deviceId);
      hasTargetFileStartChunkGroup[fileIndex].set(2);
    }
  }

  private void flushChunkToFileWriter(int subTaskId) throws IOException {
    writeRateLimit(chunkWriterMap.get(subTaskId).estimateMaxSeriesMemSize());
    while (hasTargetFileStartChunkGroup[seqFileIndexMap.get(subTaskId)].get() == 1) {
      // wait until the target file has finished flushing chunk group header
    }
    synchronized (fileWriterList.get(seqFileIndexMap.get(subTaskId))) {
      chunkWriterMap
          .get(subTaskId)
          .writeToFileWriter(fileWriterList.get(seqFileIndexMap.get(subTaskId)));
    }
  }

  protected void checkChunkSizeAndMayOpenANewChunk(TsFileIOWriter fileWriter, int subTaskId)
      throws IOException {
    if (measurementPointCountMap.get(subTaskId) % 10 == 0 && checkChunkSize(subTaskId)) {
      flushChunkToFileWriter(subTaskId);
      CompactionMetricsManager.recordWriteInfo(
          this instanceof CrossSpaceCompactionWriter
              ? CompactionType.CROSS_COMPACTION
              : CompactionType.INNER_UNSEQ_COMPACTION,
          ProcessChunkType.DESERIALIZE_CHUNK,
          this.isAlign,
          chunkWriterMap.get(subTaskId).estimateMaxSeriesMemSize());
    }
  }
}
