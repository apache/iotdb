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
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CrossSpaceCompactionWriter extends AbstractCompactionWriter {
  // target fileIOWriter
  private List<TsFileIOWriter> fileWriterList = new ArrayList<>();
  // old source tsfile
  private List<TsFileResource> seqTsFileResources;

  private int seqFileIndex;

  // tsfile -> currenetDeviceEndTime
  private Map<TsFileResource, Long> fileDeviceEndTime = new HashMap<>();

  private long currentFileDeviceEndTime;

  private boolean[] isCurrentDeviceExist;

  private long[] currentDeviceEndTime;

  private TsFileSequenceReader sequenceReader;

  public CrossSpaceCompactionWriter(
      List<TsFileResource> targetResources, List<TsFileResource> seqFileResources)
      throws IOException {
    for (TsFileResource resource : targetResources) {
      this.fileWriterList.add(new RestorableTsFileIOWriter(resource.getTsFile()));
    }
    this.seqTsFileResources = seqFileResources;
    isCurrentDeviceExist = new boolean[seqFileResources.size()];
    currentDeviceEndTime = new long[seqFileResources.size()];
    seqFileIndex = 0;
  }

  @Override
  public void startChunkGroup(String deviceId, boolean isAlign) throws IOException {
    this.deviceId = deviceId;
    this.isAlign = isAlign;
    checkIsDeviceExistAndGetDeviceEndTime();
    for (; seqFileIndex < seqTsFileResources.size(); seqFileIndex++) {
      if (isCurrentDeviceExist[seqFileIndex]) {
        fileWriterList.get(seqFileIndex).startChunkGroup(deviceId);
      }
    }
    seqFileIndex = 0;
  }

  @Override
  public void endChunkGroup() throws IOException {
    for (; seqFileIndex < seqTsFileResources.size(); seqFileIndex++) {
      if (isCurrentDeviceExist[seqFileIndex]) {
        fileWriterList.get(seqFileIndex).endChunkGroup();
      }
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
    updateDeviceEndTimeInCurrentFile();
  }

  @Override
  public void write(long timestamp, Object value) throws IOException {
    checkTimeAndMayFlushChunkToCurrentFile(timestamp);
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
    // sequenceReader.close();
    sequenceReader = null;
    fileWriterList = null;
    seqTsFileResources = null;
    chunkWriter = null;
  }

  public List<TsFileIOWriter> getFileWriters() {
    return this.fileWriterList;
  }

  private void checkTimeAndMayFlushChunkToCurrentFile(long timestamp) throws IOException {
    // if timestamp is later than the current source seq tsfile, than flush chunk writer
    while (timestamp > currentDeviceEndTime[seqFileIndex]) {
      writeRateLimit(chunkWriter.estimateMaxSeriesMemSize());
      chunkWriter.writeToFileWriter(fileWriterList.get(seqFileIndex++));
      updateDeviceEndTimeInCurrentFile();
    }
  }

  private void checkIsDeviceExistInCurrentFile() {}

  private void checkIsDeviceExistAndGetDeviceEndTime() throws IOException {
    while (seqFileIndex < seqTsFileResources.size()) {
      if (seqTsFileResources.get(seqFileIndex).getTimeIndexType() == 1) {
        // the timeIndexType of resource is deviceTimeIndex
        currentDeviceEndTime[seqFileIndex] =
            seqTsFileResources.get(seqFileIndex).getEndTime(deviceId);
        isCurrentDeviceExist[seqFileIndex] =
            seqTsFileResources.get(seqFileIndex).isDeviceIdExist(deviceId);
      } else {
        try (TsFileSequenceReader sequenceReader =
            new TsFileSequenceReader(seqTsFileResources.get(seqFileIndex).getTsFilePath())) {
          long endTime = Long.MIN_VALUE;
          Map<String, TimeseriesMetadata> deviceMetadataMap =
              sequenceReader.readDeviceMetadata(deviceId);
          isCurrentDeviceExist[seqFileIndex] = deviceMetadataMap.size() != 0;
          for (Map.Entry<String, TimeseriesMetadata> entry : deviceMetadataMap.entrySet()) {
            long tmpStartTime = entry.getValue().getStatistics().getStartTime();
            long tmpEndTime = entry.getValue().getStatistics().getEndTime();
            if (tmpEndTime >= tmpStartTime && endTime < tmpEndTime) {
              endTime = tmpEndTime;
            }
          }
          currentDeviceEndTime[seqFileIndex] = endTime;
        }
      }
      seqFileIndex++;
    }
    // reset
    seqFileIndex = 0;
  }

  private void updateDeviceEndTimeInCurrentFile() throws IOException {
    if (seqTsFileResources.get(seqFileIndex).getTimeIndexType() == 1) {
      currentFileDeviceEndTime = seqTsFileResources.get(seqFileIndex).getEndTime(deviceId);
    } else {
      try (TsFileSequenceReader sequenceReader =
          new TsFileSequenceReader(seqTsFileResources.get(seqFileIndex).getTsFilePath())) {
        long endTime = Long.MIN_VALUE;
        for (Map.Entry<String, TimeseriesMetadata> entry :
            sequenceReader.readDeviceMetadata(deviceId).entrySet()) {
          long tmpStartTime = entry.getValue().getStatistics().getStartTime();
          long tmpEndTime = entry.getValue().getStatistics().getEndTime();
          if (tmpEndTime >= tmpStartTime && endTime < tmpEndTime) {
            endTime = tmpEndTime;
          }
        }
        currentFileDeviceEndTime = endTime;
      }
    }
  }

  private void getNextSequenceReader() throws IOException {
    if (sequenceReader != null) {
      // close last sequenceReader
      sequenceReader.close();
    }
    sequenceReader = new TsFileSequenceReader(seqTsFileResources.get(seqFileIndex).getTsFilePath());
  }
}
