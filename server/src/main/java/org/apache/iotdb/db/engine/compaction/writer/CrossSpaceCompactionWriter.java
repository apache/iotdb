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
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CrossSpaceCompactionWriter extends AbstractCompactionWriter {
  // target fileIOWriters
  private List<TsFileIOWriter> fileWriterList = new ArrayList<>();

  // source tsfiles
  private List<TsFileResource> seqTsFileResources;

  private int seqFileIndex;

  // indicate is current device exist in each source file.
  private final boolean[] isCurrentDeviceExist;

  private final long[] currentDeviceEndTime;

  private final boolean[] isEmptyFile;

  private final List<TsFileResource> targetTsFileResources;

  public CrossSpaceCompactionWriter(
      List<TsFileResource> targetResources, List<TsFileResource> seqFileResources)
      throws IOException {
    isCurrentDeviceExist = new boolean[seqFileResources.size()];
    currentDeviceEndTime = new long[seqFileResources.size()];
    isEmptyFile = new boolean[seqFileResources.size()];
    for (int i = 0; i < targetResources.size(); i++) {
      this.fileWriterList.add(new RestorableTsFileIOWriter(targetResources.get(i).getTsFile()));
      isEmptyFile[i] = true;
    }
    this.seqTsFileResources = seqFileResources;
    this.targetTsFileResources = targetResources;
    seqFileIndex = 0;
  }

  @Override
  public void startChunkGroup(String deviceId, boolean isAlign) throws IOException {
    this.deviceId = deviceId;
    this.isAlign = isAlign;
    this.seqFileIndex = 0;
    checkIsDeviceExistAndGetDeviceEndTime();
    boolean isCurrentDeviceExistInAtLeastOneFile = false;
    for (int i = 0; i < seqTsFileResources.size(); i++) {
      if (isCurrentDeviceExist[i]) {
        fileWriterList.get(i).startChunkGroup(deviceId);
        isCurrentDeviceExistInAtLeastOneFile = true;
      } else if (!isCurrentDeviceExistInAtLeastOneFile && i == seqTsFileResources.size() - 1) {
        // Due to various factor, unseq files may have the device which is not exist in the seq
        // file, than write the data of this device into the last target file.
        fileWriterList.get(i).startChunkGroup(deviceId);
      }
    }
  }

  @Override
  public void endChunkGroup() throws IOException {
    for (int i = 0; i < seqTsFileResources.size(); i++) {
      if (isCurrentDeviceExist[i]) {
        fileWriterList.get(i).endChunkGroup();
      }
    }
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
    checkTimeAndMayFlushChunkToCurrentFile(timestamp);
    updateDeviceStartAndEndTime(targetTsFileResources.get(seqFileIndex), timestamp);
    checkChunkSizeAndMayOpenANewChunk(fileWriterList.get(seqFileIndex));
    writeDataPoint(timestamp, value);
    isEmptyFile[seqFileIndex] = false;
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
    chunkWriter = null;
  }

  private void checkTimeAndMayFlushChunkToCurrentFile(long timestamp) throws IOException {
    // if timestamp is later than the current source seq tsfile, than flush chunk writer
    while (timestamp > currentDeviceEndTime[seqFileIndex]) {
      writeRateLimit(chunkWriter.estimateMaxSeriesMemSize());
      chunkWriter.writeToFileWriter(fileWriterList.get(seqFileIndex));

      // If the seq file is deleted for various reasons, the following two situations may occur when
      // selecting the source files: (1) unseq files may have some devices or measurements which are
      // not exist in seq files. (2) timestamp of one timeseries in unseq files may later than any
      // seq files. Then write these data into the last target file.
      if (seqFileIndex == seqTsFileResources.size() - 1) {
        return;
      }
      seqFileIndex++;
    }
  }

  private void checkIsDeviceExistAndGetDeviceEndTime() throws IOException {
    int fileIndex = 0;
    while (fileIndex < seqTsFileResources.size()) {
      if (seqTsFileResources.get(fileIndex).getTimeIndexType() == 1) {
        // the timeIndexType of resource is deviceTimeIndex
        currentDeviceEndTime[fileIndex] = seqTsFileResources.get(fileIndex).getEndTime(deviceId);
        isCurrentDeviceExist[fileIndex] =
            seqTsFileResources.get(fileIndex).isDeviceIdExist(deviceId);
      } else {
        long endTime = Long.MIN_VALUE;
        Map<String, TimeseriesMetadata> deviceMetadataMap =
            FileReaderManager.getInstance()
                .get(seqTsFileResources.get(fileIndex).getTsFilePath(), true)
                .readDeviceMetadata(deviceId);
        isCurrentDeviceExist[fileIndex] = deviceMetadataMap.size() != 0;
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
}
