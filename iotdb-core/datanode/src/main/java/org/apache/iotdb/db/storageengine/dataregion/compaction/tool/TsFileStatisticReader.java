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

package org.apache.iotdb.db.storageengine.dataregion.compaction.tool;

import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TsFileDeviceIterator;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class TsFileStatisticReader implements Closeable {

  private final TsFileSequenceReader reader;

  private final List<ChunkGroupStatistics> chunkGroupStatisticsList;

  private final TsFileResource resource;

  public TsFileStatisticReader(String filePath) throws IOException {
    reader = new TsFileSequenceReader(filePath);
    resource = new TsFileResource(new File(filePath));
    resource.deserialize();
    chunkGroupStatisticsList = new ArrayList<>();
  }

  public List<ChunkGroupStatistics> getChunkGroupStatistics() throws IOException {
    TsFileDeviceIterator allDevicesIteratorWithIsAligned =
        reader.getAllDevicesIteratorWithIsAligned();
    while (allDevicesIteratorWithIsAligned.hasNext()) {
      Pair<String, Boolean> deviceWithIsAligned = allDevicesIteratorWithIsAligned.next();
      String deviceId = deviceWithIsAligned.left;
      boolean isAligned = deviceWithIsAligned.right;

      ChunkGroupStatistics chunkGroupStatistics =
          new ChunkGroupStatistics(
              deviceId, isAligned, resource.getStartTime(deviceId), resource.getEndTime(deviceId));
      Iterator<Map<String, List<ChunkMetadata>>> measurementChunkMetadataListMapIterator =
          reader.getMeasurementChunkMetadataListMapIterator(deviceId);

      while (measurementChunkMetadataListMapIterator.hasNext()) {
        Map<String, List<ChunkMetadata>> measurementChunkMetadataListMap =
            measurementChunkMetadataListMapIterator.next();
        for (Map.Entry<String, List<ChunkMetadata>> measurementChunkMetadataList :
            measurementChunkMetadataListMap.entrySet()) {
          List<ChunkMetadata> chunkMetadataList = measurementChunkMetadataList.getValue();
          chunkGroupStatistics.chunkMetadataList.addAll(chunkMetadataList);
          chunkGroupStatistics.totalChunkNum += chunkMetadataList.size();
        }
      }
      chunkGroupStatisticsList.add(chunkGroupStatistics);
    }
    return chunkGroupStatisticsList;
  }

  @Override
  public void close() throws IOException {
    this.reader.close();
  }

  public static class ChunkGroupStatistics {
    private final String deviceID;
    private final List<ChunkMetadata> chunkMetadataList;
    private final boolean isAligned;
    private int totalChunkNum = 0;
    private long startTime;
    private long endTime;

    private ChunkGroupStatistics(String deviceId, boolean isAligned, long startTime, long endTime) {
      this.deviceID = deviceId;
      this.isAligned = isAligned;
      this.chunkMetadataList = new ArrayList<>();
      this.startTime = startTime;
      this.endTime = endTime;
    }

    public String getDeviceID() {
      return deviceID;
    }

    public List<ChunkMetadata> getChunkMetadataList() {
      return chunkMetadataList;
    }

    public int getTotalChunkNum() {
      return totalChunkNum;
    }

    public boolean isAligned() {
      return isAligned;
    }

    public long getStartTime() {
      return startTime;
    }

    public long getEndTime() {
      return endTime;
    }
  }
}
