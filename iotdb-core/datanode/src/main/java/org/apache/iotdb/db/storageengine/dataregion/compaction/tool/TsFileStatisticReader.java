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

import org.apache.iotdb.db.utils.EncryptDBUtils;

import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.TsFileDeviceIterator;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.utils.Pair;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class TsFileStatisticReader implements Closeable {

  private final TsFileSequenceReader reader;

  public TsFileStatisticReader(String filePath) throws IOException {
    reader =
        new TsFileSequenceReader(
            filePath, EncryptDBUtils.getFirstEncryptParamFromTSFilePath(filePath));
  }

  public List<ChunkGroupStatistics> getChunkGroupStatisticsList() throws IOException {
    TsFileDeviceIterator allDevicesIteratorWithIsAligned =
        reader.getAllDevicesIteratorWithIsAligned();
    List<ChunkGroupStatistics> chunkGroupStatisticsList = new ArrayList<>();
    while (allDevicesIteratorWithIsAligned.hasNext()) {
      Pair<IDeviceID, Boolean> deviceWithIsAligned = allDevicesIteratorWithIsAligned.next();
      IDeviceID deviceId = deviceWithIsAligned.left;

      ChunkGroupStatistics chunkGroupStatistics = new ChunkGroupStatistics(deviceId);
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
    private final IDeviceID deviceID;
    private final List<ChunkMetadata> chunkMetadataList;
    private int totalChunkNum = 0;

    private ChunkGroupStatistics(IDeviceID deviceId) {
      this.deviceID = deviceId;
      this.chunkMetadataList = new ArrayList<>();
    }

    public IDeviceID getDeviceID() {
      return deviceID;
    }

    public List<ChunkMetadata> getChunkMetadataList() {
      return chunkMetadataList;
    }

    public int getTotalChunkNum() {
      return totalChunkNum;
    }
  }
}
