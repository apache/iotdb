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

package org.apache.iotdb.db.storageengine.dataregion.read.filescan.impl;

import org.apache.iotdb.db.storageengine.dataregion.read.filescan.IChunkHandle;
import org.apache.iotdb.db.storageengine.dataregion.read.filescan.IFileScanHandle;
import org.apache.iotdb.db.storageengine.dataregion.read.filescan.model.AbstractDeviceChunkMetaData;
import org.apache.iotdb.db.storageengine.dataregion.read.filescan.model.AlignedDeviceChunkMetaData;
import org.apache.iotdb.db.storageengine.dataregion.read.filescan.model.ChunkOffsetInfo;
import org.apache.iotdb.db.storageengine.dataregion.read.filescan.model.DeviceChunkMetaData;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.DeviceTimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ITimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.utils.TsFileDeviceStartEndTimeIterator;

import org.apache.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.statistics.Statistics;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class UnclosedFileScanHandleImpl implements IFileScanHandle {

  private final TsFileResource tsFileResource;
  private final Map<IDeviceID, List<IChunkMetadata>> deviceToChunkMetadataMap;
  private final Map<IDeviceID, Map<String, List<MemChunkHandleImpl>>> deviceToMemChunkHandleMap;

  public UnclosedFileScanHandleImpl(
      Map<IDeviceID, List<IChunkMetadata>> deviceToChunkMetadataMap,
      Map<IDeviceID, Map<String, List<MemChunkHandleImpl>>> deviceToMemChunkHandleMap,
      TsFileResource tsFileResource) {
    this.deviceToChunkMetadataMap = deviceToChunkMetadataMap;
    this.deviceToMemChunkHandleMap = deviceToMemChunkHandleMap;
    this.tsFileResource = tsFileResource;
  }

  @Override
  public TsFileDeviceStartEndTimeIterator getDeviceStartEndTimeIterator() throws IOException {
    ITimeIndex timeIndex = tsFileResource.getTimeIndex();
    return timeIndex instanceof DeviceTimeIndex
        ? new TsFileDeviceStartEndTimeIterator((DeviceTimeIndex) timeIndex)
        : new TsFileDeviceStartEndTimeIterator(tsFileResource.buildDeviceTimeIndex());
  }

  @Override
  public boolean isDeviceTimeDeleted(IDeviceID deviceID, long timestamp) {
    return false;
  }

  @Override
  public Iterator<AbstractDeviceChunkMetaData> getAllDeviceChunkMetaData() throws IOException {
    List<AbstractDeviceChunkMetaData> deviceChunkMetaDataList = new ArrayList<>();
    for (Map.Entry<IDeviceID, List<IChunkMetadata>> entry : deviceToChunkMetadataMap.entrySet()) {
      IDeviceID deviceID = entry.getKey();
      List<IChunkMetadata> chunkMetadataList = entry.getValue();
      if (chunkMetadataList.isEmpty()) {
        continue;
      }
      boolean isAligned = chunkMetadataList.get(0) instanceof AlignedChunkMetadata;
      if (isAligned) {
        List<AlignedChunkMetadata> alignedChunkMetadataList = new ArrayList<>();
        for (IChunkMetadata chunkMetadata : chunkMetadataList) {
          alignedChunkMetadataList.add((AlignedChunkMetadata) chunkMetadata);
        }
        deviceChunkMetaDataList.add(
            new AlignedDeviceChunkMetaData(deviceID, alignedChunkMetadataList));
      } else {
        deviceChunkMetaDataList.add(new DeviceChunkMetaData(deviceID, chunkMetadataList));
      }
    }
    return deviceChunkMetaDataList.iterator();
  }

  @Override
  public boolean isTimeSeriesTimeDeleted(String timeSeriesName, long timestamp) {
    return false;
  }

  @Override
  public Iterator<IChunkHandle> getChunkHandles(
      List<ChunkOffsetInfo> chunkInfoList, List<Statistics<? extends Serializable>> statisticsList)
      throws IOException {
    List<IChunkHandle> chunkHandleList = new ArrayList<>();
    for (ChunkOffsetInfo chunkOffsetInfo : chunkInfoList) {
      List<MemChunkHandleImpl> chunkHandle =
          deviceToMemChunkHandleMap
              .get(chunkOffsetInfo.getDevicePath())
              .get(chunkOffsetInfo.getMeasurementPath());
      chunkHandleList.addAll(chunkHandle);
    }
    return chunkHandleList.iterator();
  }

  @Override
  public boolean isClosed() {
    return false;
  }

  @Override
  public boolean isDeleted() {
    return tsFileResource.isDeleted();
  }

  @Override
  public String getFilePath() {
    return tsFileResource.getTsFilePath();
  }

  @Override
  public TsFileResource getTsResource() {
    return tsFileResource;
  }
}
