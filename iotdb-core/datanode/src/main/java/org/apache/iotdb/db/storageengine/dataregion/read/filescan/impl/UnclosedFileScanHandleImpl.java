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
import org.apache.iotdb.db.storageengine.dataregion.read.filescan.model.AbstractChunkOffset;
import org.apache.iotdb.db.storageengine.dataregion.read.filescan.model.AbstractDeviceChunkMetaData;
import org.apache.iotdb.db.storageengine.dataregion.read.filescan.model.AlignedDeviceChunkMetaData;
import org.apache.iotdb.db.storageengine.dataregion.read.filescan.model.DeviceChunkMetaData;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ArrayDeviceTimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ITimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.utils.TsFileDeviceStartEndTimeIterator;

import org.apache.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.TimeRange;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class UnclosedFileScanHandleImpl implements IFileScanHandle {

  private final TsFileResource tsFileResource;
  private final Map<IDeviceID, Map<String, List<IChunkMetadata>>> deviceToChunkMetadataMap;
  private final Map<IDeviceID, Map<String, List<IChunkHandle>>> deviceToMemChunkHandleMap;

  public UnclosedFileScanHandleImpl(
      Map<IDeviceID, Map<String, List<IChunkMetadata>>> deviceToChunkMetadataMap,
      Map<IDeviceID, Map<String, List<IChunkHandle>>> deviceToMemChunkHandleMap,
      TsFileResource tsFileResource) {
    this.deviceToChunkMetadataMap = deviceToChunkMetadataMap;
    this.deviceToMemChunkHandleMap = deviceToMemChunkHandleMap;
    this.tsFileResource = tsFileResource;
  }

  @Override
  public TsFileDeviceStartEndTimeIterator getDeviceStartEndTimeIterator() throws IOException {
    ITimeIndex timeIndex = tsFileResource.getTimeIndex();
    return timeIndex instanceof ArrayDeviceTimeIndex
        ? new TsFileDeviceStartEndTimeIterator((ArrayDeviceTimeIndex) timeIndex)
        : new TsFileDeviceStartEndTimeIterator(tsFileResource.buildDeviceTimeIndex());
  }

  @Override
  public boolean isDeviceTimeDeleted(IDeviceID deviceID, long timeArray) {
    Map<String, List<IChunkMetadata>> chunkMetadataMap = deviceToChunkMetadataMap.get(deviceID);
    for (List<IChunkMetadata> chunkMetadataList : chunkMetadataMap.values()) {
      for (IChunkMetadata chunkMetadata : chunkMetadataList) {
        if (chunkMetadata.getDeleteIntervalList() != null) {
          for (TimeRange deleteInterval : chunkMetadata.getDeleteIntervalList()) {
            if (deleteInterval.contains(timeArray)) {
              return true;
            }
          }
        }
      }
    }
    return false;
  }

  @Override
  public Iterator<AbstractDeviceChunkMetaData> getAllDeviceChunkMetaData() throws IOException {
    List<AbstractDeviceChunkMetaData> deviceChunkMetaDataList = new ArrayList<>();
    for (Map.Entry<IDeviceID, Map<String, List<IChunkMetadata>>> entry :
        deviceToChunkMetadataMap.entrySet()) {
      IDeviceID deviceID = entry.getKey();
      Map<String, List<IChunkMetadata>> chunkMetadataList = entry.getValue();
      if (chunkMetadataList.isEmpty()) {
        continue;
      }

      boolean isAligned = chunkMetadataList.containsKey("");
      if (isAligned) {
        List<AlignedChunkMetadata> alignedChunkMetadataList = new ArrayList<>();
        List<IChunkMetadata> timeChunkMetadataList = chunkMetadataList.get("");
        List<List<IChunkMetadata>> valueChunkMetadataList =
            new ArrayList<>(chunkMetadataList.values());
        for (int i = 0; i < timeChunkMetadataList.size(); i++) {
          alignedChunkMetadataList.add(
              new AlignedChunkMetadata(
                  timeChunkMetadataList.get(i), valueChunkMetadataList.get(i)));
        }
        deviceChunkMetaDataList.add(
            new AlignedDeviceChunkMetaData(deviceID, alignedChunkMetadataList));
      } else {
        for (Map.Entry<String, List<IChunkMetadata>> measurementMetaData :
            chunkMetadataList.entrySet()) {
          deviceChunkMetaDataList.add(
              new DeviceChunkMetaData(deviceID, measurementMetaData.getValue()));
        }
      }
    }
    return deviceChunkMetaDataList.iterator();
  }

  @Override
  public boolean isTimeSeriesTimeDeleted(
      IDeviceID deviceID, String timeSeriesName, long timestamp) {
    List<IChunkMetadata> chunkMetadataList =
        deviceToChunkMetadataMap.get(deviceID).get(timeSeriesName);
    // check if timestamp is deleted by deleteInterval
    for (IChunkMetadata chunkMetadata : chunkMetadataList) {
      if (chunkMetadata.getDeleteIntervalList() != null) {
        for (TimeRange deleteInterval : chunkMetadata.getDeleteIntervalList()) {
          if (deleteInterval.contains(timestamp)) {
            return true;
          }
        }
      }
    }
    return false;
  }

  @Override
  public Iterator<IChunkHandle> getChunkHandles(
      List<AbstractChunkOffset> chunkInfoList,
      List<Statistics<? extends Serializable>> statisticsList,
      List<Integer> orderedIndexList) {
    List<IChunkHandle> chunkHandleList = new ArrayList<>();
    for (int index : orderedIndexList) {
      AbstractChunkOffset chunkOffsetInfo = chunkInfoList.get(index);
      List<IChunkHandle> chunkHandle =
          deviceToMemChunkHandleMap
              .get(chunkOffsetInfo.getDeviceID())
              .get(chunkOffsetInfo.getMeasurement());
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
  public TsFileResource getTsResource() {
    return tsFileResource;
  }
}
