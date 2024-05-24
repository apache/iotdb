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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.db.queryengine.execution.fragment.QueryContext;
import org.apache.iotdb.db.storageengine.dataregion.modification.Deletion;
import org.apache.iotdb.db.storageengine.dataregion.modification.Modification;
import org.apache.iotdb.db.storageengine.dataregion.read.control.FileReaderManager;
import org.apache.iotdb.db.storageengine.dataregion.read.filescan.IChunkHandle;
import org.apache.iotdb.db.storageengine.dataregion.read.filescan.IFileScanHandle;
import org.apache.iotdb.db.storageengine.dataregion.read.filescan.model.AbstractChunkOffset;
import org.apache.iotdb.db.storageengine.dataregion.read.filescan.model.AbstractDeviceChunkMetaData;
import org.apache.iotdb.db.storageengine.dataregion.read.filescan.model.AlignedDeviceChunkMetaData;
import org.apache.iotdb.db.storageengine.dataregion.read.filescan.model.DeviceChunkMetaData;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.DeviceTimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ITimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.utils.TsFileDeviceStartEndTimeIterator;
import org.apache.iotdb.db.utils.ModificationUtils;

import org.apache.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.TsFileDeviceIterator;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.utils.Pair;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ClosedFileScanHandleImpl implements IFileScanHandle {

  private final TsFileResource tsFileResource;
  private final QueryContext queryContext;
  // Used to cache the modifications of each timeseries
  private final Map<IDeviceID, Map<String, List<TimeRange>>> deviceToModifications;

  public ClosedFileScanHandleImpl(TsFileResource tsFileResource, QueryContext context) {
    this.tsFileResource = tsFileResource;
    this.queryContext = context;
    this.deviceToModifications = new HashMap<>();
  }

  @Override
  public TsFileDeviceStartEndTimeIterator getDeviceStartEndTimeIterator() throws IOException {
    ITimeIndex timeIndex = tsFileResource.getTimeIndex();
    return timeIndex instanceof DeviceTimeIndex
        ? new TsFileDeviceStartEndTimeIterator((DeviceTimeIndex) timeIndex)
        : new TsFileDeviceStartEndTimeIterator(tsFileResource.buildDeviceTimeIndex());
  }

  @Override
  public boolean[] isDeviceTimeDeleted(IDeviceID deviceID, long[] timeArray)
      throws IllegalPathException {
    boolean[] result = new boolean[2];
    List<Modification> modifications = queryContext.getPathModifications(tsFileResource, deviceID);
    List<TimeRange> timeRangeList =
        modifications.stream()
            .filter(Deletion.class::isInstance)
            .map(Deletion.class::cast)
            .map(Deletion::getTimeRange)
            .collect(Collectors.toList());

    int[] deleteCursor = {0};
    for (int i = 0; i < timeArray.length; i++) {
      result[i] = ModificationUtils.isPointDeleted(timeArray[i], timeRangeList, deleteCursor);
    }
    return result;
  }

  private boolean[] calculateBooleanArray(List<TimeRange> timeRangeList, long[] timeArray) {
    boolean[] result = new boolean[timeArray.length];
    int[] deleteCursor = {0};
    for (int i = 0; i < timeArray.length; i++) {
      result[i] = ModificationUtils.isPointDeleted(timeArray[i], timeRangeList, deleteCursor);
    }
    return result;
  }

  @Override
  public boolean[] isTimeSeriesTimeDeleted(
      IDeviceID deviceID, String timeSeriesName, long[] timeArray) throws IllegalPathException {

    if (deviceToModifications.containsKey(deviceID)
        && deviceToModifications.get(deviceID).containsKey(timeSeriesName)) {
      return calculateBooleanArray(
          deviceToModifications.get(deviceID).get(timeSeriesName), timeArray);
    }

    List<Modification> modifications =
        queryContext.getPathModifications(tsFileResource, deviceID, timeSeriesName);
    List<TimeRange> timeRangeList =
        modifications.stream()
            .filter(Deletion.class::isInstance)
            .map(Deletion.class::cast)
            .map(Deletion::getTimeRange)
            .collect(Collectors.toList());
    deviceToModifications
        .computeIfAbsent(deviceID, k -> new HashMap<>())
        .put(timeSeriesName, timeRangeList);
    return calculateBooleanArray(timeRangeList, timeArray);
  }

  @Override
  public Iterator<AbstractDeviceChunkMetaData> getAllDeviceChunkMetaData() throws IOException {

    TsFileSequenceReader tsFileReader = FileReaderManager.getInstance().get(getFilePath(), true);
    TsFileDeviceIterator deviceIterator = tsFileReader.getAllDevicesIteratorWithIsAligned();

    List<AbstractDeviceChunkMetaData> deviceChunkMetaDataList = new LinkedList<>();
    // Traverse each device in current tsFile and get all the relating chunkMetaData
    while (deviceIterator.hasNext()) {
      Pair<IDeviceID, Boolean> deviceIDWithIsAligned = deviceIterator.next();
      Map<String, Pair<List<IChunkMetadata>, Pair<Long, Long>>> metadataForDevice =
          tsFileReader.getTimeseriesMetadataOffsetByDevice(
              deviceIterator.getFirstMeasurementNodeOfCurrentDevice(),
              Collections.emptySet(),
              true);
      if (!deviceIDWithIsAligned.right) {
        // device is not aligned
        deviceChunkMetaDataList.add(
            new DeviceChunkMetaData(
                deviceIDWithIsAligned.left,
                metadataForDevice.values().stream()
                    .flatMap(pair -> pair.getLeft().stream())
                    .collect(Collectors.toList())));
      } else {
        // device is aligned
        List<IChunkMetadata> timeChunkMetaData = metadataForDevice.get("").getLeft();
        List<List<IChunkMetadata>> valueMetaDataList = new ArrayList<>();
        for (Map.Entry<String, Pair<List<IChunkMetadata>, Pair<Long, Long>>> pair :
            metadataForDevice.entrySet()) {
          // Skip timeChunkMetaData
          if (pair.getKey().isEmpty()) {
            continue;
          }
          valueMetaDataList.add(pair.getValue().getLeft());
        }

        List<AlignedChunkMetadata> alignedDeviceChunkMetaData = new ArrayList<>();
        for (int i = 0; i < timeChunkMetaData.size(); i++) {
          alignedDeviceChunkMetaData.add(
              new AlignedChunkMetadata(timeChunkMetaData.get(i), valueMetaDataList.get(i)));
        }
        deviceChunkMetaDataList.add(
            new AlignedDeviceChunkMetaData(deviceIDWithIsAligned.left, alignedDeviceChunkMetaData));
      }
    }
    return deviceChunkMetaDataList.iterator();
  }

  @Override
  public Iterator<IChunkHandle> getChunkHandles(
      List<AbstractChunkOffset> chunkInfoList,
      List<Statistics<? extends Serializable>> statisticsList) {
    String filePath = tsFileResource.getTsFilePath();
    List<IChunkHandle> chunkHandleList = new ArrayList<>();
    for (int i = 0; i < chunkInfoList.size(); i++) {
      AbstractChunkOffset chunkOffset = chunkInfoList.get(i);
      chunkHandleList.add(chunkOffset.generateChunkHandle(filePath, statisticsList.get(i)));
    }
    return chunkHandleList.iterator();
  }

  @Override
  public boolean isClosed() {
    return true;
  }

  @Override
  public boolean isDeleted() {
    return tsFileResource.isDeleted();
  }

  public String getFilePath() {
    return tsFileResource.getTsFilePath();
  }

  @Override
  public TsFileResource getTsResource() {
    return tsFileResource;
  }
}
