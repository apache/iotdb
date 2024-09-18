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

package org.apache.iotdb.db.queryengine.execution.operator.source;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.db.queryengine.common.TimeseriesContext;
import org.apache.iotdb.db.storageengine.dataregion.read.filescan.model.AbstractChunkOffset;
import org.apache.iotdb.db.storageengine.dataregion.read.filescan.model.AbstractDeviceChunkMetaData;
import org.apache.iotdb.db.storageengine.dataregion.read.filescan.model.DeviceStartEndTime;
import org.apache.iotdb.db.storageengine.dataregion.utils.TsFileDeviceStartEndTimeIterator;

import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RegionScanForActiveTimeSeriesUtil extends AbstractRegionScanForActiveDataUtil {

  private final Map<IDeviceID, Set<String>> timeSeriesForCurrentTsFile;
  private final Map<IDeviceID, List<String>> activeTimeSeries;
  private final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(Map.class)
          + RamUsageEstimator.shallowSizeOfInstance(Map.class);

  public RegionScanForActiveTimeSeriesUtil(Filter timeFilter, Map<IDeviceID, Long> ttlCache) {
    super(timeFilter, ttlCache);
    this.timeSeriesForCurrentTsFile = new HashMap<>();
    this.activeTimeSeries = new HashMap<>();
  }

  public boolean nextTsFileHandle(Map<IDeviceID, Map<String, TimeseriesContext>> targetTimeseries)
      throws IOException {
    if (!queryDataSource.hasNext()) {
      // There is no more TsFileHandles to be scanned.
      return false;
    }

    curFileScanHandle = queryDataSource.next();
    deviceChunkMetaDataIterator = null;

    // Init timeSeries for current tsFileHandle
    TsFileDeviceStartEndTimeIterator iterator = curFileScanHandle.getDeviceStartEndTimeIterator();
    while (iterator.hasNext()) {
      DeviceStartEndTime deviceStartEndTime = iterator.next();
      IDeviceID deviceID = deviceStartEndTime.getDevicePath();
      long startTime = deviceStartEndTime.getStartTime();
      long endTime = deviceStartEndTime.getEndTime();
      if (!targetTimeseries.containsKey(deviceID)
          || (endTime >= 0 && !timeFilter.satisfyStartEndTime(startTime, endTime, deviceID))) {
        continue;
      }

      timeSeriesForCurrentTsFile.put(
          deviceID, new HashSet<>(targetTimeseries.get(deviceID).keySet()));
    }
    return true;
  }

  @Override
  public boolean isCurrentTsFileFinished() {
    return timeSeriesForCurrentTsFile.isEmpty();
  }

  @Override
  public void processDeviceChunkMetadata(AbstractDeviceChunkMetaData deviceChunkMetaData)
      throws IllegalPathException {
    if (timeSeriesForCurrentTsFile.containsKey(deviceChunkMetaData.getDevicePath())) {
      checkChunkMetaDataOfTimeSeries(deviceChunkMetaData.getDevicePath(), deviceChunkMetaData);
    }
  }

  @Override
  public boolean isCurrentChunkHandleValid() {
    IDeviceID deviceID = currentChunkHandle.getDeviceID();
    String measurementId = currentChunkHandle.getMeasurement();
    Set<String> measurements = timeSeriesForCurrentTsFile.get(deviceID);
    return measurements != null && measurements.contains(measurementId);
  }

  @Override
  public void processActiveChunk(IDeviceID deviceID, String measurementPath) {
    removeTimeSeriesForCurrentTsFile(deviceID, measurementPath);
    activeTimeSeries.computeIfAbsent(deviceID, k -> new ArrayList<>()).add(measurementPath);
    currentChunkHandle = null;
  }

  @Override
  public void finishCurrentFile() {
    super.finishCurrentFile();
    queryDataSource.releaseFileScanHandle();
    timeSeriesForCurrentTsFile.clear();
    activeTimeSeries.clear();
  }

  private void checkChunkMetaDataOfTimeSeries(
      IDeviceID deviceID, AbstractDeviceChunkMetaData deviceChunkMetaData)
      throws IllegalPathException {
    List<AbstractChunkOffset> chunkOffsetsForCurrentDevice = new ArrayList<>();
    List<Statistics<? extends Serializable>> chunkStatisticsForCurrentDevice = new ArrayList<>();
    while (deviceChunkMetaData.hasNextValueChunkMetadata()) {
      IChunkMetadata valueChunkMetaData = deviceChunkMetaData.nextValueChunkMetadata();
      String measurementId = valueChunkMetaData.getMeasurementUid();
      long startTime = valueChunkMetaData.getStartTime();
      long endTime = valueChunkMetaData.getEndTime();
      Set<String> measurementForCurrentTsFile = timeSeriesForCurrentTsFile.get(deviceID);
      if (!(measurementForCurrentTsFile != null
              && measurementForCurrentTsFile.contains(measurementId))
          || !timeFilter.satisfyStartEndTime(startTime, endTime, deviceID)) {
        continue;
      }

      if ((timeFilter.satisfy(startTime, deviceID)
              && !curFileScanHandle.isTimeSeriesTimeDeleted(deviceID, measurementId, startTime))
          || (timeFilter.satisfy(endTime, deviceID)
              && !curFileScanHandle.isTimeSeriesTimeDeleted(deviceID, measurementId, endTime))) {
        removeTimeSeriesForCurrentTsFile(deviceID, measurementId);
        activeTimeSeries.computeIfAbsent(deviceID, k -> new ArrayList<>()).add(measurementId);
      } else {
        chunkOffsetsForCurrentDevice.add(deviceChunkMetaData.getChunkOffset());
        chunkStatisticsForCurrentDevice.add(valueChunkMetaData.getStatistics());
      }
    }
    chunkToBeScanned.addAll(chunkOffsetsForCurrentDevice);
    chunkStatistics.addAll(chunkStatisticsForCurrentDevice);
  }

  public Map<IDeviceID, List<String>> getActiveTimeSeries() {
    return activeTimeSeries;
  }

  private void removeTimeSeriesForCurrentTsFile(IDeviceID deviceID, String measurementPath) {
    Set<String> measurements = timeSeriesForCurrentTsFile.get(deviceID);
    if (measurements != null) {
      measurements.remove(measurementPath);
      if (measurements.isEmpty()) {
        timeSeriesForCurrentTsFile.remove(deviceID);
        timeFilter.removeTTLCache(deviceID);
      }
    }
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE + super.ramBytesUsed();
  }
}
