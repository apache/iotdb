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
import org.apache.iotdb.db.queryengine.common.DeviceContext;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RegionScanForActiveDeviceUtil extends AbstractRegionScanForActiveDataUtil {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(RegionScanForActiveDeviceUtil.class)
          + RamUsageEstimator.shallowSizeOfInstance(Set.class)
          + RamUsageEstimator.shallowSizeOfInstance(List.class);

  private final Set<IDeviceID> deviceSetForCurrentTsFile;
  private final List<IDeviceID> activeDevices;

  public RegionScanForActiveDeviceUtil(Filter timeFilter, Map<IDeviceID, Long> ttlCache) {
    super(timeFilter, ttlCache);
    this.deviceSetForCurrentTsFile = new HashSet<>();
    this.activeDevices = new ArrayList<>();
  }

  @Override
  public boolean isCurrentTsFileFinished() {
    return deviceSetForCurrentTsFile.isEmpty();
  }

  public boolean nextTsFileHandle(Map<IDeviceID, DeviceContext> targetDevices)
      throws IOException, IllegalPathException {

    if (!queryDataSource.hasNext()) {
      // There is no more TsFileHandles to be scanned.
      return false;
    }

    curFileScanHandle = queryDataSource.next();
    deviceChunkMetaDataIterator = null;

    // Init deviceSet for current tsFileHandle
    TsFileDeviceStartEndTimeIterator iterator = curFileScanHandle.getDeviceStartEndTimeIterator();
    while (iterator.hasNext()) {
      DeviceStartEndTime deviceStartEndTime = iterator.next();
      IDeviceID deviceID = deviceStartEndTime.getDevicePath();
      long startTime = deviceStartEndTime.getStartTime();
      long endTime = deviceStartEndTime.getEndTime();
      // If this device has already been removed by another TsFileHandle, we should skip it.
      // If the time range is filtered, the devicePath is not active in this time range.
      if (!targetDevices.containsKey(deviceID)
          || (endTime >= 0 && !timeFilter.satisfyStartEndTime(startTime, endTime, deviceID))) {
        continue;
      }

      if ((timeFilter.satisfy(startTime, deviceID)
              && !curFileScanHandle.isDeviceTimeDeleted(deviceID, startTime))
          || (endTime >= 0
              && timeFilter.satisfy(endTime, deviceID)
              && !curFileScanHandle.isDeviceTimeDeleted(deviceID, endTime))) {
        activeDevices.add(deviceID);
      } else {
        // We need more information to check if the device is active in the following procedure
        deviceSetForCurrentTsFile.add(deviceID);
      }
    }
    return true;
  }

  @Override
  public void processDeviceChunkMetadata(AbstractDeviceChunkMetaData deviceChunkMetaData)
      throws IllegalPathException {
    IDeviceID curDevice = deviceChunkMetaData.getDevicePath();
    if (deviceSetForCurrentTsFile.contains(curDevice)
        && checkChunkMetaDataOfDevice(curDevice, deviceChunkMetaData)) {
      // If the chunkMeta in curDevice has valid start or end time, curDevice is active in this
      // time range.
      deviceSetForCurrentTsFile.remove(curDevice);
      activeDevices.add(curDevice);
    }
  }

  @Override
  public boolean isCurrentChunkHandleValid() {
    return deviceSetForCurrentTsFile.contains(currentChunkHandle.getDeviceID());
  }

  @Override
  public void processActiveChunk(IDeviceID deviceID, String measurementId) {
    // Chunk is active means relating device is active, too.
    deviceSetForCurrentTsFile.remove(deviceID);
    activeDevices.add(deviceID);
    timeFilter.removeTTLCache(deviceID);
    currentChunkHandle = null;
  }

  private boolean checkChunkMetaDataOfDevice(
      IDeviceID deviceID, AbstractDeviceChunkMetaData deviceChunkMetaData)
      throws IllegalPathException {
    List<AbstractChunkOffset> chunkOffsetsForCurrentDevice = new ArrayList<>();
    List<Statistics<? extends Serializable>> chunkStatisticsForCurrentDevice = new ArrayList<>();
    while (deviceChunkMetaData.hasNextValueChunkMetadata()) {
      IChunkMetadata valueChunkMetaData = deviceChunkMetaData.nextValueChunkMetadata();
      long startTime = valueChunkMetaData.getStartTime();
      long endTime = valueChunkMetaData.getEndTime();
      if (!timeFilter.satisfyStartEndTime(startTime, endTime, deviceID)) {
        continue;
      }
      String measurement = valueChunkMetaData.getMeasurementUid();
      // If the chunkMeta in curDevice has valid start or end time, curDevice is active in this
      // time range.
      if ((timeFilter.satisfy(startTime, deviceID)
              && !curFileScanHandle.isTimeSeriesTimeDeleted(deviceID, measurement, startTime))
          || (timeFilter.satisfy(endTime, deviceID)
              && !curFileScanHandle.isTimeSeriesTimeDeleted(deviceID, measurement, endTime))) {
        return true;
      }
      chunkOffsetsForCurrentDevice.add(deviceChunkMetaData.getChunkOffset());
      chunkStatisticsForCurrentDevice.add(valueChunkMetaData.getStatistics());
    }
    chunkToBeScanned.addAll(chunkOffsetsForCurrentDevice);
    chunkStatistics.addAll(chunkStatisticsForCurrentDevice);
    return false;
  }

  public List<IDeviceID> getActiveDevices() {
    return activeDevices;
  }

  @Override
  public void finishCurrentFile() {
    super.finishCurrentFile();
    queryDataSource.releaseFileScanHandle();
    deviceSetForCurrentTsFile.clear();
    activeDevices.clear();
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE + super.ramBytesUsed();
  }
}
