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

package org.apache.iotdb.db.storageengine.dataregion.compaction.selector.utils;

import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.DeviceTimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ITimeIndex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TsFileResourceCandidate {
  @SuppressWarnings("squid:S1104")
  public TsFileResource resource;

  @SuppressWarnings("squid:S1104")
  public boolean selected;

  @SuppressWarnings("squid:S1104")
  public boolean isValidCandidate;

  private Map<String, DeviceInfo> deviceInfoMap;

  private boolean hasDetailedDeviceInfo;

  protected TsFileResourceCandidate(TsFileResource tsFileResource) {
    this.resource = tsFileResource;
    this.selected = false;
    // although we do the judgement here, the task should be validated before executing because
    // the status of file may be changed after the task is submitted to queue
    this.isValidCandidate = tsFileResource.getStatus() == TsFileResourceStatus.NORMAL;
  }

  /**
   * The TsFile is unsealed means there may be more data which will be inserted into this file.
   *
   * @return Whether the TsFile is unsealed.
   */
  public boolean unsealed() {
    return resource.getStatus() == TsFileResourceStatus.UNCLOSED;
  }

  private void prepareDeviceInfos() throws IOException {
    if (deviceInfoMap != null) {
      return;
    }
    deviceInfoMap = new LinkedHashMap<>();
    if (resource.getTimeIndexType() == ITimeIndex.FILE_TIME_INDEX_TYPE) {
      // deserialize resource file
      resource.readLock();
      try {
        if (!resource.resourceFileExists()) {
          hasDetailedDeviceInfo = false;
          return;
        }
        DeviceTimeIndex timeIndex = resource.buildDeviceTimeIndex();
        for (String deviceId : timeIndex.getDevices()) {
          deviceInfoMap.put(
              deviceId,
              new DeviceInfo(
                  deviceId, timeIndex.getStartTime(deviceId), timeIndex.getEndTime(deviceId)));
        }
      } finally {
        resource.readUnlock();
      }
    } else {
      for (String deviceId : resource.getDevices()) {
        deviceInfoMap.put(
            deviceId,
            new DeviceInfo(
                deviceId, resource.getStartTime(deviceId), resource.getEndTime(deviceId)));
      }
    }
    hasDetailedDeviceInfo = true;
  }

  public void markAsSelected() {
    this.selected = true;
  }

  public List<DeviceInfo> getDevices() throws IOException {
    prepareDeviceInfos();
    return new ArrayList<>(deviceInfoMap.values());
  }

  public DeviceInfo getDeviceInfoById(String deviceId) throws IOException {
    prepareDeviceInfos();
    return deviceInfoMap.get(deviceId);
  }

  public boolean containsDevice(String deviceId) throws IOException {
    prepareDeviceInfos();
    return deviceInfoMap.containsKey(deviceId);
  }

  public boolean hasDetailedDeviceInfo() throws IOException {
    prepareDeviceInfos();
    return hasDetailedDeviceInfo;
  }

  public boolean mayHasOverlapWithUnseqFile(DeviceInfo unseqFileDeviceInfo)
      throws IOException {
    prepareDeviceInfos();
    long endTime =
        containsDevice(unseqFileDeviceInfo.deviceId)
            ? getDeviceInfoById(unseqFileDeviceInfo.deviceId).endTime
            : resource.getFileEndTime();
    return unseqFileDeviceInfo.startTime <= endTime;
  }

}