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

package org.apache.iotdb.db.engine.compaction.cross.rewrite;

import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.timeindex.DeviceTimeIndex;
import org.apache.iotdb.db.engine.storagegroup.timeindex.TimeIndexLevel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TsFileDeviceInfoStore {

  private Map<TsFileResource, TsFileDeviceInfo> cache;

  public TsFileDeviceInfoStore() {
    cache = new HashMap<>();
  }

  public TsFileDeviceInfo get(TsFileResource tsFileResource) {
    return cache.computeIfAbsent(tsFileResource, TsFileDeviceInfo::new);
  }

  public static class TsFileDeviceInfo {
    public TsFileResource resource;
    private Map<String, DeviceInfo> deviceInfoMap;

    public TsFileDeviceInfo(TsFileResource tsFileResource) {
      this.resource = tsFileResource;
    }

    private void prepareDeviceInfos() throws IOException {
      if (deviceInfoMap != null) {
        return;
      }
      deviceInfoMap = new LinkedHashMap<>();
      if (TimeIndexLevel.valueOf(resource.getTimeIndexType()) == TimeIndexLevel.FILE_TIME_INDEX) {
        DeviceTimeIndex timeIndex = resource.buildDeviceTimeIndex();
        for (String deviceId : timeIndex.getDevices()) {
          deviceInfoMap.put(
              deviceId,
              new DeviceInfo(
                  deviceId, timeIndex.getStartTime(deviceId), timeIndex.getEndTime(deviceId)));
        }
      } else {
        for (String deviceId : resource.getDevices()) {
          deviceInfoMap.put(
              deviceId,
              new DeviceInfo(
                  deviceId, resource.getStartTime(deviceId), resource.getEndTime(deviceId)));
        }
      }
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
  }

  public static class DeviceInfo {
    public String deviceId;
    public long startTime;
    public long endTime;

    public DeviceInfo(String deviceId, long startTime, long endTime) {
      this.deviceId = deviceId;
      this.startTime = startTime;
      this.endTime = endTime;
    }
  }
}
