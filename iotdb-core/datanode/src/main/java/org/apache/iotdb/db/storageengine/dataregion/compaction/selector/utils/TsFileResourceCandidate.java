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

import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionScheduleContext;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileRepairStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ArrayDeviceTimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ITimeIndex;

import org.apache.tsfile.file.metadata.IDeviceID;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

@SuppressWarnings("OptionalGetWithoutIsPresent")
public class TsFileResourceCandidate {
  @SuppressWarnings("squid:S1104")
  public TsFileResource resource;

  @SuppressWarnings("squid:S1104")
  public boolean selected;

  @SuppressWarnings("squid:S1104")
  public boolean isValidCandidate;

  private ArrayDeviceTimeIndex deviceTimeIndex;

  private boolean hasDetailedDeviceInfo;
  private final CompactionScheduleContext compactionScheduleContext;

  public TsFileResourceCandidate(TsFileResource tsFileResource, CompactionScheduleContext context) {
    this.resource = tsFileResource;
    this.selected = false;
    // although we do the judgement here, the task should be validated before executing because
    // the status of file may be changed after the task is submitted to queue
    this.isValidCandidate =
        tsFileResource.getStatus() == TsFileResourceStatus.NORMAL
            && tsFileResource.getTsFileRepairStatus() == TsFileRepairStatus.NORMAL;
    this.compactionScheduleContext = context;
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
    boolean canCacheDeviceInfo = resource.getStatus() != TsFileResourceStatus.UNCLOSED;
    if (deviceTimeIndex == null && compactionScheduleContext != null) {
      // get device info from cache
      deviceTimeIndex = compactionScheduleContext.getResourceDeviceInfo(this.resource);
    }
    if (deviceTimeIndex != null) {
      hasDetailedDeviceInfo = true;
      return;
    }
    ITimeIndex timeIndex = resource.getTimeIndex();
    if (timeIndex instanceof ArrayDeviceTimeIndex) {
      if (resource.isClosed()) {
        deviceTimeIndex = (ArrayDeviceTimeIndex) timeIndex;
      } else {
        deviceTimeIndex = new ArrayDeviceTimeIndex();
        for (IDeviceID device : ((ArrayDeviceTimeIndex) timeIndex).getDevices()) {
          deviceTimeIndex.updateStartTime(device, timeIndex.getStartTime(device).get());
          deviceTimeIndex.updateEndTime(device, timeIndex.getEndTime(device).get());
        }
      }
    } else {
      // deserialize resource file
      resource.readLock();
      try {
        // deleted file with degraded time index
        if (!resource.resourceFileExists()) {
          hasDetailedDeviceInfo = false;
          deviceTimeIndex = new ArrayDeviceTimeIndex();
          return;
        }
        deviceTimeIndex =
            CompactionUtils.buildDeviceTimeIndex(
                resource,
                compactionScheduleContext == null
                    ? IDeviceID.Deserializer.DEFAULT_DESERIALIZER
                    : compactionScheduleContext.getCachedDeviceIdDeserializer());
      } finally {
        resource.readUnlock();
      }
    }
    hasDetailedDeviceInfo = true;
    if (compactionScheduleContext != null && canCacheDeviceInfo) {
      compactionScheduleContext.addResourceDeviceTimeIndex(this.resource, deviceTimeIndex);
    }
  }

  public void markAsSelected() {
    this.selected = true;
  }

  public Iterator<DeviceInfo> getDeviceInfoIterator() throws IOException {
    prepareDeviceInfos();
    return new Iterator<DeviceInfo>() {

      private final Iterator<IDeviceID> deviceIterator = deviceTimeIndex.getDevices().iterator();

      @Override
      public boolean hasNext() {
        return deviceIterator.hasNext();
      }

      @Override
      public DeviceInfo next() {
        IDeviceID deviceId = deviceIterator.next();
        return new DeviceInfo(
            deviceId,
            deviceTimeIndex.getStartTime(deviceId).get(),
            deviceTimeIndex.getEndTime(deviceId).get());
      }
    };
  }

  public Set<IDeviceID> getDevices() throws IOException {
    prepareDeviceInfos();
    return deviceTimeIndex.getDevices();
  }

  public DeviceInfo getDeviceInfoById(IDeviceID deviceId) throws IOException {
    prepareDeviceInfos();
    return new DeviceInfo(
        deviceId,
        deviceTimeIndex.getStartTime(deviceId).get(),
        deviceTimeIndex.getEndTime(deviceId).get());
  }

  public boolean containsDevice(IDeviceID deviceId) throws IOException {
    prepareDeviceInfos();
    return !deviceTimeIndex.definitelyNotContains(deviceId);
  }

  public boolean hasDetailedDeviceInfo() throws IOException {
    prepareDeviceInfos();
    return hasDetailedDeviceInfo;
  }

  public boolean mayHasOverlapWithUnseqFile(DeviceInfo unseqFileDeviceInfo) throws IOException {
    prepareDeviceInfos();
    long endTime =
        containsDevice(unseqFileDeviceInfo.deviceId)
            ? getDeviceInfoById(unseqFileDeviceInfo.deviceId).endTime
            : resource.getFileEndTime();
    return unseqFileDeviceInfo.startTime <= endTime;
  }
}
