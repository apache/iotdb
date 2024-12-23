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

package org.apache.iotdb.db.storageengine.dataregion.utils;

import org.apache.iotdb.db.storageengine.dataregion.read.filescan.model.DeviceStartEndTime;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ArrayDeviceTimeIndex;

import org.apache.tsfile.file.metadata.IDeviceID;

import java.util.Iterator;

/**
 * This class is used to iterate over the devicesTimeIndex in a TsFile to get the start and end
 * times of each device.
 */
public class TsFileDeviceStartEndTimeIterator {

  private final ArrayDeviceTimeIndex deviceTimeIndex;
  private final Iterator<IDeviceID> currentDevice;

  public TsFileDeviceStartEndTimeIterator(ArrayDeviceTimeIndex deviceTimeIndex) {
    this.deviceTimeIndex = deviceTimeIndex;
    this.currentDevice = deviceTimeIndex.getDevices().iterator();
  }

  public boolean hasNext() {
    return currentDevice.hasNext();
  }

  public DeviceStartEndTime next() {
    IDeviceID deviceID = currentDevice.next();
    return new DeviceStartEndTime(
        deviceID, deviceTimeIndex.getStartTime(deviceID), deviceTimeIndex.getEndTime(deviceID));
  }
}
