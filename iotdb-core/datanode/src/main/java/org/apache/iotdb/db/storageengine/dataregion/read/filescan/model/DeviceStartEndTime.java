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

package org.apache.iotdb.db.storageengine.dataregion.read.filescan.model;

import org.apache.tsfile.file.metadata.IDeviceID;

public class DeviceStartEndTime {
  private final IDeviceID devicePath;
  private final long startTime;
  private final long endTime;

  public DeviceStartEndTime(IDeviceID devicePath, long startTime, long endTime) {
    this.devicePath = devicePath;
    this.startTime = startTime;
    this.endTime = endTime;
  }

  public IDeviceID getDevicePath() {
    return devicePath;
  }

  public long getEndTime() {
    return endTime;
  }

  public long getStartTime() {
    return startTime;
  }
}
