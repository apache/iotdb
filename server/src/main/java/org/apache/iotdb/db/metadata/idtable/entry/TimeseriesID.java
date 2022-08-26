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
package org.apache.iotdb.db.metadata.idtable.entry;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.idtable.deviceID.DeviceIDFactory;
import org.apache.iotdb.db.metadata.idtable.deviceID.IDeviceID;

/** A po class contains device id and measurement, represents a timeseries */
public class TimeseriesID {

  private IDeviceID deviceID;
  private String measurement;

  /** build timeseries id from full path */
  public TimeseriesID(PartialPath fullPath) {
    deviceID = DeviceIDFactory.getInstance().getDeviceID(fullPath.getDevicePath());
    measurement = fullPath.getMeasurement();
  }

  public IDeviceID getDeviceID() {
    return deviceID;
  }

  public String getMeasurement() {
    return measurement;
  }

  public void setMeasurement(String measurement) {
    this.measurement = measurement;
  }

  @Override
  public String toString() {
    return "TimeseriesID{" + "deviceID=" + deviceID + ", measurement='" + measurement + '\'' + '}';
  }
}
