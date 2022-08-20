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
package org.apache.iotdb.db.metadata.idtable.DeviceID;

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/** Using auto-incrementing int as device id */
public class AutoIncrementDeviceID implements IDeviceID {

  // Maintain the mapping relationship of devicePath to autoIncrementID，in order to reduce memory
  // consumption, the devicePath will actually be converted to SHA256DeviceID in advance
  private static final Map<String, Integer> devicePaths2autoIncrementID;

  // Atomic counter that records the number of unique devices
  private static final AtomicInteger uniqueDevicesCounter;

  // increment from 0
  private int autoIncrementID;

  static {
    devicePaths2autoIncrementID = new ConcurrentHashMap<>();
    uniqueDevicesCounter = new AtomicInteger();
  }

  public AutoIncrementDeviceID() {}

  public AutoIncrementDeviceID(String deviceID) {
    // if the device id string is a autoIncrementDeviceID form, like: "`1`",
    // convert string directly to autoIncrementID
    if (deviceID.startsWith("`") && deviceID.endsWith("`")) {
      deviceID = deviceID.substring(1, deviceID.length() - 1);
      this.autoIncrementID = Integer.parseInt(deviceID);
    } else {
      buildAutoIncrementDeviceID(deviceID);
    }
  }

  /**
   * build device id from a device path
   *
   * @param deviceID device path, like: "root.sg.x.d1"
   */
  private void buildAutoIncrementDeviceID(String deviceID) {
    SHA256DeviceID sha256DeviceID = new SHA256DeviceID(deviceID);
    if (devicePaths2autoIncrementID.containsKey(sha256DeviceID.toStringID())) {
      this.autoIncrementID = devicePaths2autoIncrementID.get(sha256DeviceID.toStringID());
    } else {
      this.autoIncrementID = uniqueDevicesCounter.getAndIncrement();
      devicePaths2autoIncrementID.put(sha256DeviceID.toStringID(), autoIncrementID);
    }
  }

  /**
   * make sure the hashcode of any AutoIncrementDeviceID object with the equal autoIncrementID are
   * equal,so use the autoIncrementID variable of type int as the hashcode of the object
   */
  @Override
  public int hashCode() {
    return this.autoIncrementID;
  }

  /** make sure any AutoIncrementDeviceID objects with equal autoIncrementID variables are equal */
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof AutoIncrementDeviceID)) {
      return false;
    }
    AutoIncrementDeviceID that = (AutoIncrementDeviceID) o;
    return this.autoIncrementID == that.getAutoIncrementID();
  }

  @Override
  public String toString() {
    return "AutoIncrementDeviceID{" + "autoIncrementID" + autoIncrementID + '}';
  }

  @Override
  public String toStringID() {
    return "`" + autoIncrementID + "`";
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(autoIncrementID, byteBuffer);
  }

  public static AutoIncrementDeviceID deserialize(ByteBuffer byteBuffer) {
    AutoIncrementDeviceID autoIncrementDeviceID = new AutoIncrementDeviceID();
    autoIncrementDeviceID.autoIncrementID = ReadWriteIOUtils.readInt(byteBuffer);
    return autoIncrementDeviceID;
  }

  public int getAutoIncrementID() {
    return autoIncrementID;
  }

  @TestOnly
  public void setAutoIncrementID(int autoIncrementID) {
    this.autoIncrementID = autoIncrementID;
  }
}
