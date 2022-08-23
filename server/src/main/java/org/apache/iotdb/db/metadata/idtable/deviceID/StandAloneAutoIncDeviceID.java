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
package org.apache.iotdb.db.metadata.idtable.deviceID;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.idtable.IDTable;
import org.apache.iotdb.db.metadata.idtable.IDTableManager;
import org.apache.iotdb.db.metadata.idtable.entry.DeviceEntry;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/** Using auto-incrementing id as device id */
public class StandAloneAutoIncDeviceID extends SHA256DeviceID implements IStatefulDeviceID {

  /** logger */
  private static Logger logger = LoggerFactory.getLogger(IDTable.class);

  // using list to find the corresponding deviceID according to the ID
  private static final List<IDeviceID> deviceIDs;

  // auto-incrementing id starting with 0
  int autoIncrementID;

  static {
    deviceIDs = new ArrayList<>();
  }

  public StandAloneAutoIncDeviceID() {}

  public static StandAloneAutoIncDeviceID generateDeviceID(String deviceID) {
    if (deviceID.startsWith("`") && deviceID.endsWith("`")) {
      return fromAutoIncDeviceID(deviceID);
    } else {
      return buildAutoIncDeviceID(deviceID);
    }
  }

  /**
   * build device id from a standAloneAutoIncDeviceID
   *
   * @param deviceID StandAloneAutoIncDeviceID deviceID, like: "`1`"
   * @return standAloneAutoIncDeviceID
   */
  private static StandAloneAutoIncDeviceID fromAutoIncDeviceID(String deviceID) {
    deviceID = deviceID.substring(1, deviceID.length() - 1);
    int id = Integer.parseInt(deviceID);
    try {
      synchronized (deviceIDs) {
        return (StandAloneAutoIncDeviceID) deviceIDs.get(id);
      }
    } catch (IndexOutOfBoundsException e) {
      logger.info(e.getMessage());
      return null;
    }
  }

  /**
   * build device id from a devicePath
   *
   * @param devicePath device path, like: "root.sg.x.d1"
   * @return standAloneAutoIncDeviceID
   */
  private static StandAloneAutoIncDeviceID buildAutoIncDeviceID(String devicePath) {
    try {
      // Use idtable to determine whether the device has been created
      IDTable idTable = IDTableManager.getInstance().getIDTable(new PartialPath(devicePath));
      StandAloneAutoIncDeviceID deviceID = new StandAloneAutoIncDeviceID();
      deviceID.parseAutoIncrementDeviceID(new SHA256DeviceID(devicePath));
      // this device is added for the first time
      if (idTable.getDeviceEntry(deviceID) == null) {
        synchronized (deviceIDs) {
          deviceID.autoIncrementID = deviceIDs.size();
          deviceIDs.add(deviceIDs.size(), deviceID);
        }
        // write a useless deviceEntry to idTable to prevent repeated generation of different
        // AutoIncrementDeviceID objects for the same devicePath
        idTable.putDeviceEntry(deviceID, new DeviceEntry(deviceID, false));
      } else {
        deviceID = (StandAloneAutoIncDeviceID) idTable.getDeviceEntry(deviceID).getDeviceID();
      }
      return deviceID;
    } catch (IllegalPathException e) {
      logger.error(e.getMessage());
      return null;
    }
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof StandAloneAutoIncDeviceID)) {
      return false;
    }
    return super.equals(o);
  }

  @Override
  public String toString() {
    return "AutoIncrementDeviceID{"
        + "l1="
        + l1
        + ", l2="
        + l2
        + ", l3="
        + l3
        + ", l4="
        + l4
        + ", autoIncrementID="
        + autoIncrementID
        + '}';
  }

  @Override
  public String toStringID() {
    return "`" + autoIncrementID + '`';
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    super.serialize(byteBuffer);
    ReadWriteIOUtils.write(autoIncrementID, byteBuffer);
  }

  public static StandAloneAutoIncDeviceID deserialize(ByteBuffer byteBuffer) {
    StandAloneAutoIncDeviceID autoIncrementDeviceID = new StandAloneAutoIncDeviceID();
    autoIncrementDeviceID.l1 = ReadWriteIOUtils.readLong(byteBuffer);
    autoIncrementDeviceID.l2 = ReadWriteIOUtils.readLong(byteBuffer);
    autoIncrementDeviceID.l3 = ReadWriteIOUtils.readLong(byteBuffer);
    autoIncrementDeviceID.l4 = ReadWriteIOUtils.readLong(byteBuffer);
    autoIncrementDeviceID.autoIncrementID = ReadWriteIOUtils.readInt(byteBuffer);
    return autoIncrementDeviceID;
  }

  private void parseAutoIncrementDeviceID(SHA256DeviceID sha256DeviceID) {
    this.l1 = sha256DeviceID.l1;
    this.l2 = sha256DeviceID.l2;
    this.l3 = sha256DeviceID.l3;
    this.l4 = sha256DeviceID.l4;
  }

  /**
   * write device id to the static variable deviceIDs
   *
   * @param devicePath device path of the time series
   * @param deviceID device id
   */
  @Override
  public void recover(String devicePath, String deviceID) {
    buildSHA256(devicePath);
    deviceID = deviceID.substring(1, deviceID.length() - 1);
    this.autoIncrementID = Integer.parseInt(deviceID);
    // if there is out-of-order data, write the deviceID to the correct index of the array
    synchronized (deviceIDs) {
      if (autoIncrementID < deviceIDs.size()) {
        deviceIDs.set(autoIncrementID, this);
      } else {
        for (int i = deviceIDs.size(); i < autoIncrementID; i++) {
          deviceIDs.add(i, null);
        }
        deviceIDs.add(autoIncrementID, this);
      }
    }
  }

  @Override
  public void clean() {
    synchronized (deviceIDs) {
      deviceIDs.clear();
    }
  }
}
