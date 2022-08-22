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
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Using auto-incrementing id as device id */
public class AutoIncrementDeviceID extends SHA256DeviceID implements StatefulIDeviceID {

  /** logger */
  private static Logger logger = LoggerFactory.getLogger(IDTable.class);

  // using list to find the corresponding deviceID according to the ID，使用map保存idtableid->deviceid
  // list
  private static Map<Integer, List<IDeviceID>> deviceIDsMap;

  // auto-incrementing id starting with 0,由idtableid(高32位)和自增id(低32位)组成
  long ID;

  static {
    deviceIDsMap = new ConcurrentHashMap<>();
  }

  public AutoIncrementDeviceID() {}

  public AutoIncrementDeviceID(String deviceID) {
    if (deviceID.startsWith("`") && deviceID.endsWith("`")) {
      fromAutoIncrementIntDeviceID(deviceID);
    } else {
      buildAutoIncrementIntDeviceID(deviceID);
    }
  }

  /**
   * build device id from a AutoIncrementIntDeviceID
   *
   * @param deviceID deviceID, like: "`1`"
   */
  private void fromAutoIncrementIntDeviceID(String deviceID) {
    deviceID = deviceID.substring(1, deviceID.length() - 1);
    this.ID = Long.parseLong(deviceID);
    int idTableID = (int) (this.ID >>> 32);
    int autoIncrementID = (int) this.ID;
    List<IDeviceID> deviceIDS =
        deviceIDsMap.computeIfAbsent(idTableID, integer -> new ArrayList<>());
    synchronized (deviceIDS) {
      parseAutoIncrementDeviceID((AutoIncrementDeviceID) deviceIDS.get(autoIncrementID));
    }
  }

  /**
   * build device id from a AutoIncrementIntDeviceID
   *
   * @param devicePath device path, like: "root.sg.x.d1"
   */
  private void buildAutoIncrementIntDeviceID(String devicePath) {
    try {
      // Use idtable to determine whether the device has been created
      IDTable idTable = IDTableManager.getInstance().getIDTable(new PartialPath(devicePath));
      List<IDeviceID> deviceIDS =
          deviceIDsMap.computeIfAbsent(idTable.getIdTableID(), integer -> new ArrayList<>());
      parseAutoIncrementDeviceID(new SHA256DeviceID(devicePath));
      // this device is added for the first time
      if (idTable.getDeviceEntry(this) == null) {
        this.ID = ((long) idTable.getIdTableID() << 32 | deviceIDS.size());
        synchronized (deviceIDS) {
          deviceIDS.add(deviceIDS.size(), this);
        }
      } else {
        AutoIncrementDeviceID deviceID =
            (AutoIncrementDeviceID) idTable.getDeviceEntry(this).getDeviceID();
        this.ID = deviceID.ID;
      }
    } catch (IllegalPathException e) {
      logger.error(e.getMessage());
    }
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof AutoIncrementDeviceID)) {
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
        + ", ID="
        + ID
        + '}';
  }

  @Override
  public String toStringID() {
    return "`" + ID + '`';
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    super.serialize(byteBuffer);
    ReadWriteIOUtils.write(ID, byteBuffer);
  }

  public static AutoIncrementDeviceID deserialize(ByteBuffer byteBuffer) {
    AutoIncrementDeviceID autoIncrementDeviceID = new AutoIncrementDeviceID();
    autoIncrementDeviceID.l1 = ReadWriteIOUtils.readLong(byteBuffer);
    autoIncrementDeviceID.l2 = ReadWriteIOUtils.readLong(byteBuffer);
    autoIncrementDeviceID.l3 = ReadWriteIOUtils.readLong(byteBuffer);
    autoIncrementDeviceID.l4 = ReadWriteIOUtils.readLong(byteBuffer);
    autoIncrementDeviceID.ID = ReadWriteIOUtils.readLong(byteBuffer);
    return autoIncrementDeviceID;
  }

  private void parseAutoIncrementDeviceID(SHA256DeviceID sha256DeviceID) {
    this.l1 = sha256DeviceID.l1;
    this.l2 = sha256DeviceID.l2;
    this.l3 = sha256DeviceID.l3;
    this.l4 = sha256DeviceID.l4;
  }

  /**
   *
   * @param devicePath
   * @param deviceID
   */
  @Override
  public void recover(String devicePath,String deviceID) {
    buildSHA256(devicePath);
    deviceID = deviceID.substring(1, deviceID.length() - 1);
    this.ID = Long.parseLong(deviceID);
    int idTableID = (int) (this.ID >>> 32);
    int autoIncrementID = (int) this.ID;
    List<IDeviceID> deviceIDS =
        deviceIDsMap.computeIfAbsent(idTableID, integer -> new ArrayList<>());
    // if there is out-of-order data, write the deviceID to the correct index of the array
    synchronized (deviceIDS) {
      for (int i = deviceIDS.size(); i < autoIncrementID; i++) {
        deviceIDS.add(i, null);
      }
      deviceIDS.add(autoIncrementID, this);
    }
  }

  /**
   *
   */
  public static void clear() {
    deviceIDsMap.clear();
  }

}
