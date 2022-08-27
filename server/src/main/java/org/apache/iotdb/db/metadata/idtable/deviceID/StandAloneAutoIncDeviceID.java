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

import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.localconfignode.LocalConfigNode;
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

/**
 * Using auto-incrementing id as device id,A complete auto-increment id consists of schemaRegionID
 * and autoIncrementID, where the upper 32 bits are schemaRegionID and the lower 32 bits are
 * autoIncrementID
 */
public class StandAloneAutoIncDeviceID extends SHA256DeviceID {

  /** logger */
  private static Logger logger = LoggerFactory.getLogger(IDTable.class);

  // stand-alone auto-increment id uses LocalConfigNode to obtain schemaRegionId
  private static LocalConfigNode configManager;

  // using map to maintain the mapping from schemaRegionId to list<deviceID>, each list<deviceID>
  // maintains the auto-increment id of the schemaRegion
  private static Map<Integer, List<IDeviceID>> deviceIDsMap;

  // if the device represented by devicePath is not written to the metadata module, use this
  // constant instead of devicePath to generate a sha266 value of StandAloneAutoIncDeviceID instance
  private static final String INVALID_DEVICE_PATH = "invalid.device.path";

  // if the schemaRegionId==-1 of a StandAloneAutoIncDeviceID instance, it means that the device
  // corresponding to the StandAloneAutoIncDeviceID instance does not exist
  // return the deviceIdOfNonExistentDevice object for all devicePaths of unmanaged devices in the
  // metadata module, which can avoid unnecessary object creation while ensuring correctness
  private static StandAloneAutoIncDeviceID deviceIdOfNonExistentDevice;

  // starting with 0,the maximum value is Integer.MAX_VALUE
  int schemaRegionId;

  // starting with 0,the maximum value is Integer.MAX_VALUE
  int autoIncrementID;

  static {
    deviceIDsMap = new ConcurrentHashMap<>();
    configManager = LocalConfigNode.getInstance();
    setDeviceIdOfNonExistentDevice();
  }

  /**
   * use the sha256 value of INVALID_DEVICE_PATH and schemaRegionId=-1, autoIncrementID=1 to
   * generate a deviceID of the devicePath of the device that is not managed by the metadata
   * management module, which can ensure that the deviceID is used, and no information can be found
   * as expected.
   */
  private static void setDeviceIdOfNonExistentDevice() {
    deviceIdOfNonExistentDevice = new StandAloneAutoIncDeviceID(INVALID_DEVICE_PATH);
    deviceIdOfNonExistentDevice.schemaRegionId = -1;
    deviceIdOfNonExistentDevice.autoIncrementID = 0;
  }

  public StandAloneAutoIncDeviceID() {}

  public StandAloneAutoIncDeviceID(String devicePath) {
    super(devicePath);
  }

  public StandAloneAutoIncDeviceID(String deviceID, String devicePath) {
    super(devicePath);
    long id = parseFromDeviceID(deviceID);
    this.schemaRegionId = (int) (id >>> 32);
    this.autoIncrementID = (int) id;
  }

  /**
   * get a StandAloneAutoIncDeviceID instance, create it if it doesn't exist
   *
   * @param deviceID device path for insert/query, and device id for query
   * @return a StandAloneAutoIncDeviceID instance
   */
  public static StandAloneAutoIncDeviceID getDeviceIDWithAutoCreate(String deviceID) {
    if (deviceID.startsWith("`") && deviceID.endsWith("`")) {
      return fromAutoIncDeviceID(deviceID);
    } else {
      return buildDeviceID(deviceID);
    }
  }

  /**
   * get a StandAloneAutoIncDeviceID instance, only for query
   *
   * @param deviceID device path or device id for query
   * @return if the device exists, return a StandAloneAutoIncDeviceID instance, if it does not
   *     exist,return a StandAloneAutoIncDeviceID instance,the object is guaranteed to be different
   *     from the deviceID object of any device managed by the system (equals==false).
   */
  public static StandAloneAutoIncDeviceID getDeviceID(String deviceID) {
    if (deviceID.startsWith("`") && deviceID.endsWith("`")) {
      return fromAutoIncDeviceID(deviceID);
    } else {
      return fromDevicePath(deviceID);
    }
  }

  /**
   * get a StandAloneAutoIncDeviceID instance, only for recover
   *
   * @param device union by deviceID and devicePath
   * @return a StandAloneAutoIncDeviceID instance
   */
  public static StandAloneAutoIncDeviceID getDeviceIDWithRecover(String... device) {
    String deviceID = device[0];
    String devicePath = device[1];
    StandAloneAutoIncDeviceID id = new StandAloneAutoIncDeviceID(deviceID, devicePath);
    List<IDeviceID> deviceIDs =
        deviceIDsMap.computeIfAbsent(id.schemaRegionId, integer -> new ArrayList<>());
    // if there is out-of-order data, write the deviceID to the correct index of the array
    synchronized (deviceIDs) {
      if (id.autoIncrementID < deviceIDs.size() && deviceIDs.get(id.autoIncrementID) != null) {
        return (StandAloneAutoIncDeviceID) deviceIDs.get(id.autoIncrementID);
      } else {
        for (int i = deviceIDs.size(); i < id.autoIncrementID; i++) {
          deviceIDs.add(i, null);
        }
        deviceIDs.add(id.autoIncrementID, id);
        return id;
      }
    }
  }

  /**
   * get device id from a standAloneAutoIncDeviceID
   *
   * @param deviceID StandAloneAutoIncDeviceID deviceID, like: "`1`"
   * @return a standAloneAutoIncDeviceID instance
   */
  private static StandAloneAutoIncDeviceID fromAutoIncDeviceID(String deviceID) {
    long id = parseFromDeviceID(deviceID);
    int schemaRegionId = (int) (id >>> 32);
    int autoIncrementID = (int) id;
    if (schemaRegionId == -1) {
      return deviceIdOfNonExistentDevice;
    }
    List<IDeviceID> deviceIDs = deviceIDsMap.get(schemaRegionId);
    synchronized (deviceIDs) {
      return (StandAloneAutoIncDeviceID) deviceIDs.get(autoIncrementID);
    }
  }

  /**
   * get device id from a device path
   *
   * @param devicePath device path, like: "root.sg.x.d1"
   * @return a standAloneAutoIncDeviceID instance
   */
  private static StandAloneAutoIncDeviceID fromDevicePath(String devicePath) {
    try {
      // use idTable to determine whether the device has been created
      IDTable idTable = IDTableManager.getInstance().getIDTable(new PartialPath(devicePath));
      StandAloneAutoIncDeviceID deviceID = new StandAloneAutoIncDeviceID(devicePath);
      if (idTable.getDeviceEntry(deviceID) != null) {
        deviceID = (StandAloneAutoIncDeviceID) idTable.getDeviceEntry(deviceID).getDeviceID();
        return deviceID;
      } else {
        return deviceIdOfNonExistentDevice;
      }
    } catch (IllegalPathException e) {
      logger.info(e.getMessage());
      return deviceIdOfNonExistentDevice;
    }
  }

  /**
   * get device id from a device path, if the device represented by the path does not exist, a
   * StandAloneAutoIncDeviceID instance is generated for the path
   *
   * @param devicePath device path, like: "root.sg.x.d1"
   * @return a standAloneAutoIncDeviceID instance
   */
  private static StandAloneAutoIncDeviceID buildDeviceID(String devicePath) {
    try {
      PartialPath path = new PartialPath(devicePath);
      // use idTable to determine whether the device has been created
      IDTable idTable = IDTableManager.getInstance().getIDTable(path);
      StandAloneAutoIncDeviceID deviceID = new StandAloneAutoIncDeviceID(devicePath);
      // this device is added for the first time
      if (idTable.getDeviceEntry(deviceID) == null) {
        SchemaRegionId schemaRegionId = configManager.getBelongedSchemaRegionId(path);
        deviceID.schemaRegionId = schemaRegionId.getId();
        List<IDeviceID> deviceIDs =
            deviceIDsMap.computeIfAbsent(deviceID.schemaRegionId, integer -> new ArrayList<>());
        synchronized (deviceIDs) {
          deviceID.autoIncrementID = deviceIDs.size();
          deviceIDs.add(deviceID.autoIncrementID, deviceID);
        }
      } else {
        deviceID = (StandAloneAutoIncDeviceID) idTable.getDeviceEntry(deviceID).getDeviceID();
      }
      return deviceID;
    } catch (MetadataException e) {
      logger.error(e.getMessage());
      return null;
    }
  }

  private static long parseFromDeviceID(String deviceID) {
    deviceID = deviceID.substring(1, deviceID.length() - 1);
    return Long.parseLong(deviceID);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  /** use sha256 value to determine whether it is equal */
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
        + ", schemaRegionId="
        + schemaRegionId
        + ", autoIncrementID="
        + autoIncrementID
        + '}';
  }

  @Override
  public String toStringID() {
    long stringID = (long) schemaRegionId << 32;
    stringID |= autoIncrementID;
    return "`" + stringID + '`';
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    super.serialize(byteBuffer);
    ReadWriteIOUtils.write(schemaRegionId, byteBuffer);
    ReadWriteIOUtils.write(autoIncrementID, byteBuffer);
  }

  public static StandAloneAutoIncDeviceID deserialize(ByteBuffer byteBuffer) {
    StandAloneAutoIncDeviceID autoIncrementDeviceID = new StandAloneAutoIncDeviceID();
    autoIncrementDeviceID.l1 = ReadWriteIOUtils.readLong(byteBuffer);
    autoIncrementDeviceID.l2 = ReadWriteIOUtils.readLong(byteBuffer);
    autoIncrementDeviceID.l3 = ReadWriteIOUtils.readLong(byteBuffer);
    autoIncrementDeviceID.l4 = ReadWriteIOUtils.readLong(byteBuffer);
    autoIncrementDeviceID.schemaRegionId = ReadWriteIOUtils.readInt(byteBuffer);
    autoIncrementDeviceID.autoIncrementID = ReadWriteIOUtils.readInt(byteBuffer);
    return autoIncrementDeviceID;
  }

  @TestOnly
  public static void reset() {
    deviceIDsMap.clear();
    configManager = LocalConfigNode.getInstance();
    setDeviceIdOfNonExistentDevice();
  }

  @TestOnly
  public static void clear() {
    deviceIDsMap.clear();
    configManager = null;
    deviceIdOfNonExistentDevice = null;
  }
}
