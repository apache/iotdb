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
package org.apache.iotdb.db.metadata.idtable;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.localconfignode.LocalConfigNode;
import org.apache.iotdb.db.metadata.idtable.deviceID.AutoIncDeviceID;
import org.apache.iotdb.db.metadata.idtable.deviceID.IDeviceID;
import org.apache.iotdb.db.metadata.idtable.deviceID.SHA256DeviceID;
import org.apache.iotdb.db.metadata.idtable.entry.DeviceEntry;
import org.apache.iotdb.db.metadata.idtable.entry.SchemaEntry;
import org.apache.iotdb.db.metadata.idtable.entry.TimeseriesID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class IDTableAutoIncImpl extends IDTableHashmapImpl {

  /** logger */
  private static final Logger logger = LoggerFactory.getLogger(IDTableAutoIncImpl.class);

  protected static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  // if the schemaRegionId==-1 of a StandAloneAutoIncDeviceID instance, it means that the device
  // corresponding to the StandAloneAutoIncDeviceID instance does not exist
  // return the deviceIdOfNonExistentDevice object for all devicePaths of unmanaged devices in the
  // metadata module, which can avoid unnecessary object creation while ensuring correctness
  private static AutoIncDeviceID deviceIdOfNonExistentDevice;

  // stand-alone auto-increment id uses LocalConfigNode to obtain schemaRegionId
  private static LocalConfigNode configManager;

  // using map to maintain the mapping from schemaRegionId to list<deviceID>, each list<deviceID>
  // maintains the auto-increment id of the schemaRegion
  private static Map<Integer, List<DeviceEntry>> deviceEntrysMap;

  static {
    deviceEntrysMap = new ConcurrentHashMap<>();
    configManager = LocalConfigNode.getInstance();
    deviceIdOfNonExistentDevice = new AutoIncDeviceID(-1, 0);
  }

  public static AutoIncDeviceID getDeviceID(String deviceID) {
    if (deviceID.startsWith("`") && deviceID.endsWith("`")) {
      return fromAutoIncDeviceID(deviceID);
    } else {
      return fromIdTable(deviceID);
    }
  }

  public static AutoIncDeviceID fromAutoIncDeviceID(String deviceID) {
    AutoIncDeviceID autoIncID = new AutoIncDeviceID(deviceID);
    int schemaRegionId = autoIncID.getSchemaRegionId();
    int autoIncrementID = autoIncID.getAutoIncrementID();
    if (autoIncID.getSchemaRegionId() == -1) {
      return deviceIdOfNonExistentDevice;
    }
    List<DeviceEntry> deviceEntries = deviceEntrysMap.get(schemaRegionId);
    synchronized (deviceEntries) {
      DeviceEntry deviceEntry = deviceEntries.get(autoIncrementID);
      return (AutoIncDeviceID) deviceEntry.getDeviceID();
    }
  }

  public static AutoIncDeviceID fromIdTable(String devicePath) {
    try {
      // use idTable to determine whether the device has been created
      IDTable idTable = IDTableManager.getInstance().getIDTable(new PartialPath(devicePath));
      SHA256DeviceID sha256DeviceID = new SHA256DeviceID(devicePath);
      DeviceEntry deviceEntry = idTable.getDeviceEntry(sha256DeviceID);
      if (idTable.getDeviceEntry(sha256DeviceID) != null) {
        return (AutoIncDeviceID) deviceEntry.getDeviceID();
      } else {
        return deviceIdOfNonExistentDevice;
      }
    } catch (IllegalPathException e) {
      logger.info(e.getMessage());
      return deviceIdOfNonExistentDevice;
    }
  }

  public IDTableAutoIncImpl(File storageGroupDir) {
    super(storageGroupDir);
  }

  private DeviceEntry buildDeviceEntry(String devicePath) throws MetadataException {
    int schemaRegionID =
        configManager.getBelongedSchemaRegionId(new PartialPath(devicePath)).getId();
    List<DeviceEntry> deviceEntries =
        deviceEntrysMap.computeIfAbsent(schemaRegionID, integer -> new ArrayList<>());
    synchronized (deviceEntries) {
      int autoIncrementID = deviceEntries.size();
      AutoIncDeviceID deviceID = new AutoIncDeviceID(schemaRegionID, autoIncrementID);
      DeviceEntry deviceEntry = new DeviceEntry(deviceID);
      deviceEntries.add(deviceEntry);
      return deviceEntry;
    }
  }

  @Override
  protected DeviceEntry getDeviceEntryWithAlignedCheck(String deviceName, boolean isAligned)
      throws MetadataException {
    DeviceEntry deviceEntry = null;
    if (deviceName.startsWith("`") && deviceName.endsWith("`")) {
      AutoIncDeviceID deviceID = new AutoIncDeviceID(deviceName);
      List<DeviceEntry> deviceEntries = deviceEntrysMap.get(deviceID.getAutoIncrementID());
      synchronized (deviceEntries) {
        deviceEntry = deviceEntries.get(deviceID.getAutoIncrementID());
      }
    } else {
      SHA256DeviceID sha256DeviceID = new SHA256DeviceID(deviceName);
      int slot = calculateSlot(sha256DeviceID);
      Map<IDeviceID, DeviceEntry>[] idtables = getIdTables();
      deviceEntry = idtables[slot].get(sha256DeviceID);

      // new device
      if (deviceEntry == null) {
        deviceEntry = buildDeviceEntry(deviceName);
        deviceEntry.setAligned(isAligned);
        idtables[slot].put(sha256DeviceID, deviceEntry);
      }
    }
    // check aligned
    if (deviceEntry.isAligned() != isAligned) {
      throw new MetadataException(
          String.format(
              "Timeseries under path [%s]'s align value is [%b], which is not consistent with insert plan",
              deviceName, deviceEntry.isAligned()));
    }

    // reuse device entry in map
    return deviceEntry;
  }

  public void putSchemaEntry(
      String deviceID,
      String devicePath,
      String measurement,
      SchemaEntry schemaEntry,
      boolean isAligned)
      throws MetadataException {
    AutoIncDeviceID autoIncDeviceID = new AutoIncDeviceID(deviceID);
    int schemaRegionID = autoIncDeviceID.getSchemaRegionId();
    int autoIncrementID = autoIncDeviceID.getAutoIncrementID();

    SHA256DeviceID sha256DeviceID = new SHA256DeviceID(devicePath);
    DeviceEntry deviceEntry = getDeviceEntry(sha256DeviceID);
    if (deviceEntry == null) {
      deviceEntry = new DeviceEntry(autoIncDeviceID);
      List<DeviceEntry> deviceEntries =
          deviceEntrysMap.computeIfAbsent(schemaRegionID, integer -> new ArrayList<>());
      synchronized (deviceEntries) {
        if (autoIncrementID < deviceEntries.size()) {
          deviceEntries.set(autoIncrementID, deviceEntry);
        } else {
          for (int i = deviceEntries.size(); i < autoIncrementID; i++) {
            deviceEntries.add(i, null);
          }
          deviceEntries.add(autoIncrementID, deviceEntry);
        }
      }
      int slot = calculateSlot(sha256DeviceID);
      Map<IDeviceID, DeviceEntry>[] idtables = getIdTables();
      idtables[slot].put(sha256DeviceID, deviceEntry);
      deviceEntry.setAligned(isAligned);
    }
    deviceEntry.putSchemaEntry(measurement, schemaEntry);
  }

  @Override
  public DeviceEntry getDeviceEntry(String deviceName) {
    if (deviceName.startsWith("`") && deviceName.endsWith("`")) {
      AutoIncDeviceID deviceID = new AutoIncDeviceID(deviceName);
      int schemaRegionID = deviceID.getSchemaRegionId();
      int autoIncrementID = deviceID.getAutoIncrementID();
      List<DeviceEntry> deviceEntries =
          deviceEntrysMap.computeIfAbsent(schemaRegionID, integer -> new ArrayList<>());
      synchronized (deviceEntries) {
        return deviceEntries.get(autoIncrementID);
      }
    } else {
      SHA256DeviceID sha256DeviceID = new SHA256DeviceID(deviceName);
      return getDeviceEntry(sha256DeviceID);
    }
  }

  @Override
  protected SchemaEntry getSchemaEntry(TimeseriesID timeseriesID) throws MetadataException {
    AutoIncDeviceID deviceID = (AutoIncDeviceID) timeseriesID.getDeviceID();
    List<DeviceEntry> deviceEntries = deviceEntrysMap.get(deviceID.getSchemaRegionId());
    if (deviceEntries == null) {
      throw new MetadataException(
          "get non exist timeseries's schema entry, timeseries id is: " + timeseriesID);
    }
    synchronized (deviceEntries) {
      DeviceEntry deviceEntry = deviceEntries.get(deviceID.getAutoIncrementID());
      if (deviceEntry == null) {
        throw new MetadataException(
            "get non exist timeseries's schema entry, timeseries id is: " + timeseriesID);
      }

      SchemaEntry schemaEntry = deviceEntry.getSchemaEntry(timeseriesID.getMeasurement());
      if (schemaEntry == null) {
        throw new MetadataException(
            "get non exist timeseries's schema entry, timeseries id is: " + timeseriesID);
      }
      return schemaEntry;
    }
  }

  @Override
  public void clear() throws IOException {
    super.clear();
    deviceEntrysMap.clear();
    configManager = null;
    deviceIdOfNonExistentDevice = null;
  }

  @TestOnly
  public static void reset() {
    deviceEntrysMap.clear();
    configManager = LocalConfigNode.getInstance();
    deviceIdOfNonExistentDevice = new AutoIncDeviceID(-1, 0);
  }
}
