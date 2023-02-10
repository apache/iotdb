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

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.idtable.entry.DeviceEntry;
import org.apache.iotdb.db.metadata.idtable.entry.DeviceIDFactory;
import org.apache.iotdb.db.metadata.idtable.entry.DiskSchemaEntry;
import org.apache.iotdb.db.metadata.idtable.entry.IDeviceID;
import org.apache.iotdb.db.metadata.idtable.entry.SchemaEntry;
import org.apache.iotdb.db.metadata.idtable.entry.TimeseriesID;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.ICreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.ICreateTimeSeriesPlan;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** id table belongs to a database and mapping timeseries path to it's schema */
public class IDTableHashmapImpl implements IDTable {

  // number of table slot
  private static final int NUM_OF_SLOTS = 256;
  /** logger */
  private static final Logger logger = LoggerFactory.getLogger(IDTableHashmapImpl.class);

  /**
   * 256 hashmap for avoiding rehash performance issue and lock competition device ID ->
   * (measurement name -> schema entry)
   */
  private Map<IDeviceID, DeviceEntry>[] idTables;

  /** disk schema manager to manage disk schema entry */
  private IDiskSchemaManager IDiskSchemaManager;
  /** iotdb config */
  protected static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  public IDTableHashmapImpl(File storageGroupDir) {
    idTables = new Map[NUM_OF_SLOTS];
    for (int i = 0; i < NUM_OF_SLOTS; i++) {
      idTables[i] = new HashMap<>();
    }
    if (config.isEnableIDTableLogFile()) {
      IDiskSchemaManager = new AppendOnlyDiskSchemaManager(storageGroupDir);
      IDiskSchemaManager.recover(this);
    }
  }

  /**
   * create aligned timeseries
   *
   * @param plan create aligned timeseries plan
   * @throws MetadataException if the device is not aligned, throw it
   */
  @Override
  public synchronized void createAlignedTimeseries(ICreateAlignedTimeSeriesPlan plan)
      throws MetadataException {
    DeviceEntry deviceEntry = getDeviceEntryWithAlignedCheck(plan.getDevicePath().toString(), true);

    for (int i = 0; i < plan.getMeasurements().size(); i++) {
      PartialPath fullPath =
          new PartialPath(plan.getDevicePath().toString(), plan.getMeasurements().get(i));
      SchemaEntry schemaEntry =
          new SchemaEntry(
              plan.getDataTypes().get(i),
              plan.getEncodings().get(i),
              plan.getCompressors().get(i),
              deviceEntry.getDeviceID(),
              fullPath,
              true,
              IDiskSchemaManager);
      deviceEntry.putSchemaEntry(plan.getMeasurements().get(i), schemaEntry);
    }
  }

  /**
   * create timeseries
   *
   * @param plan create timeseries plan
   * @throws MetadataException if the device is aligned, throw it
   */
  @Override
  public synchronized void createTimeseries(ICreateTimeSeriesPlan plan) throws MetadataException {
    DeviceEntry deviceEntry = getDeviceEntryWithAlignedCheck(plan.getPath().getDevice(), false);
    SchemaEntry schemaEntry =
        new SchemaEntry(
            plan.getDataType(),
            plan.getEncoding(),
            plan.getCompressor(),
            deviceEntry.getDeviceID(),
            plan.getPath(),
            false,
            IDiskSchemaManager);
    deviceEntry.putSchemaEntry(plan.getPath().getMeasurement(), schemaEntry);
  }

  /**
   * Delete all timeseries matching the given paths
   *
   * @param fullPaths paths to be deleted
   * @return deletion failed Timeseries
   * @throws MetadataException
   */
  @Override
  public synchronized Pair<Integer, Set<String>> deleteTimeseries(List<PartialPath> fullPaths)
      throws MetadataException {
    int deletedNum = 0;
    Set<String> failedNames = new HashSet<>();
    List<Pair<PartialPath, Long>> deletedPairs = new ArrayList<>(fullPaths.size());
    for (PartialPath fullPath : fullPaths) {
      Map<String, SchemaEntry> map = getDeviceEntry(fullPath.getDevice()).getMeasurementMap();
      if (map == null) {
        failedNames.add(fullPath.getFullPath());
      } else {
        SchemaEntry schemaEntry = map.get(fullPath.getMeasurement());
        if (schemaEntry == null) {
          failedNames.add(fullPath.getFullPath());
        } else {
          deletedPairs.add(new Pair<>(fullPath, schemaEntry.getDiskPointer()));
        }
      }
    }
    // Sort by the offset of the disk records,transpose the random I/O to the order I/O
    deletedPairs.sort(Comparator.comparingLong(o -> o.right));
    for (Pair<PartialPath, Long> pair : deletedPairs) {
      try {
        getIDiskSchemaManager().deleteDiskSchemaEntryByOffset(pair.right);
        DeviceEntry deviceEntry = getDeviceEntry(pair.left.getDevice());
        Map<String, SchemaEntry> map = getDeviceEntry(pair.left.getDevice()).getMeasurementMap();
        map.keySet().remove(pair.left.getMeasurement());
        deletedNum++;
      } catch (MetadataException e) {
        failedNames.add(pair.left.getFullPath());
      }
    }
    return new Pair<>(deletedNum, failedNames);
  }

  @Override
  public void clear() throws IOException {
    if (IDiskSchemaManager != null) {
      IDiskSchemaManager.close();
    }
  }

  /**
   * get device entry from device path
   *
   * @param deviceName device name of the time series
   * @return device entry of the timeseries
   */
  @Override
  public DeviceEntry getDeviceEntry(String deviceName) {
    IDeviceID deviceID = DeviceIDFactory.getInstance().getDeviceID(deviceName);
    int slot = calculateSlot(deviceID);

    // reuse device entry in map
    return idTables[slot].get(deviceID);
  }

  /**
   * get schema from device and measurements
   *
   * @param deviceName device name of the time series
   * @param measurementName measurement name of the time series
   * @return schema entry of the timeseries
   */
  @Override
  public IMeasurementSchema getSeriesSchema(String deviceName, String measurementName) {
    DeviceEntry deviceEntry = getDeviceEntry(deviceName);
    if (deviceEntry == null) {
      return null;
    }

    SchemaEntry schemaEntry = deviceEntry.getSchemaEntry(measurementName);
    if (schemaEntry == null) {
      return null;
    }

    // build measurement schema
    return new MeasurementSchema(
        measurementName,
        schemaEntry.getTSDataType(),
        schemaEntry.getTSEncoding(),
        schemaEntry.getCompressionType());
  }

  @Override
  public List<DeviceEntry> getAllDeviceEntry() {
    List<DeviceEntry> res = new ArrayList<>();
    for (int i = 0; i < NUM_OF_SLOTS; i++) {
      res.addAll(idTables[i].values());
    }

    return res;
  }

  @Override
  public void putSchemaEntry(
      String devicePath, String measurement, SchemaEntry schemaEntry, boolean isAligned)
      throws MetadataException {
    DeviceEntry deviceEntry = getDeviceEntryWithAlignedCheck(devicePath, isAligned);
    deviceEntry.putSchemaEntry(measurement, schemaEntry);
  }

  /**
   * get DiskSchemaEntries from disk file
   *
   * @param schemaEntries get the disk pointers from schemaEntries
   * @return DiskSchemaEntries
   */
  @Override
  @TestOnly
  public synchronized List<DiskSchemaEntry> getDiskSchemaEntries(List<SchemaEntry> schemaEntries) {
    List<Long> offsets = new ArrayList<>(schemaEntries.size());
    for (SchemaEntry schemaEntry : schemaEntries) {
      offsets.add(schemaEntry.getDiskPointer());
    }
    return getIDiskSchemaManager().getDiskSchemaEntriesByOffset(offsets);
  }

  /**
   * get device id from device path and check is aligned,
   *
   * @param deviceName device name of the time series
   * @param isAligned whether the insert plan is aligned
   * @return device entry of the timeseries
   */
  private DeviceEntry getDeviceEntryWithAlignedCheck(String deviceName, boolean isAligned)
      throws MetadataException {
    IDeviceID deviceID = DeviceIDFactory.getInstance().getDeviceID(deviceName);
    int slot = calculateSlot(deviceID);

    DeviceEntry deviceEntry = idTables[slot].get(deviceID);
    // new device
    if (deviceEntry == null) {
      deviceEntry = new DeviceEntry(deviceID);
      deviceEntry.setAligned(isAligned);
      idTables[slot].put(deviceID, deviceEntry);

      return deviceEntry;
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

  /**
   * calculate slot that this deviceID should in
   *
   * @param deviceID device id
   * @return slot number
   */
  private int calculateSlot(IDeviceID deviceID) {
    int hashVal = deviceID.hashCode();
    return Math.abs(hashVal == Integer.MIN_VALUE ? 0 : hashVal) % NUM_OF_SLOTS;
  }

  /**
   * get schema entry
   *
   * @param timeseriesID the timeseries ID
   * @return schema entry of the timeseries
   * @throws MetadataException throw if this timeseries is not exist
   */
  private SchemaEntry getSchemaEntry(TimeseriesID timeseriesID) throws MetadataException {
    IDeviceID deviceID = timeseriesID.getDeviceID();
    int slot = calculateSlot(deviceID);

    DeviceEntry deviceEntry = idTables[slot].get(deviceID);
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

  @Override
  @TestOnly
  public Map<IDeviceID, DeviceEntry>[] getIdTables() {
    return idTables;
  }

  @Override
  @TestOnly
  public IDiskSchemaManager getIDiskSchemaManager() {
    return IDiskSchemaManager;
  }
}
