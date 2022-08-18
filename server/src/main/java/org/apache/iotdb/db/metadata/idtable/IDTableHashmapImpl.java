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
import org.apache.iotdb.db.exception.metadata.DataTypeMismatchException;
import org.apache.iotdb.db.metadata.idtable.entry.DeviceEntry;
import org.apache.iotdb.db.metadata.idtable.entry.DeviceIDFactory;
import org.apache.iotdb.db.metadata.idtable.entry.IDeviceID;
import org.apache.iotdb.db.metadata.idtable.entry.InsertMeasurementMNode;
import org.apache.iotdb.db.metadata.idtable.entry.SchemaEntry;
import org.apache.iotdb.db.metadata.idtable.entry.TimeseriesID;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.schemaregion.SchemaRegionUtils;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** id table belongs to a storage group and mapping timeseries path to it's schema */
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
  public synchronized void createAlignedTimeseries(CreateAlignedTimeSeriesPlan plan)
      throws MetadataException {
    DeviceEntry deviceEntry = getDeviceEntryWithAlignedCheck(plan.getPrefixPath().toString(), true);

    for (int i = 0; i < plan.getMeasurements().size(); i++) {
      PartialPath fullPath =
          new PartialPath(plan.getPrefixPath().toString(), plan.getMeasurements().get(i));
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
  public synchronized void createTimeseries(CreateTimeSeriesPlan plan) throws MetadataException {
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
   * check inserting timeseries existence and fill their measurement mnode
   *
   * @param plan insert plan
   * @return reusable device id
   * @throws MetadataException if insert plan's aligned value is inconsistent with device
   */
  @Override
  public synchronized IDeviceID getSeriesSchemas(InsertPlan plan) throws MetadataException {
    PartialPath devicePath = plan.getDevicePath();
    String[] measurementList = plan.getMeasurements();
    IMeasurementMNode[] measurementMNodes = plan.getMeasurementMNodes();

    // 1. get device entry and check align
    DeviceEntry deviceEntry =
        getDeviceEntryWithAlignedCheck(devicePath.toString(), plan.isAligned());

    // 2. get schema of each measurement
    for (int i = 0; i < measurementList.length; i++) {
      try {
        // get MeasurementMNode, auto create if absent
        try {
          IMeasurementMNode measurementMNode =
              getOrCreateMeasurementIfNotExist(deviceEntry, plan, i);

          SchemaRegionUtils.checkDataTypeMatch(plan, i, measurementMNode.getSchema().getType());
          measurementMNodes[i] = measurementMNode;
        } catch (DataTypeMismatchException mismatchException) {
          logger.warn(mismatchException.getMessage());
          if (!config.isEnablePartialInsert()) {
            throw mismatchException;
          } else {
            // mark failed measurement
            plan.markFailedMeasurementInsertion(i, mismatchException);
          }
        }
      } catch (MetadataException e) {
        if (config.isClusterMode()) {
          logger.debug(
              "meet error when check {}.{}, message: {}",
              devicePath,
              measurementList[i],
              e.getMessage());
        } else {
          logger.warn(
              "meet error when check {}.{}, message: {}",
              devicePath,
              measurementList[i],
              e.getMessage());
        }
        if (config.isEnablePartialInsert()) {
          // mark failed measurement
          plan.markFailedMeasurementInsertion(i, e);
        } else {
          throw e;
        }
      }
    }

    // set reusable device id
    plan.setDeviceID(deviceEntry.getDeviceID());
    // change device path to device id string for insertion
    plan.setDevicePath(new PartialPath(deviceEntry.getDeviceID().toStringID(), false));

    return deviceEntry.getDeviceID();
  }

  /**
   * register trigger to the timeseries
   *
   * @param fullPath full path of the timeseries
   * @param measurementMNode the timeseries measurement mnode
   * @throws MetadataException if the timeseries is not exits
   */
  @Override
  public synchronized void registerTrigger(PartialPath fullPath, IMeasurementMNode measurementMNode)
      throws MetadataException {
    boolean isAligned = measurementMNode.getParent().isAligned();
    DeviceEntry deviceEntry = getDeviceEntryWithAlignedCheck(fullPath.getDevice(), isAligned);

    deviceEntry.getSchemaEntry(fullPath.getMeasurement()).setUsingTrigger();
  }

  /**
   * deregister trigger to the timeseries
   *
   * @param fullPath full path of the timeseries
   * @param measurementMNode the timeseries measurement mnode
   * @throws MetadataException if the timeseries is not exits
   */
  @Override
  public synchronized void deregisterTrigger(
      PartialPath fullPath, IMeasurementMNode measurementMNode) throws MetadataException {
    boolean isAligned = measurementMNode.getParent().isAligned();
    DeviceEntry deviceEntry = getDeviceEntryWithAlignedCheck(fullPath.getDevice(), isAligned);

    deviceEntry.getSchemaEntry(fullPath.getMeasurement()).setUnUsingTrigger();
  }

  /**
   * get last cache of the timeseies
   *
   * @param timeseriesID timeseries ID of the timeseries
   * @throws MetadataException if the timeseries is not exits
   */
  @Override
  public synchronized TimeValuePair getLastCache(TimeseriesID timeseriesID)
      throws MetadataException {
    return getSchemaEntry(timeseriesID).getCachedLast();
  }

  /**
   * update last cache of the timeseies
   *
   * @param timeseriesID timeseries ID of the timeseries
   * @param pair last time value pair
   * @param highPriorityUpdate is high priority update
   * @param latestFlushedTime last flushed time
   * @throws MetadataException if the timeseries is not exits
   */
  @Override
  public synchronized void updateLastCache(
      TimeseriesID timeseriesID,
      TimeValuePair pair,
      boolean highPriorityUpdate,
      Long latestFlushedTime)
      throws MetadataException {
    getSchemaEntry(timeseriesID).updateCachedLast(pair, highPriorityUpdate, latestFlushedTime);
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
   * check whether a time series is exist if exist, check the type consistency if not exist, call
   * SchemaProcessor to create it
   *
   * @return measurement MNode of the time series or null if type is not match
   */
  private IMeasurementMNode getOrCreateMeasurementIfNotExist(
      DeviceEntry deviceEntry, InsertPlan plan, int loc) throws MetadataException {
    String measurementName = plan.getMeasurements()[loc];
    PartialPath seriesKey = new PartialPath(plan.getDevicePath().toString(), measurementName);

    SchemaEntry schemaEntry = deviceEntry.getSchemaEntry(measurementName);

    // if not exist, we create it
    if (schemaEntry == null) {
      // we have to copy plan's mnode for using id table's last cache
      IMeasurementMNode[] insertPlanMNodeBackup =
          new IMeasurementMNode[plan.getMeasurementMNodes().length];
      System.arraycopy(
          plan.getMeasurementMNodes(), 0, insertPlanMNodeBackup, 0, insertPlanMNodeBackup.length);
      try {
        IoTDB.schemaProcessor.getSeriesSchemasAndReadLockDevice(plan);
      } catch (IOException e) {
        throw new MetadataException(e);
      }

      // if the timeseries is in template, SchemaProcessor will not create timeseries. so we have to
      // put it
      // in id table here
      for (IMeasurementMNode measurementMNode : plan.getMeasurementMNodes()) {
        if (measurementMNode != null && !deviceEntry.contains(measurementMNode.getName())) {
          IMeasurementSchema schema = measurementMNode.getSchema();
          SchemaEntry curEntry =
              new SchemaEntry(
                  schema.getType(),
                  schema.getEncodingType(),
                  schema.getCompressor(),
                  deviceEntry.getDeviceID(),
                  seriesKey,
                  deviceEntry.isAligned(),
                  IDiskSchemaManager);
          deviceEntry.putSchemaEntry(measurementMNode.getName(), curEntry);
        }
      }

      // copy back measurement mnode list
      System.arraycopy(
          insertPlanMNodeBackup, 0, plan.getMeasurementMNodes(), 0, insertPlanMNodeBackup.length);

      schemaEntry = deviceEntry.getSchemaEntry(measurementName);
    }

    // timeseries is using trigger, we should get trigger from SchemaProcessor
    if (schemaEntry.isUsingTrigger()) {
      IMeasurementMNode measurementMNode = IoTDB.schemaProcessor.getMeasurementMNode(seriesKey);
      return new InsertMeasurementMNode(
          measurementName, schemaEntry, measurementMNode.getTriggerExecutor());
    }

    return new InsertMeasurementMNode(measurementName, schemaEntry);
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
