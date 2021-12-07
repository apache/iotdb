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

package org.apache.iotdb.db.metadata.id_table;

import java.util.HashMap;
import java.util.Map;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.id_table.entry.DeviceEntry;
import org.apache.iotdb.db.metadata.id_table.entry.IDeviceID;
import org.apache.iotdb.db.metadata.id_table.entry.SchemaEntry;
import org.apache.iotdb.db.metadata.id_table.entry.TimeseriesID;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** id table belongs to a storage group and mapping timeseries path to it's schema */
public class IDTable {

  // number of table slot
  private static final int NUM_OF_SLOTS = 256;
  /** logger */
  private static final Logger logger = LoggerFactory.getLogger(IDTable.class);
  /**
   * 256 hashmap for avoiding rehash performance issue and lock competition device ID ->
   * (measurement name -> schema entry)
   */
  private Map<IDeviceID, DeviceEntry>[] idTables;
  /** disk schema manager to manage disk schema entry */
  private DiskSchemaManager diskSchemaManager;

  public IDTable() {
    idTables = new Map[NUM_OF_SLOTS];
    for (int i = 0; i < NUM_OF_SLOTS; i++) {
      idTables[i] = new HashMap<>();
    }
  }

  /**
   * get device id from device path and check is aligned,
   *
   * @param seriesKey path of the time series
   * @param isAligned whether the insert plan is aligned
   * @return reused device id of the timeseries
   */
  public synchronized IDeviceID getDeviceID(PartialPath seriesKey, boolean isAligned)
      throws MetadataException {
    TimeseriesID timeseriesID = new TimeseriesID(seriesKey);
    IDeviceID deviceID = timeseriesID.getDeviceID();
    int slot = calculateSlot(deviceID);

    DeviceEntry deviceEntry = idTables[slot].get(deviceID);
    // new device
    if (deviceEntry == null) {
      deviceEntry = new DeviceEntry(deviceID);
      deviceEntry.setAligned(isAligned);
      idTables[slot].put(deviceID, deviceEntry);

      return deviceID;
    }

    // check aligned
    if (deviceEntry.isAligned() != isAligned) {
      throw new MetadataException(
          String.format(
              "Timeseries under path [%s]'s align value is [%b], which is not consistent with insert plan",
              seriesKey.getDevice(), deviceEntry.isAligned()));
    }

    // reuse device id in map
    return deviceEntry.getDeviceID();
  }

  /**
   * check whether a time series is exist if exist, check the type consistency if not exist, call
   * MManager to create it
   *
   * @param seriesKey path of the time series
   * @param dataType type of the time series
   * @return measurement MNode of the time series or null if type is not match
   */
  public synchronized IMeasurementMNode checkOrCreateIfNotExist(
      PartialPath seriesKey, TSDataType dataType) {
    TimeseriesID timeseriesID = new TimeseriesID(seriesKey);
    IDeviceID deviceID = timeseriesID.getDeviceID();
    int slot = calculateSlot(deviceID);

    DeviceEntry deviceEntry =
        idTables[slot].computeIfAbsent(deviceID, id -> new DeviceEntry(deviceID));
    SchemaEntry schemaEntry = deviceEntry.getSchemaEntry(timeseriesID.getMeasurement());

    // if not exist, we create it
    if (schemaEntry == null) {
      schemaEntry = new SchemaEntry(dataType);

      // 1. create new timeseries in mmanager
      try {
        MManager.getInstance()
            .createTimeseries(
                seriesKey,
                dataType,
                schemaEntry.getTSEncoding(),
                schemaEntry.getCompressionType(),
                null);
      } catch (MetadataException e) {
        logger.error("create timeseries failed, path is:" + seriesKey + " type is: " + dataType);
      }

      // 2. insert this schema into id table
      deviceEntry.putSchemaEntry(timeseriesID.getMeasurement(), schemaEntry);

      return null;
    }

    // type mismatch, we return null and this will be handled by upper level
    if (!schemaEntry.getTSDataType().equals(dataType)) {
      return null;
    }

    return null;
  }

  public synchronized IMeasurementMNode getSeriesSchemas(InsertPlan insertPlan) {}

  /**
   * update latest flushed time of one timeseries
   *
   * @param timeseriesID timeseries id
   * @param flushTime latest flushed time
   * @throws MetadataException throw if this timeseries is not exist
   */
  public synchronized void updateLatestFlushTime(TimeseriesID timeseriesID, long flushTime)
      throws MetadataException {
    getSchemaEntry(timeseriesID).updateLastedFlushTime(flushTime);
  }

  /**
   * update latest flushed time of one timeseries
   *
   * @param timeseriesID timeseries id
   * @return latest flushed time of one timeseries
   * @throws MetadataException throw if this timeseries is not exist
   */
  public synchronized long getLatestFlushedTime(TimeseriesID timeseriesID)
      throws MetadataException {
    return getSchemaEntry(timeseriesID).getFlushTime();
  }

  /**
   * calculate slot that this deviceID should in
   *
   * @param deviceID device id
   * @return slot number
   */
  private int calculateSlot(IDeviceID deviceID) {
    return deviceID.hashCode() % NUM_OF_SLOTS;
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
          "update non exist timeseries's latest flushed time, timeseries id is: " + timeseriesID);
    }

    SchemaEntry schemaEntry = deviceEntry.getSchemaEntry(timeseriesID.getMeasurement());
    if (schemaEntry == null) {
      throw new MetadataException(
          "update non exist timeseries's latest flushed time, timeseries id is: " + timeseriesID);
    }

    return schemaEntry;
  }
}
