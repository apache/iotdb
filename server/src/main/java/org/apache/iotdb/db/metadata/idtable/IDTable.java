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
import org.apache.iotdb.db.metadata.idtable.deviceID.DeviceIDFactory;
import org.apache.iotdb.db.metadata.idtable.deviceID.IDeviceID;
import org.apache.iotdb.db.metadata.idtable.entry.DeviceEntry;
import org.apache.iotdb.db.metadata.idtable.entry.DiskSchemaEntry;
import org.apache.iotdb.db.metadata.idtable.entry.SchemaEntry;
import org.apache.iotdb.db.metadata.idtable.entry.TimeseriesID;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.path.AlignedPath;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface IDTable {
  /** logger */
  Logger logger = LoggerFactory.getLogger(IDTable.class);

  /** iotdb config */
  IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  /**
   * create aligned timeseries
   *
   * @param plan create aligned timeseries plan
   * @throws MetadataException if the device is not aligned, throw it
   */
  void createAlignedTimeseries(CreateAlignedTimeSeriesPlan plan) throws MetadataException;

  /**
   * create timeseries
   *
   * @param plan create timeseries plan
   * @throws MetadataException if the device is aligned, throw it
   */
  void createTimeseries(CreateTimeSeriesPlan plan) throws MetadataException;

  /**
   * Delete all timeseries matching the given paths
   *
   * @param fullPaths paths to be deleted
   * @return deletion failed Timeseries
   * @throws MetadataException
   */
  Pair<Integer, Set<String>> deleteTimeseries(List<PartialPath> fullPaths) throws MetadataException;

  /**
   * check inserting timeseries existence and fill their measurement mnode
   *
   * @param plan insert plan
   * @return reusable device id
   * @throws MetadataException if insert plan's aligned value is inconsistent with device
   */
  IDeviceID getSeriesSchemas(InsertPlan plan) throws MetadataException;

  /**
   * register trigger to the timeseries
   *
   * @param fullPath full path of the timeseries
   * @param measurementMNode the timeseries measurement mnode
   * @throws MetadataException if the timeseries is not exits
   */
  void registerTrigger(PartialPath fullPath, IMeasurementMNode measurementMNode)
      throws MetadataException;

  /**
   * deregister trigger to the timeseries
   *
   * @param fullPath full path of the timeseries
   * @param measurementMNode the timeseries measurement mnode
   * @throws MetadataException if the timeseries is not exits
   */
  void deregisterTrigger(PartialPath fullPath, IMeasurementMNode measurementMNode)
      throws MetadataException;
  /**
   * get last cache of the timeseies
   *
   * @param timeseriesID timeseries ID of the timeseries
   * @throws MetadataException if the timeseries is not exits
   */
  TimeValuePair getLastCache(TimeseriesID timeseriesID) throws MetadataException;

  /**
   * update last cache of the timeseies
   *
   * @param timeseriesID timeseries ID of the timeseries
   * @param pair last time value pair
   * @param highPriorityUpdate is high priority update
   * @param latestFlushedTime last flushed time
   * @throws MetadataException if the timeseries is not exits
   */
  void updateLastCache(
      TimeseriesID timeseriesID,
      TimeValuePair pair,
      boolean highPriorityUpdate,
      Long latestFlushedTime)
      throws MetadataException;

  /** clear id table and close file */
  @TestOnly
  void clear() throws IOException;

  /**
   * get device entry from device path
   *
   * @param deviceName device name of the time series
   * @return device entry of the timeseries
   */
  DeviceEntry getDeviceEntry(String deviceName);

  /**
   * get device entry from device id
   *
   * @param deviceID device id of the device path
   * @return device entry
   */
  DeviceEntry getDeviceEntry(IDeviceID deviceID);

  /**
   * get schema from device and measurements
   *
   * @param deviceName device name of the time series
   * @param measurementName measurement name of the time series
   * @return schema entry of the timeseries
   */
  IMeasurementSchema getSeriesSchema(String deviceName, String measurementName);

  /**
   * get all device entries
   *
   * @return all device entries
   */
  List<DeviceEntry> getAllDeviceEntry();

  /**
   * put schema entry to id table, currently used in recover
   *
   * @param deviceID device id
   * @param devicePath device path
   * @param measurement measurement name
   * @param schemaEntry schema entry to put
   * @param isAligned is the device aligned
   * @throws MetadataException
   */
  void putSchemaEntry(
      String deviceID,
      String devicePath,
      String measurement,
      SchemaEntry schemaEntry,
      boolean isAligned)
      throws MetadataException;

  /**
   * translate query path's device path to device id
   *
   * @param fullPath full query path
   * @return translated query path
   */
  static PartialPath translateQueryPath(PartialPath fullPath) {
    // if not enable id table, just return original path
    if (!config.isEnableIDTable()) {
      return fullPath;
    }

    try {
      // handle aligned path
      if (fullPath instanceof AlignedPath) {
        AlignedPath cur = (AlignedPath) fullPath;

        return new AlignedPath(
            DeviceIDFactory.getInstance().getDeviceID(cur).toStringID(),
            cur.getMeasurementList(),
            cur.getSchemaList());
      }

      // normal path
      TimeseriesID timeseriesID = new TimeseriesID(fullPath);
      return new MeasurementPath(
          timeseriesID.getDeviceID().toStringID(),
          timeseriesID.getMeasurement(),
          fullPath.getMeasurementSchema());
    } catch (MetadataException e) {
      logger.error("Error when translate query path: " + fullPath);
      throw new IllegalArgumentException("can't translate path to device id, path is: " + fullPath);
    }
  }

  /**
   * get DiskSchemaEntries from disk file
   *
   * @param schemaEntries get the disk pointers from schemaEntries
   * @return DiskSchemaEntries
   */
  @TestOnly
  List<DiskSchemaEntry> getDiskSchemaEntries(List<SchemaEntry> schemaEntries);

  @TestOnly
  Map<IDeviceID, DeviceEntry>[] getIdTables();

  @TestOnly
  IDiskSchemaManager getIDiskSchemaManager();
}
