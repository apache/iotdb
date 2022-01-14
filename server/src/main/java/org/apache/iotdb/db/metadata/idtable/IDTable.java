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

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.idtable.entry.DeviceEntry;
import org.apache.iotdb.db.metadata.idtable.entry.DeviceIDFactory;
import org.apache.iotdb.db.metadata.idtable.entry.IDeviceID;
import org.apache.iotdb.db.metadata.idtable.entry.TimeseriesID;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.path.AlignedPath;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.read.TimeValuePair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

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
   * check inserting timeseries existence and fill their measurement mnode
   *
   * @param plan insert plan
   * @return reusable device id
   * @throws MetadataException if insert plan's aligned value is inconsistent with device
   */
  IDeviceID getSeriesSchemas(InsertPlan plan) throws MetadataException;

  /**
   * update latest flushed time of one timeseries
   *
   * @param timeseriesID timeseries id
   * @param flushTime latest flushed time
   * @throws MetadataException throw if this timeseries is not exist
   */
  void updateLatestFlushTime(TimeseriesID timeseriesID, long flushTime) throws MetadataException;

  /**
   * update latest flushed time of one timeseries
   *
   * @param timeseriesID timeseries id
   * @return latest flushed time of one timeseries
   * @throws MetadataException throw if this timeseries is not exist
   */
  long getLatestFlushedTime(TimeseriesID timeseriesID) throws MetadataException;

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
  void clear() throws IOException;

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

  @TestOnly
  Map<IDeviceID, DeviceEntry>[] getIdTables();

  @TestOnly
  IDiskSchemaManager getIDiskSchemaManager();
}
