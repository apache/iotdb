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

import static org.apache.iotdb.db.utils.EncodingInferenceUtils.getDefaultEncoding;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.DataTypeMismatchException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.metadata.id_table.entry.DeviceEntry;
import org.apache.iotdb.db.metadata.id_table.entry.DeviceIDFactory;
import org.apache.iotdb.db.metadata.id_table.entry.IDeviceID;
import org.apache.iotdb.db.metadata.id_table.entry.InsertMeasurementMNode;
import org.apache.iotdb.db.metadata.id_table.entry.SchemaEntry;
import org.apache.iotdb.db.metadata.id_table.entry.TimeseriesID;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.TypeInferenceUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
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
  /** iotdb config */
  protected static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  public IDTable() {
    idTables = new Map[NUM_OF_SLOTS];
    for (int i = 0; i < NUM_OF_SLOTS; i++) {
      idTables[i] = new HashMap<>();
    }
  }

  public synchronized void createAlignedTimeseries(CreateAlignedTimeSeriesPlan plan)
      throws MetadataException {
    DeviceEntry deviceEntry = getDeviceEntry(plan.getPrefixPath(), true);

    for (int i = 0; i < plan.getMeasurements().size(); i++) {
      SchemaEntry schemaEntry =
          new SchemaEntry(
              plan.getDataTypes().get(i), plan.getEncodings().get(i), plan.getCompressors().get(i));
      deviceEntry.putSchemaEntry(plan.getMeasurements().get(i), schemaEntry);
    }
  }

  public synchronized void createTimeseries(CreateTimeSeriesPlan plan) throws MetadataException {
    DeviceEntry deviceEntry = getDeviceEntry(plan.getPath().getDevicePath(), false);
    SchemaEntry schemaEntry =
        new SchemaEntry(plan.getDataType(), plan.getEncoding(), plan.getCompressor());
    deviceEntry.putSchemaEntry(plan.getPath().getMeasurement(), schemaEntry);
  }

  /**
   * check whether a time series is exist if exist, check the type consistency if not exist, call
   * MManager to create it
   *
   * @return measurement MNode of the time series or null if type is not match
   */
  public synchronized IMeasurementMNode getOrCreateMeasurementIfNotExist(
      DeviceEntry deviceEntry, InsertPlan plan, int loc) throws MetadataException {
    String measurementName = plan.getMeasurements()[loc];
    PartialPath seriesKey = new PartialPath(plan.getDeviceId().toString(), measurementName);

    SchemaEntry schemaEntry = deviceEntry.getSchemaEntry(measurementName);

    // if not exist, we create it
    if (schemaEntry == null) {
      if (!config.isAutoCreateSchemaEnabled()) {
        throw new PathNotExistException(seriesKey.toString());
      }

      // create new timeseries in mmanager
      try {
        if (plan.isAligned()) {
          List<TSEncoding> encodings = new ArrayList<>();
          List<CompressionType> compressors = new ArrayList<>();
          for (TSDataType dataType : plan.getDataTypes()) {
            encodings.add(getDefaultEncoding(dataType));
            compressors.add(TSFileDescriptor.getInstance().getConfig().getCompressor());
          }

          CreateAlignedTimeSeriesPlan createAlignedTimeSeriesPlan =
              new CreateAlignedTimeSeriesPlan(
                  plan.getDeviceId(),
                  Arrays.asList(plan.getMeasurements()),
                  Arrays.asList(plan.getDataTypes()),
                  encodings,
                  compressors,
                  null);

          IoTDB.metaManager.createAlignedTimeSeriesEntry(createAlignedTimeSeriesPlan);
        } else {
          CreateTimeSeriesPlan createTimeSeriesPlan =
              new CreateTimeSeriesPlan(
                  seriesKey,
                  plan.getDataTypes()[loc],
                  getDefaultEncoding(plan.getDataTypes()[loc]),
                  TSFileDescriptor.getInstance().getConfig().getCompressor(),
                  null,
                  null,
                  null,
                  null);

          IoTDB.metaManager.createTimeseriesEntry(createTimeSeriesPlan, -1);
        }
      } catch (MetadataException e) {
        logger.error("create timeseries failed, path is:" + seriesKey);
      }

      schemaEntry = deviceEntry.getSchemaEntry(measurementName);
    }

    return new InsertMeasurementMNode(measurementName, schemaEntry);
  }

  public synchronized IDeviceID getSeriesSchemas(InsertPlan plan) throws MetadataException {
    PartialPath devicePath = plan.getDeviceId();
    String[] measurementList = plan.getMeasurements();
    IMeasurementMNode[] measurementMNodes = plan.getMeasurementMNodes();

    // 1. get device entry and check align
    DeviceEntry deviceEntry = getDeviceEntry(devicePath, plan.isAligned());

    // 2. get schema of each measurement
    for (int i = 0; i < measurementList.length; i++) {
      try {
        // get MeasurementMNode, auto create if absent
        try {
          IMeasurementMNode measurementMNode =
              getOrCreateMeasurementIfNotExist(deviceEntry, plan, i);

          checkDataTypeMatch(plan, i, measurementMNode.getSchema().getType());
          measurementMNodes[i] = measurementMNode;
        } catch (DataTypeMismatchException mismatchException) {
          if (!config.isEnablePartialInsert()) {
            throw mismatchException;
          } else {
            // mark failed measurement
            plan.markFailedMeasurementInsertion(i, mismatchException);
          }
        }
      } catch (MetadataException e) {
        if (IoTDB.isClusterMode()) {
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

    return deviceEntry.getDeviceID();
  }

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
   * get device id from device path and check is aligned,
   *
   * @param deviceName device name of the time series
   * @param isAligned whether the insert plan is aligned
   * @return device entry of the timeseries
   */
  private DeviceEntry getDeviceEntry(PartialPath deviceName, boolean isAligned)
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
    return Math.abs(deviceID.hashCode()) % NUM_OF_SLOTS;
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

  // from mmanger
  private void checkDataTypeMatch(InsertPlan plan, int loc, TSDataType dataType)
      throws MetadataException {
    //    TSDataType insertDataType;
    //    if (plan instanceof InsertRowPlan) {
    //      if (!((InsertRowPlan) plan).isNeedInferType()) {
    //        // only when InsertRowPlan's values is object[], we should check type
    //        insertDataType = getTypeInLoc(plan, loc);
    //      } else {
    //        insertDataType = dataType;
    //      }
    //    } else {
    //      insertDataType = getTypeInLoc(plan, loc);
    //    }
    TSDataType insertDataType = plan.getDataTypes()[loc];
    if (dataType != insertDataType) {
      String measurement = plan.getMeasurements()[loc];
      logger.warn(
          "DataType mismatch, Insert measurement {} type {}, metadata tree type {}",
          measurement,
          insertDataType,
          dataType);
      throw new DataTypeMismatchException(measurement, insertDataType, dataType);
    }
  }

  /** get dataType of plan, in loc measurements only support InsertRowPlan and InsertTabletPlan */
  private TSDataType getTypeInLoc(InsertPlan plan, int loc) throws MetadataException {
    TSDataType dataType;
    if (plan instanceof InsertRowPlan) {
      InsertRowPlan tPlan = (InsertRowPlan) plan;
      dataType =
          TypeInferenceUtils.getPredictedDataType(tPlan.getValues()[loc], tPlan.isNeedInferType());
    } else if (plan instanceof InsertTabletPlan) {
      dataType = (plan).getDataTypes()[loc];
    } else {
      throw new MetadataException(
          String.format(
              "Only support insert and insertTablet, plan is [%s]", plan.getOperatorType()));
    }
    return dataType;
  }
}
