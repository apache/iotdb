/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.pipe.receiver.transform.statement;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.pipe.resource.log.PipeLogger;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.pipe.receiver.transform.converter.ValueConverter;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;

import org.apache.tsfile.annotations.TableModel;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class PipeConvertedInsertRowStatement extends InsertRowStatement {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeConvertedInsertRowStatement.class);

  public PipeConvertedInsertRowStatement(final InsertRowStatement insertRowStatement) {
    super();
    // Statement
    isDebug = insertRowStatement.isDebug();
    // InsertBaseStatement
    devicePath = insertRowStatement.getDevicePath();
    isAligned = insertRowStatement.isAligned();
    measurementSchemas = insertRowStatement.getMeasurementSchemas();
    measurements = insertRowStatement.getMeasurements();
    dataTypes = insertRowStatement.getDataTypes();
    columnCategories = insertRowStatement.getColumnCategories();
    tagColumnIndices = insertRowStatement.getTagColumnIndices();
    attrColumnIndices = insertRowStatement.getAttrColumnIndices();
    writeToTable = insertRowStatement.isWriteToTable();
    databaseName = insertRowStatement.getDatabaseName().orElse(null);
    // InsertRowStatement
    time = insertRowStatement.getTime();
    values = insertRowStatement.getValues();
    isNeedInferType = insertRowStatement.isNeedInferType();
    deviceID = insertRowStatement.getRawTableDeviceID();

    // To ensure that the measurement remains unchanged during the WAL writing process, the array
    // needs to be copied before the failed Measurement mark can be deleted.
    final MeasurementSchema[] measurementSchemas = insertRowStatement.getMeasurementSchemas();
    if (measurementSchemas != null) {
      this.measurementSchemas = Arrays.copyOf(measurementSchemas, measurementSchemas.length);
    }

    final String[] measurements = insertRowStatement.getMeasurements();
    if (measurements != null) {
      this.measurements = Arrays.copyOf(measurements, measurements.length);
    }

    final TSDataType[] dataTypes = insertRowStatement.getDataTypes();
    if (dataTypes != null) {
      this.dataTypes = Arrays.copyOf(dataTypes, dataTypes.length);
    }

    final Map<Integer, FailedMeasurementInfo> failedMeasurementIndex2Info =
        insertRowStatement.getFailedMeasurementInfoMap();
    if (failedMeasurementIndex2Info != null) {
      this.failedMeasurementIndex2Info = new HashMap<>(failedMeasurementIndex2Info);
    }

    removeAllFailedMeasurementMarks();
  }

  @Override
  protected boolean checkAndCastDataType(int columnIndex, TSDataType dataType) {
    PipeLogger.log(
        LOGGER::info,
        "Pipe: Inserting row to %s.%s. Casting type from %s to %s.",
        devicePath,
        measurements[columnIndex],
        dataTypes[columnIndex],
        dataType);
    values[columnIndex] =
        ValueConverter.convert(dataTypes[columnIndex], dataType, values[columnIndex]);
    dataTypes[columnIndex] = dataType;
    return true;
  }

  @Override
  public void transferType(ZoneId zoneId) throws QueryProcessException {
    for (int i = 0; i < measurementSchemas.length; i++) {
      // null when time series doesn't exist
      if (measurementSchemas[i] == null) {
        if (!IoTDBDescriptor.getInstance().getConfig().isEnablePartialInsert()) {
          throw new QueryProcessException(
              new PathNotExistException(
                  devicePath.getFullPath() + IoTDBConstant.PATH_SEPARATOR + measurements[i]));
        } else {
          markFailedMeasurement(
              i,
              new QueryProcessException(
                  new PathNotExistException(
                      devicePath.getFullPath() + IoTDBConstant.PATH_SEPARATOR + measurements[i])));
        }
        continue;
      }

      // parse string value to specific type
      dataTypes[i] = measurementSchemas[i].getType();
      try {
        values[i] = ValueConverter.parse(values[i].toString(), dataTypes[i]);
      } catch (Exception e) {
        PipeLogger.log(
            LOGGER::warn,
            "data type of %s.%s is not consistent, "
                + "registered type %s, inserting timestamp %s, value %s",
            devicePath,
            measurements[i],
            dataTypes[i],
            time,
            values[i]);
        if (!IoTDBDescriptor.getInstance().getConfig().isEnablePartialInsert()) {
          throw e;
        } else {
          markFailedMeasurement(i, e);
        }
      }
    }

    isNeedInferType = false;
  }

  @TableModel
  @Override
  public boolean isForceTypeConversion() {
    return true;
  }
}
