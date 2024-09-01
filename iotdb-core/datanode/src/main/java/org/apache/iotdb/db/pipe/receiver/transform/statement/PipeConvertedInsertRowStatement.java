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
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.pipe.receiver.transform.converter.ValueConverter;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;

import org.apache.tsfile.enums.TSDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;

public class PipeConvertedInsertRowStatement extends InsertRowStatement {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeConvertedInsertRowStatement.class);

  public PipeConvertedInsertRowStatement(final InsertRowStatement insertRowStatement) {
    super();
    // Statement
    isDebug = insertRowStatement.isDebug();
    // InsertBaseStatement
    insertRowStatement.removeAllFailedMeasurementMarks();
    devicePath = insertRowStatement.getDevicePath();
    isAligned = insertRowStatement.isAligned();
    measurementSchemas = insertRowStatement.getMeasurementSchemas();
    measurements = insertRowStatement.getMeasurements();
    dataTypes = insertRowStatement.getDataTypes();
    // InsertRowStatement
    time = insertRowStatement.getTime();
    values = insertRowStatement.getValues();
    isNeedInferType = insertRowStatement.isNeedInferType();
  }

  @Override
  protected boolean checkAndCastDataType(int columnIndex, TSDataType dataType) {
    LOGGER.info(
        "Pipe: Inserting row to {}.{}. Casting type from {} to {}.",
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
        LOGGER.warn(
            "data type of {}.{} is not consistent, "
                + "registered type {}, inserting timestamp {}, value {}",
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
}
