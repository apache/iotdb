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

import org.apache.iotdb.db.pipe.receiver.transform.converter.ArrayConverter;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;

import org.apache.tsfile.annotations.TableModel;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class PipeConvertedInsertTabletStatement extends InsertTabletStatement {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeConvertedInsertTabletStatement.class);

  public PipeConvertedInsertTabletStatement(
      final InsertTabletStatement insertTabletStatement, boolean isCopyMeasurement) {
    super();
    // Statement
    isDebug = insertTabletStatement.isDebug();
    // InsertBaseStatement
    devicePath = insertTabletStatement.getDevicePath();
    isAligned = insertTabletStatement.isAligned();
    columnCategories = insertTabletStatement.getColumnCategories();
    tagColumnIndices = insertTabletStatement.getTagColumnIndices();
    attrColumnIndices = insertTabletStatement.getAttrColumnIndices();
    writeToTable = insertTabletStatement.isWriteToTable();
    databaseName = insertTabletStatement.getDatabaseName().orElse(null);
    // InsertTabletStatement
    times = insertTabletStatement.getTimes();
    nullBitMaps = insertTabletStatement.getBitMaps();
    columns = insertTabletStatement.getColumns();
    deviceIDs = insertTabletStatement.getRawTableDeviceIDs();
    singleDevice = insertTabletStatement.isSingleDevice();
    rowCount = insertTabletStatement.getRowCount();

    // To ensure that the measurement remains unchanged during the WAL writing process, the array
    // needs to be copied before the failed Measurement mark can be deleted.
    if (isCopyMeasurement) {
      final MeasurementSchema[] measurementSchemas = insertTabletStatement.getMeasurementSchemas();
      if (measurementSchemas != null) {
        this.measurementSchemas = Arrays.copyOf(measurementSchemas, measurementSchemas.length);
      }

      final String[] measurements = insertTabletStatement.getMeasurements();
      if (measurements != null) {
        this.measurements = Arrays.copyOf(measurements, measurements.length);
      }

      final TSDataType[] dataTypes = insertTabletStatement.getDataTypes();
      if (dataTypes != null) {
        this.dataTypes = Arrays.copyOf(dataTypes, dataTypes.length);
      }

      final Map<Integer, FailedMeasurementInfo> failedMeasurementIndex2Info =
          insertTabletStatement.getFailedMeasurementInfoMap();
      if (failedMeasurementIndex2Info != null) {
        this.failedMeasurementIndex2Info = new HashMap<>(failedMeasurementIndex2Info);
      }
    } else {
      this.measurementSchemas = insertTabletStatement.getMeasurementSchemas();
      this.measurements = insertTabletStatement.getMeasurements();
      this.dataTypes = insertTabletStatement.getDataTypes();
      this.failedMeasurementIndex2Info = insertTabletStatement.getFailedMeasurementInfoMap();
    }

    removeAllFailedMeasurementMarks();
  }

  public PipeConvertedInsertTabletStatement(final InsertTabletStatement insertTabletStatement) {
    this(insertTabletStatement, true);
  }

  @Override
  protected boolean checkAndCastDataType(int columnIndex, TSDataType dataType) {
    LOGGER.info(
        "Pipe: Inserting tablet to {}.{}. Casting type from {} to {}.",
        devicePath,
        measurements[columnIndex],
        dataTypes[columnIndex],
        dataType);
    columns[columnIndex] =
        ArrayConverter.convert(dataTypes[columnIndex], dataType, columns[columnIndex]);
    dataTypes[columnIndex] = dataType;
    return true;
  }

  protected boolean originalCheckAndCastDataType(int columnIndex, TSDataType dataType) {
    return super.checkAndCastDataType(columnIndex, dataType);
  }

  @TableModel
  @Override
  public boolean isForceTypeConversion() {
    return true;
  }
}
