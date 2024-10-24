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

import org.apache.tsfile.enums.TSDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipeConvertedInsertTabletStatement extends InsertTabletStatement {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeConvertedInsertTabletStatement.class);

  public PipeConvertedInsertTabletStatement(final InsertTabletStatement insertTabletStatement) {
    super();
    // Statement
    isDebug = insertTabletStatement.isDebug();
    // InsertBaseStatement
    insertTabletStatement.removeAllFailedMeasurementMarks();
    devicePath = insertTabletStatement.getDevicePath();
    isAligned = insertTabletStatement.isAligned();
    measurementSchemas = insertTabletStatement.getMeasurementSchemas();
    measurements = insertTabletStatement.getMeasurements();
    dataTypes = insertTabletStatement.getDataTypes();
    // InsertTabletStatement
    times = insertTabletStatement.getTimes();
    nullBitMaps = insertTabletStatement.getBitMaps();
    columns = insertTabletStatement.getColumns();
    rowCount = insertTabletStatement.getRowCount();
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
}
