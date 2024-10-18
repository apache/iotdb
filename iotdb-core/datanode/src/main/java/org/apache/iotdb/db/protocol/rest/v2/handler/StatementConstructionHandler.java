/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.protocol.rest.v2.handler;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.db.exception.metadata.DataTypeMismatchException;
import org.apache.iotdb.db.protocol.rest.utils.InsertRowDataUtils;
import org.apache.iotdb.db.protocol.rest.utils.InsertTabletDataUtils;
import org.apache.iotdb.db.protocol.rest.v2.model.InsertRecordsRequest;
import org.apache.iotdb.db.protocol.rest.v2.model.InsertTabletRequest;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeDevicePathCache;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.utils.TimestampPrecisionUtils;
import org.apache.iotdb.rpc.IoTDBConnectionException;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.BitMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class StatementConstructionHandler {
  private StatementConstructionHandler() {}

  public static InsertTabletStatement constructInsertTabletStatement(
      InsertTabletRequest insertTabletRequest) throws MetadataException {
    TimestampPrecisionUtils.checkTimestampPrecision(
        insertTabletRequest.getTimestamps().get(insertTabletRequest.getTimestamps().size() - 1));
    // construct insert statement
    InsertTabletStatement insertStatement = new InsertTabletStatement();
    insertStatement.setDevicePath(
        DataNodeDevicePathCache.getInstance().getPartialPath(insertTabletRequest.getDevice()));
    // check whether measurement is legal according to syntax convention
    // TODO: remove the check for table model
    insertStatement.setMeasurements(
        PathUtils.checkIsLegalSingleMeasurementsAndUpdate(insertTabletRequest.getMeasurements())
            .toArray(new String[0]));
    List<List<Object>> rawData = insertTabletRequest.getValues();
    List<String> rawDataType = insertTabletRequest.getDataTypes();

    int rowSize = insertTabletRequest.getTimestamps().size();
    int columnSize = rawDataType.size();

    BitMap[] bitMaps = new BitMap[columnSize];
    TSDataType[] dataTypes = new TSDataType[columnSize];

    for (int i = 0; i < columnSize; i++) {
      dataTypes[i] = TSDataType.valueOf(rawDataType.get(i).toUpperCase(Locale.ROOT));
    }
    insertStatement.setTimes(
        insertTabletRequest.getTimestamps().stream().mapToLong(Long::longValue).toArray());
    Object[] columns =
        InsertTabletDataUtils.genTabletValue(
            insertTabletRequest.getDevice(),
            insertStatement.getMeasurements(),
            insertStatement.getTimes(),
            dataTypes,
            rawData,
            bitMaps);

    insertStatement.setColumns(columns);
    insertStatement.setBitMaps(bitMaps);
    insertStatement.setRowCount(insertTabletRequest.getTimestamps().size());
    insertStatement.setDataTypes(dataTypes);
    insertStatement.setAligned(insertTabletRequest.getIsAligned());
    return insertStatement;
  }

  public static InsertRowsStatement createInsertRowsStatement(
      InsertRecordsRequest insertRecordsRequest)
      throws MetadataException, IoTDBConnectionException {

    // construct insert statement
    InsertRowsStatement insertStatement = new InsertRowsStatement();
    List<InsertRowStatement> insertRowStatementList = new ArrayList<>();
    List<List<TSDataType>> dataTypesList = new ArrayList<>();

    for (int i = 0; i < insertRecordsRequest.getDataTypesList().size(); i++) {
      List<TSDataType> dataTypes = new ArrayList<>();
      for (int c = 0; c < insertRecordsRequest.getDataTypesList().get(i).size(); c++) {
        dataTypes.add(
            TSDataType.valueOf(
                insertRecordsRequest.getDataTypesList().get(i).get(c).toUpperCase(Locale.ROOT)));
      }
      dataTypesList.add(dataTypes);
    }

    InsertRowDataUtils.filterNullValueAndMeasurement(
        insertRecordsRequest.getDevices(),
        insertRecordsRequest.getTimestamps(),
        insertRecordsRequest.getMeasurementsList(),
        insertRecordsRequest.getValuesList(),
        dataTypesList);

    for (int i = 0; i < insertRecordsRequest.getDevices().size(); i++) {
      InsertRowStatement statement = new InsertRowStatement();
      statement.setDevicePath(
          DataNodeDevicePathCache.getInstance()
              .getPartialPath(insertRecordsRequest.getDevices().get(i)));
      // TODO: remove the check for table model
      statement.setMeasurements(
          PathUtils.checkIsLegalSingleMeasurementsAndUpdate(
                  insertRecordsRequest.getMeasurementsList().get(i))
              .toArray(new String[0]));
      TimestampPrecisionUtils.checkTimestampPrecision(insertRecordsRequest.getTimestamps().get(i));
      statement.setTime(insertRecordsRequest.getTimestamps().get(i));
      statement.setDataTypes(dataTypesList.get(i).toArray(new TSDataType[0]));
      List<Integer> dataTypeMismatchInfo = new ArrayList<>();
      List<Object> values =
          InsertRowDataUtils.genRowValues(
              dataTypesList.get(i),
              insertRecordsRequest.getValuesList().get(i),
              dataTypeMismatchInfo);
      statement.setValues(values.toArray());
      statement.setAligned(insertRecordsRequest.getIsAligned());
      // skip empty statement
      if (statement.isEmpty()) {
        continue;
      }
      if (!dataTypeMismatchInfo.isEmpty()) {
        for (int index : dataTypeMismatchInfo) {
          String measurement = statement.getMeasurements()[index];
          TSDataType dataType = statement.getDataTypes()[index];
          // TODO: reimplement partial insert, the previous implementation will cause error when
          // dispatch remotely
          throw new DataTypeMismatchException(
              insertRecordsRequest.getDevices().get(i),
              measurement,
              dataType,
              statement.getTime(),
              insertRecordsRequest.getValuesList().get(i).get(index));
        }
      }
      insertRowStatementList.add(statement);
    }
    insertStatement.setInsertRowStatementList(insertRowStatementList);

    return insertStatement;
  }
}
