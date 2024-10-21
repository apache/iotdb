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
import org.apache.iotdb.db.exception.WriteProcessRejectException;
import org.apache.iotdb.db.exception.metadata.DataTypeMismatchException;
import org.apache.iotdb.db.protocol.rest.utils.InsertRowDataUtils;
import org.apache.iotdb.db.protocol.rest.v2.model.InsertRecordsRequest;
import org.apache.iotdb.db.protocol.rest.v2.model.InsertTabletRequest;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeDevicePathCache;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.utils.TimestampPrecisionUtils;
import org.apache.iotdb.rpc.IoTDBConnectionException;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class StatementConstructionHandler {
  private StatementConstructionHandler() {}

  public static InsertTabletStatement constructInsertTabletStatement(
      InsertTabletRequest insertTabletRequest)
      throws MetadataException, WriteProcessRejectException {
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

    Object[] columns = new Object[columnSize];
    BitMap[] bitMaps = new BitMap[columnSize];
    TSDataType[] dataTypes = new TSDataType[columnSize];

    for (int i = 0; i < columnSize; i++) {
      dataTypes[i] = TSDataType.valueOf(rawDataType.get(i).toUpperCase(Locale.ROOT));
    }

    for (int columnIndex = 0; columnIndex < columnSize; columnIndex++) {
      bitMaps[columnIndex] = new BitMap(rowSize);
      switch (dataTypes[columnIndex]) {
        case BOOLEAN:
          boolean[] booleanValues = new boolean[rowSize];
          for (int rowIndex = 0; rowIndex < rowSize; rowIndex++) {
            Object data = rawData.get(columnIndex).get(rowIndex);
            if (data == null) {
              bitMaps[columnIndex].mark(rowIndex);
            } else {
              if ("1".equals(data.toString())) {
                booleanValues[rowIndex] = true;
              } else if ("0".equals(data.toString())) {
                booleanValues[rowIndex] = false;
              } else {
                booleanValues[rowIndex] = (Boolean) data;
              }
            }
          }
          columns[columnIndex] = booleanValues;
          break;
        case INT32:
        case DATE:
          int[] intValues = new int[rowSize];
          for (int rowIndex = 0; rowIndex < rowSize; rowIndex++) {
            Object object = rawData.get(columnIndex).get(rowIndex);
            if (object == null) {
              bitMaps[columnIndex].mark(rowIndex);
            } else if (object instanceof Integer) {
              intValues[rowIndex] = (int) object;
            } else {
              throw new WriteProcessRejectException(
                  "unsupported data type: " + object.getClass().toString());
            }
          }
          columns[columnIndex] = intValues;
          break;
        case INT64:
        case TIMESTAMP:
          long[] longValues = new long[rowSize];
          for (int rowIndex = 0; rowIndex < rowSize; rowIndex++) {
            Object object = rawData.get(columnIndex).get(rowIndex);
            if (object == null) {
              bitMaps[columnIndex].mark(rowIndex);
            } else if (object instanceof Integer) {
              longValues[rowIndex] = (int) object;
            } else if (object instanceof Long) {
              longValues[rowIndex] = (long) object;
            } else {
              throw new WriteProcessRejectException(
                  "unsupported data type: " + object.getClass().toString());
            }
          }
          columns[columnIndex] = longValues;
          break;
        case FLOAT:
          float[] floatValues = new float[rowSize];
          for (int rowIndex = 0; rowIndex < rowSize; rowIndex++) {
            Object data = rawData.get(columnIndex).get(rowIndex);
            if (data == null) {
              bitMaps[columnIndex].mark(rowIndex);
            } else {
              floatValues[rowIndex] = Float.parseFloat(String.valueOf(data));
            }
          }
          columns[columnIndex] = floatValues;
          break;
        case DOUBLE:
          double[] doubleValues = new double[rowSize];
          for (int rowIndex = 0; rowIndex < rowSize; rowIndex++) {
            if (rawData.get(columnIndex).get(rowIndex) == null) {
              bitMaps[columnIndex].mark(rowIndex);
            } else {
              doubleValues[rowIndex] =
                  Double.parseDouble(String.valueOf(rawData.get(columnIndex).get(rowIndex)));
            }
          }
          columns[columnIndex] = doubleValues;
          break;
        case TEXT:
        case BLOB:
        case STRING:
          Binary[] binaryValues = new Binary[rowSize];
          for (int rowIndex = 0; rowIndex < rowSize; rowIndex++) {
            if (rawData.get(columnIndex).get(rowIndex) == null) {
              bitMaps[columnIndex].mark(rowIndex);
              binaryValues[rowIndex] = new Binary("".getBytes(StandardCharsets.UTF_8));
            } else {
              binaryValues[rowIndex] =
                  new Binary(
                      rawData
                          .get(columnIndex)
                          .get(rowIndex)
                          .toString()
                          .getBytes(StandardCharsets.UTF_8));
            }
          }
          columns[columnIndex] = binaryValues;
          break;
        default:
          throw new IllegalArgumentException("Invalid input: " + rawDataType.get(columnIndex));
      }
    }

    insertStatement.setTimes(
        insertTabletRequest.getTimestamps().stream().mapToLong(Long::longValue).toArray());
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
      Map<Integer, Object> dataTypeMismatchInfo = new HashMap<>();
      List<Object> values =
          InsertRowDataUtils.reGenValues(
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
        for (Map.Entry<Integer, Object> entry : dataTypeMismatchInfo.entrySet()) {
          int index = entry.getKey();
          String measurement = statement.getMeasurements()[index];
          TSDataType dataType = statement.getDataTypes()[index];
          statement.markFailedMeasurement(
              index,
              new DataTypeMismatchException(
                  insertRecordsRequest.getDevices().get(i),
                  statement.getMeasurements()[index],
                  statement.getDataTypes()[index],
                  statement.getTime(),
                  entry.getValue()));
          // markFailedMeasurement will set datatype and measurements null
          // setting them back in order to pass the schema validation
          statement.getDataTypes()[index] = dataType;
          statement.getMeasurements()[index] = measurement;
        }
      }
      insertRowStatementList.add(statement);
    }
    insertStatement.setInsertRowStatementList(insertRowStatementList);

    return insertStatement;
  }
}
