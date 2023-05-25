/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.isession;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.IoTDBRpcDataSet;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.service.rpc.thrift.IClientRPCService;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import org.apache.thrift.TException;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.rpc.IoTDBRpcDataSet.START_INDEX;

public class SessionDataSet implements AutoCloseable {

  private final IoTDBRpcDataSet ioTDBRpcDataSet;

  public SessionDataSet(
      String sql,
      List<String> columnNameList,
      List<String> columnTypeList,
      Map<String, Integer> columnNameIndex,
      long queryId,
      long statementId,
      IClientRPCService.Iface client,
      long sessionId,
      List<ByteBuffer> queryResult,
      boolean ignoreTimeStamp,
      boolean moreData) {
    this.ioTDBRpcDataSet =
        new IoTDBRpcDataSet(
            sql,
            columnNameList,
            columnTypeList,
            columnNameIndex,
            ignoreTimeStamp,
            moreData,
            queryId,
            statementId,
            client,
            sessionId,
            queryResult,
            SessionConfig.DEFAULT_FETCH_SIZE,
            0);
  }

  public SessionDataSet(
      String sql,
      List<String> columnNameList,
      List<String> columnTypeList,
      Map<String, Integer> columnNameIndex,
      long queryId,
      long statementId,
      IClientRPCService.Iface client,
      long sessionId,
      List<ByteBuffer> queryResult,
      boolean ignoreTimeStamp,
      long timeout,
      boolean moreData) {
    this.ioTDBRpcDataSet =
        new IoTDBRpcDataSet(
            sql,
            columnNameList,
            columnTypeList,
            columnNameIndex,
            ignoreTimeStamp,
            moreData,
            queryId,
            statementId,
            client,
            sessionId,
            queryResult,
            SessionConfig.DEFAULT_FETCH_SIZE,
            timeout);
  }

  public SessionDataSet(
      String sql,
      List<String> columnNameList,
      List<String> columnTypeList,
      Map<String, Integer> columnNameIndex,
      long queryId,
      long statementId,
      IClientRPCService.Iface client,
      long sessionId,
      List<ByteBuffer> queryResult,
      boolean ignoreTimeStamp,
      long timeout,
      boolean moreData,
      int fetchSize) {
    this.ioTDBRpcDataSet =
        new IoTDBRpcDataSet(
            sql,
            columnNameList,
            columnTypeList,
            columnNameIndex,
            ignoreTimeStamp,
            moreData,
            queryId,
            statementId,
            client,
            sessionId,
            queryResult,
            fetchSize,
            timeout);
  }

  public int getFetchSize() {
    return ioTDBRpcDataSet.fetchSize;
  }

  public void setFetchSize(int fetchSize) {
    ioTDBRpcDataSet.fetchSize = fetchSize;
  }

  public List<String> getColumnNames() {
    return new ArrayList<>(ioTDBRpcDataSet.columnNameList);
  }

  public List<String> getColumnTypes() {
    return new ArrayList<>(ioTDBRpcDataSet.columnTypeList);
  }

  public boolean hasNext() throws StatementExecutionException, IoTDBConnectionException {
    return ioTDBRpcDataSet.next();
  }

  private RowRecord constructRowRecordFromValueArray() throws StatementExecutionException {
    List<Field> outFields = new ArrayList<>();
    for (int i = 0; i < ioTDBRpcDataSet.columnSize; i++) {
      Field field;

      int index = i + 1;
      int datasetColumnIndex = i + START_INDEX;
      if (ioTDBRpcDataSet.ignoreTimeStamp) {
        index--;
        datasetColumnIndex--;
      }
      int loc =
          ioTDBRpcDataSet.columnOrdinalMap.get(ioTDBRpcDataSet.columnNameList.get(index))
              - START_INDEX;

      if (!ioTDBRpcDataSet.isNull(datasetColumnIndex)) {
        TSDataType dataType = ioTDBRpcDataSet.columnTypeDeduplicatedList.get(loc);
        field = new Field(dataType);
        switch (dataType) {
          case BOOLEAN:
            boolean booleanValue = ioTDBRpcDataSet.getBoolean(datasetColumnIndex);
            field.setBoolV(booleanValue);
            break;
          case INT32:
            int intValue = ioTDBRpcDataSet.getInt(datasetColumnIndex);
            field.setIntV(intValue);
            break;
          case INT64:
            long longValue = ioTDBRpcDataSet.getLong(datasetColumnIndex);
            field.setLongV(longValue);
            break;
          case FLOAT:
            float floatValue = ioTDBRpcDataSet.getFloat(datasetColumnIndex);
            field.setFloatV(floatValue);
            break;
          case DOUBLE:
            double doubleValue = ioTDBRpcDataSet.getDouble(datasetColumnIndex);
            field.setDoubleV(doubleValue);
            break;
          case TEXT:
            field.setBinaryV(ioTDBRpcDataSet.getBinary(datasetColumnIndex));
            break;
          default:
            throw new UnSupportedDataTypeException(
                String.format(
                    "Data type %s is not supported.",
                    ioTDBRpcDataSet.columnTypeDeduplicatedList.get(i)));
        }
      } else {
        field = new Field(null);
      }
      outFields.add(field);
    }
    return new RowRecord(ioTDBRpcDataSet.time, outFields);
  }

  public RowRecord next() throws StatementExecutionException, IoTDBConnectionException {
    if (!ioTDBRpcDataSet.hasCachedRecord && !hasNext()) {
      return null;
    }
    ioTDBRpcDataSet.hasCachedRecord = false;

    return constructRowRecordFromValueArray();
  }

  public void closeOperationHandle() throws StatementExecutionException, IoTDBConnectionException {
    try {
      ioTDBRpcDataSet.close();
    } catch (TException e) {
      throw new IoTDBConnectionException(e.getMessage());
    }
  }

  public DataIterator iterator() {
    return new DataIterator();
  }

  @Override
  public void close() throws IoTDBConnectionException, StatementExecutionException {
    closeOperationHandle();
  }

  public class DataIterator {

    public boolean next() throws StatementExecutionException, IoTDBConnectionException {
      return ioTDBRpcDataSet.next();
    }

    public boolean isNull(int columnIndex) throws StatementExecutionException {
      return ioTDBRpcDataSet.isNull(columnIndex);
    }

    public boolean isNull(String columnName) throws StatementExecutionException {
      return ioTDBRpcDataSet.isNull(columnName);
    }

    public boolean getBoolean(int columnIndex) throws StatementExecutionException {
      return ioTDBRpcDataSet.getBoolean(columnIndex);
    }

    public boolean getBoolean(String columnName) throws StatementExecutionException {
      return ioTDBRpcDataSet.getBoolean(columnName);
    }

    public double getDouble(int columnIndex) throws StatementExecutionException {
      return ioTDBRpcDataSet.getDouble(columnIndex);
    }

    public double getDouble(String columnName) throws StatementExecutionException {
      return ioTDBRpcDataSet.getDouble(columnName);
    }

    public float getFloat(int columnIndex) throws StatementExecutionException {
      return ioTDBRpcDataSet.getFloat(columnIndex);
    }

    public float getFloat(String columnName) throws StatementExecutionException {
      return ioTDBRpcDataSet.getFloat(columnName);
    }

    public int getInt(int columnIndex) throws StatementExecutionException {
      return ioTDBRpcDataSet.getInt(columnIndex);
    }

    public int getInt(String columnName) throws StatementExecutionException {
      return ioTDBRpcDataSet.getInt(columnName);
    }

    public long getLong(int columnIndex) throws StatementExecutionException {
      return ioTDBRpcDataSet.getLong(columnIndex);
    }

    public long getLong(String columnName) throws StatementExecutionException {
      return ioTDBRpcDataSet.getLong(columnName);
    }

    public Object getObject(int columnIndex) throws StatementExecutionException {
      return ioTDBRpcDataSet.getObject(columnIndex);
    }

    public Object getObject(String columnName) throws StatementExecutionException {
      return ioTDBRpcDataSet.getObject(columnName);
    }

    public String getString(int columnIndex) throws StatementExecutionException {
      return ioTDBRpcDataSet.getString(columnIndex);
    }

    public String getString(String columnName) throws StatementExecutionException {
      return ioTDBRpcDataSet.getString(columnName);
    }

    public Timestamp getTimestamp(int columnIndex) throws StatementExecutionException {
      return ioTDBRpcDataSet.getTimestamp(columnIndex);
    }

    public Timestamp getTimestamp(String columnName) throws StatementExecutionException {
      return ioTDBRpcDataSet.getTimestamp(columnName);
    }

    public int findColumn(String columnName) {
      return ioTDBRpcDataSet.findColumn(columnName);
    }
  }
}
