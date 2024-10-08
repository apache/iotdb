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
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.service.rpc.thrift.IClientRPCService;

import org.apache.thrift.TException;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SessionDataSet implements ISessionDataSet {

  private final IoTDBRpcDataSet ioTDBRpcDataSet;

  @SuppressWarnings("squid:S107") // ignore Methods should not have too many parameters
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
      boolean moreData,
      ZoneId zoneId,
      int timeFactor,
      boolean tableModel,
      List<Integer> columnIndex2TsBlockColumnIndexList) {
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
            0,
            zoneId,
            RpcUtils.DEFAULT_TIME_FORMAT,
            timeFactor,
            tableModel,
            columnIndex2TsBlockColumnIndexList);
  }

  @SuppressWarnings("squid:S107") // ignore Methods should not have too many parameters
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
      int fetchSize,
      ZoneId zoneId,
      int timeFactor,
      boolean tableModel,
      List<Integer> columnIndex2TsBlockColumnIndexList) {
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
            timeout,
            zoneId,
            RpcUtils.DEFAULT_TIME_FORMAT,
            timeFactor,
            tableModel,
            columnIndex2TsBlockColumnIndexList);
  }

  public int getFetchSize() {
    return ioTDBRpcDataSet.getFetchSize();
  }

  public void setFetchSize(int fetchSize) {
    ioTDBRpcDataSet.setFetchSize(fetchSize);
  }

  @Override
  public List<String> getColumnNames() {
    return new ArrayList<>(ioTDBRpcDataSet.getColumnNameList());
  }

  @Override
  public List<String> getColumnTypes() {
    return new ArrayList<>(ioTDBRpcDataSet.getColumnTypeList());
  }

  @Override
  public boolean hasNext() throws StatementExecutionException, IoTDBConnectionException {
    if (ioTDBRpcDataSet.hasCachedRecord()) {
      return true;
    } else {
      return ioTDBRpcDataSet.next();
    }
  }

  private RowRecord constructRowRecordFromValueArray() throws StatementExecutionException {
    List<Field> outFields = new ArrayList<>();
    for (int i = ioTDBRpcDataSet.getValueColumnStartIndex();
        i < ioTDBRpcDataSet.getColumnSize();
        i++) {
      Field field;

      String columnName = ioTDBRpcDataSet.getColumnNameList().get(i);

      if (!ioTDBRpcDataSet.isNull(columnName)) {
        TSDataType dataType = ioTDBRpcDataSet.getDataType(columnName);
        field = new Field(dataType);
        switch (dataType) {
          case BOOLEAN:
            boolean booleanValue = ioTDBRpcDataSet.getBoolean(columnName);
            field.setBoolV(booleanValue);
            break;
          case INT32:
          case DATE:
            int intValue = ioTDBRpcDataSet.getInt(columnName);
            field.setIntV(intValue);
            break;
          case INT64:
          case TIMESTAMP:
            long longValue = ioTDBRpcDataSet.getLong(columnName);
            field.setLongV(longValue);
            break;
          case FLOAT:
            float floatValue = ioTDBRpcDataSet.getFloat(columnName);
            field.setFloatV(floatValue);
            break;
          case DOUBLE:
            double doubleValue = ioTDBRpcDataSet.getDouble(columnName);
            field.setDoubleV(doubleValue);
            break;
          case TEXT:
          case BLOB:
          case STRING:
            field.setBinaryV(ioTDBRpcDataSet.getBinary(columnName));
            break;
          default:
            throw new UnSupportedDataTypeException(
                String.format("Data type %s is not supported.", dataType));
        }
      } else {
        field = new Field(null);
      }
      outFields.add(field);
    }
    return new RowRecord(ioTDBRpcDataSet.getCurrentRowTime(), outFields);
  }

  /**
   * Iterate ResultSet using this method isn't very efficient, because it will use RowRecord to
   * represent a row which contains much object creation and converting overhead If you just want to
   * get each value of each column row by row, you can use SessionDataSet.iterator() to get
   * DataIterator, and use DataIterator.getXXX() function to get current row's specified column
   * value.
   *
   * @return One complete row saved in RowRecord
   */
  @Override
  public RowRecord next() throws StatementExecutionException, IoTDBConnectionException {
    if (!ioTDBRpcDataSet.hasCachedRecord() && !hasNext()) {
      return null;
    }
    ioTDBRpcDataSet.setHasCachedRecord(false);

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

    public boolean isNull(String columnName) {
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

    public List<String> getColumnNameList() {
      return ioTDBRpcDataSet.getColumnNameList();
    }

    public List<String> getColumnTypeList() {
      return ioTDBRpcDataSet.getColumnTypeList();
    }
  }
}
