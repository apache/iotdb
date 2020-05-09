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
package org.apache.iotdb.session;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.service.rpc.thrift.TSCloseOperationReq;
import org.apache.iotdb.service.rpc.thrift.TSFetchResultsReq;
import org.apache.iotdb.service.rpc.thrift.TSFetchResultsResp;
import org.apache.iotdb.service.rpc.thrift.TSIService;
import org.apache.iotdb.service.rpc.thrift.TSQueryDataSet;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.thrift.TException;

public class SessionDataSet {

  private static final String TIMESTAMP_STR = "Time";
  private static final int START_INDEX = 2;
  private static final String VALUE_IS_NULL = "The value got by %s (column name) is NULL.";

  private boolean hasCachedRecord = false;
  // indicate that there is no more data
  private boolean emptyResultSet = false;
  private String sql;
  private long queryId;
  private long sessionId;
  private TSIService.Iface client;
  private int fetchSize = 1024;
  private List<String> columnNameList;
  protected List<TSDataType> columnTypeDeduplicatedList; // deduplicated from columnTypeList
  // column name -> column location
  private Map<String, Integer> columnOrdinalMap;
  // column size
  int columnSize;


  private int rowsIndex = 0; // used to record the row index in current TSQueryDataSet
  private TSQueryDataSet tsQueryDataSet;
  private byte[] currentBitmap; // used to cache the current bitmap for every column
  private static final int flag = 0x80; // used to do `or` operation with bitmap to judge whether the value is null

  private byte[] time; // used to cache the current time value
  private byte[][] values; // used to cache the current row record value


  public SessionDataSet(String sql, List<String> columnNameList, List<String> columnTypeList,
      Map<String, Integer> columnNameIndex,
      long queryId, TSIService.Iface client, long sessionId, TSQueryDataSet queryDataSet) {
    this.sessionId = sessionId;
    this.sql = sql;
    this.queryId = queryId;
    this.client = client;
    this.columnNameList = columnNameList;
    currentBitmap = new byte[columnNameList.size()];
    columnSize = columnNameList.size();

    this.columnNameList = new ArrayList<>();
    this.columnNameList.add(TIMESTAMP_STR);
    // deduplicate and map
    this.columnOrdinalMap = new HashMap<>();
    this.columnOrdinalMap.put(TIMESTAMP_STR, 1);


    // deduplicate and map
    if (columnNameIndex != null) {
      this.columnTypeDeduplicatedList = new ArrayList<>(columnNameIndex.size());
      for (int i = 0; i < columnNameIndex.size(); i++) {
        columnTypeDeduplicatedList.add(null);
      }
      for (int i = 0; i < columnNameList.size(); i++) {
        String name = columnNameList.get(i);
        this.columnNameList.add(name);
        if (!columnOrdinalMap.containsKey(name)) {
          int index = columnNameIndex.get(name);
          columnOrdinalMap.put(name, index + START_INDEX);
          columnTypeDeduplicatedList.set(index, TSDataType.valueOf(columnTypeList.get(i)));
        }
      }
    } else {
      this.columnTypeDeduplicatedList = new ArrayList<>();
      int index = START_INDEX;
      for (int i = 0; i < columnNameList.size(); i++) {
        String name = columnNameList.get(i);
        this.columnNameList.add(name);
        if (!columnOrdinalMap.containsKey(name)) {
          columnOrdinalMap.put(name, index++);
          columnTypeDeduplicatedList.add(TSDataType.valueOf(columnTypeList.get(i)));
        }
      }
    }

    time = new byte[Long.BYTES];
    values = new byte[columnNameList.size()][];
    this.tsQueryDataSet = queryDataSet;
  }

  public int getFetchSize() {
    return fetchSize;
  }

  public void setFetchSize(int fetchSize) {
    this.fetchSize = fetchSize;
  }

  public List<String> getColumnNames() {
    return columnNameList;
  }


  private boolean fetchResults() throws IoTDBConnectionException, StatementExecutionException {
    rowsIndex = 0;
    TSFetchResultsReq req = new TSFetchResultsReq(sessionId, sql, fetchSize, queryId, true);
    try {
      TSFetchResultsResp resp = client.fetchResults(req);

      RpcUtils.verifySuccess(resp.getStatus());
      if (!resp.hasResultSet) {
        emptyResultSet = true;
      } else {
        tsQueryDataSet = resp.getQueryDataSet();
      }
      return resp.hasResultSet;
    } catch (TException e) {
      throw new IoTDBConnectionException(
          "Cannot fetch result from server, because of network connection: {} ", e);
    }
  }

  private boolean hasCachedResults() {
    return (tsQueryDataSet != null && tsQueryDataSet.time.hasRemaining());
  }

  public boolean hasNext() throws StatementExecutionException, IoTDBConnectionException {

    if (hasCachedRecord) {
      return true;
    }

    if (hasCachedResults()) {
      constructOneRow();
      return true;
    }
    if (emptyResultSet) {
      return false;
    }
    if (fetchResults()) {
      constructOneRow();
      return true;
    }
    return false;
  }

  private void constructOneRow() {
    tsQueryDataSet.time.get(time);
    for (int i = 0; i < tsQueryDataSet.bitmapList.size(); i++) {
      ByteBuffer bitmapBuffer = tsQueryDataSet.bitmapList.get(i);
      // another new 8 row, should move the bitmap buffer position to next byte
      if (rowsIndex % 8 == 0) {
        currentBitmap[i] = bitmapBuffer.get();
      }
      values[i] = null;
      if (!isNull(i, rowsIndex)) {
        ByteBuffer valueBuffer = tsQueryDataSet.valueList.get(i);
        TSDataType dataType = columnTypeDeduplicatedList.get(i);
        switch (dataType) {
          case BOOLEAN:
            if (values[i] == null) {
              values[i] = new byte[1];
            }
            valueBuffer.get(values[i]);
            break;
          case INT32:
            if (values[i] == null) {
              values[i] = new byte[Integer.BYTES];
            }
            valueBuffer.get(values[i]);
            break;
          case INT64:
            if (values[i] == null) {
              values[i] = new byte[Long.BYTES];
            }
            valueBuffer.get(values[i]);
            break;
          case FLOAT:
            if (values[i] == null) {
              values[i] = new byte[Float.BYTES];
            }
            valueBuffer.get(values[i]);
            break;
          case DOUBLE:
            if (values[i] == null) {
              values[i] = new byte[Double.BYTES];
            }
            valueBuffer.get(values[i]);
            break;
          case TEXT:
            int length = valueBuffer.getInt();
            values[i] = ReadWriteIOUtils.readBytes(valueBuffer, length);
            break;
          default:
            throw new UnSupportedDataTypeException(
                String
                    .format("Data type %s is not supported.", columnTypeDeduplicatedList.get(i)));
        }
      }
    }
    rowsIndex++;
    hasCachedRecord = true;
  }


  private RowRecord constructRowRecordFromValueArray() {
    List<Field> outFields = new ArrayList<>();
    for (int i = 0; i < columnSize; i++) {
      Field field;

      int loc = columnOrdinalMap.get(columnNameList.get(i + 1)) - START_INDEX;
      byte[] valueBytes = values[loc];

      if (valueBytes != null) {
        TSDataType dataType = columnTypeDeduplicatedList.get(loc);
        field = new Field(dataType);
        switch (dataType) {
          case BOOLEAN:
            boolean booleanValue = BytesUtils.bytesToBool(valueBytes);
            field.setBoolV(booleanValue);
            break;
          case INT32:
            int intValue = BytesUtils.bytesToInt(valueBytes);
            field.setIntV(intValue);
            break;
          case INT64:
            long longValue = BytesUtils.bytesToLong(valueBytes);
            field.setLongV(longValue);
            break;
          case FLOAT:
            float floatValue = BytesUtils.bytesToFloat(valueBytes);
            field.setFloatV(floatValue);
            break;
          case DOUBLE:
            double doubleValue = BytesUtils.bytesToDouble(valueBytes);
            field.setDoubleV(doubleValue);
            break;
          case TEXT:
            field.setBinaryV(new Binary(valueBytes));
            break;
          default:
            throw new UnSupportedDataTypeException(String
                .format("Data type %s is not supported.", columnTypeDeduplicatedList.get(i)));
        }
      } else {
        field = new Field(null);
      }
      outFields.add(field);
    }
    return new RowRecord(BytesUtils.bytesToLong(time), outFields);
  }

  /**
   * judge whether the specified column value is null in the current position
   *
   * @param index column index
   */
  private boolean isNull(int index, int rowNum) {
    byte bitmap = currentBitmap[index];
    int shift = rowNum % 8;
    return ((flag >>> shift) & bitmap) == 0;
  }

  public RowRecord next() throws StatementExecutionException, IoTDBConnectionException {
    if (!hasCachedRecord) {
      if (!hasNext()) {
        return null;
      }
    }
    hasCachedRecord = false;

    return constructRowRecordFromValueArray();
  }

  public void closeOperationHandle() throws StatementExecutionException, IoTDBConnectionException {
    try {
      TSCloseOperationReq closeReq = new TSCloseOperationReq(sessionId);
      closeReq.setQueryId(queryId);
      TSStatus closeResp = client.closeOperation(closeReq);
      RpcUtils.verifySuccess(closeResp);
    } catch (TException e) {
      throw new IoTDBConnectionException(
          "Error occurs when connecting to server for close operation, because: " + e, e);
    }
  }

  public DataIterator iterator() {
    return new DataIterator();
  }

  public class DataIterator {

    public boolean next() throws StatementExecutionException, IoTDBConnectionException {
      if (hasCachedResults()) {
        constructOneRow();
        return true;
      }
      if (emptyResultSet) {
        return false;
      }
      if (fetchResults()) {
        constructOneRow();
        return true;
      }
      return false;
    }

    public boolean getBoolean(int columnIndex) throws StatementExecutionException {
      return getBoolean(findColumnNameByIndex(columnIndex));
    }

    public boolean getBoolean(String columnName) throws StatementExecutionException {
      checkRecord();
      int index = columnOrdinalMap.get(columnName) - START_INDEX;
      if (values[index] != null) {
        return BytesUtils.bytesToBool(values[index]);
      } else {
        throw new StatementExecutionException(String.format(VALUE_IS_NULL, columnName));
      }
    }

    public double getDouble(int columnIndex) throws StatementExecutionException {
      return getDouble(findColumnNameByIndex(columnIndex));
    }

    public double getDouble(String columnName) throws StatementExecutionException {
      checkRecord();
      int index = columnOrdinalMap.get(columnName) - START_INDEX;
      if (values[index] != null) {
        return BytesUtils.bytesToDouble(values[index]);
      } else {
        throw new StatementExecutionException(String.format(VALUE_IS_NULL, columnName));
      }
    }

    public float getFloat(int columnIndex) throws StatementExecutionException {
      return getFloat(findColumnNameByIndex(columnIndex));
    }

    public float getFloat(String columnName) throws StatementExecutionException {
      checkRecord();
      int index = columnOrdinalMap.get(columnName) - START_INDEX;
      if (values[index] != null) {
        return BytesUtils.bytesToFloat(values[index]);
      } else {
        throw new StatementExecutionException(String.format(VALUE_IS_NULL, columnName));
      }
    }

    public int getInt(int columnIndex) throws StatementExecutionException {
      return getInt(findColumnNameByIndex(columnIndex));
    }

    public int getInt(String columnName) throws StatementExecutionException {
      checkRecord();
      int index = columnOrdinalMap.get(columnName) - START_INDEX;
      if (values[index] != null) {
        return BytesUtils.bytesToInt(values[index]);
      } else {
        throw new StatementExecutionException(String.format(VALUE_IS_NULL, columnName));
      }
    }

    public long getLong(int columnIndex) throws StatementExecutionException {
      return getLong(findColumnNameByIndex(columnIndex));
    }

    public long getLong(String columnName) throws StatementExecutionException {
      checkRecord();
      if (columnName.equals(TIMESTAMP_STR)) {
        return BytesUtils.bytesToLong(time);
      }
      int index = columnOrdinalMap.get(columnName) - START_INDEX;
      if (values[index] != null) {
        return BytesUtils.bytesToLong(values[index]);
      } else {
        throw new StatementExecutionException(String.format(VALUE_IS_NULL, columnName));
      }
    }

    public Object getObject(int columnIndex) throws StatementExecutionException {
      return getObject(findColumnNameByIndex(columnIndex));
    }

    public Object getObject(String columnName) throws StatementExecutionException {
      return getValueByName(columnName);
    }

    public String getString(int columnIndex) throws StatementExecutionException {
      return getString(findColumnNameByIndex(columnIndex));
    }

    public String getString(String columnName) throws StatementExecutionException {
      return getValueByName(columnName);
    }

    public Timestamp getTimestamp(int columnIndex) throws StatementExecutionException {
      return new Timestamp(getLong(columnIndex));
    }

    public Timestamp getTimestamp(String columnName) throws StatementExecutionException {
      return getTimestamp(findColumn(columnName));
    }

    public int findColumn(String columnName) {
      return columnOrdinalMap.get(columnName);
    }

    private String getValueByName(String columnName) throws StatementExecutionException {
      checkRecord();
      if (columnName.equals(TIMESTAMP_STR)) {
        return String.valueOf(BytesUtils.bytesToLong(time));
      }
      int index = columnOrdinalMap.get(columnName) - START_INDEX;
      if (index < 0 || index >= values.length || values[index] == null) {
        return null;
      }
      return getString(index, columnTypeDeduplicatedList.get(index), values);
    }

    protected String getString(int index, TSDataType tsDataType, byte[][] values) {
      switch (tsDataType) {
        case BOOLEAN:
          return String.valueOf(BytesUtils.bytesToBool(values[index]));
        case INT32:
          return String.valueOf(BytesUtils.bytesToInt(values[index]));
        case INT64:
          return String.valueOf(BytesUtils.bytesToLong(values[index]));
        case FLOAT:
          return String.valueOf(BytesUtils.bytesToFloat(values[index]));
        case DOUBLE:
          return String.valueOf(BytesUtils.bytesToDouble(values[index]));
        case TEXT:
          return new String(values[index]);
        default:
          return null;
      }
    }

    private void checkRecord() throws StatementExecutionException {
      if (Objects.isNull(tsQueryDataSet)) {
        throw new StatementExecutionException("No record remains");
      }
    }
  }

  private String findColumnNameByIndex(int columnIndex) throws StatementExecutionException {
    if (columnIndex <= 0) {
      throw new StatementExecutionException("column index should start from 1");
    }
    if (columnIndex > columnNameList.size()) {
      throw new StatementExecutionException(
          String.format("column index %d out of range %d", columnIndex, columnNameList.size()));
    }
    return columnNameList.get(columnIndex - 1);
  }
}
