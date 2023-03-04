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

package org.apache.iotdb.rpc;

import org.apache.iotdb.service.rpc.thrift.TSCloseOperationReq;
import org.apache.iotdb.service.rpc.thrift.TSFetchResultsReq;
import org.apache.iotdb.service.rpc.thrift.TSFetchResultsResp;
import org.apache.iotdb.service.rpc.thrift.TSIService;
import org.apache.iotdb.service.rpc.thrift.TSQueryDataSet;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.apache.thrift.TException;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class IoTDBJDBCDataSet {

  public static final String TIMESTAMP_STR = "Time";
  public static final String VALUE_IS_NULL = "The value got by %s (column name) is NULL.";
  public static final int START_INDEX = 2;
  public String sql;
  public boolean isClosed = false;
  public TSIService.Iface client;
  public List<String> columnNameList; // no deduplication
  public List<String> columnTypeList; // no deduplication
  public Map<String, Integer>
      columnOrdinalMap; // used because the server returns deduplicated columns
  public List<TSDataType> columnTypeDeduplicatedList; // deduplicated from columnTypeList
  public int fetchSize;
  public final long timeout;
  public boolean emptyResultSet = false;
  public boolean hasCachedRecord = false;
  public boolean lastReadWasNull;

  public byte[][] values; // used to cache the current row record value
  // column size
  public int columnSize;

  public long sessionId;
  public long queryId;
  public long statementId;
  public boolean ignoreTimeStamp;

  public int rowsIndex = 0; // used to record the row index in current TSQueryDataSet

  public TSQueryDataSet tsQueryDataSet = null;
  public byte[] time; // used to cache the current time value
  public byte[] currentBitmap; // used to cache the current bitmap for every column
  public static final int FLAG =
      0x80; // used to do `and` operation with bitmap to judge whether the value is null

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public IoTDBJDBCDataSet(
      String sql,
      List<String> columnNameList,
      List<String> columnTypeList,
      Map<String, Integer> columnNameIndex,
      boolean ignoreTimeStamp,
      long queryId,
      long statementId,
      TSIService.Iface client,
      long sessionId,
      TSQueryDataSet queryDataSet,
      int fetchSize,
      long timeout) {
    this.sessionId = sessionId;
    this.statementId = statementId;
    this.ignoreTimeStamp = ignoreTimeStamp;
    this.sql = sql;
    this.queryId = queryId;
    this.client = client;
    this.fetchSize = fetchSize;
    this.timeout = timeout;
    columnSize = columnNameList.size();

    this.columnNameList = new ArrayList<>();
    this.columnTypeList = new ArrayList<>();
    if (!ignoreTimeStamp) {
      this.columnNameList.add(TIMESTAMP_STR);
      this.columnTypeList.add(String.valueOf(TSDataType.INT64));
    }
    // deduplicate and map
    this.columnOrdinalMap = new HashMap<>();
    if (!ignoreTimeStamp) {
      this.columnOrdinalMap.put(TIMESTAMP_STR, 1);
    }

    // deduplicate and map
    if (columnNameIndex != null) {
      this.columnTypeDeduplicatedList = new ArrayList<>(columnNameIndex.size());
      for (int i = 0; i < columnNameIndex.size(); i++) {
        columnTypeDeduplicatedList.add(null);
      }
      for (int i = 0; i < columnNameList.size(); i++) {
        String name = columnNameList.get(i);
        this.columnNameList.add(name);
        this.columnTypeList.add(columnTypeList.get(i));
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
        this.columnTypeList.add(columnTypeList.get(i));
        if (!columnOrdinalMap.containsKey(name)) {
          columnOrdinalMap.put(name, index++);
          columnTypeDeduplicatedList.add(TSDataType.valueOf(columnTypeList.get(i)));
        }
      }
    }

    time = new byte[Long.BYTES];
    currentBitmap = new byte[columnTypeDeduplicatedList.size()];
    values = new byte[columnTypeDeduplicatedList.size()][];
    for (int i = 0; i < values.length; i++) {
      TSDataType dataType = columnTypeDeduplicatedList.get(i);
      switch (dataType) {
        case BOOLEAN:
          values[i] = new byte[1];
          break;
        case INT32:
          values[i] = new byte[Integer.BYTES];
          break;
        case INT64:
          values[i] = new byte[Long.BYTES];
          break;
        case FLOAT:
          values[i] = new byte[Float.BYTES];
          break;
        case DOUBLE:
          values[i] = new byte[Double.BYTES];
          break;
        case TEXT:
          values[i] = null;
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("Data type %s is not supported.", columnTypeDeduplicatedList.get(i)));
      }
    }
    this.tsQueryDataSet = queryDataSet;
    this.emptyResultSet = (queryDataSet == null || !queryDataSet.time.hasRemaining());
  }

  public IoTDBJDBCDataSet(
      String sql,
      List<String> columnNameList,
      List<String> columnTypeList,
      Map<String, Integer> columnNameIndex,
      boolean ignoreTimeStamp,
      long queryId,
      long statementId,
      TSIService.Iface client,
      long sessionId,
      TSQueryDataSet queryDataSet,
      int fetchSize,
      long timeout,
      List<String> sgList,
      BitSet aliasColumnMap) {
    this.sessionId = sessionId;
    this.statementId = statementId;
    this.ignoreTimeStamp = ignoreTimeStamp;
    this.sql = sql;
    this.queryId = queryId;
    this.client = client;
    this.fetchSize = fetchSize;
    this.timeout = timeout;
    columnSize = columnNameList.size();

    this.columnNameList = new ArrayList<>();
    this.columnTypeList = new ArrayList<>();
    if (!ignoreTimeStamp) {
      this.columnNameList.add(TIMESTAMP_STR);
      this.columnTypeList.add(String.valueOf(TSDataType.INT64));
    }
    // deduplicate and map
    this.columnOrdinalMap = new HashMap<>();
    if (!ignoreTimeStamp) {
      this.columnOrdinalMap.put(TIMESTAMP_STR, 1);
    }

    // deduplicate and map
    if (columnNameIndex != null) {
      this.columnTypeDeduplicatedList = new ArrayList<>(columnNameIndex.size());
      for (int i = 0; i < columnNameIndex.size(); i++) {
        columnTypeDeduplicatedList.add(null);
      }
      for (int i = 0; i < columnNameList.size(); i++) {
        String name = "";
        if (sgList != null
            && sgList.size() > 0
            && (aliasColumnMap == null || !aliasColumnMap.get(i))) {
          name = sgList.get(i) + "." + columnNameList.get(i);
        } else {
          name = columnNameList.get(i);
        }

        this.columnNameList.add(name);
        this.columnTypeList.add(columnTypeList.get(i));
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
        this.columnTypeList.add(columnTypeList.get(i));
        if (!columnOrdinalMap.containsKey(name)) {
          columnOrdinalMap.put(name, index++);
          columnTypeDeduplicatedList.add(TSDataType.valueOf(columnTypeList.get(i)));
        }
      }
    }

    time = new byte[Long.BYTES];
    currentBitmap = new byte[columnTypeDeduplicatedList.size()];
    values = new byte[columnTypeDeduplicatedList.size()][];
    for (int i = 0; i < values.length; i++) {
      TSDataType dataType = columnTypeDeduplicatedList.get(i);
      switch (dataType) {
        case BOOLEAN:
          values[i] = new byte[1];
          break;
        case INT32:
          values[i] = new byte[Integer.BYTES];
          break;
        case INT64:
          values[i] = new byte[Long.BYTES];
          break;
        case FLOAT:
          values[i] = new byte[Float.BYTES];
          break;
        case DOUBLE:
          values[i] = new byte[Double.BYTES];
          break;
        case TEXT:
          values[i] = null;
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("Data type %s is not supported.", columnTypeDeduplicatedList.get(i)));
      }
    }
    this.tsQueryDataSet = queryDataSet;
    this.emptyResultSet = (queryDataSet == null || !queryDataSet.time.hasRemaining());
  }

  public void close() throws StatementExecutionException, TException {
    if (isClosed) {
      return;
    }
    if (client != null) {
      try {
        TSCloseOperationReq closeReq = new TSCloseOperationReq(sessionId);
        closeReq.setStatementId(statementId);
        closeReq.setQueryId(queryId);
        TSStatus closeResp = client.closeOperation(closeReq);
        RpcUtils.verifySuccess(closeResp);
      } catch (StatementExecutionException e) {
        throw new StatementExecutionException(
            "Error occurs for close operation in server side because ", e);
      } catch (TException e) {
        throw new TException("Error occurs when connecting to server for close operation ", e);
      }
    }
    client = null;
    isClosed = true;
  }

  public boolean next() throws StatementExecutionException, IoTDBConnectionException {
    if (hasCachedResults()) {
      constructOneRow();
      return true;
    }
    if (emptyResultSet) {
      try {
        close();
        return false;
      } catch (TException e) {
        throw new IoTDBConnectionException(
            "Cannot close dataset, because of network connection: {} ", e);
      }
    }
    if (fetchResults() && hasCachedResults()) {
      constructOneRow();
      return true;
    } else {
      try {
        close();
        return false;
      } catch (TException e) {
        throw new IoTDBConnectionException(
            "Cannot close dataset, because of network connection: {} ", e);
      }
    }
  }

  public boolean fetchResults() throws StatementExecutionException, IoTDBConnectionException {
    rowsIndex = 0;
    TSFetchResultsReq req = new TSFetchResultsReq(sessionId, sql, fetchSize, queryId, true);
    req.setTimeout(timeout);
    try {
      TSFetchResultsResp resp = client.fetchResults(req);

      RpcUtils.verifySuccess(resp.getStatus());
      if (!resp.hasResultSet) {
        emptyResultSet = true;
        close();
      } else {
        tsQueryDataSet = resp.getQueryDataSet();
      }
      return resp.hasResultSet;
    } catch (TException e) {
      throw new IoTDBConnectionException(
          "Cannot fetch result from server, because of network connection: {} ", e);
    }
  }

  public boolean hasCachedResults() {
    return (tsQueryDataSet != null && tsQueryDataSet.time.hasRemaining());
  }

  public void constructOneRow() {
    tsQueryDataSet.time.get(time);
    for (int i = 0; i < tsQueryDataSet.bitmapList.size(); i++) {
      ByteBuffer bitmapBuffer = tsQueryDataSet.bitmapList.get(i);
      // another new 8 row, should move the bitmap buffer position to next byte
      if (rowsIndex % 8 == 0) {
        currentBitmap[i] = bitmapBuffer.get();
      }
      if (!isNull(i, rowsIndex)) {
        ByteBuffer valueBuffer = tsQueryDataSet.valueList.get(i);
        TSDataType dataType = columnTypeDeduplicatedList.get(i);
        switch (dataType) {
          case BOOLEAN:
          case INT32:
          case INT64:
          case FLOAT:
          case DOUBLE:
            valueBuffer.get(values[i]);
            break;
          case TEXT:
            int length = valueBuffer.getInt();
            values[i] = ReadWriteIOUtils.readBytes(valueBuffer, length);
            break;
          default:
            throw new UnSupportedDataTypeException(
                String.format("Data type %s is not supported.", columnTypeDeduplicatedList.get(i)));
        }
      }
    }
    rowsIndex++;
    hasCachedRecord = true;
  }

  public boolean isNull(int columnIndex) throws StatementExecutionException {
    int index = columnOrdinalMap.get(findColumnNameByIndex(columnIndex)) - START_INDEX;
    // time column will never be null
    if (index < 0) {
      return true;
    }
    return isNull(index, rowsIndex - 1);
  }

  public boolean isNull(String columnName) {
    int index = columnOrdinalMap.get(columnName) - START_INDEX;
    // time column will never be null
    if (index < 0) {
      return true;
    }
    return isNull(index, rowsIndex - 1);
  }

  private boolean isNull(int index, int rowNum) {
    byte bitmap = currentBitmap[index];
    int shift = rowNum % 8;
    return ((FLAG >>> shift) & (bitmap & 0xff)) == 0;
  }

  public boolean getBoolean(int columnIndex) throws StatementExecutionException {
    return getBoolean(findColumnNameByIndex(columnIndex));
  }

  public boolean getBoolean(String columnName) throws StatementExecutionException {
    checkRecord();
    int index = columnOrdinalMap.get(columnName) - START_INDEX;
    if (!isNull(index, rowsIndex - 1)) {
      lastReadWasNull = false;
      return BytesUtils.bytesToBool(values[index]);
    } else {
      lastReadWasNull = true;
      return false;
    }
  }

  public double getDouble(int columnIndex) throws StatementExecutionException {
    return getDouble(findColumnNameByIndex(columnIndex));
  }

  public double getDouble(String columnName) throws StatementExecutionException {
    checkRecord();
    int index = columnOrdinalMap.get(columnName) - START_INDEX;
    if (!isNull(index, rowsIndex - 1)) {
      lastReadWasNull = false;
      return BytesUtils.bytesToDouble(values[index]);
    } else {
      lastReadWasNull = true;
      return 0;
    }
  }

  public float getFloat(int columnIndex) throws StatementExecutionException {
    return getFloat(findColumnNameByIndex(columnIndex));
  }

  public float getFloat(String columnName) throws StatementExecutionException {
    checkRecord();
    int index = columnOrdinalMap.get(columnName) - START_INDEX;
    if (!isNull(index, rowsIndex - 1)) {
      lastReadWasNull = false;
      return BytesUtils.bytesToFloat(values[index]);
    } else {
      lastReadWasNull = true;
      return 0;
    }
  }

  public int getInt(int columnIndex) throws StatementExecutionException {
    return getInt(findColumnNameByIndex(columnIndex));
  }

  public int getInt(String columnName) throws StatementExecutionException {
    checkRecord();
    int index = columnOrdinalMap.get(columnName) - START_INDEX;
    if (!isNull(index, rowsIndex - 1)) {
      lastReadWasNull = false;
      return BytesUtils.bytesToInt(values[index]);
    } else {
      lastReadWasNull = true;
      return 0;
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
    if (!isNull(index, rowsIndex - 1)) {
      lastReadWasNull = false;
      return BytesUtils.bytesToLong(values[index]);
    } else {
      lastReadWasNull = true;
      return 0;
    }
  }

  public Object getObject(int columnIndex) throws StatementExecutionException {
    return getObject(findColumnNameByIndex(columnIndex));
  }

  public Object getObject(String columnName) throws StatementExecutionException {
    return getObjectByName(columnName);
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

  public String getValueByName(String columnName) throws StatementExecutionException {
    checkRecord();
    if (columnName.equals(TIMESTAMP_STR)) {
      return String.valueOf(BytesUtils.bytesToLong(time));
    }
    int index = columnOrdinalMap.get(columnName) - START_INDEX;
    if (index < 0 || index >= values.length || isNull(index, rowsIndex - 1)) {
      lastReadWasNull = true;
      return null;
    }
    lastReadWasNull = false;
    return getString(index, columnTypeDeduplicatedList.get(index), values);
  }

  public String getString(int index, TSDataType tsDataType, byte[][] values) {
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
        return new String(values[index], StandardCharsets.UTF_8);
      default:
        return null;
    }
  }

  public Object getObjectByName(String columnName) throws StatementExecutionException {
    checkRecord();
    if (columnName.equals(TIMESTAMP_STR)) {
      return BytesUtils.bytesToLong(time);
    }
    int index = columnOrdinalMap.get(columnName) - START_INDEX;
    if (index < 0 || index >= values.length || isNull(index, rowsIndex - 1)) {
      lastReadWasNull = true;
      return null;
    }
    lastReadWasNull = false;
    return getObject(index, columnTypeDeduplicatedList.get(index), values);
  }

  public Object getObject(int index, TSDataType tsDataType, byte[][] values) {
    switch (tsDataType) {
      case BOOLEAN:
        return BytesUtils.bytesToBool(values[index]);
      case INT32:
        return BytesUtils.bytesToInt(values[index]);
      case INT64:
        return BytesUtils.bytesToLong(values[index]);
      case FLOAT:
        return BytesUtils.bytesToFloat(values[index]);
      case DOUBLE:
        return BytesUtils.bytesToDouble(values[index]);
      case TEXT:
        return new String(values[index], StandardCharsets.UTF_8);
      default:
        return null;
    }
  }

  public String findColumnNameByIndex(int columnIndex) throws StatementExecutionException {
    if (columnIndex <= 0) {
      throw new StatementExecutionException("column index should start from 1");
    }
    if (columnIndex > columnNameList.size()) {
      throw new StatementExecutionException(
          String.format("column index %d out of range %d", columnIndex, columnNameList.size()));
    }
    return columnNameList.get(columnIndex - 1);
  }

  public void checkRecord() throws StatementExecutionException {
    if (Objects.isNull(tsQueryDataSet)) {
      throw new StatementExecutionException("No record remains");
    }
  }

  public void setTsQueryDataSet(TSQueryDataSet tsQueryDataSet) {
    this.tsQueryDataSet = tsQueryDataSet;
    this.emptyResultSet = (tsQueryDataSet == null || !tsQueryDataSet.time.hasRemaining());
  }
}
