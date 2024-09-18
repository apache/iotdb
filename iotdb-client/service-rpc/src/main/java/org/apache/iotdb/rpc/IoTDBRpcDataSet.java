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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.service.rpc.thrift.IClientRPCService;
import org.apache.iotdb.service.rpc.thrift.TSCloseOperationReq;
import org.apache.iotdb.service.rpc.thrift.TSFetchResultsReq;
import org.apache.iotdb.service.rpc.thrift.TSFetchResultsResp;

import org.apache.thrift.TException;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.TsBlockSerde;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.DateUtils;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.rpc.RpcUtils.convertToTimestamp;
import static org.apache.iotdb.rpc.RpcUtils.getTimePrecision;

public class IoTDBRpcDataSet {

  private static final String TIMESTAMP_STR = "Time";
  private static final TsBlockSerde SERDE = new TsBlockSerde();

  private final String sql;
  private boolean isClosed = false;
  private IClientRPCService.Iface client;
  private final List<String> columnNameList; // no deduplication
  private final List<String> columnTypeList; // no deduplication
  private final Map<String, Integer>
      columnOrdinalMap; // used because the server returns deduplicated columns
  private final Map<String, Integer> columnName2TsBlockColumnIndexMap;

  // column index -> TsBlock column index
  private final List<Integer> columnIndex2TsBlockColumnIndexList;

  private final List<TSDataType> dataTypeForTsBlockColumn;
  private int fetchSize;
  private final long timeout;
  private boolean hasCachedRecord = false;
  private boolean lastReadWasNull;

  private final long sessionId;
  private final long queryId;
  private final long statementId;
  private long time;
  private final boolean ignoreTimeStamp;
  // indicates that there is still more data in server side and we can call fetchResult to get more
  private boolean moreData;

  private List<ByteBuffer> queryResult;
  private TsBlock curTsBlock;
  private int queryResultSize; // the length of queryResult
  private int queryResultIndex; // the index of bytebuffer in queryResult
  private int tsBlockSize; // the size of current tsBlock
  private int tsBlockIndex; // the row index in current tsBlock
  private final ZoneId zoneId;
  private final String timeFormat;

  private final int timeFactor;

  private final String timePrecision;

  @SuppressWarnings({"squid:S3776", "squid:S107"}) // Suppress high Cognitive Complexity warning
  public IoTDBRpcDataSet(
      String sql,
      List<String> columnNameList,
      List<String> columnTypeList,
      Map<String, Integer> columnNameIndex,
      boolean ignoreTimeStamp,
      boolean moreData,
      long queryId,
      long statementId,
      IClientRPCService.Iface client,
      long sessionId,
      List<ByteBuffer> queryResult,
      int fetchSize,
      long timeout,
      ZoneId zoneId,
      String timeFormat,
      int timeFactor,
      boolean tableModel,
      List<Integer> columnIndex2TsBlockColumnIndexList) {
    this.sessionId = sessionId;
    this.statementId = statementId;
    // only used for tree model, table model this field will always be true
    this.ignoreTimeStamp = ignoreTimeStamp;
    this.sql = sql;
    this.queryId = queryId;
    this.client = client;
    this.fetchSize = fetchSize;
    this.timeout = timeout;
    this.moreData = moreData;

    this.columnNameList = new ArrayList<>();
    this.columnTypeList = new ArrayList<>();
    this.columnOrdinalMap = new HashMap<>();
    this.columnName2TsBlockColumnIndexMap = new HashMap<>();
    int columnStartIndex = 1;
    int resultSetColumnSize = columnNameList.size();

    // newly generated or updated columnIndex2TsBlockColumnIndexList.size() may not be equal to
    // columnNameList.size()
    // so we need startIndexForColumnIndex2TsBlockColumnIndexList to adjust the mapping relation
    int startIndexForColumnIndex2TsBlockColumnIndexList = 0;

    // for Time Column in tree model which should always be the first column and its index for
    // TsBlockColumn is -1
    if (!ignoreTimeStamp) {
      this.columnNameList.add(TIMESTAMP_STR);
      this.columnTypeList.add(String.valueOf(TSDataType.INT64));
      this.columnName2TsBlockColumnIndexMap.put(TIMESTAMP_STR, -1);
      this.columnOrdinalMap.put(TIMESTAMP_STR, 1);
      if (columnIndex2TsBlockColumnIndexList != null) {
        columnIndex2TsBlockColumnIndexList.add(0, -1);
        startIndexForColumnIndex2TsBlockColumnIndexList = 1;
      }
      columnStartIndex++;
      resultSetColumnSize++;
    }

    if (columnIndex2TsBlockColumnIndexList == null) {
      columnIndex2TsBlockColumnIndexList = new ArrayList<>(resultSetColumnSize);
      if (!ignoreTimeStamp) {
        startIndexForColumnIndex2TsBlockColumnIndexList = 1;
        columnIndex2TsBlockColumnIndexList.add(-1);
      }
      for (int i = 0, size = columnNameList.size(); i < size; i++) {
        columnIndex2TsBlockColumnIndexList.add(i);
      }
    }

    int tsBlockColumnSize =
        columnIndex2TsBlockColumnIndexList.stream().mapToInt(Integer::intValue).max().orElse(0) + 1;
    this.dataTypeForTsBlockColumn = new ArrayList<>(tsBlockColumnSize);
    for (int i = 0; i < tsBlockColumnSize; i++) {
      dataTypeForTsBlockColumn.add(null);
    }

    for (int i = 0, size = columnNameList.size(); i < size; i++) {
      String name = columnNameList.get(i);
      this.columnNameList.add(name);
      this.columnTypeList.add(columnTypeList.get(i));
      int tsBlockColumnIndex =
          columnIndex2TsBlockColumnIndexList.get(
              startIndexForColumnIndex2TsBlockColumnIndexList + i);
      if (tsBlockColumnIndex != -1) {
        TSDataType columnType = TSDataType.valueOf(columnTypeList.get(i));
        dataTypeForTsBlockColumn.set(tsBlockColumnIndex, columnType);
      }
      if (!columnName2TsBlockColumnIndexMap.containsKey(name)) {
        columnOrdinalMap.put(name, i + columnStartIndex);
        columnName2TsBlockColumnIndexMap.put(name, tsBlockColumnIndex);
      }
    }

    this.queryResult = queryResult;
    this.queryResultSize = 0;
    if (queryResult != null) {
      queryResultSize = queryResult.size();
    }
    this.queryResultIndex = 0;
    this.tsBlockSize = 0;
    this.tsBlockIndex = -1;
    this.zoneId = zoneId;
    this.timeFormat = timeFormat;
    this.timeFactor = timeFactor;
    this.timePrecision = getTimePrecision(timeFactor);

    if (columnIndex2TsBlockColumnIndexList.size() != this.columnNameList.size()) {
      throw new IllegalArgumentException(
          String.format(
              "Size of columnIndex2TsBlockColumnIndexList %s doesn't equal to size of columnNameList %s.",
              columnIndex2TsBlockColumnIndexList.size(), this.columnNameList.size()));
    }
    this.columnIndex2TsBlockColumnIndexList = columnIndex2TsBlockColumnIndexList;
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
    if (hasCachedBlock()) {
      lastReadWasNull = false;
      constructOneRow();
      return true;
    }
    if (hasCachedByteBuffer()) {
      constructOneTsBlock();
      constructOneRow();
      return true;
    }

    if (moreData && fetchResults() && hasCachedByteBuffer()) {
      constructOneTsBlock();
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
    TSFetchResultsReq req = new TSFetchResultsReq(sessionId, sql, fetchSize, queryId, true);
    req.setTimeout(timeout);
    try {
      TSFetchResultsResp resp = client.fetchResultsV2(req);
      RpcUtils.verifySuccess(resp.getStatus());
      moreData = resp.moreData;
      if (!resp.hasResultSet) {
        close();
      } else {
        queryResult = resp.getQueryResult();
        queryResultIndex = 0;
        queryResultSize = 0;
        if (queryResult != null) {
          queryResultSize = queryResult.size();
        }
        this.tsBlockSize = 0;
        this.tsBlockIndex = -1;
      }
      return resp.hasResultSet;
    } catch (TException e) {
      throw new IoTDBConnectionException(
          "Cannot fetch result from server, because of network connection: {} ", e);
    }
  }

  public boolean hasCachedBlock() {
    return (curTsBlock != null && tsBlockIndex < tsBlockSize - 1);
  }

  public boolean hasCachedByteBuffer() {
    return (queryResult != null && queryResultIndex < queryResultSize);
  }

  public void constructOneRow() {
    tsBlockIndex++;
    hasCachedRecord = true;
    time = curTsBlock.getTimeColumn().getLong(tsBlockIndex);
  }

  public void constructOneTsBlock() {
    lastReadWasNull = false;
    ByteBuffer byteBuffer = queryResult.get(queryResultIndex);
    queryResultIndex++;
    curTsBlock = SERDE.deserialize(byteBuffer);
    tsBlockIndex = -1;
    tsBlockSize = curTsBlock.getPositionCount();
  }

  public boolean isNull(int columnIndex) throws StatementExecutionException {
    return isNull(getTsBlockColumnIndexForColumnIndex(columnIndex), tsBlockIndex);
  }

  public boolean isNull(String columnName) {
    return isNull(getTsBlockColumnIndexForColumnName(columnName), tsBlockIndex);
  }

  private boolean isNull(int index, int rowNum) {
    // -1 for time column which will never be null
    return index >= 0 && curTsBlock.getColumn(index).isNull(rowNum);
  }

  public boolean getBoolean(int columnIndex) throws StatementExecutionException {
    return getBooleanByTsBlockColumnIndex(getTsBlockColumnIndexForColumnIndex(columnIndex));
  }

  public boolean getBoolean(String columnName) throws StatementExecutionException {
    return getBooleanByTsBlockColumnIndex(getTsBlockColumnIndexForColumnName(columnName));
  }

  private boolean getBooleanByTsBlockColumnIndex(int tsBlockColumnIndex)
      throws StatementExecutionException {
    checkRecord();
    if (!isNull(tsBlockColumnIndex, tsBlockIndex)) {
      lastReadWasNull = false;
      return curTsBlock.getColumn(tsBlockColumnIndex).getBoolean(tsBlockIndex);
    } else {
      lastReadWasNull = true;
      return false;
    }
  }

  public double getDouble(int columnIndex) throws StatementExecutionException {
    return getDoubleByTsBlockColumnIndex(getTsBlockColumnIndexForColumnIndex(columnIndex));
  }

  public double getDouble(String columnName) throws StatementExecutionException {
    return getDoubleByTsBlockColumnIndex(getTsBlockColumnIndexForColumnName(columnName));
  }

  private double getDoubleByTsBlockColumnIndex(int tsBlockColumnIndex)
      throws StatementExecutionException {
    checkRecord();
    if (!isNull(tsBlockColumnIndex, tsBlockIndex)) {
      lastReadWasNull = false;
      return curTsBlock.getColumn(tsBlockColumnIndex).getDouble(tsBlockIndex);
    } else {
      lastReadWasNull = true;
      return 0;
    }
  }

  public float getFloat(int columnIndex) throws StatementExecutionException {
    return getFloatByTsBlockColumnIndex(getTsBlockColumnIndexForColumnIndex(columnIndex));
  }

  public float getFloat(String columnName) throws StatementExecutionException {
    return getFloatByTsBlockColumnIndex(getTsBlockColumnIndexForColumnName(columnName));
  }

  private float getFloatByTsBlockColumnIndex(int tsBlockColumnIndex)
      throws StatementExecutionException {
    checkRecord();
    if (!isNull(tsBlockColumnIndex, tsBlockIndex)) {
      lastReadWasNull = false;
      return curTsBlock.getColumn(tsBlockColumnIndex).getFloat(tsBlockIndex);
    } else {
      lastReadWasNull = true;
      return 0;
    }
  }

  public int getInt(int columnIndex) throws StatementExecutionException {
    return getIntByTsBlockColumnIndex(getTsBlockColumnIndexForColumnIndex(columnIndex));
  }

  public int getInt(String columnName) throws StatementExecutionException {
    return getIntByTsBlockColumnIndex(getTsBlockColumnIndexForColumnName(columnName));
  }

  private int getIntByTsBlockColumnIndex(int tsBlockColumnIndex)
      throws StatementExecutionException {
    checkRecord();
    if (!isNull(tsBlockColumnIndex, tsBlockIndex)) {
      lastReadWasNull = false;
      TSDataType type = curTsBlock.getColumn(tsBlockColumnIndex).getDataType();
      if (type == TSDataType.INT64) {
        return (int) curTsBlock.getColumn(tsBlockColumnIndex).getLong(tsBlockIndex);
      } else {
        return curTsBlock.getColumn(tsBlockColumnIndex).getInt(tsBlockIndex);
      }
    } else {
      lastReadWasNull = true;
      return 0;
    }
  }

  public long getLong(int columnIndex) throws StatementExecutionException {
    return getLongByTsBlockColumnIndex(getTsBlockColumnIndexForColumnIndex(columnIndex));
  }

  public long getLong(String columnName) throws StatementExecutionException {
    int index = getTsBlockColumnIndexForColumnName(columnName);
    return getLongByTsBlockColumnIndex(index);
  }

  private long getLongByTsBlockColumnIndex(int tsBlockColumnIndex)
      throws StatementExecutionException {
    checkRecord();

    // take care of time column
    if (tsBlockColumnIndex < 0) {
      lastReadWasNull = false;
      return curTsBlock.getTimeByIndex(tsBlockIndex);
    } else {
      if (!isNull(tsBlockColumnIndex, tsBlockIndex)) {
        lastReadWasNull = false;
        TSDataType type = curTsBlock.getColumn(tsBlockColumnIndex).getDataType();
        if (type == TSDataType.INT32) {
          return curTsBlock.getColumn(tsBlockColumnIndex).getInt(tsBlockIndex);
        } else {
          return curTsBlock.getColumn(tsBlockColumnIndex).getLong(tsBlockIndex);
        }
      } else {
        lastReadWasNull = true;
        return 0;
      }
    }
  }

  public Binary getBinary(int columIndex) throws StatementExecutionException {
    return getBinaryTsBlockColumnIndex(getTsBlockColumnIndexForColumnIndex(columIndex));
  }

  public Binary getBinary(String columnName) throws StatementExecutionException {
    return getBinaryTsBlockColumnIndex(getTsBlockColumnIndexForColumnName(columnName));
  }

  private Binary getBinaryTsBlockColumnIndex(int tsBlockColumnIndex)
      throws StatementExecutionException {
    checkRecord();
    if (!isNull(tsBlockColumnIndex, tsBlockIndex)) {
      lastReadWasNull = false;
      return curTsBlock.getColumn(tsBlockColumnIndex).getBinary(tsBlockIndex);
    } else {
      lastReadWasNull = true;
      return null;
    }
  }

  public Object getObject(int columnIndex) throws StatementExecutionException {
    return getObjectByTsBlockIndex(getTsBlockColumnIndexForColumnIndex(columnIndex));
  }

  public Object getObject(String columnName) throws StatementExecutionException {
    return getObjectByTsBlockIndex(getTsBlockColumnIndexForColumnName(columnName));
  }

  private Object getObjectByTsBlockIndex(int tsBlockColumnIndex)
      throws StatementExecutionException {
    checkRecord();
    if (isNull(tsBlockColumnIndex, tsBlockIndex)) {
      lastReadWasNull = true;
      return null;
    }
    lastReadWasNull = false;
    TSDataType tsDataType = getDataTypeByTsBlockColumnIndex(tsBlockColumnIndex);
    switch (tsDataType) {
      case BOOLEAN:
      case INT32:
      case INT64:
      case FLOAT:
      case DOUBLE:
        return curTsBlock.getColumn(tsBlockColumnIndex).getObject(tsBlockIndex);
      case TIMESTAMP:
        long timestamp =
            (tsBlockColumnIndex == -1
                ? curTsBlock.getTimeByIndex(tsBlockIndex)
                : curTsBlock.getColumn(tsBlockColumnIndex).getLong(tsBlockIndex));
        return convertToTimestamp(timestamp, timeFactor);
      case TEXT:
      case STRING:
        return curTsBlock
            .getColumn(tsBlockColumnIndex)
            .getBinary(tsBlockIndex)
            .getStringValue(TSFileConfig.STRING_CHARSET);
      case BLOB:
        return BytesUtils.parseBlobByteArrayToString(
            curTsBlock.getColumn(tsBlockColumnIndex).getBinary(tsBlockIndex).getValues());
      case DATE:
        return DateUtils.formatDate(curTsBlock.getColumn(tsBlockColumnIndex).getInt(tsBlockIndex));
      default:
        return null;
    }
  }

  public String getString(int columnIndex) throws StatementExecutionException {
    return getStringByTsBlockColumnIndex(getTsBlockColumnIndexForColumnIndex(columnIndex));
  }

  public String getString(String columnName) throws StatementExecutionException {
    return getStringByTsBlockColumnIndex(getTsBlockColumnIndexForColumnName(columnName));
  }

  private String getStringByTsBlockColumnIndex(int tsBlockColumnIndex)
      throws StatementExecutionException {
    checkRecord();
    // to keep compatibility, tree model should return a long value for time column
    if (tsBlockColumnIndex == -1) {
      return String.valueOf(curTsBlock.getTimeByIndex(tsBlockIndex));
    }
    if (isNull(tsBlockColumnIndex, tsBlockIndex)) {
      lastReadWasNull = true;
      return null;
    }
    lastReadWasNull = false;
    return getString(tsBlockColumnIndex, getDataTypeByTsBlockColumnIndex(tsBlockColumnIndex));
  }

  private String getString(int index, TSDataType tsDataType) {
    switch (tsDataType) {
      case BOOLEAN:
        return String.valueOf(curTsBlock.getColumn(index).getBoolean(tsBlockIndex));
      case INT32:
        return String.valueOf(curTsBlock.getColumn(index).getInt(tsBlockIndex));
      case INT64:
        return String.valueOf(
            (index == -1
                ? curTsBlock.getTimeByIndex(tsBlockIndex)
                : curTsBlock.getColumn(index).getLong(tsBlockIndex)));
      case TIMESTAMP:
        long timestamp =
            (index == -1
                ? curTsBlock.getTimeByIndex(tsBlockIndex)
                : curTsBlock.getColumn(index).getLong(tsBlockIndex));
        return RpcUtils.formatDatetime(timeFormat, timePrecision, timestamp, zoneId);
      case FLOAT:
        return String.valueOf(curTsBlock.getColumn(index).getFloat(tsBlockIndex));
      case DOUBLE:
        return String.valueOf(curTsBlock.getColumn(index).getDouble(tsBlockIndex));
      case TEXT:
      case STRING:
        return curTsBlock
            .getColumn(index)
            .getBinary(tsBlockIndex)
            .getStringValue(TSFileConfig.STRING_CHARSET);
      case BLOB:
        return BytesUtils.parseBlobByteArrayToString(
            curTsBlock.getColumn(index).getBinary(tsBlockIndex).getValues());
      case DATE:
        return DateUtils.formatDate(curTsBlock.getColumn(index).getInt(tsBlockIndex));
      default:
        return null;
    }
  }

  public Timestamp getTimestamp(int columnIndex) throws StatementExecutionException {
    return getTimestampByTsBlockColumnIndex(getTsBlockColumnIndexForColumnIndex(columnIndex));
  }

  public Timestamp getTimestamp(String columnName) throws StatementExecutionException {
    return getTimestampByTsBlockColumnIndex(getTsBlockColumnIndexForColumnName(columnName));
  }

  private Timestamp getTimestampByTsBlockColumnIndex(int tsBlockColumnIndex)
      throws StatementExecutionException {
    long timestamp = getLongByTsBlockColumnIndex(tsBlockColumnIndex);
    return convertToTimestamp(timestamp, timeFactor);
  }

  public TSDataType getDataType(int columnIndex) {
    return getDataTypeByTsBlockColumnIndex(getTsBlockColumnIndexForColumnIndex(columnIndex));
  }

  public TSDataType getDataType(String columnName) {
    return getDataTypeByTsBlockColumnIndex(getTsBlockColumnIndexForColumnName(columnName));
  }

  private TSDataType getDataTypeByTsBlockColumnIndex(int tsBlockColumnIndex) {
    return tsBlockColumnIndex < 0
        ? TSDataType.TIMESTAMP
        : dataTypeForTsBlockColumn.get(tsBlockColumnIndex);
  }

  public int findColumn(String columnName) {
    return columnOrdinalMap.get(columnName);
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

  // return -1 for time column of tree model
  private int getTsBlockColumnIndexForColumnName(String columnName) {
    Integer index = columnName2TsBlockColumnIndexMap.get(columnName);
    if (index == null) {
      throw new IllegalArgumentException("Unknown column name: " + columnName);
    }
    return index;
  }

  private int getTsBlockColumnIndexForColumnIndex(int columnIndex) {
    return columnIndex2TsBlockColumnIndexList.get(columnIndex - 1);
  }

  public void checkRecord() throws StatementExecutionException {
    if (queryResultIndex > queryResultSize
        || tsBlockIndex >= tsBlockSize
        || queryResult == null
        || curTsBlock == null) {
      throw new StatementExecutionException("No record remains");
    }
  }

  public int getValueColumnStartIndex() {
    return ignoreTimeStamp ? 0 : 1;
  }

  public int getColumnSize() {
    return columnNameList.size();
  }

  public List<String> getColumnTypeList() {
    return columnTypeList;
  }

  public List<String> getColumnNameList() {
    return columnNameList;
  }

  public boolean isClosed() {
    return isClosed;
  }

  public int getFetchSize() {
    return fetchSize;
  }

  public void setFetchSize(int fetchSize) {
    this.fetchSize = fetchSize;
  }

  public boolean hasCachedRecord() {
    return hasCachedRecord;
  }

  public void setHasCachedRecord(boolean hasCachedRecord) {
    this.hasCachedRecord = hasCachedRecord;
  }

  public boolean isLastReadWasNull() {
    return lastReadWasNull;
  }

  public long getCurrentRowTime() {
    return time;
  }

  public boolean isIgnoreTimeStamp() {
    return ignoreTimeStamp;
  }
}
