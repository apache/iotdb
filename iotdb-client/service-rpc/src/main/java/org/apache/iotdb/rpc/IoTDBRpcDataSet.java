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
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.rpc.RpcUtils.convertToTimestamp;
import static org.apache.iotdb.rpc.RpcUtils.getTimePrecision;

public class IoTDBRpcDataSet {

  public static final String TIMESTAMP_STR = "Time";
  public String sql;
  public boolean isClosed = false;
  public IClientRPCService.Iface client;
  public List<String> columnNameList; // no deduplication
  public List<String> columnTypeList; // no deduplication
  private final Map<String, Integer>
      columnOrdinalMap; // used because the server returns deduplicated columns
  private final Map<String, Integer> columnName2TsBlockColumnIndexMap;
  private final List<TSDataType> columnTypeDeduplicatedList; // deduplicated from columnTypeList
  public int fetchSize;
  public final long timeout;
  public boolean hasCachedRecord = false;
  public boolean lastReadWasNull;

  public long sessionId;
  public long queryId;
  public long statementId;
  public long time;
  public boolean ignoreTimeStamp;
  // indicates that there is still more data in server side and we can call fetchResult to get more
  public boolean moreData;

  public static final TsBlockSerde serde = new TsBlockSerde();
  public List<ByteBuffer> queryResult;
  public TsBlock curTsBlock;
  public int queryResultSize; // the length of queryResult
  public int queryResultIndex; // the index of bytebuffer in queryResult
  public int tsBlockSize; // the size of current tsBlock
  public int tsBlockIndex; // the row index in current tsBlock
  private final ZoneId zoneId;
  private final String timeFormat;

  private final int timeFactor;

  private final String timePrecision;

  // 2 for tree model and 1 for table model
  private final int startIndex;

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
      boolean tableModel) {
    this.startIndex = tableModel ? 1 : 2;
    this.sessionId = sessionId;
    this.statementId = statementId;
    this.ignoreTimeStamp = ignoreTimeStamp;
    this.sql = sql;
    this.queryId = queryId;
    this.client = client;
    this.fetchSize = fetchSize;
    this.timeout = timeout;
    this.moreData = moreData;

    this.columnNameList = new ArrayList<>();
    this.columnTypeList = new ArrayList<>();
    if (!ignoreTimeStamp) {
      this.columnNameList.add(TIMESTAMP_STR);
      this.columnTypeList.add(String.valueOf(TSDataType.INT64));
    }
    // deduplicate and map
    this.columnOrdinalMap = new HashMap<>();
    this.columnName2TsBlockColumnIndexMap = new HashMap<>();
    if (!ignoreTimeStamp) {
      this.columnName2TsBlockColumnIndexMap.put(TIMESTAMP_STR, -1);
      this.columnOrdinalMap.put(TIMESTAMP_STR, 1);
    }

    // deduplicate and map
    if (columnNameIndex != null) {
      int deduplicatedColumnSize =
          columnNameIndex.values().stream().mapToInt(Integer::intValue).max().orElse(0) + 1;
      this.columnTypeDeduplicatedList = new ArrayList<>(deduplicatedColumnSize);
      for (int i = 0; i < deduplicatedColumnSize; i++) {
        columnTypeDeduplicatedList.add(null);
      }
      for (int i = 0; i < columnNameList.size(); i++) {
        String name = columnNameList.get(i);
        this.columnNameList.add(name);
        this.columnTypeList.add(columnTypeList.get(i));
        if (!columnName2TsBlockColumnIndexMap.containsKey(name)) {
          int index = columnNameIndex.get(name);
          if (columnTypeDeduplicatedList.get(index) == null) {
            columnTypeDeduplicatedList.set(index, TSDataType.valueOf(columnTypeList.get(i)));
          }
          columnOrdinalMap.put(name, i + startIndex);
          columnName2TsBlockColumnIndexMap.put(name, index);
        }
      }
    } else {
      this.columnTypeDeduplicatedList = new ArrayList<>();
      AtomicInteger index = new AtomicInteger(startIndex);
      for (int i = 0; i < columnNameList.size(); i++) {
        String name = columnNameList.get(i);
        this.columnNameList.add(name);
        String columnType = columnTypeList.get(i);
        this.columnTypeList.add(columnType);
        Integer ordinal =
            columnOrdinalMap.computeIfAbsent(
                name, v -> addColumnTypeListReturnIndex(index, TSDataType.valueOf(columnType)));
        columnName2TsBlockColumnIndexMap.put(name, ordinal - startIndex);
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
  }

  public Integer addColumnTypeListReturnIndex(AtomicInteger index, TSDataType dataType) {
    columnTypeDeduplicatedList.add(dataType);
    return index.getAndIncrement();
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
    curTsBlock = serde.deserialize(byteBuffer);
    tsBlockIndex = -1;
    tsBlockSize = curTsBlock.getPositionCount();
  }

  public boolean isNull(int columnIndex) throws StatementExecutionException {
    return isNull(findColumnNameByIndex(columnIndex));
  }

  public boolean isNull(String columnName) {
    return isNull(getTsBlockColumnIndex(columnName), tsBlockIndex);
  }

  private boolean isNull(int index, int rowNum) {
    // -1 for time column which will never be null
    return index >= 0 && curTsBlock.getColumn(index).isNull(rowNum);
  }

  public boolean getBoolean(int columnIndex) throws StatementExecutionException {
    return getBoolean(findColumnNameByIndex(columnIndex));
  }

  public boolean getBoolean(String columnName) throws StatementExecutionException {
    checkRecord();
    int index = getTsBlockColumnIndex(columnName);
    if (!isNull(index, tsBlockIndex)) {
      lastReadWasNull = false;
      return curTsBlock.getColumn(index).getBoolean(tsBlockIndex);
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
    int index = getTsBlockColumnIndex(columnName);
    if (!isNull(index, tsBlockIndex)) {
      lastReadWasNull = false;
      return curTsBlock.getColumn(index).getDouble(tsBlockIndex);
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
    int index = getTsBlockColumnIndex(columnName);
    if (!isNull(index, tsBlockIndex)) {
      lastReadWasNull = false;
      return curTsBlock.getColumn(index).getFloat(tsBlockIndex);
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
    int index = getTsBlockColumnIndex(columnName);
    if (!isNull(index, tsBlockIndex)) {
      lastReadWasNull = false;
      TSDataType type = curTsBlock.getColumn(index).getDataType();
      if (type == TSDataType.INT64) {
        return (int) curTsBlock.getColumn(index).getLong(tsBlockIndex);
      } else {
        return curTsBlock.getColumn(index).getInt(tsBlockIndex);
      }
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
    int index = getTsBlockColumnIndex(columnName);

    // take care of time column
    if (index < 0) {
      lastReadWasNull = false;
      return curTsBlock.getTimeByIndex(tsBlockIndex);
    } else {
      if (!isNull(index, tsBlockIndex)) {
        lastReadWasNull = false;
        TSDataType type = curTsBlock.getColumn(index).getDataType();
        if (type == TSDataType.INT32) {
          return curTsBlock.getColumn(index).getInt(tsBlockIndex);
        } else {
          return curTsBlock.getColumn(index).getLong(tsBlockIndex);
        }
      } else {
        lastReadWasNull = true;
        return 0;
      }
    }
  }

  public Binary getBinary(int columIndex) throws StatementExecutionException {
    return getBinary(findColumnNameByIndex(columIndex));
  }

  public Binary getBinary(String columnName) throws StatementExecutionException {
    checkRecord();
    int index = getTsBlockColumnIndex(columnName);
    if (!isNull(index, tsBlockIndex)) {
      lastReadWasNull = false;
      return curTsBlock.getColumn(index).getBinary(tsBlockIndex);
    } else {
      lastReadWasNull = true;
      return null;
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
    return getTimestamp(findColumnNameByIndex(columnIndex));
  }

  public Timestamp getTimestamp(String columnName) throws StatementExecutionException {
    return convertToTimestamp(getLong(columnName), timeFactor);
  }

  public TSDataType getDataType(int columnIndex) throws StatementExecutionException {
    return getDataType(findColumnNameByIndex(columnIndex));
  }

  public TSDataType getDataType(String columnName) {
    final int index = getTsBlockColumnIndex(columnName);
    if (index == -1) {
      return TSDataType.TIMESTAMP;
    } else if (index >= 0 && index < columnTypeDeduplicatedList.size()) {
      return columnTypeDeduplicatedList.get(index);
    } else {
      return null;
    }
  }

  public int findColumn(String columnName) {
    return columnOrdinalMap.get(columnName);
  }

  public String getValueByName(String columnName) throws StatementExecutionException {
    checkRecord();
    // to keep compatibility, tree model should return a long value for time column
    if (startIndex == 2 && columnName.equals(TIMESTAMP_STR)) {
      return String.valueOf(curTsBlock.getTimeByIndex(tsBlockIndex));
    }
    int index = getTsBlockColumnIndex(columnName);
    if (isNull(index, tsBlockIndex)) {
      lastReadWasNull = true;
      return null;
    }
    lastReadWasNull = false;
    return getString(index, getDataTypeByTsBlockColumnIndex(index));
  }

  public String getString(int index, TSDataType tsDataType) {
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

  public Object getObjectByName(String columnName) throws StatementExecutionException {
    checkRecord();
    int index = getTsBlockColumnIndex(columnName);
    if (isNull(index, tsBlockIndex)) {
      lastReadWasNull = true;
      return null;
    }
    lastReadWasNull = false;
    TSDataType tsDataType = getDataTypeByTsBlockColumnIndex(index);
    switch (tsDataType) {
      case BOOLEAN:
      case INT32:
      case INT64:
      case FLOAT:
      case DOUBLE:
        return curTsBlock.getColumn(index).getObject(tsBlockIndex);
      case TIMESTAMP:
        long timestamp =
            (index == -1
                ? curTsBlock.getTimeByIndex(tsBlockIndex)
                : curTsBlock.getColumn(index).getLong(tsBlockIndex));
        return convertToTimestamp(timestamp, timeFactor);
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

  private TSDataType getDataTypeByTsBlockColumnIndex(int tsBlockColumnIndex) {
    return tsBlockColumnIndex < 0
        ? TSDataType.TIMESTAMP
        : columnTypeDeduplicatedList.get(tsBlockColumnIndex);
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
  private int getTsBlockColumnIndex(String columnName) {
    Integer index = columnName2TsBlockColumnIndexMap.get(columnName);
    if (index == null) {
      throw new IllegalArgumentException("Unknown column name :" + columnName);
    }
    return index;
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
}
