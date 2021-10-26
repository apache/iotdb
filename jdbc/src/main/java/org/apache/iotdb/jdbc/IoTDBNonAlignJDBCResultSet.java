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

package org.apache.iotdb.jdbc;

import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.service.rpc.thrift.TSFetchResultsReq;
import org.apache.iotdb.service.rpc.thrift.TSFetchResultsResp;
import org.apache.iotdb.service.rpc.thrift.TSIService;
import org.apache.iotdb.service.rpc.thrift.TSQueryNonAlignDataSet;
import org.apache.iotdb.service.rpc.thrift.TSTracingInfo;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.apache.thrift.TException;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.iotdb.rpc.IoTDBRpcDataSet.START_INDEX;
import static org.apache.iotdb.rpc.IoTDBRpcDataSet.TIMESTAMP_STR;

public class IoTDBNonAlignJDBCResultSet extends AbstractIoTDBJDBCResultSet {

  private static final int TIMESTAMP_STR_LENGTH = 4;
  private static final String EMPTY_STR = "";
  private String operationType = "";
  private TSQueryNonAlignDataSet tsQueryNonAlignDataSet;
  private byte[][] times; // used for disable align
  private List<String> sgColumns = null;
  // for disable align clause
  IoTDBNonAlignJDBCResultSet(
      Statement statement,
      List<String> columnNameList,
      List<String> columnTypeList,
      Map<String, Integer> columnNameIndex,
      boolean ignoreTimeStamp,
      TSIService.Iface client,
      String sql,
      long queryId,
      long sessionId,
      TSQueryNonAlignDataSet dataset,
      TSTracingInfo tracingInfo,
      long timeout,
      String operationType,
      List<String> sgColumns,
      BitSet aliasColumnMap)
      throws SQLException {
    super(
        statement,
        columnNameList,
        columnTypeList,
        columnNameIndex,
        ignoreTimeStamp,
        client,
        sql,
        queryId,
        sessionId,
        timeout,
        sgColumns,
        aliasColumnMap);
    times = new byte[columnNameList.size()][Long.BYTES];
    this.operationType = operationType;
    ioTDBRpcDataSet.columnNameList = new ArrayList<>();
    ioTDBRpcDataSet.columnTypeList = new ArrayList<>();
    // deduplicate and map
    ioTDBRpcDataSet.columnOrdinalMap = new HashMap<>();
    ioTDBRpcDataSet.columnOrdinalMap.put(TIMESTAMP_STR, 1);
    ioTDBRpcDataSet.columnTypeDeduplicatedList = new ArrayList<>();
    ioTDBRpcDataSet.columnTypeDeduplicatedList = new ArrayList<>(columnNameIndex.size());
    for (int i = 0; i < columnNameIndex.size(); i++) {
      ioTDBRpcDataSet.columnTypeDeduplicatedList.add(null);
    }
    List<String> newSgColumns = new ArrayList<>();
    for (int i = 0; i < columnNameList.size(); i++) {
      String name = "";
      if (sgColumns != null && sgColumns.size() > 0) {
        name = sgColumns.get(i) + "." + columnNameList.get(i);
        newSgColumns.add(sgColumns.get(i));
        newSgColumns.add(sgColumns.get(i));
      } else {
        name = columnNameList.get(i);
        newSgColumns.add("");
        newSgColumns.add("");
      }
      ioTDBRpcDataSet.columnNameList.add(TIMESTAMP_STR + name);
      ioTDBRpcDataSet.columnNameList.add(name);
      ioTDBRpcDataSet.columnTypeList.add(String.valueOf(TSDataType.INT64));
      ioTDBRpcDataSet.columnTypeList.add(columnTypeList.get(i));
      if (!ioTDBRpcDataSet.columnOrdinalMap.containsKey(name)) {
        int index = columnNameIndex.get(name);
        ioTDBRpcDataSet.columnOrdinalMap.put(name, index + START_INDEX);
        ioTDBRpcDataSet.columnTypeDeduplicatedList.set(
            index, TSDataType.valueOf(columnTypeList.get(i)));
      }
    }
    this.sgColumns = newSgColumns;
    this.tsQueryNonAlignDataSet = dataset;
    if (tracingInfo != null) {
      ioTDBRpcTracingInfo = new IoTDBTracingInfo();
      ioTDBRpcTracingInfo.setTsTracingInfo(tracingInfo);
    }
  }

  @Override
  public long getLong(String columnName) throws SQLException {
    checkRecord();
    if (columnName.startsWith(TIMESTAMP_STR)) {
      String column = columnName.substring(TIMESTAMP_STR_LENGTH);
      int index = ioTDBRpcDataSet.columnOrdinalMap.get(column) - START_INDEX;
      if (times[index] != null) {
        ioTDBRpcDataSet.lastReadWasNull = false;
        return BytesUtils.bytesToLong(times[index]);
      } else {
        ioTDBRpcDataSet.lastReadWasNull = true;
        return 0;
      }
    }
    int index = ioTDBRpcDataSet.columnOrdinalMap.get(columnName) - START_INDEX;
    if (ioTDBRpcDataSet.values[index] != null) {
      ioTDBRpcDataSet.lastReadWasNull = false;
      return BytesUtils.bytesToLong(ioTDBRpcDataSet.values[index]);
    } else {
      ioTDBRpcDataSet.lastReadWasNull = true;
      return 0;
    }
  }

  @Override
  protected boolean fetchResults() throws SQLException {
    TSFetchResultsReq req =
        new TSFetchResultsReq(
            ioTDBRpcDataSet.sessionId,
            ioTDBRpcDataSet.sql,
            ioTDBRpcDataSet.fetchSize,
            ioTDBRpcDataSet.queryId,
            false);
    req.setTimeout(ioTDBRpcDataSet.timeout);
    try {
      TSFetchResultsResp resp = ioTDBRpcDataSet.client.fetchResults(req);

      try {
        RpcUtils.verifySuccess(resp.getStatus());
      } catch (StatementExecutionException e) {
        throw new IoTDBSQLException(e.getMessage(), resp.getStatus());
      }
      if (!resp.hasResultSet) {
        ioTDBRpcDataSet.emptyResultSet = true;
        close();
      } else {
        tsQueryNonAlignDataSet = resp.getNonAlignQueryDataSet();
        if (tsQueryNonAlignDataSet == null) {
          ioTDBRpcDataSet.emptyResultSet = true;
          close();
          return false;
        }
      }
      return resp.hasResultSet;
    } catch (TException e) {
      throw new SQLException(
          "Cannot fetch result from server, because of network connection: {} ", e);
    }
  }

  @Override
  protected boolean hasCachedResults() {
    return (tsQueryNonAlignDataSet != null && hasTimesRemaining());
  }

  // check if has times remaining for disable align clause
  private boolean hasTimesRemaining() {
    for (ByteBuffer time : tsQueryNonAlignDataSet.timeList) {
      if (time.hasRemaining()) {
        return true;
      }
    }
    return false;
  }

  @Override
  protected void constructOneRow() {
    for (int i = 0; i < tsQueryNonAlignDataSet.timeList.size(); i++) {
      times[i] = null;
      ioTDBRpcDataSet.values[i] = null;
      if (tsQueryNonAlignDataSet.timeList.get(i).remaining() >= Long.BYTES) {

        times[i] = new byte[Long.BYTES];

        tsQueryNonAlignDataSet.timeList.get(i).get(times[i]);
        ByteBuffer valueBuffer = tsQueryNonAlignDataSet.valueList.get(i);
        TSDataType dataType = ioTDBRpcDataSet.columnTypeDeduplicatedList.get(i);
        switch (dataType) {
          case BOOLEAN:
            ioTDBRpcDataSet.values[i] = new byte[1];
            valueBuffer.get(ioTDBRpcDataSet.values[i]);
            break;
          case INT32:
            ioTDBRpcDataSet.values[i] = new byte[Integer.BYTES];
            valueBuffer.get(ioTDBRpcDataSet.values[i]);
            break;
          case INT64:
            ioTDBRpcDataSet.values[i] = new byte[Long.BYTES];
            valueBuffer.get(ioTDBRpcDataSet.values[i]);
            break;
          case FLOAT:
            ioTDBRpcDataSet.values[i] = new byte[Float.BYTES];
            valueBuffer.get(ioTDBRpcDataSet.values[i]);
            break;
          case DOUBLE:
            ioTDBRpcDataSet.values[i] = new byte[Double.BYTES];
            valueBuffer.get(ioTDBRpcDataSet.values[i]);
            break;
          case TEXT:
            int length = valueBuffer.getInt();
            ioTDBRpcDataSet.values[i] = ReadWriteIOUtils.readBytes(valueBuffer, length);
            break;
          default:
            throw new UnSupportedDataTypeException(
                String.format(
                    "Data type %s is not supported.",
                    ioTDBRpcDataSet.columnTypeDeduplicatedList.get(i)));
        }
      } else {
        ioTDBRpcDataSet.values[i] = EMPTY_STR.getBytes();
      }
    }
  }

  @Override
  protected void checkRecord() throws SQLException {
    if (Objects.isNull(tsQueryNonAlignDataSet)) {
      throw new SQLException("No record remains");
    }
  }

  @Override
  protected String getValueByName(String columnName) throws SQLException {
    checkRecord();
    if (columnName.startsWith(TIMESTAMP_STR)) {
      String column = columnName.substring(TIMESTAMP_STR_LENGTH);
      int index = ioTDBRpcDataSet.columnOrdinalMap.get(column) - START_INDEX;
      if (times[index] == null || times[index].length == 0) {
        return null;
      }
      return String.valueOf(BytesUtils.bytesToLong(times[index]));
    }
    int index = ioTDBRpcDataSet.columnOrdinalMap.get(columnName) - START_INDEX;
    if (index < 0
        || index >= ioTDBRpcDataSet.values.length
        || ioTDBRpcDataSet.values[index] == null
        || ioTDBRpcDataSet.values[index].length < 1) {
      return null;
    }
    return ioTDBRpcDataSet.getString(
        index, ioTDBRpcDataSet.columnTypeDeduplicatedList.get(index), ioTDBRpcDataSet.values);
  }

  @Override
  protected Object getObjectByName(String columnName) throws SQLException {
    checkRecord();
    if (columnName.startsWith(TIMESTAMP_STR)) {
      String column = columnName.substring(TIMESTAMP_STR_LENGTH);
      int index = ioTDBRpcDataSet.columnOrdinalMap.get(column) - START_INDEX;
      if (times[index] == null || times[index].length == 0) {
        return null;
      }
      return BytesUtils.bytesToLong(times[index]);
    }
    int index = ioTDBRpcDataSet.columnOrdinalMap.get(columnName) - START_INDEX;
    if (index < 0
        || index >= ioTDBRpcDataSet.values.length
        || ioTDBRpcDataSet.values[index] == null
        || ioTDBRpcDataSet.values[index].length < 1) {
      return null;
    }
    return ioTDBRpcDataSet.getObject(
        index, ioTDBRpcDataSet.columnTypeDeduplicatedList.get(index), ioTDBRpcDataSet.values);
  }

  public String getOperationType() {
    return this.operationType;
  }

  public List<String> getSgColumns() {
    return sgColumns;
  }
}
