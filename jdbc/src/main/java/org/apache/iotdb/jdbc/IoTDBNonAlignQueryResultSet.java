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

import org.apache.iotdb.rpc.IoTDBRPCException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.service.rpc.thrift.TSFetchResultsReq;
import org.apache.iotdb.service.rpc.thrift.TSFetchResultsResp;
import org.apache.iotdb.service.rpc.thrift.TSIService;
import org.apache.iotdb.service.rpc.thrift.TSQueryNonAlignDataSet;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.thrift.TException;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

public class IoTDBNonAlignQueryResultSet extends AbstractIoTDBResultSet {

  private static final int TIMESTAMP_STR_LENGTH = 4;
  private static final String EMPTY_STR = "";

  private TSQueryNonAlignDataSet tsQueryNonAlignDataSet = null;
  private byte[][] times; // used for disable align

  // for disable align clause
  public IoTDBNonAlignQueryResultSet(Statement statement, List<String> columnNameList,
                                     List<String> columnTypeList, boolean ignoreTimeStamp, TSIService.Iface client,
                                     String sql, long queryId, long sessionId, TSQueryNonAlignDataSet dataset)
          throws SQLException {
    super(statement, columnNameList, columnTypeList, ignoreTimeStamp, client, sql, queryId, sessionId);

    times = new byte[columnNameList.size()][Long.BYTES];

    super.columnNameList = new ArrayList<>();
    // deduplicate and map
    super.columnOrdinalMap = new HashMap<>();
    super.columnOrdinalMap.put(TIMESTAMP_STR, 1);
    super.columnTypeDeduplicatedList = new ArrayList<>();
    int index = START_INDEX;
    for (int i = 0; i < columnNameList.size(); i++) {
      String name = columnNameList.get(i);
      super.columnNameList.add(TIMESTAMP_STR + name);
      super.columnNameList.add(name);
      if (!columnOrdinalMap.containsKey(name)) {
        columnOrdinalMap.put(name, index++);
        columnTypeDeduplicatedList.add(TSDataType.valueOf(columnTypeList.get(i)));
      }
    }
    this.tsQueryNonAlignDataSet = dataset;
  }

  @Override
  public long getLong(String columnName) throws SQLException {
    checkRecord();
    if (columnName.startsWith(TIMESTAMP_STR)) {
      String column = columnName.substring(TIMESTAMP_STR_LENGTH);
      int index = columnOrdinalMap.get(column) - START_INDEX;
      if (times[index] != null)
        return BytesUtils.bytesToLong(times[index]);
      else
        throw new SQLException(String.format(VALUE_IS_NULL, columnName));
    }
    int index = columnOrdinalMap.get(columnName) - START_INDEX;
    if (values[index] != null) {
      return BytesUtils.bytesToLong(values[index]);
    } else {
      throw new SQLException(String.format(VALUE_IS_NULL, columnName));
    }
  }

  @Override
  protected boolean fetchResults() throws SQLException {
    TSFetchResultsReq req = new TSFetchResultsReq(sessionId, sql, fetchSize, queryId, false);
    try {
      TSFetchResultsResp resp = client.fetchResults(req);

      try {
        RpcUtils.verifySuccess(resp.getStatus());
      } catch (IoTDBRPCException e) {
        throw new IoTDBSQLException(e.getMessage(), resp.getStatus());
      }
      if (!resp.hasResultSet) {
        emptyResultSet = true;
      } else {
        tsQueryNonAlignDataSet = resp.getNonAlignQueryDataSet();
        if (tsQueryNonAlignDataSet == null) {
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
      values[i] = null;
      if (tsQueryNonAlignDataSet.timeList.get(i).remaining() >= Long.BYTES) {

        times[i] = new byte[Long.BYTES];

        tsQueryNonAlignDataSet.timeList.get(i).get(times[i]);
        ByteBuffer valueBuffer = tsQueryNonAlignDataSet.valueList.get(i);
        TSDataType dataType = columnTypeDeduplicatedList.get(i);
        switch (dataType) {
          case BOOLEAN:
            values[i] = new byte[1];
            valueBuffer.get(values[i]);
            break;
          case INT32:
            values[i] = new byte[Integer.BYTES];
            valueBuffer.get(values[i]);
            break;
          case INT64:
            values[i] = new byte[Long.BYTES];
            valueBuffer.get(values[i]);
            break;
          case FLOAT:
            values[i] = new byte[Float.BYTES];
            valueBuffer.get(values[i]);
            break;
          case DOUBLE:
            values[i] = new byte[Double.BYTES];
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
      else {
        values[i] = EMPTY_STR.getBytes();
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
      int index = columnOrdinalMap.get(column) - START_INDEX;
      if (times[index] == null || times[index].length == 0) {
        return null;
      }
      return String.valueOf(BytesUtils.bytesToLong(times[index]));
    }
    int index = columnOrdinalMap.get(columnName) - START_INDEX;
    if (index < 0 || index >= values.length || values[index] == null || values[index].length < 1) {
      return null;
    }
    return getString(index, columnTypeDeduplicatedList.get(index), values);
  }
}
