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

import static org.apache.iotdb.rpc.AbstractIoTDBDataSet.START_INDEX;
import static org.apache.iotdb.rpc.AbstractIoTDBDataSet.TIMESTAMP_STR;
import static org.apache.iotdb.rpc.AbstractIoTDBDataSet.VALUE_IS_NULL;

import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.StatementExecutionException;
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
import java.util.*;

public class IoTDBNonAlignQueryResultSet extends AbstractIoTDBResultSet {

  private static final int TIMESTAMP_STR_LENGTH = 4;
  private static final String EMPTY_STR = "";

  private TSQueryNonAlignDataSet tsQueryNonAlignDataSet;
  private byte[][] times; // used for disable align

  // for disable align clause
  IoTDBNonAlignQueryResultSet(Statement statement, List<String> columnNameList,
      List<String> columnTypeList, Map<String, Integer> columnNameIndex, boolean ignoreTimeStamp,
      TSIService.Iface client,
      String sql, long queryId, long sessionId, TSQueryNonAlignDataSet dataset)
      throws SQLException {
    super(statement, columnNameList, columnTypeList, columnNameIndex, ignoreTimeStamp, client, sql,
        queryId, sessionId);

    times = new byte[columnNameList.size()][Long.BYTES];

    abstractIoTDBDataSet.columnNameList = new ArrayList<>();
    // deduplicate and map
    abstractIoTDBDataSet.columnOrdinalMap = new HashMap<>();
    abstractIoTDBDataSet.columnOrdinalMap.put(TIMESTAMP_STR, 1);
    abstractIoTDBDataSet.columnTypeDeduplicatedList = new ArrayList<>();
    abstractIoTDBDataSet.columnTypeDeduplicatedList = new ArrayList<>(columnNameIndex.size());
    for (int i = 0; i < columnNameIndex.size(); i++) {
      abstractIoTDBDataSet.columnTypeDeduplicatedList.add(null);
    }
    for (int i = 0; i < columnNameList.size(); i++) {
      String name = columnNameList.get(i);
      abstractIoTDBDataSet.columnNameList.add(TIMESTAMP_STR + name);
      abstractIoTDBDataSet.columnNameList.add(name);
      if (!abstractIoTDBDataSet.columnOrdinalMap.containsKey(name)) {
        int index = columnNameIndex.get(name);
        abstractIoTDBDataSet.columnOrdinalMap.put(name, index + START_INDEX);
        abstractIoTDBDataSet.columnTypeDeduplicatedList
            .set(index, TSDataType.valueOf(columnTypeList.get(i)));
      }
    }
    this.tsQueryNonAlignDataSet = dataset;
  }

  @Override
  public long getLong(String columnName) throws SQLException {
    checkRecord();
    if (columnName.startsWith(TIMESTAMP_STR)) {
      String column = columnName.substring(TIMESTAMP_STR_LENGTH);
      int index = abstractIoTDBDataSet.columnOrdinalMap.get(column) - START_INDEX;
      if (times[index] != null) {
        return BytesUtils.bytesToLong(times[index]);
      } else {
        throw new SQLException(String.format(VALUE_IS_NULL, columnName));
      }
    }
    int index = abstractIoTDBDataSet.columnOrdinalMap.get(columnName) - START_INDEX;
    if (abstractIoTDBDataSet.values[index] != null) {
      return BytesUtils.bytesToLong(abstractIoTDBDataSet.values[index]);
    } else {
      throw new SQLException(String.format(VALUE_IS_NULL, columnName));
    }
  }

  @Override
  protected boolean fetchResults() throws SQLException {
    TSFetchResultsReq req = new TSFetchResultsReq(abstractIoTDBDataSet.sessionId,
        abstractIoTDBDataSet.sql, abstractIoTDBDataSet.fetchSize, abstractIoTDBDataSet.queryId,
        false);
    try {
      TSFetchResultsResp resp = abstractIoTDBDataSet.client.fetchResults(req);

      try {
        RpcUtils.verifySuccess(resp.getStatus());
      } catch (StatementExecutionException e) {
        throw new IoTDBSQLException(e.getMessage(), resp.getStatus());
      }
      if (!resp.hasResultSet) {
        abstractIoTDBDataSet.emptyResultSet = true;
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
      abstractIoTDBDataSet.values[i] = null;
      if (tsQueryNonAlignDataSet.timeList.get(i).remaining() >= Long.BYTES) {

        times[i] = new byte[Long.BYTES];

        tsQueryNonAlignDataSet.timeList.get(i).get(times[i]);
        ByteBuffer valueBuffer = tsQueryNonAlignDataSet.valueList.get(i);
        TSDataType dataType = abstractIoTDBDataSet.columnTypeDeduplicatedList.get(i);
        switch (dataType) {
          case BOOLEAN:
            abstractIoTDBDataSet.values[i] = new byte[1];
            valueBuffer.get(abstractIoTDBDataSet.values[i]);
            break;
          case INT32:
            abstractIoTDBDataSet.values[i] = new byte[Integer.BYTES];
            valueBuffer.get(abstractIoTDBDataSet.values[i]);
            break;
          case INT64:
            abstractIoTDBDataSet.values[i] = new byte[Long.BYTES];
            valueBuffer.get(abstractIoTDBDataSet.values[i]);
            break;
          case FLOAT:
            abstractIoTDBDataSet.values[i] = new byte[Float.BYTES];
            valueBuffer.get(abstractIoTDBDataSet.values[i]);
            break;
          case DOUBLE:
            abstractIoTDBDataSet.values[i] = new byte[Double.BYTES];
            valueBuffer.get(abstractIoTDBDataSet.values[i]);
            break;
          case TEXT:
            int length = valueBuffer.getInt();
            abstractIoTDBDataSet.values[i] = ReadWriteIOUtils.readBytes(valueBuffer, length);
            break;
          default:
            throw new UnSupportedDataTypeException(
                String.format("Data type %s is not supported.",
                    abstractIoTDBDataSet.columnTypeDeduplicatedList.get(i)));
        }
      } else {
        abstractIoTDBDataSet.values[i] = EMPTY_STR.getBytes();
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
      int index = abstractIoTDBDataSet.columnOrdinalMap.get(column) - START_INDEX;
      if (times[index] == null || times[index].length == 0) {
        return null;
      }
      return String.valueOf(BytesUtils.bytesToLong(times[index]));
    }
    int index = abstractIoTDBDataSet.columnOrdinalMap.get(columnName) - START_INDEX;
    if (index < 0 || index >= abstractIoTDBDataSet.values.length || abstractIoTDBDataSet.values[index] == null || abstractIoTDBDataSet.values[index].length < 1) {
      return null;
    }
    return getString(index, columnTypeDeduplicatedList.get(index), values);
  }
}
