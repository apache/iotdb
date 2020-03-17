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
import org.apache.iotdb.service.rpc.thrift.TSQueryDataSet;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.thrift.TException;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Objects;

public class IoTDBQueryResultSet extends AbstractIoTDBResultSet {

  private static final int START_INDEX = 2;
  private static final String VALUE_IS_NULL = "The value got by %s (column name) is NULL.";
  private int rowsIndex = 0; // used to record the row index in current TSQueryDataSet
  private boolean align = true;

  private TSQueryDataSet tsQueryDataSet = null;
  private byte[] time; // used to cache the current time value
  private byte[] currentBitmap; // used to cache the current bitmap for every column
  private static final int FLAG = 0x80; // used to do `and` operation with bitmap to judge whether the value is null


  public IoTDBQueryResultSet(Statement statement, List<String> columnNameList,
      List<String> columnTypeList, boolean ignoreTimeStamp, TSIService.Iface client,
      String sql, long queryId, long sessionId, TSQueryDataSet dataset)
      throws SQLException {
    super(statement, columnNameList, columnTypeList, ignoreTimeStamp, client, sql, queryId, sessionId);
    time = new byte[Long.BYTES];
    currentBitmap = new byte[columnNameList.size()];
    this.tsQueryDataSet = dataset;
  }

  @Override
  public long getLong(String columnName) throws SQLException {
    checkRecord();
    if (columnName.equals(TIMESTAMP_STR)) {
      return BytesUtils.bytesToLong(time);
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
    rowsIndex = 0;
    TSFetchResultsReq req = new TSFetchResultsReq(sessionId, sql, fetchSize, queryId, align);
    try {
      TSFetchResultsResp resp = client.fetchResults(req);

      try {
        RpcUtils.verifySuccess(resp.getStatus());
      } catch (StatementExecutionException e) {
        throw new IoTDBSQLException(e.getMessage(), resp.getStatus());
      }
      if (!resp.hasResultSet) {
        emptyResultSet = true;
      } else {
        tsQueryDataSet = resp.getQueryDataSet();
      }
      return resp.hasResultSet;
    } catch (TException e) {
      throw new SQLException(
          "Cannot fetch result from server, because of network connection: {} ", e);
    }
  }

  @Override
  protected boolean hasCachedResults() {
    return (tsQueryDataSet != null && tsQueryDataSet.time.hasRemaining());
  }

  @Override
  protected void constructOneRow() {
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
                String.format("Data type %s is not supported.", columnTypeDeduplicatedList.get(i)));
        }
      }
    }
    rowsIndex++;
  }

  /**
   * judge whether the specified column value is null in the current position
   *
   * @param index series index
   * @param rowNum current position
   */
  private boolean isNull(int index, int rowNum) {
    byte bitmap = currentBitmap[index];
    int shift = rowNum % 8;
    return ((FLAG >>> shift) & bitmap) == 0;
  }

  @Override
  protected void checkRecord() throws SQLException {
    if (Objects.isNull(tsQueryDataSet)) {
      throw new SQLException("No record remains");
    }
  }

  @Override
  protected String getValueByName(String columnName) throws SQLException {
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

  public boolean isIgnoreTimeStamp() {
    return ignoreTimeStamp;
  }

  
  public boolean isAlign() {
    return align;
  }
}
