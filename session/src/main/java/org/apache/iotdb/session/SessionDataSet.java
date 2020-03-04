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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.rpc.IoTDBRPCException;
import org.apache.iotdb.rpc.RpcUtils;
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
import org.apache.thrift.TException;

public class SessionDataSet {

  private boolean hasCachedRecord = false;
  private String sql;
  private long queryId;
  private long sessionId;
  private TSIService.Iface client;
  private int batchSize = 1024;
  private List<String> columnTypeDeduplicatedList;
  // duplicated column index -> origin index
  Map<Integer, Integer> duplicateLocation;
  // column name -> column location
  Map<String, Integer> columnMap;
  // column size
  int columnSize = 0;


  private int rowsIndex = 0; // used to record the row index in current TSQueryDataSet
  private TSQueryDataSet tsQueryDataSet;
  private RowRecord rowRecord = null;
  private byte[] currentBitmap; // used to cache the current bitmap for every column
  private static final int flag = 0x80; // used to do `or` operation with bitmap to judge whether the value is null


  public SessionDataSet(String sql, List<String> columnNameList, List<String> columnTypeList,
      long queryId, TSIService.Iface client, long sessionId, TSQueryDataSet queryDataSet) {
    this.sessionId = sessionId;
    this.sql = sql;
    this.queryId = queryId;
    this.client = client;
    currentBitmap = new byte[columnNameList.size()];
    columnSize = columnNameList.size();

    // deduplicate columnTypeList according to columnNameList
    this.columnTypeDeduplicatedList = new ArrayList<>();
    // duplicated column index -> origin index
    duplicateLocation = new HashMap<>();
    // column name -> column location
    columnMap = new HashMap<>();
    for (int i = 0; i < columnNameList.size(); i++) {
      String name = columnNameList.get(i);
      if (columnMap.containsKey(name)) {
        duplicateLocation.put(i, columnMap.get(name));
      } else {
        columnMap.put(name, i);
        columnTypeDeduplicatedList.add(columnTypeList.get(i));
      }
    }

    this.tsQueryDataSet = queryDataSet;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  public boolean hasNext() throws SQLException, IoTDBRPCException {
    if (hasCachedRecord) {
      return true;
    }
    if (tsQueryDataSet == null || !tsQueryDataSet.time.hasRemaining()) {
      TSFetchResultsReq req = new TSFetchResultsReq(sessionId, sql, batchSize, queryId, true);
      try {
        TSFetchResultsResp resp = client.fetchResults(req);
        RpcUtils.verifySuccess(resp.getStatus());

        if (!resp.hasResultSet) {
          return false;
        } else {
          tsQueryDataSet = resp.getQueryDataSet();
          rowsIndex = 0;
        }
      } catch (TException e) {
        throw new SQLException(
            "Cannot fetch result from server, because of network connection: {} ", e);
      }

    }

    constructOneRow();
    hasCachedRecord = true;
    return true;
  }

  private void constructOneRow() {
    List<Field> outFields = new ArrayList<>();
    int loc = 0;
    for (int i = 0; i < columnSize; i++) {
      Field field;

      if (duplicateLocation.containsKey(i)) {
        field = Field.copy(outFields.get(duplicateLocation.get(i)));
      } else {
        ByteBuffer bitmapBuffer = tsQueryDataSet.bitmapList.get(loc);
        // another new 8 row, should move the bitmap buffer position to next byte
        if (rowsIndex % 8 == 0) {
          currentBitmap[loc] = bitmapBuffer.get();
        }

        if (!isNull(loc, rowsIndex)) {
          ByteBuffer valueBuffer = tsQueryDataSet.valueList.get(loc);
          TSDataType dataType = TSDataType.valueOf(columnTypeDeduplicatedList.get(loc));
          field = new Field(dataType);
          switch (dataType) {
            case BOOLEAN:
              boolean booleanValue = BytesUtils.byteToBool(valueBuffer.get());
              field.setBoolV(booleanValue);
              break;
            case INT32:
              int intValue = valueBuffer.getInt();
              field.setIntV(intValue);
              break;
            case INT64:
              long longValue = valueBuffer.getLong();
              field.setLongV(longValue);
              break;
            case FLOAT:
              float floatValue = valueBuffer.getFloat();
              field.setFloatV(floatValue);
              break;
            case DOUBLE:
              double doubleValue = valueBuffer.getDouble();
              field.setDoubleV(doubleValue);
              break;
            case TEXT:
              int binarySize = valueBuffer.getInt();
              byte[] binaryValue = new byte[binarySize];
              valueBuffer.get(binaryValue);
              field.setBinaryV(new Binary(binaryValue));
              break;
            default:
              throw new UnSupportedDataTypeException(String
                  .format("Data type %s is not supported.", columnTypeDeduplicatedList.get(i)));
          }
        } else {
          field = new Field(null);
        }
        loc++;
      }
      outFields.add(field);
    }

    rowRecord = new RowRecord(tsQueryDataSet.time.getLong(), outFields);
    rowsIndex++;
  }

  /**
   * judge whether the specified column value is null in the current position
   *
   * @param index column index
   * @return
   */
  private boolean isNull(int index, int rowNum) {
    byte bitmap = currentBitmap[index];
    int shift = rowNum % 8;
    return ((flag >>> shift) & bitmap) == 0;
  }

  public RowRecord next() throws SQLException, IoTDBRPCException {
    if (!hasCachedRecord) {
      if (!hasNext()) {
        return null;
      }
    }

    hasCachedRecord = false;
    return rowRecord;
  }

  public void closeOperationHandle() throws SQLException {
    try {
      TSCloseOperationReq closeReq = new TSCloseOperationReq(sessionId);
      closeReq.setQueryId(queryId);
      TSStatus closeResp = client.closeOperation(closeReq);
      RpcUtils.verifySuccess(closeResp);
    } catch (IoTDBRPCException e) {
      throw new SQLException("Error occurs for close opeation in server side. The reason is " + e);
    } catch (TException e) {
      throw new SQLException(
          "Error occurs when connecting to server for close operation, because: " + e);
    }
  }
}
