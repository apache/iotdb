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
import org.apache.iotdb.service.rpc.thrift.*;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.thrift.TException;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.MathContext;
import java.net.URL;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.*;
import java.util.*;

public class IoTDBQueryResultSet implements ResultSet {

  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(IoTDBQueryResultSet.class);
  private static final String TIMESTAMP_STR = "Time";
  private static final int START_INDEX = 2;
  private static final String LIMIT_STR = "LIMIT";
  private static final String OFFSET_STR = "OFFSET";
  private Statement statement = null;
  private String sql;
  private SQLWarning warningChain = null;
  private boolean isClosed = false;
  private TSIService.Iface client = null;
  private TSOperationHandle operationHandle = null;
  private List<String> columnInfoList; // no deduplication
  private List<String> columnTypeList; // no deduplication
  private Map<String, Integer> columnInfoMap; // used because the server returns deduplicated columns
  private List<String> columnTypeDeduplicatedList; // deduplicated from columnTypeList
  private int rowsFetched = 0;
  private int rowsIndex = 0;
  private int maxRows; // defined in TsfileStatement
  private int fetchSize;
  private boolean emptyResultSet = false;
  
  private TSQueryDataSet tsQueryDataSet = null;
  Object[] record;
  private static final int flag = 0x80;

  // 0 means it is not constrained in sql
  private int rowsLimit = 0;

  // 0 means it is not constrained in sql, or the offset position has been reached
  private int rowsOffset = 0;

  private long queryId;
  private boolean ignoreTimeStamp = false;

  /*
   * Combine maxRows and the LIMIT constraints. maxRowsOrRowsLimit = 0 means that neither maxRows
   * nor LIMIT is constrained. maxRowsOrRowsLimit > 0 means that maxRows and/or
   * LIMIT are constrained.
   * 1) When neither maxRows nor LIMIT is constrained, i.e., maxRows=0 and rowsLimit=0,
   * maxRowsOrRowsLimit = 0 2). When both maxRows and LIMIT are constrained, i.e., maxRows>0 and
   * rowsLimit>0, maxRowsOrRowsLimit = min(maxRows, rowsLimit) 3) When maxRows is constrained and
   * LIMIT is NOT constrained, i.e., maxRows>0 and rowsLimit=0, maxRowsOrRowsLimit = maxRows
   * 4) When maxRows is NOT constrained and LIMIT is constrained, i.e., maxRows=0 and
   * rowsLimit>0, maxRowsOrRowsLimit = rowsLimit
   */
  private int maxRowsOrRowsLimit;

  public IoTDBQueryResultSet() {
    // do nothing
  }

  public IoTDBQueryResultSet(Statement statement, List<String> columnNameList,
      List<String> columnTypeList, boolean ignoreTimeStamp, TSIService.Iface client,
      TSOperationHandle operationHandle, String sql, long queryId)
      throws SQLException {
    this.statement = statement;
    this.maxRows = statement.getMaxRows();
    this.fetchSize = statement.getFetchSize();
    this.columnTypeList = columnTypeList;
    record = new Object[columnNameList.size()+1];

    this.columnInfoList = new ArrayList<>();
    this.columnInfoList.add(TIMESTAMP_STR);
    // deduplicate and map
    this.columnInfoMap = new HashMap<>();
    this.columnInfoMap.put(TIMESTAMP_STR, 1);
    this.columnTypeDeduplicatedList = new ArrayList<>();
    int index = START_INDEX;
    for (int i = 0; i < columnNameList.size(); i++) {
      String name = columnNameList.get(i);
      columnInfoList.add(name);
      if (!columnInfoMap.containsKey(name)) {
        columnInfoMap.put(name, index++);
        columnTypeDeduplicatedList.add(columnTypeList.get(i));
      }
    }

    this.ignoreTimeStamp = ignoreTimeStamp;
    this.client = client;
    this.operationHandle = operationHandle;
    this.sql = sql;
    this.queryId = queryId;

    maxRowsOrRowsLimit = maxRows;
    // parse the LIMIT&OFFSET parameters from sql
    String[] splited = sql.toUpperCase().split("\\s+");
    List<String> arraySplited = Arrays.asList(splited);
    try {
      int posLimit = arraySplited.indexOf(LIMIT_STR);
      if (posLimit != -1) {
        rowsLimit = Integer.parseInt(splited[posLimit + 1]);

        // NOTE that maxRows=0 in TsfileStatement means it is not constrained
        // NOTE that LIMIT <N>: N is ensured to be a positive integer by the server side
        if (maxRows == 0) {
          maxRowsOrRowsLimit = rowsLimit;
        } else {
          maxRowsOrRowsLimit = Math.min(rowsLimit, maxRows);
        }

        // check if OFFSET is constrained after LIMIT has been constrained
        int posOffset = arraySplited.indexOf(OFFSET_STR);
        if (posOffset != -1) {
          // NOTE that OFFSET <OFFSETValue>: OFFSETValue is ensured to be a non-negative
          // integer by the server side
          rowsOffset = Integer.parseInt(splited[posOffset + 1]);
        }
      }

    } catch (NumberFormatException e) {
      throw new IoTDBSQLException("Out of range: LIMIT&SLIMIT parameter should be Int32.");
    }
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public boolean absolute(int arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void afterLast() throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void beforeFirst() throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void cancelRowUpdates() throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void clearWarnings() throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void close() throws SQLException {
    if (isClosed) {
      return;
    }

    closeOperationHandle();
    client = null;
    isClosed = true;
  }

  private void closeOperationHandle() throws SQLException {
    try {
      if (operationHandle != null) {
        TSCloseOperationReq closeReq = new TSCloseOperationReq(operationHandle, queryId);
        TSStatus closeResp = client.closeOperation(closeReq);
        RpcUtils.verifySuccess(closeResp);
      }
    } catch (IoTDBRPCException e) {
      throw new SQLException("Error occurs for close opeation in server side becasuse " + e);
    } catch (TException e) {
      throw new SQLException(
          "Error occurs when connecting to server for close operation, becasue: " + e);
    }
  }

  @Override
  public void deleteRow() throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public int findColumn(String columnName) throws SQLException {
    return columnInfoMap.get(columnName);
  }

  @Override
  public boolean first() throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public Array getArray(int arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public Array getArray(String arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public InputStream getAsciiStream(int arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public InputStream getAsciiStream(String arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    return getBigDecimal(findColumnNameByIndex(columnIndex));
  }

  @Override
  public BigDecimal getBigDecimal(String columnName) throws SQLException {
    return new BigDecimal(getValueByName(columnName));
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
    MathContext mc = new MathContext(scale);
    return getBigDecimal(columnIndex).round(mc);
  }

  @Override
  public BigDecimal getBigDecimal(String columnName, int scale) throws SQLException {
    return getBigDecimal(findColumn(columnName), scale);
  }

  @Override
  public InputStream getBinaryStream(int arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public InputStream getBinaryStream(String arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public Blob getBlob(int arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public Blob getBlob(String arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public boolean getBoolean(int columnIndex) throws SQLException {
    return getBoolean(findColumnNameByIndex(columnIndex));
  }

  @Override
  public boolean getBoolean(String columnName) throws SQLException {
    checkRecord();
    int index = columnInfoMap.get(columnName) - START_INDEX;
    if (!isNull(index, rowsIndex-1) &&
            TSDataType.valueOf(columnTypeList.get(index)) == TSDataType.BOOLEAN) {
      ByteBuffer valueBuffer = tsQueryDataSet.valueList.get(index);
      int beforePosition = valueBuffer.position();
      boolean res = ReadWriteIOUtils.readBool(valueBuffer);
      valueBuffer.position(beforePosition);
      return res;
    }
    else {
      throw new SQLException(
          String.format("The value got by %s (column name) is NULL.", columnName));
    }
  }

  @Override
  public byte getByte(int columnIndex) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public byte getByte(String columnName) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public byte[] getBytes(int columnIndex) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public byte[] getBytes(String columnName) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public Reader getCharacterStream(int arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public Reader getCharacterStream(String arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public Clob getClob(int arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public Clob getClob(String arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public int getConcurrency() throws SQLException {
    return ResultSet.CONCUR_READ_ONLY;
  }

  @Override
  public String getCursorName() throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public Date getDate(int columnIndex) throws SQLException {
    return new Date(getLong(columnIndex));
  }

  @Override
  public Date getDate(String columnName) throws SQLException {
    return getDate(findColumn(columnName));
  }

  @Override
  public Date getDate(int arg0, Calendar arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public Date getDate(String arg0, Calendar arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public double getDouble(int columnIndex) throws SQLException {
    return getDouble(findColumnNameByIndex(columnIndex));
  }

  @Override
  public double getDouble(String columnName) throws SQLException {
    checkRecord();
    int index = columnInfoMap.get(columnName) - START_INDEX;
    if (!isNull(index, rowsIndex-1) &&
            TSDataType.valueOf(columnTypeList.get(index)) == TSDataType.DOUBLE) {
      ByteBuffer valueBuffer = tsQueryDataSet.valueList.get(index);
      int beforePosition = valueBuffer.position();
      double res = ReadWriteIOUtils.readDouble(valueBuffer);
      valueBuffer.position(beforePosition);
      return res;
    }
    else {
      throw new SQLException(
          String.format("The value got by %s (column name) is NULL.", columnName));
    }
  }

  @Override
  public int getFetchDirection() throws SQLException {
    return ResultSet.FETCH_FORWARD;
  }

  @Override
  public void setFetchDirection(int arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public int getFetchSize() throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void setFetchSize(int arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public float getFloat(int columnIndex) throws SQLException {
    return getFloat(findColumnNameByIndex(columnIndex));
  }

  @Override
  public float getFloat(String columnName) throws SQLException {
    checkRecord();
    int index = columnInfoMap.get(columnName) - START_INDEX;
    if (!isNull(index, rowsIndex-1) &&
            TSDataType.valueOf(columnTypeList.get(index)) == TSDataType.FLOAT) {
      ByteBuffer valueBuffer = tsQueryDataSet.valueList.get(index);
      int beforePosition = valueBuffer.position();
      float res = ReadWriteIOUtils.readFloat(valueBuffer);
      valueBuffer.position(beforePosition);
      return res;
    }
    else {
      throw new SQLException(
          String.format("The value got by %s (column name) is NULL.", columnName));
    }
  }

  @Override
  public int getHoldability() throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public int getInt(int columnIndex) throws SQLException {
    return getInt(findColumnNameByIndex(columnIndex));
  }

  @Override
  public int getInt(String columnName) throws SQLException {
    checkRecord();
    int index = columnInfoMap.get(columnName) - START_INDEX;
    if (!isNull(index, rowsIndex-1) &&
            TSDataType.valueOf(columnTypeList.get(index)) == TSDataType.INT32) {
      ByteBuffer valueBuffer = tsQueryDataSet.valueList.get(index);
      int beforePosition = valueBuffer.position();
      int res = ReadWriteIOUtils.readInt(valueBuffer);
      valueBuffer.position(beforePosition);
      return res;
    }
    else {
      throw new SQLException(
          String.format("The value got by %s (column name) is NULL.", columnName));
    }
  }

  @Override
  public long getLong(int columnIndex) throws SQLException {
    return getLong(findColumnNameByIndex(columnIndex));
  }

  @Override
  public long getLong(String columnName) throws SQLException {
    checkRecord();
    if (columnName.equals(TIMESTAMP_STR)) {
      ByteBuffer timeBuffer = tsQueryDataSet.time;
      int beforePosition = timeBuffer.position();
      timeBuffer.position(beforePosition-Long.BYTES);
      return ReadWriteIOUtils.readLong(timeBuffer);
    }
    int index = columnInfoMap.get(columnName) - START_INDEX;
    if (!isNull(index, rowsIndex-1) &&
            TSDataType.valueOf(columnTypeList.get(index)) == TSDataType.INT64) {
      ByteBuffer valueBuffer = tsQueryDataSet.valueList.get(index);
      int beforePosition = valueBuffer.position();
      long res = ReadWriteIOUtils.readLong(valueBuffer);
      valueBuffer.position(beforePosition);
      return res;
    }
    else {
      throw new SQLException(
          String.format("The value got by %s (column name) is NULL.", columnName));
    }
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    return new IoTDBResultMetadata(columnInfoList, columnTypeList);
  }

  @Override
  public Reader getNCharacterStream(int arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public Reader getNCharacterStream(String arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public NClob getNClob(int arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public NClob getNClob(String arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public String getNString(int arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public String getNString(String arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    return getObject(findColumnNameByIndex(columnIndex));
  }

  @Override
  public Object getObject(String columnName) throws SQLException {
    return getValueByName(columnName);
  }

  @Override
  public Object getObject(int arg0, Map<String, Class<?>> arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public Object getObject(String arg0, Map<String, Class<?>> arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public <T> T getObject(int arg0, Class<T> arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public <T> T getObject(String arg0, Class<T> arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public Ref getRef(int arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public Ref getRef(String arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public int getRow() throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public RowId getRowId(int arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public RowId getRowId(String arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public SQLXML getSQLXML(int arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public SQLXML getSQLXML(String arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public short getShort(int columnIndex) throws SQLException {
    return getShort(findColumnNameByIndex(columnIndex));
  }

  @Override
  public short getShort(String columnName) throws SQLException {
    return Short.parseShort(getValueByName(columnName));
  }

  @Override
  public Statement getStatement() throws SQLException {
    return this.statement;
  }

  @Override
  public String getString(int columnIndex) throws SQLException {
    return getString(findColumnNameByIndex(columnIndex));
  }

  @Override
  public String getString(String columnName) throws SQLException {
    return getValueByName(columnName);
  }

  @Override
  public Time getTime(int columnIndex) throws SQLException {
    return new Time(getLong(columnIndex));
  }

  @Override
  public Time getTime(String columnName) throws SQLException {
    return getTime(findColumn(columnName));
  }

  @Override
  public Time getTime(int arg0, Calendar arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public Time getTime(String arg0, Calendar arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    return new Timestamp(getLong(columnIndex));
  }

  @Override
  public Timestamp getTimestamp(String columnName) throws SQLException {
    return getTimestamp(findColumn(columnName));
  }

  @Override
  public Timestamp getTimestamp(int arg0, Calendar arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public Timestamp getTimestamp(String arg0, Calendar arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public int getType() throws SQLException {
    return ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public URL getURL(int arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public URL getURL(String arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public InputStream getUnicodeStream(int arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public InputStream getUnicodeStream(String arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return warningChain;
  }

  @Override
  public void insertRow() throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public boolean isBeforeFirst() throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public boolean isClosed() throws SQLException {
    return isClosed;
  }

  @Override
  public boolean isFirst() throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public boolean isLast() throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public boolean last() throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void moveToCurrentRow() throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void moveToInsertRow() throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  // the next record rule without constraints
  private boolean nextWithoutConstraints(int limitFetchSize) throws SQLException {
    if ((tsQueryDataSet == null || !tsQueryDataSet.time.hasRemaining()) && !emptyResultSet) {
      int adaFetchSize = Math.min(limitFetchSize, fetchSize);
      TSFetchResultsReq req = new TSFetchResultsReq(sql, adaFetchSize, queryId);

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
          tsQueryDataSet = resp.getQueryDataSet();
        }
      } catch (TException e) {
        throw new SQLException(
            "Cannot fetch result from server, because of network connection: {} ", e);
      }

    }
    if (emptyResultSet) {
      return false;
    }
    constructOneRow();
    return true;
  }

  private void constructOneRow() {
    long time = ReadWriteIOUtils.readLong(tsQueryDataSet.time);
    record[0] = time;
    for (int i = 0; i < tsQueryDataSet.bitmapList.size(); i++) {
      ByteBuffer bitmapBuffer = tsQueryDataSet.bitmapList.get(i);
      // another new 8 row, should move the bitmap buffer position to next byte
      if (rowsIndex % 8 == 0) {
        System.out.println(rowsIndex);
        System.out.println("index: " + i + " position: " + bitmapBuffer.position()  + " limit: " + bitmapBuffer.limit());
        bitmapBuffer.get();
      }

      record[i+1] = null;
      // the column value is not null, we should move the corresponding value buffer to next position
      if (!isNull(i, rowsIndex)) {
        ByteBuffer valueBuffer = tsQueryDataSet.valueList.get(i);
        TSDataType dataType = TSDataType.valueOf(columnTypeList.get(i));
        Object field;
        switch (dataType) {
          case BOOLEAN:
            field = ReadWriteIOUtils.readBool(valueBuffer);
            break;
          case INT32:
            field = ReadWriteIOUtils.readInt(valueBuffer);
            break;
          case INT64:
            field = ReadWriteIOUtils.readLong(valueBuffer);
            break;
          case FLOAT:
            field = ReadWriteIOUtils.readFloat(valueBuffer);
            break;
          case DOUBLE:
            field = ReadWriteIOUtils.readDouble(valueBuffer);
            break;
          case TEXT:
            field = ReadWriteIOUtils.readBinary(valueBuffer);
            break;
          default:
            throw new UnSupportedDataTypeException(
                    String.format("Data type %s is not supported.", columnTypeList.get(i)));
        }
        record[i+1] = field;
      }
    }
    rowsIndex++;
  }

  /**
   * judge whether the specified column value is null in the current position
   * @param index column index
   * @return
   */
  private boolean isNull(int index, int rowNum) {
    ByteBuffer bitmapBuffer = tsQueryDataSet.bitmapList.get(index);
    int beforePosition = bitmapBuffer.position();
    bitmapBuffer.position(beforePosition-1);
    byte bitmap = bitmapBuffer.get();
    bitmapBuffer.position(beforePosition);
    int shift = rowNum % 8;
    return ((flag >>> shift) & bitmap) == 0;
  }

  @Override
  // the next record rule considering both the maxRows constraint and the LIMIT&OFFSET constraint
  public boolean next() throws SQLException {
    if (maxRowsOrRowsLimit > 0 && rowsFetched >= maxRowsOrRowsLimit) {
      // The constraint of maxRows instead of rowsLimit is embodied
      if (rowsLimit == 0 || (maxRows > 0 && maxRows < rowsLimit)) {
        logger.debug("Reach max rows " + maxRows);
      }
      return false;
    }

    // When rowsOffset is constrained and the offset position has NOT been reached
    if (rowsOffset != 0) {
      for (int i = 0; i < rowsOffset; i++) { // Try to move to the offset position
        if (!nextWithoutConstraints(rowsOffset + maxRowsOrRowsLimit - i)) {
          return false; // No next record, i.e, fail to move to the offset position
        }
      }
      rowsOffset = 0; // The offset position has been reached
    }

    boolean isNext;
    if (maxRowsOrRowsLimit > 0) {
      isNext = nextWithoutConstraints(maxRowsOrRowsLimit - rowsFetched);
    } else { // maxRowsOrRowsLimit=0 means neither maxRows nor LIMIT is constrained.
      isNext = nextWithoutConstraints(fetchSize);
    }
    System.out.println("isNext: " + isNext);

    if (isNext) {
      /*
       * Note that 'rowsFetched' is not increased in the nextWithoutConstraints(),
       * because 'rowsFetched' should not count the rows fetched before the OFFSET position.
       */
      rowsFetched++;
    }

    return isNext;
  }

  @Override
  public boolean previous() throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void refreshRow() throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public boolean relative(int arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public boolean rowDeleted() throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public boolean rowInserted() throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public boolean rowUpdated() throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateArray(int arg0, Array arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateArray(String arg0, Array arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateAsciiStream(int arg0, InputStream arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateAsciiStream(String arg0, InputStream arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateAsciiStream(int arg0, InputStream arg1, int arg2) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateAsciiStream(String arg0, InputStream arg1, int arg2) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateAsciiStream(int arg0, InputStream arg1, long arg2) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateAsciiStream(String arg0, InputStream arg1, long arg2) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateBigDecimal(int arg0, BigDecimal arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateBigDecimal(String arg0, BigDecimal arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateBinaryStream(int arg0, InputStream arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateBinaryStream(String arg0, InputStream arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateBinaryStream(int arg0, InputStream arg1, int arg2) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateBinaryStream(String arg0, InputStream arg1, int arg2) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateBinaryStream(int arg0, InputStream arg1, long arg2) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateBinaryStream(String arg0, InputStream arg1, long arg2) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateBlob(int arg0, Blob arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateBlob(String arg0, Blob arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateBlob(int arg0, InputStream arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateBlob(String arg0, InputStream arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateBlob(int arg0, InputStream arg1, long arg2) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateBlob(String arg0, InputStream arg1, long arg2) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateBoolean(int arg0, boolean arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateBoolean(String arg0, boolean arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateByte(int arg0, byte arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateByte(String arg0, byte arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateBytes(int arg0, byte[] arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateBytes(String arg0, byte[] arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateCharacterStream(int arg0, Reader arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateCharacterStream(String arg0, Reader arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateCharacterStream(int arg0, Reader arg1, int arg2) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateCharacterStream(String arg0, Reader arg1, int arg2) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateCharacterStream(int arg0, Reader arg1, long arg2) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateCharacterStream(String arg0, Reader arg1, long arg2) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateClob(int arg0, Clob arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateClob(String arg0, Clob arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateClob(int arg0, Reader arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateClob(String arg0, Reader arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateClob(int arg0, Reader arg1, long arg2) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);

  }

  @Override
  public void updateClob(String arg0, Reader arg1, long arg2) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateDate(int arg0, Date arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateDate(String arg0, Date arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateDouble(int arg0, double arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateDouble(String arg0, double arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateFloat(int arg0, float arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateFloat(String arg0, float arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateInt(int arg0, int arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateInt(String arg0, int arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateLong(int arg0, long arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateLong(String arg0, long arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateNCharacterStream(int arg0, Reader arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateNCharacterStream(String arg0, Reader arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateNCharacterStream(int arg0, Reader arg1, long arg2) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateNCharacterStream(String arg0, Reader arg1, long arg2) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateNClob(int arg0, NClob arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateNClob(String arg0, NClob arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateNClob(int arg0, Reader arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateNClob(String arg0, Reader arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateNClob(int arg0, Reader arg1, long arg2) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateNClob(String arg0, Reader arg1, long arg2) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateNString(int arg0, String arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateNString(String arg0, String arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateNull(int arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateNull(String arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateObject(int arg0, Object arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateObject(String arg0, Object arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateObject(int arg0, Object arg1, int arg2) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateObject(String arg0, Object arg1, int arg2) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateRef(int arg0, Ref arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateRef(String arg0, Ref arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateRow() throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateRowId(int arg0, RowId arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateRowId(String arg0, RowId arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateSQLXML(int arg0, SQLXML arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateSQLXML(String arg0, SQLXML arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateShort(int arg0, short arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateShort(String arg0, short arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateString(int arg0, String arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateString(String arg0, String arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateTime(int arg0, Time arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateTime(String arg0, Time arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateTimestamp(int arg0, Timestamp arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateTimestamp(String arg0, Timestamp arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public boolean wasNull() throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  private void checkRecord() throws SQLException {
    if (tsQueryDataSet == null || tsQueryDataSet.time.limit() == 0) {
      throw new SQLException("No record remains");
    }
  }

  private String findColumnNameByIndex(int columnIndex) throws SQLException {
    if (columnIndex <= 0) {
      throw new SQLException("column index should start from 1");
    }
    if (columnIndex > columnInfoList.size()) {
      throw new SQLException(
          String.format("column index %d out of range %d", columnIndex, columnInfoList.size()));
    }
    return columnInfoList.get(columnIndex - 1);
  }

  private String getValueByName(String columnName) throws SQLException {
    checkRecord();
    if (columnName.equals(TIMESTAMP_STR)) {
      ByteBuffer timeBuffer = tsQueryDataSet.time;
      int beforePosition = timeBuffer.position();
      timeBuffer.position(beforePosition-Long.BYTES);
      return String.valueOf(ReadWriteIOUtils.readLong(timeBuffer));
    }
    int index = columnInfoMap.get(columnName) - START_INDEX;
    if (index < 0 || index >= columnTypeList.size()) {
      return null;
    }
    if (!isNull(index, rowsIndex-1)) {
      ByteBuffer valueBuffer = tsQueryDataSet.valueList.get(index);
      int beforePosition = valueBuffer.position();
      TSDataType dataType = TSDataType.valueOf(columnTypeList.get(index));
      switch (dataType) {
        case BOOLEAN:
          boolean resBool = ReadWriteIOUtils.readBool(valueBuffer);
          valueBuffer.position(beforePosition);
          return String.valueOf(resBool);
        case INT32:
          int resInt = ReadWriteIOUtils.readInt(valueBuffer);
          valueBuffer.position(beforePosition);
          return String.valueOf(resInt);
        case INT64:
          long resLong = ReadWriteIOUtils.readLong(valueBuffer);
          valueBuffer.position(beforePosition);
          return String.valueOf(resLong);
        case FLOAT:
          float resFloat = ReadWriteIOUtils.readFloat(valueBuffer);
          valueBuffer.position(beforePosition);
          return String.valueOf(resFloat);
        case DOUBLE:
          double resDouble = ReadWriteIOUtils.readDouble(valueBuffer);
          valueBuffer.position(beforePosition);
          return String.valueOf(resDouble);
        case TEXT:
          int binarySize = valueBuffer.getInt();
          byte[] binaryValue = new byte[binarySize];
          valueBuffer.get(binaryValue);
          valueBuffer.position(beforePosition);
          return new String(binaryValue);
        default:
          return null;
      }
    }
    return null;
  }

  public boolean isIgnoreTimeStamp() {
    return ignoreTimeStamp;
  }

  public void setIgnoreTimeStamp(boolean ignoreTimeStamp) {
    this.ignoreTimeStamp = ignoreTimeStamp;
  }
}
