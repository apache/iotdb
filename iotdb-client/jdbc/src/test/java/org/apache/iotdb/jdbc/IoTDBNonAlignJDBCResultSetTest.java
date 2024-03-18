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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.service.rpc.thrift.IClientRPCService;
import org.apache.iotdb.service.rpc.thrift.TSFetchResultsResp;
import org.apache.iotdb.service.rpc.thrift.TSQueryNonAlignDataSet;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class IoTDBNonAlignJDBCResultSetTest {

  private IoTDBNonAlignJDBCResultSet ioTDBNonAlignJDBCResultSet;

  @Mock private IClientRPCService.Iface client;

  @Mock private IoTDBStatement statement;

  @Mock private BitSet aliasColumnMap;

  @Mock private TSQueryNonAlignDataSet dataSet;

  @Mock private ByteBuffer buffer;

  private TSStatus successStatus = RpcUtils.SUCCESS_STATUS;

  @Before
  public void setUp() throws SQLException {

    MockitoAnnotations.initMocks(this);
    Long sessionId = 1L;
    List<String> columnNameList = new ArrayList<>();
    columnNameList.add("Time");
    columnNameList.add("a");
    columnNameList.add("b");
    List<String> columnTypeList = new ArrayList<>();
    columnTypeList.add("INT64");
    columnTypeList.add("INT32");
    columnTypeList.add("FLOAT");
    Map<String, Integer> columnNameIndex = new HashMap<>();
    columnNameIndex.put("Time", 1);
    columnNameIndex.put("a", 0);
    columnNameIndex.put("b", 0);
    List<ByteBuffer> byteBuffers = new ArrayList<>();
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {});
    byteBuffers.add(buffer);
    List<String> sgColumns = new ArrayList<>();

    ioTDBNonAlignJDBCResultSet =
        new IoTDBNonAlignJDBCResultSet(
            statement,
            columnNameList,
            columnTypeList,
            columnNameIndex,
            false,
            client,
            "",
            1L,
            sessionId,
            dataSet,
            null,
            (long) 60 * 1000,
            "COUNT",
            sgColumns,
            aliasColumnMap);
  }

  @After
  public void tearDown() {}

  @Test
  public void getLong() throws SQLException {
    long a = ioTDBNonAlignJDBCResultSet.getLong("a");
    assertEquals(a, 0L);
  }

  @Test
  public void fetchResults() throws SQLException, TException {
    when(client.fetchResults(any())).thenReturn(new TSFetchResultsResp(successStatus, true, true));
    when(client.closeOperation(any())).thenReturn(successStatus);
    boolean b = ioTDBNonAlignJDBCResultSet.fetchResults();
    assertEquals(b, false);
  }

  @Test
  public void constructOneRow() throws SQLException, TException {
    dataSet.timeList = new ArrayList<>();
    dataSet.timeList.add(buffer);
    ioTDBNonAlignJDBCResultSet.constructOneRow();
  }

  @Test
  public void getOperationType() {
    assertEquals(ioTDBNonAlignJDBCResultSet.getOperationType(), "COUNT");
  }

  @Test
  public void getSgColumns() {
    assertEquals(ioTDBNonAlignJDBCResultSet.getSgColumns().size(), 6);
  }

  @Test(expected = SQLException.class)
  public void isWrapperFor() throws SQLException {
    ioTDBNonAlignJDBCResultSet.isWrapperFor(String.class);
  }

  @Test(expected = SQLException.class)
  public void unwrap() throws SQLException {
    ioTDBNonAlignJDBCResultSet.unwrap(String.class);
  }

  @Test(expected = SQLException.class)
  public void absolute() throws SQLException {
    ioTDBNonAlignJDBCResultSet.absolute(1);
  }

  @Test(expected = SQLException.class)
  public void afterLast() throws SQLException {
    ioTDBNonAlignJDBCResultSet.afterLast();
  }

  @Test(expected = SQLException.class)
  public void beforeFirst() throws SQLException {
    ioTDBNonAlignJDBCResultSet.beforeFirst();
  }

  @Test(expected = SQLException.class)
  public void cancelRowUpdates() throws SQLException {
    ioTDBNonAlignJDBCResultSet.cancelRowUpdates();
  }

  @Test(expected = SQLException.class)
  public void clearWarnings() throws SQLException {
    ioTDBNonAlignJDBCResultSet.clearWarnings();
  }

  @Test(expected = SQLException.class)
  public void deleteRow() throws SQLException {
    ioTDBNonAlignJDBCResultSet.deleteRow();
  }

  @Test
  public void findColumn() {
    assertEquals(ioTDBNonAlignJDBCResultSet.findColumn("a"), 2);
  }

  @Test(expected = SQLException.class)
  public void first() throws SQLException {
    ioTDBNonAlignJDBCResultSet.first();
  }

  @Test(expected = SQLException.class)
  public void getArray1() throws SQLException {
    ioTDBNonAlignJDBCResultSet.getArray(1);
  }

  @Test(expected = SQLException.class)
  public void getArray2() throws SQLException {
    ioTDBNonAlignJDBCResultSet.getArray("");
  }

  @Test(expected = SQLException.class)
  public void getAsciiStream1() throws SQLException {
    ioTDBNonAlignJDBCResultSet.getAsciiStream(1);
  }

  @Test(expected = SQLException.class)
  public void getAsciiStream2() throws SQLException {
    ioTDBNonAlignJDBCResultSet.getAsciiStream("");
  }

  @Test
  public void getBigDecimal1() throws SQLException {
    assertEquals(ioTDBNonAlignJDBCResultSet.getBigDecimal(4).doubleValue(), 0.0, 1);
  }

  @Test
  public void getBigDecimal2() throws SQLException {
    assertEquals(ioTDBNonAlignJDBCResultSet.getBigDecimal("a").doubleValue(), 0.0, 1);
  }

  @Test
  public void getBigDecimal3() throws SQLException {
    assertEquals(ioTDBNonAlignJDBCResultSet.getBigDecimal(4, 1).doubleValue(), 0.0, 1);
  }

  @Test(expected = SQLException.class)
  public void getBinaryStream1() throws SQLException {
    ioTDBNonAlignJDBCResultSet.getBinaryStream(1);
  }

  @Test(expected = SQLException.class)
  public void getBinaryStream2() throws SQLException {
    ioTDBNonAlignJDBCResultSet.getBinaryStream("");
  }

  @Test(expected = SQLException.class)
  public void getBlob1() throws SQLException {
    ioTDBNonAlignJDBCResultSet.getBlob(1);
  }

  @Test(expected = SQLException.class)
  public void getBlob2() throws SQLException {
    ioTDBNonAlignJDBCResultSet.getBlob("");
  }

  @Test(expected = SQLException.class)
  public void getBoolean1() throws SQLException {
    ioTDBNonAlignJDBCResultSet.getBoolean(1);
  }

  @Test(expected = SQLException.class)
  public void getBoolean2() throws SQLException {
    ioTDBNonAlignJDBCResultSet.getBoolean("");
  }

  @Test(expected = SQLException.class)
  public void getByte1() throws SQLException {
    ioTDBNonAlignJDBCResultSet.getByte(1);
  }

  @Test(expected = SQLException.class)
  public void getByte2() throws SQLException {
    ioTDBNonAlignJDBCResultSet.getByte("");
  }

  @Test(expected = SQLException.class)
  public void getBytes1() throws SQLException {
    ioTDBNonAlignJDBCResultSet.getBytes(1);
  }

  @Test(expected = SQLException.class)
  public void getBytes2() throws SQLException {
    ioTDBNonAlignJDBCResultSet.getBytes("");
  }

  @Test(expected = SQLException.class)
  public void getCharacterStream1() throws SQLException {
    ioTDBNonAlignJDBCResultSet.getCharacterStream(1);
  }

  @Test(expected = SQLException.class)
  public void getCharacterStream2() throws SQLException {
    ioTDBNonAlignJDBCResultSet.getCharacterStream("");
  }

  @Test(expected = SQLException.class)
  public void getClob1() throws SQLException {
    ioTDBNonAlignJDBCResultSet.getClob(1);
  }

  @Test(expected = SQLException.class)
  public void getClob2() throws SQLException {
    ioTDBNonAlignJDBCResultSet.getClob("");
  }

  @Test
  public void getConcurrency() {
    assertEquals(ioTDBNonAlignJDBCResultSet.getConcurrency(), 1007);
  }

  @Test(expected = SQLException.class)
  public void getCursorName() throws SQLException {
    ioTDBNonAlignJDBCResultSet.getCursorName();
  }

  @Test
  public void getDate1() throws SQLException {
    assertEquals(ioTDBNonAlignJDBCResultSet.getDate(4).getTime(), 0L);
  }

  @Test(expected = SQLException.class)
  public void getDate3() throws SQLException {
    ioTDBNonAlignJDBCResultSet.getDate(1, null);
  }

  @Test(expected = SQLException.class)
  public void getDate4() throws SQLException {
    ioTDBNonAlignJDBCResultSet.getDate("", null);
  }

  @Test(expected = SQLException.class)
  public void getDouble1() throws SQLException {
    ioTDBNonAlignJDBCResultSet.getDouble(1);
  }

  @Test(expected = SQLException.class)
  public void getDouble2() throws SQLException {
    ioTDBNonAlignJDBCResultSet.getDouble("");
  }

  @Test
  public void getFetchDirection() {
    assertEquals(ioTDBNonAlignJDBCResultSet.getFetchDirection(), 1000);
  }

  @Test(expected = SQLException.class)
  public void setFetchDirection() throws SQLException {
    ioTDBNonAlignJDBCResultSet.setFetchDirection(1);
  }

  @Test(expected = SQLException.class)
  public void getFetchSize1() throws SQLException {
    ioTDBNonAlignJDBCResultSet.getFetchSize();
  }

  @Test(expected = SQLException.class)
  public void setFetchSize2() throws SQLException {
    ioTDBNonAlignJDBCResultSet.setFetchSize(1);
  }

  @Test(expected = SQLException.class)
  public void getFloat1() throws SQLException {
    ioTDBNonAlignJDBCResultSet.getFloat(1);
  }

  @Test(expected = SQLException.class)
  public void getFloat2() throws SQLException {
    ioTDBNonAlignJDBCResultSet.getFloat("");
  }

  @Test(expected = SQLException.class)
  public void getHoldability() throws SQLException {
    ioTDBNonAlignJDBCResultSet.getHoldability();
  }

  @Test(expected = SQLException.class)
  public void getInt1() throws SQLException {
    ioTDBNonAlignJDBCResultSet.getInt(1);
  }

  @Test(expected = SQLException.class)
  public void getInt2() throws SQLException {
    ioTDBNonAlignJDBCResultSet.getInt("");
  }

  @Test
  public void getLong1() throws SQLException {
    assertEquals(ioTDBNonAlignJDBCResultSet.getLong(4), 0);
  }

  @Test
  public void getMetaData() throws SQLException {
    ResultSetMetaData metaData = ioTDBNonAlignJDBCResultSet.getMetaData();
    assertEquals(metaData.getColumnName(1), "TimeTime");
  }

  @Test(expected = SQLException.class)
  public void getNCharacterStream1() throws SQLException {
    ioTDBNonAlignJDBCResultSet.getNCharacterStream(1);
  }

  @Test(expected = SQLException.class)
  public void getNCharacterStream2() throws SQLException {
    ioTDBNonAlignJDBCResultSet.getNCharacterStream("");
  }

  @Test(expected = SQLException.class)
  public void getNClob1() throws SQLException {
    ioTDBNonAlignJDBCResultSet.getNClob(1);
  }

  @Test(expected = SQLException.class)
  public void getNClob2() throws SQLException {
    ioTDBNonAlignJDBCResultSet.getNClob("");
  }

  @Test(expected = SQLException.class)
  public void getNString1() throws SQLException {
    ioTDBNonAlignJDBCResultSet.getNString(1);
  }

  @Test(expected = SQLException.class)
  public void getNString2() throws SQLException {
    ioTDBNonAlignJDBCResultSet.getNString("");
  }

  @Test
  public void getObject1() throws SQLException {
    ioTDBNonAlignJDBCResultSet.getObject(4);
  }

  @Test
  public void getObject2() throws SQLException {
    Object a = ioTDBNonAlignJDBCResultSet.getObject("a");
    assertEquals(a.toString(), "0.0");
  }

  @Test(expected = SQLException.class)
  public void getObject3() throws SQLException {
    ioTDBNonAlignJDBCResultSet.getObject(1, new HashMap<>());
  }

  @Test(expected = SQLException.class)
  public void getObject4() throws SQLException {
    ioTDBNonAlignJDBCResultSet.getObject("", new HashMap<>());
  }

  @Test(expected = SQLException.class)
  public void getObject5() throws SQLException {
    ioTDBNonAlignJDBCResultSet.getObject(1, String.class);
  }

  @Test(expected = SQLException.class)
  public void getObject6() throws SQLException {
    ioTDBNonAlignJDBCResultSet.getObject("", String.class);
  }

  @Test(expected = SQLException.class)
  public void getRef1() throws SQLException {
    ioTDBNonAlignJDBCResultSet.getRef(1);
  }

  @Test(expected = SQLException.class)
  public void getRef2() throws SQLException {
    ioTDBNonAlignJDBCResultSet.getRef("");
  }

  @Test(expected = SQLException.class)
  public void getRow() throws SQLException {
    ioTDBNonAlignJDBCResultSet.getRow();
  }

  @Test(expected = SQLException.class)
  public void getRowId1() throws SQLException {
    ioTDBNonAlignJDBCResultSet.getRowId(1);
  }

  @Test(expected = SQLException.class)
  public void getRowId2() throws SQLException {
    ioTDBNonAlignJDBCResultSet.getRowId("");
  }

  @Test(expected = SQLException.class)
  public void getSQLXML1() throws SQLException {
    ioTDBNonAlignJDBCResultSet.getSQLXML(1);
  }

  @Test(expected = SQLException.class)
  public void getSQLXML2() throws SQLException {
    ioTDBNonAlignJDBCResultSet.getSQLXML("");
  }

  @Test(expected = SQLException.class)
  public void getShort1() throws SQLException {
    ioTDBNonAlignJDBCResultSet.getShort(1);
  }

  @Test(expected = SQLException.class)
  public void getShort2() throws SQLException {
    ioTDBNonAlignJDBCResultSet.getShort("");
  }

  @Test
  public void getStatement() {
    assertEquals(ioTDBNonAlignJDBCResultSet.getStatement(), statement);
  }

  @Test
  public void getString1() throws SQLException {
    assertEquals(ioTDBNonAlignJDBCResultSet.getString(4), "0.0");
  }

  @Test
  public void getString2() throws SQLException {
    assertEquals(ioTDBNonAlignJDBCResultSet.getString("a"), "0.0");
  }

  @Test
  public void getTime1() throws SQLException {
    Time time = ioTDBNonAlignJDBCResultSet.getTime(4);
    assertEquals(time.getTime(), 0L);
  }

  @Test(expected = SQLException.class)
  public void getTime3() throws SQLException {
    ioTDBNonAlignJDBCResultSet.getTime(1, null);
  }

  @Test(expected = SQLException.class)
  public void getTime4() throws SQLException {
    ioTDBNonAlignJDBCResultSet.getTime("", null);
  }

  @Test
  public void getTimestamp1() throws SQLException {
    Timestamp time = ioTDBNonAlignJDBCResultSet.getTimestamp(4);
    assertEquals(time.getTime(), 0L);
  }

  @Test(expected = SQLException.class)
  public void getTimestamp3() throws SQLException {
    ioTDBNonAlignJDBCResultSet.getTimestamp(1, null);
  }

  @Test(expected = SQLException.class)
  public void getTimestamp4() throws SQLException {
    ioTDBNonAlignJDBCResultSet.getTimestamp("", null);
  }

  @Test
  public void getType() {
    assertEquals(ioTDBNonAlignJDBCResultSet.getType(), 1003);
  }

  @Test(expected = SQLException.class)
  public void getURL1() throws SQLException {
    ioTDBNonAlignJDBCResultSet.getURL(1);
  }

  @Test(expected = SQLException.class)
  public void getURL2() throws SQLException {
    ioTDBNonAlignJDBCResultSet.getURL("");
  }

  @Test(expected = SQLException.class)
  public void getUnicodeStream1() throws SQLException {
    ioTDBNonAlignJDBCResultSet.getUnicodeStream(1);
  }

  @Test(expected = SQLException.class)
  public void getUnicodeStream2() throws SQLException {
    ioTDBNonAlignJDBCResultSet.getUnicodeStream("");
  }

  @Test(expected = SQLException.class)
  public void insertRow() throws SQLException {
    ioTDBNonAlignJDBCResultSet.insertRow();
  }

  @Test(expected = SQLException.class)
  public void isAfterLast() throws SQLException {
    ioTDBNonAlignJDBCResultSet.isAfterLast();
  }

  @Test(expected = SQLException.class)
  public void isBeforeFirst() throws SQLException {
    ioTDBNonAlignJDBCResultSet.isBeforeFirst();
  }

  @Test
  public void isClosed() {
    assertEquals(ioTDBNonAlignJDBCResultSet.isClosed(), false);
  }

  @Test(expected = SQLException.class)
  public void isFirst() throws SQLException {
    ioTDBNonAlignJDBCResultSet.isFirst();
  }

  @Test(expected = SQLException.class)
  public void isLast() throws SQLException {
    ioTDBNonAlignJDBCResultSet.isLast();
  }

  @Test(expected = SQLException.class)
  public void last() throws SQLException {
    ioTDBNonAlignJDBCResultSet.last();
  }

  @Test(expected = SQLException.class)
  public void moveToCurrentRow() throws SQLException {
    ioTDBNonAlignJDBCResultSet.moveToCurrentRow();
  }

  @Test(expected = SQLException.class)
  public void moveToInsertRow() throws SQLException {
    ioTDBNonAlignJDBCResultSet.moveToInsertRow();
  }

  @Test
  public void next() throws SQLException {
    dataSet.timeList = new ArrayList<>();
    assertEquals(ioTDBNonAlignJDBCResultSet.next(), false);
  }

  @Test(expected = SQLException.class)
  public void previous() throws SQLException {
    ioTDBNonAlignJDBCResultSet.previous();
  }

  @Test(expected = SQLException.class)
  public void refreshRow() throws SQLException {
    ioTDBNonAlignJDBCResultSet.refreshRow();
  }

  @Test(expected = SQLException.class)
  public void relative() throws SQLException {
    ioTDBNonAlignJDBCResultSet.relative(1);
  }

  @Test(expected = SQLException.class)
  public void rowDeleted() throws SQLException {
    ioTDBNonAlignJDBCResultSet.rowDeleted();
  }

  @Test(expected = SQLException.class)
  public void rowInserted() throws SQLException {
    ioTDBNonAlignJDBCResultSet.rowInserted();
  }

  @Test(expected = SQLException.class)
  public void rowUpdated() throws SQLException {
    ioTDBNonAlignJDBCResultSet.rowUpdated();
  }

  @Test(expected = SQLException.class)
  public void updateArray1() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateArray(1, null);
  }

  @Test(expected = SQLException.class)
  public void updateArray2() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateArray("", null);
  }

  @Test(expected = SQLException.class)
  public void updateAsciiStream1() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateAsciiStream(1, null);
  }

  @Test(expected = SQLException.class)
  public void updateAsciiStream2() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateAsciiStream("", null);
  }

  @Test(expected = SQLException.class)
  public void updateAsciiStream3() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateAsciiStream(1, null, 1);
  }

  @Test(expected = SQLException.class)
  public void updateAsciiStream4() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateAsciiStream("", null, 1);
  }

  @Test(expected = SQLException.class)
  public void updateAsciiStream5() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateAsciiStream(1, null, 1L);
  }

  @Test(expected = SQLException.class)
  public void updateAsciiStream6() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateAsciiStream("", null, 1L);
  }

  @Test(expected = SQLException.class)
  public void updateBigDecimal1() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateBigDecimal(1, null);
  }

  @Test(expected = SQLException.class)
  public void updateBigDecimal2() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateBigDecimal("", null);
  }

  @Test(expected = SQLException.class)
  public void updateBinaryStream1() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateBinaryStream(1, null);
  }

  @Test(expected = SQLException.class)
  public void updateBinaryStream2() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateBinaryStream("", null);
  }

  @Test(expected = SQLException.class)
  public void updateBinaryStream3() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateBinaryStream(1, null, 1);
  }

  @Test(expected = SQLException.class)
  public void updateBinaryStream4() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateBinaryStream("", null, 1);
  }

  @Test(expected = SQLException.class)
  public void updateBinaryStream5() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateBinaryStream(1, null, 1L);
  }

  @Test(expected = SQLException.class)
  public void updateBinaryStream6() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateBinaryStream("", null, 1L);
  }

  @Test(expected = SQLException.class)
  public void updateBlob1() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateBlob(1, new SerialBlob(new byte[] {}));
  }

  @Test(expected = SQLException.class)
  public void updateBlob2() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateBlob("", new SerialBlob(new byte[] {}));
  }

  @Test(expected = SQLException.class)
  public void updateBlob3() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateBlob(1, new ByteArrayInputStream(new byte[] {}));
  }

  @Test(expected = SQLException.class)
  public void updateBlob4() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateBlob("", new ByteArrayInputStream(new byte[] {}));
  }

  @Test(expected = SQLException.class)
  public void updateBlob5() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateBlob(1, null, 1L);
  }

  @Test(expected = SQLException.class)
  public void updateBlob6() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateBlob("", null, 1L);
  }

  @Test(expected = SQLException.class)
  public void updateBoolean1() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateBoolean(1, true);
  }

  @Test(expected = SQLException.class)
  public void updateBoolean2() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateBoolean("", true);
  }

  @Test(expected = SQLException.class)
  public void updateByte1() throws SQLException {
    byte b = 1;
    ioTDBNonAlignJDBCResultSet.updateByte(1, b);
  }

  @Test(expected = SQLException.class)
  public void updateByte2() throws SQLException {
    byte b = 1;
    ioTDBNonAlignJDBCResultSet.updateByte("", b);
  }

  @Test(expected = SQLException.class)
  public void updateBytes1() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateBytes(1, null);
  }

  @Test(expected = SQLException.class)
  public void updateBytes2() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateBytes("", null);
  }

  @Test(expected = SQLException.class)
  public void updateCharacterStream1() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateCharacterStream(1, null);
  }

  @Test(expected = SQLException.class)
  public void updateCharacterStream2() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateCharacterStream("", null);
  }

  @Test(expected = SQLException.class)
  public void updateCharacterStream3() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateCharacterStream(1, null, 1);
  }

  @Test(expected = SQLException.class)
  public void updateCharacterStream4() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateCharacterStream("", null, 1);
  }

  @Test(expected = SQLException.class)
  public void updateCharacterStream5() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateCharacterStream(1, null, 1L);
  }

  @Test(expected = SQLException.class)
  public void updateCharacterStream6() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateCharacterStream("", null, 1L);
  }

  @Test(expected = SQLException.class)
  public void updateClob1() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateClob(1, new SerialClob(new char[] {}));
  }

  @Test(expected = SQLException.class)
  public void updateClob2() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateClob("", new SerialClob(new char[] {}));
  }

  @Test(expected = SQLException.class)
  public void updateClob3() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateClob(
        1, new InputStreamReader(new ByteArrayInputStream(new byte[] {})));
  }

  @Test(expected = SQLException.class)
  public void updateClob4() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateClob(
        "", new InputStreamReader(new ByteArrayInputStream(new byte[] {})));
  }

  @Test(expected = SQLException.class)
  public void updateClob5() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateClob(1, null, 1L);
  }

  @Test(expected = SQLException.class)
  public void updateClob6() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateClob("", null, 1L);
  }

  @Test(expected = SQLException.class)
  public void updateDate1() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateDate(1, null);
  }

  @Test(expected = SQLException.class)
  public void updateDate2() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateDate("", null);
  }

  @Test(expected = SQLException.class)
  public void updateDouble1() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateDouble(1, 1d);
  }

  @Test(expected = SQLException.class)
  public void updateDouble2() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateDouble("", 1d);
  }

  @Test(expected = SQLException.class)
  public void updateFloat1() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateFloat(1, 1f);
  }

  @Test(expected = SQLException.class)
  public void updateFloat2() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateFloat("", 1f);
  }

  @Test(expected = SQLException.class)
  public void updateInt1() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateInt(1, 1);
  }

  @Test(expected = SQLException.class)
  public void updateInt2() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateInt("", 1);
  }

  @Test(expected = SQLException.class)
  public void updateLong1() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateLong(1, 1L);
  }

  @Test(expected = SQLException.class)
  public void updateLong2() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateLong("", 1L);
  }

  @Test(expected = SQLException.class)
  public void updateNCharacterStream1() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateNCharacterStream(1, null);
  }

  @Test(expected = SQLException.class)
  public void updateNCharacterStream2() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateNCharacterStream("", null);
  }

  @Test(expected = SQLException.class)
  public void updateNCharacterStream3() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateNCharacterStream(1, null, 1L);
  }

  @Test(expected = SQLException.class)
  public void updateNCharacterStream4() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateNCharacterStream("", null, 1L);
  }

  @Test(expected = SQLException.class)
  public void updateNClob3() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateNClob(
        1, new InputStreamReader(new ByteArrayInputStream(new byte[] {})));
  }

  @Test(expected = SQLException.class)
  public void updateNClob4() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateNClob(
        "", new InputStreamReader(new ByteArrayInputStream(new byte[] {})));
  }

  @Test(expected = SQLException.class)
  public void updateNClob5() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateNClob(1, null, 1L);
  }

  @Test(expected = SQLException.class)
  public void updateNClob6() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateNClob("", null, 1L);
  }

  @Test(expected = SQLException.class)
  public void updateNString1() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateNString(1, "");
  }

  @Test(expected = SQLException.class)
  public void updateNString2() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateNString("", "");
  }

  @Test(expected = SQLException.class)
  public void updateNull1() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateNull(1);
  }

  @Test(expected = SQLException.class)
  public void updateNull2() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateNull("");
  }

  @Test(expected = SQLException.class)
  public void updateObject1() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateObject(1, "");
  }

  @Test(expected = SQLException.class)
  public void updateObject2() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateObject("", "");
  }

  @Test(expected = SQLException.class)
  public void updateObject3() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateObject(1, "", 1);
  }

  @Test(expected = SQLException.class)
  public void updateObject4() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateObject("", "", 1);
  }

  @Test(expected = SQLException.class)
  public void updateRef1() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateRef(1, null);
  }

  @Test(expected = SQLException.class)
  public void updateRef2() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateRef("", null);
  }

  @Test(expected = SQLException.class)
  public void updateRow() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateRow();
  }

  @Test(expected = SQLException.class)
  public void updateRowId1() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateRowId(1, null);
  }

  @Test(expected = SQLException.class)
  public void updateRowId2() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateRowId("", null);
  }

  @Test(expected = SQLException.class)
  public void updateSQLXML1() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateSQLXML(1, null);
  }

  @Test(expected = SQLException.class)
  public void updateSQLXML2() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateSQLXML("", null);
  }

  @Test(expected = SQLException.class)
  public void updateShort1() throws SQLException {
    short s = 1;
    ioTDBNonAlignJDBCResultSet.updateShort(1, s);
  }

  @Test(expected = SQLException.class)
  public void updateShort2() throws SQLException {
    short s = 1;
    ioTDBNonAlignJDBCResultSet.updateShort("", s);
  }

  @Test(expected = SQLException.class)
  public void updateString1() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateString(1, "");
  }

  @Test(expected = SQLException.class)
  public void updateString2() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateString("", "");
  }

  @Test(expected = SQLException.class)
  public void updateTime1() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateTime(1, null);
  }

  @Test(expected = SQLException.class)
  public void updateTime2() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateTime("", null);
  }

  @Test(expected = SQLException.class)
  public void updateTimestamp1() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateTimestamp(1, null);
  }

  @Test(expected = SQLException.class)
  public void updateTimestamp2() throws SQLException {
    ioTDBNonAlignJDBCResultSet.updateTimestamp("", null);
  }
}
