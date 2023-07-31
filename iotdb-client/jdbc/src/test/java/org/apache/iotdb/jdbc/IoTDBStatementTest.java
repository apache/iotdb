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

import org.apache.iotdb.rpc.IoTDBRpcDataSet;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.service.rpc.thrift.IClientRPCService.Iface;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;
import org.apache.iotdb.service.rpc.thrift.TSFetchMetadataReq;
import org.apache.iotdb.service.rpc.thrift.TSFetchMetadataResp;
import org.apache.iotdb.service.rpc.thrift.TSQueryDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.BinaryColumn;
import org.apache.iotdb.tsfile.read.common.block.column.BooleanColumn;
import org.apache.iotdb.tsfile.read.common.block.column.DoubleColumn;
import org.apache.iotdb.tsfile.read.common.block.column.FloatColumn;
import org.apache.iotdb.tsfile.read.common.block.column.IntColumn;
import org.apache.iotdb.tsfile.read.common.block.column.LongColumn;
import org.apache.iotdb.tsfile.utils.Binary;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

public class IoTDBStatementTest {

  @Mock private IoTDBConnection connection;

  @Mock private Iface client;

  @Mock private TSExecuteStatementResp execResp;

  @Mock private IoTDBStatement statement;

  @Mock private TsBlock curTsBlock;

  @Mock private LongColumn longColumn;

  @Mock private DoubleColumn doubleColumn;

  @Mock private BinaryColumn binaryColumn;

  @Mock private BooleanColumn booleanColumn;

  @Mock private FloatColumn floatColumn;

  @Mock private IntColumn intColumn;

  @Mock private TSQueryDataSet tsQueryDataSet;

  @Mock private IoTDBRpcDataSet ioTDBRpcDataSet;

  private long sessionId;

  @Mock private TSFetchMetadataResp fetchMetadataResp;

  @Mock ResultSet result1;

  private ZoneId zoneID = ZoneId.systemDefault();

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    when(connection.getMetaData())
        .thenReturn(new IoTDBDatabaseMetadata(connection, client, sessionId));
    when(connection.isClosed()).thenReturn(false);
    when(client.fetchMetadata(any(TSFetchMetadataReq.class))).thenReturn(fetchMetadataResp);
    when(fetchMetadataResp.getStatus()).thenReturn(RpcUtils.SUCCESS_STATUS);
  }

  @After
  public void tearDown() {}

  @SuppressWarnings("resource")
  @Test
  public void testSetFetchSize1() throws SQLException {
    IoTDBStatement stmt = new IoTDBStatement(connection, client, sessionId, zoneID);
    stmt.setFetchSize(123);
    assertEquals(123, stmt.getFetchSize());
  }

  @SuppressWarnings("resource")
  @Test
  public void testSetFetchSize2() throws SQLException {
    IoTDBStatement stmt = new IoTDBStatement(connection, client, sessionId, zoneID);
    int initial = stmt.getFetchSize();
    stmt.setFetchSize(0);
    assertEquals(initial, stmt.getFetchSize());
  }

  @SuppressWarnings("resource")
  @Test
  public void testSetFetchSize3() throws SQLException {
    final int fetchSize = 10000;
    IoTDBStatement stmt = new IoTDBStatement(connection, client, sessionId, fetchSize, zoneID, 0);
    assertEquals(fetchSize, stmt.getFetchSize());
  }

  @SuppressWarnings("resource")
  @Test(expected = SQLException.class)
  public void testSetFetchSize4() throws SQLException {
    IoTDBStatement stmt = new IoTDBStatement(connection, client, sessionId, zoneID);
    stmt.setFetchSize(-1);
  }

  @Test
  public void setTimeoutTest() throws SQLException {
    IoTDBStatement statement = new IoTDBStatement(connection, client, sessionId, zoneID, 60);
    Assert.assertEquals(60, statement.getQueryTimeout());
    statement.setQueryTimeout(100);
    Assert.assertEquals(100, statement.getQueryTimeout());
  }

  @Test
  public void testExecuteQuery() throws SQLException, TException {

    List<String> columnNameList = new ArrayList<>();
    columnNameList.add("a");
    columnNameList.add("b");
    List<String> columnTypeList = new ArrayList<>();
    columnTypeList.add("INT64");
    columnTypeList.add("INT64");
    Map<String, Integer> columnNameIndex = new HashMap<>();
    columnNameIndex.put("Time", 1);
    columnNameIndex.put("a", 0);
    columnNameIndex.put("b", 0);
    List<ByteBuffer> byteBuffers = new ArrayList<>();
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {});
    byteBuffers.add(buffer);
    ResultSet result =
        new IoTDBJDBCResultSet(
            statement,
            columnNameList,
            columnTypeList,
            columnNameIndex,
            false,
            client,
            "",
            1L,
            sessionId,
            byteBuffers,
            null,
            (long) 60 * 1000,
            false);
    when(statement.executeQuery(any(String.class))).thenReturn(result);
    //    when(statement.execute(any(String.class))).thenReturn(true);
    //    when(client.executeStatementV2(any())).thenReturn(execResp);
    //    execResp.setQueryDataSet(tsQueryDataSet);
    IoTDBJDBCResultSet resultSet = (IoTDBJDBCResultSet) statement.executeQuery("");

    when(curTsBlock.getColumn(any(Integer.class))).thenReturn(longColumn);
    when(longColumn.getLong(any(Integer.class))).thenReturn(1L);
    resultSet.ioTDBRpcDataSet.curTsBlock = curTsBlock;

    Long a = resultSet.getLong("b");
    assertEquals(1L, a.longValue());
    a = resultSet.getLong(2);
    assertEquals(1L, a.longValue());

    Date date = resultSet.getDate("b");
    assertEquals(new Date(1), date);
    date = resultSet.getDate(2);
    assertEquals(new Date(1), date);

    Time time = resultSet.getTime("b");
    assertEquals(new Time(1), time);
    time = resultSet.getTime(2);
    assertEquals(new Time(1), time);

    Timestamp timestamp = resultSet.getTimestamp("b");
    assertEquals(new Timestamp(1), timestamp);
    timestamp = resultSet.getTimestamp(2);
    assertEquals(new Timestamp(1), timestamp);

    when(curTsBlock.getColumn(any(Integer.class))).thenReturn(doubleColumn);
    when(doubleColumn.getDouble(any(Integer.class))).thenReturn(1d);
    resultSet.ioTDBRpcDataSet.curTsBlock = curTsBlock;

    Double a1 = resultSet.getDouble("a");
    assertEquals(1d, a1.doubleValue(), 0.1);
    a1 = resultSet.getDouble(2);
    assertEquals(1d, a1.doubleValue(), 0.1);

    when(curTsBlock.getColumn(any(Integer.class))).thenReturn(binaryColumn);
    when(binaryColumn.getBinary(any(Integer.class))).thenReturn(new Binary("1"));
    resultSet.ioTDBRpcDataSet.columnTypeDeduplicatedList.set(0, TSDataType.TEXT);

    BigDecimal a2 = resultSet.getBigDecimal("a");
    assertEquals(a2, new BigDecimal(1));
    a2 = resultSet.getBigDecimal("a", 1);
    assertEquals(a2, new BigDecimal(1));
    a2 = resultSet.getBigDecimal(2);
    assertEquals(a2, new BigDecimal(1));
    a2 = resultSet.getBigDecimal(2, 1);
    assertEquals(a2, new BigDecimal(1));

    String a3 = resultSet.getString("a");
    assertEquals(a3, "1");
    a3 = resultSet.getString(2);
    assertEquals(a3, "1");

    Object a4 = resultSet.getObject("a");
    assertEquals(a4, null);
    a4 = resultSet.getObject(2);
    assertEquals(a4, null);

    when(curTsBlock.getColumn(any(Integer.class))).thenReturn(floatColumn);
    when(floatColumn.getFloat(any(Integer.class))).thenReturn(1f);
    resultSet.ioTDBRpcDataSet.curTsBlock = curTsBlock;

    Float a5 = resultSet.getFloat("a");
    assertEquals(1f, a5.floatValue(), 0.1);
    a5 = resultSet.getFloat(2);
    assertEquals(1f, a5.floatValue(), 0.1);

    when(curTsBlock.getColumn(any(Integer.class))).thenReturn(intColumn);
    when(intColumn.getInt(any(Integer.class))).thenReturn(1);
    resultSet.ioTDBRpcDataSet.curTsBlock = curTsBlock;

    Integer a6 = resultSet.getInt("a");
    assertEquals(1, a6.intValue());
    a6 = resultSet.getInt(2);
    assertEquals(1, a6.intValue());

    when(curTsBlock.getColumn(any(Integer.class))).thenReturn(booleanColumn);
    when(booleanColumn.getBoolean(any(Integer.class))).thenReturn(true);
    resultSet.ioTDBRpcDataSet.curTsBlock = curTsBlock;

    Boolean a7 = resultSet.getBoolean("a");
    assertEquals(a7, true);
    a7 = resultSet.getBoolean(2);
    assertEquals(a7, true);

    //
    //    statement.cancel();
    //
    //    boolean execute = statement.execute("");
    //    assertEquals(true,execute);
  }
}
