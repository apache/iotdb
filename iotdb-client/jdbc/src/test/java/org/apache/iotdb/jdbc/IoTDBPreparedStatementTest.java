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
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.IClientRPCService.Iface;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.Date;
import java.sql.ParameterMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.ZoneId;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class IoTDBPreparedStatementTest {

  @Mock TSExecuteStatementResp execStatementResp;
  @Mock TSStatus getOperationStatusResp;
  private ZoneId zoneId = ZoneId.systemDefault();
  @Mock private IoTDBConnection connection;
  @Mock private Iface client;
  @Mock private TSStatus successStatus = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  private TSStatus Status_SUCCESS = new TSStatus(successStatus);
  private long queryId;
  private long sessionId;

  @Before
  public void before() throws Exception {
    MockitoAnnotations.initMocks(this);
    when(execStatementResp.getStatus()).thenReturn(Status_SUCCESS);
    when(execStatementResp.getQueryId()).thenReturn(queryId);

    when(client.executeStatementV2(any(TSExecuteStatementReq.class))).thenReturn(execStatementResp);
    when(client.closeOperation(any())).thenReturn(Status_SUCCESS);
  }

  @SuppressWarnings("resource")
  @Test
  public void testConstructorRejectsNullSqlBeforeRequestingStatementId() throws Exception {
    assertThrows(
        SQLException.class,
        () -> new IoTDBPreparedStatement(connection, client, sessionId, null, zoneId));

    verify(client, never()).requestStatementId(anyLong());
    verify(client, never()).closeOperation(any());
  }

  @SuppressWarnings("resource")
  @Test
  public void testNonParameterized() throws Exception {
    String sql =
        "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < 24 and time > 2017-11-1 0:13:00";
    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(connection, client, sessionId, sql, zoneId);
    ps.execute();

    ArgumentCaptor<TSExecuteStatementReq> argument =
        ArgumentCaptor.forClass(TSExecuteStatementReq.class);
    verify(client).executeStatementV2(argument.capture());
    assertEquals(
        "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < 24 and time > 2017-11-1 0:13:00",
        argument.getValue().getStatement());
  }

  @SuppressWarnings("resource")
  @Test
  public void testParameterMetadataWrapperMethods() throws Exception {
    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(connection, client, sessionId, "SELECT ?", zoneId);
    ParameterMetaData metadata = ps.getParameterMetaData();

    assertTrue(metadata.isWrapperFor(ParameterMetaData.class));
    assertFalse(metadata.isWrapperFor(String.class));
    assertFalse(metadata.isWrapperFor(null));
    assertSame(metadata, metadata.unwrap(ParameterMetaData.class));
    assertThrows(SQLException.class, () -> metadata.unwrap(String.class));
  }

  @SuppressWarnings("resource")
  @Test
  public void testParameterMetadataRejectsUnsetIndexAndHandlesNullValue() throws Exception {
    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(
            connection, client, sessionId, "SELECT ? FROM root.sg.d WHERE s = '?'", zoneId);
    ParameterMetaData metadata = ps.getParameterMetaData();

    assertEquals(1, metadata.getParameterCount());
    assertEquals(0, metadata.getPrecision(1));
    assertThrows(SQLException.class, () -> metadata.getPrecision(0));
    assertThrows(SQLException.class, () -> metadata.getParameterMode(2));
    assertThrows(SQLException.class, () -> ps.setString(0, "x"));
    assertThrows(SQLException.class, () -> ps.setInt(2, 1));

    ps.setString(1, null);

    assertEquals(1, metadata.getParameterCount());
    assertEquals(0, metadata.getPrecision(1));
    assertEquals(Types.NULL, metadata.getParameterType(1));
    assertEquals(ParameterMetaData.parameterModeUnknown, metadata.getParameterMode(1));
  }

  @SuppressWarnings("resource")
  @Test
  public void getMetaDataReturnsNullWhenNoResultSetExists() throws Exception {
    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(connection, client, sessionId, "SELECT ?", zoneId);

    assertNull(ps.getMetaData());
  }

  @SuppressWarnings("resource")
  @Test
  public void testClosedPreparedStatementRejectsParameterOperations() throws Exception {
    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(connection, client, sessionId, "SELECT ?", zoneId);
    ParameterMetaData metadata = ps.getParameterMetaData();

    ps.close();

    assertTrue(ps.isClosed());
    assertThrows(SQLException.class, () -> ps.clearParameters());
    assertThrows(SQLException.class, () -> ps.setInt(1, 1));
    assertThrows(SQLException.class, () -> ps.setString(1, "x"));
    assertThrows(
        SQLException.class,
        () -> ps.setBinaryStream(1, new ByteArrayInputStream(new byte[] {1}), 1));
    assertThrows(SQLException.class, () -> ps.getParameterMetaData());
    assertThrows(SQLException.class, () -> metadata.isWrapperFor(ParameterMetaData.class));
    assertThrows(SQLException.class, () -> metadata.unwrap(ParameterMetaData.class));
    assertThrows(SQLException.class, () -> metadata.getParameterCount());
    assertThrows(SQLException.class, () -> metadata.getPrecision(1));

    SQLException unsupportedException =
        assertThrows(SQLException.class, () -> ps.setArray(1, null));
    assertTrue(unsupportedException.getMessage().contains("statement has been closed"));
    unsupportedException = assertThrows(SQLException.class, () -> ps.setRowId(1, null));
    assertTrue(unsupportedException.getMessage().contains("statement has been closed"));
    unsupportedException = assertThrows(SQLException.class, () -> ps.setObject(1, new Object()));
    assertTrue(unsupportedException.getMessage().contains("statement has been closed"));
  }

  @SuppressWarnings("resource")
  @Test
  public void invalidParameterIndex() throws SQLException {
    String sql =
        "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < 24 and time > 2017-11-1 0:13:00";
    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(connection, client, sessionId, sql, zoneId);
    assertThrows(SQLException.class, () -> ps.setString(1, "123"));
  }

  @SuppressWarnings("resource")
  @Test
  public void unsetArgument() throws SQLException {
    String sql =
        "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < 24 and time > ?";
    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(connection, client, sessionId, sql, zoneId);
    assertThrows(SQLException.class, () -> ps.execute());
  }

  @SuppressWarnings("resource")
  @Test
  public void executeWithUnsetParameterClosesPreviousResultSet() throws SQLException {
    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(connection, client, sessionId, "SELECT ?", zoneId);
    ResultSet previousResultSet = mock(ResultSet.class);
    ps.resultSet = previousResultSet;

    assertThrows(SQLException.class, ps::execute);

    verify(previousResultSet).close();
    assertNull(ps.resultSet);
  }

  @SuppressWarnings("resource")
  @Test
  public void oneIntArgument() throws Exception {
    String sql =
        "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < ? and time > 2017-11-1 0:13:00";
    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(connection, client, sessionId, sql, zoneId);
    ps.setInt(1, 123);
    ps.execute();
    ArgumentCaptor<TSExecuteStatementReq> argument =
        ArgumentCaptor.forClass(TSExecuteStatementReq.class);
    verify(client).executeStatementV2(argument.capture());
    assertEquals(
        "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < 123 and time > 2017-11-1 0:13:00",
        argument.getValue().getStatement());
  }

  @SuppressWarnings("resource")
  @Test
  public void oneLongArgument() throws Exception {
    String sql =
        "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < ? and time > 2017-11-1 0:13:00";
    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(connection, client, sessionId, sql, zoneId);
    ps.setLong(1, 123);
    ps.execute();
    ArgumentCaptor<TSExecuteStatementReq> argument =
        ArgumentCaptor.forClass(TSExecuteStatementReq.class);
    verify(client).executeStatementV2(argument.capture());
    assertEquals(
        "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < 123 and time > 2017-11-1 0:13:00",
        argument.getValue().getStatement());
  }

  @SuppressWarnings("resource")
  @Test
  public void oneFloatArgument() throws Exception {
    String sql =
        "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < ? and time > 2017-11-1 0:13:00";
    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(connection, client, sessionId, sql, zoneId);
    ps.setFloat(1, 123.133f);
    ps.execute();
    ArgumentCaptor<TSExecuteStatementReq> argument =
        ArgumentCaptor.forClass(TSExecuteStatementReq.class);
    verify(client).executeStatementV2(argument.capture());
    assertEquals(
        "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < 123.133 and time > 2017-11-1 0:13:00",
        argument.getValue().getStatement());
  }

  @SuppressWarnings("resource")
  @Test
  public void oneDoubleArgument() throws Exception {
    String sql =
        "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < ? and time > 2017-11-1 0:13:00";
    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(connection, client, sessionId, sql, zoneId);
    ps.setDouble(1, 123.456);
    ps.execute();
    ArgumentCaptor<TSExecuteStatementReq> argument =
        ArgumentCaptor.forClass(TSExecuteStatementReq.class);
    verify(client).executeStatementV2(argument.capture());
    assertEquals(
        "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < 123.456 and time > 2017-11-1 0:13:00",
        argument.getValue().getStatement());
  }

  @SuppressWarnings("resource")
  @Test
  public void oneBooleanArgument() throws Exception {
    String sql =
        "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < ? and time > 2017-11-1 0:13:00";
    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(connection, client, sessionId, sql, zoneId);
    ps.setBoolean(1, false);
    ps.execute();
    ArgumentCaptor<TSExecuteStatementReq> argument =
        ArgumentCaptor.forClass(TSExecuteStatementReq.class);
    verify(client).executeStatementV2(argument.capture());
    assertEquals(
        "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < false and time > 2017-11-1 0:13:00",
        argument.getValue().getStatement());
  }

  @SuppressWarnings("resource")
  @Test
  public void oneStringArgument1() throws Exception {
    String sql =
        "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < ? and time > 2017-11-1 0:13:00";
    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(connection, client, sessionId, sql, zoneId);
    ps.setString(1, "'abcde'");
    ps.execute();
    ArgumentCaptor<TSExecuteStatementReq> argument =
        ArgumentCaptor.forClass(TSExecuteStatementReq.class);
    verify(client).executeStatementV2(argument.capture());
    assertEquals(
        "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < '''abcde''' and time > 2017-11-1 0:13:00",
        argument.getValue().getStatement());
  }

  @SuppressWarnings("resource")
  @Test
  public void oneStringArgument2() throws Exception {
    String sql =
        "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < ? and time > 2017-11-1 0:13:00";
    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(connection, client, sessionId, sql, zoneId);
    ps.setString(1, "\"abcde\"");
    ps.execute();
    ArgumentCaptor<TSExecuteStatementReq> argument =
        ArgumentCaptor.forClass(TSExecuteStatementReq.class);
    verify(client).executeStatementV2(argument.capture());
    assertEquals(
        "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < '\"abcde\"' and time > 2017-11-1 0:13:00",
        argument.getValue().getStatement());
  }

  @SuppressWarnings("resource")
  @Test
  public void oneStringArgument3() throws Exception {
    String sql = "SELECT status, ? FROM root.ln.wf01.wt01";
    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(connection, client, sessionId, sql, zoneId);
    ps.setString(1, "temperature");
    ps.execute();
    ArgumentCaptor<TSExecuteStatementReq> argument =
        ArgumentCaptor.forClass(TSExecuteStatementReq.class);
    verify(client).executeStatementV2(argument.capture());
    assertEquals(
        "SELECT status, 'temperature' FROM root.ln.wf01.wt01", argument.getValue().getStatement());
  }

  @SuppressWarnings("resource")
  @Test
  public void nullArgumentsUseSqlNullLiteral() throws Exception {
    String sql = "INSERT INTO root.ln.wf01.wt01(time,a,b,c,d,e,f,g) VALUES(1,?,?,?,?,?,?,?)";
    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(connection, client, sessionId, sql, zoneId);

    ps.setString(1, null);
    ps.setObject(2, null);
    ps.setDate(3, (Date) null);
    ps.setTime(4, (Time) null);
    ps.setTimestamp(5, (Timestamp) null);
    ps.setBytes(6, null);
    ps.setBinaryStream(7, (InputStream) null, 0);
    ps.execute();

    ArgumentCaptor<TSExecuteStatementReq> argument =
        ArgumentCaptor.forClass(TSExecuteStatementReq.class);
    verify(client).executeStatementV2(argument.capture());
    assertEquals(
        "INSERT INTO root.ln.wf01.wt01(time,a,b,c,d,e,f,g) VALUES(1,NULL,NULL,NULL,NULL,NULL,NULL,NULL)",
        argument.getValue().getStatement());
  }

  @SuppressWarnings("resource")
  @Test
  public void setBinaryStreamRejectsNegativeLength() throws Exception {
    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(connection, client, sessionId, "SELECT ?", zoneId);

    assertThrows(
        SQLException.class, () -> ps.setBinaryStream(1, new ByteArrayInputStream(new byte[0]), -1));
  }

  @SuppressWarnings("resource")
  @Test
  public void oneTimeLongArgument() throws Exception {
    String sql = "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE time > ?";
    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(connection, client, sessionId, sql, zoneId);
    ps.setLong(1, 1233);
    ps.execute();
    ArgumentCaptor<TSExecuteStatementReq> argument =
        ArgumentCaptor.forClass(TSExecuteStatementReq.class);
    verify(client).executeStatementV2(argument.capture());
    assertEquals(
        "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE time > 1233",
        argument.getValue().getStatement());
  }

  @SuppressWarnings("resource")
  @Test
  public void oneTimeTimestampArgument() throws Exception {
    String sql = "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE time > ?";
    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(connection, client, sessionId, sql, zoneId);
    ps.setTimestamp(1, Timestamp.valueOf("2017-11-01 00:13:00"));
    ps.execute();
    ArgumentCaptor<TSExecuteStatementReq> argument =
        ArgumentCaptor.forClass(TSExecuteStatementReq.class);
    verify(client).executeStatementV2(argument.capture());
    assertEquals(
        "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE time > 2017-11-01T00:13:00",
        argument.getValue().getStatement());
  }

  @SuppressWarnings("resource")
  @Test
  public void escapingOfStringArgument() throws Exception {
    String sql =
        "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE status = '134' and temperature = ?";
    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(connection, client, sessionId, sql, zoneId);
    ps.setLong(1, 1333);
    ps.execute();

    ArgumentCaptor<TSExecuteStatementReq> argument =
        ArgumentCaptor.forClass(TSExecuteStatementReq.class);
    verify(client).executeStatementV2(argument.capture());
    assertEquals(
        "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE status = '134' and temperature = 1333",
        argument.getValue().getStatement());
  }

  @SuppressWarnings("resource")
  @Test
  public void pastingIntoEscapedQuery() throws Exception {
    String sql =
        "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE status = '\\044e' || temperature = ?";

    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(connection, client, sessionId, sql, zoneId);
    ps.setDouble(1, -1323.0);
    ps.execute();

    ArgumentCaptor<TSExecuteStatementReq> argument =
        ArgumentCaptor.forClass(TSExecuteStatementReq.class);
    verify(client).executeStatementV2(argument.capture());
    assertEquals(
        "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE status = '\\044e' || temperature = -1323.0",
        argument.getValue().getStatement());
  }

  @SuppressWarnings("resource")
  @Test
  public void testInsertStatement1() throws Exception {
    String sql = "INSERT INTO root.ln.wf01.wt01(time,a,b,c,d,e,f) VALUES(?,?,?,?,?,?,?)";

    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(connection, client, sessionId, sql, zoneId);
    ps.setLong(1, 12324);
    ps.setBoolean(2, false);
    ps.setInt(3, 123);
    ps.setLong(4, 123234345);
    ps.setFloat(5, 123.423f);
    ps.setDouble(6, -1323.0);
    ps.setString(7, "'abc'");
    ps.execute();

    ArgumentCaptor<TSExecuteStatementReq> argument =
        ArgumentCaptor.forClass(TSExecuteStatementReq.class);
    verify(client).executeStatementV2(argument.capture());
    assertEquals(
        "INSERT INTO root.ln.wf01.wt01(time,a,b,c,d,e,f) VALUES(12324,false,123,123234345,123.423,-1323.0,'''abc''')",
        argument.getValue().getStatement());
  }

  @SuppressWarnings("resource")
  @Test
  public void testInsertStatement2() throws Exception {
    String sql = "INSERT INTO root.ln.wf01.wt01(time,a,b,c,d,e,f,g,h) VALUES(?,?,?,?,?,?,?,?,?)";

    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(connection, client, sessionId, sql, zoneId);
    ps.setTimestamp(1, Timestamp.valueOf("2017-11-01 00:13:00"));
    ps.setBoolean(2, false);
    ps.setInt(3, 123);
    ps.setLong(4, 123234345);
    ps.setFloat(5, 123.423f);
    ps.setDouble(6, -1323.0);
    ps.setString(7, "\"abc\"");
    ps.setString(8, "abc");
    ps.setString(9, "'abc'");
    ps.execute();

    ArgumentCaptor<TSExecuteStatementReq> argument =
        ArgumentCaptor.forClass(TSExecuteStatementReq.class);
    verify(client).executeStatementV2(argument.capture());
    assertEquals(
        "INSERT INTO root.ln.wf01.wt01(time,a,b,c,d,e,f,g,h) VALUES(2017-11-01T00:13:00,false,123,123234345,123.423,-1323.0,'\"abc\"','abc','''abc''')",
        argument.getValue().getStatement());
  }

  @Test
  public void testInsertStatement3() throws Exception {
    String sql = "INSERT INTO root.ln.wf01.wt02(time,a,b,c,d,e,f) VALUES(?,?,?,?,?,?,?)";

    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(connection, client, sessionId, sql, zoneId);
    ps.setObject(1, "2020-01-01 10:10:10", Types.TIMESTAMP, -1);
    ps.setObject(2, false, Types.BOOLEAN, -1);
    ps.setObject(3, 123, Types.INTEGER, -1);
    ps.setObject(4, 123234345, Types.BIGINT);
    ps.setObject(5, 123.423f, Types.FLOAT);
    ps.setObject(6, -1323.0, Types.DOUBLE);
    ps.setObject(7, "\"abc\"", Types.VARCHAR);
    ps.execute();

    ArgumentCaptor<TSExecuteStatementReq> argument =
        ArgumentCaptor.forClass(TSExecuteStatementReq.class);
    verify(client).executeStatementV2(argument.capture());
    assertEquals(
        "INSERT INTO root.ln.wf01.wt02(time,a,b,c,d,e,f) VALUES(2020-01-01T10:10:10,false,123,123234345,123.423,-1323.0,'\"abc\"')",
        argument.getValue().getStatement());
  }

  @Test
  public void testInsertStatement4() throws Exception {
    String sql = "INSERT INTO root.ln.wf01.wt02(time,a,b,c,d,e,f) VALUES(?,?,?,?,?,?,?)";

    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(connection, client, sessionId, sql, zoneId);
    ps.setObject(1, "2020-01-01 10:10:10", Types.TIMESTAMP, -1);
    ps.setObject(2, false, Types.BOOLEAN, -1);
    ps.setObject(3, 123, Types.INTEGER, -1);
    ps.setObject(4, 123234345, Types.BIGINT);
    ps.setObject(5, 123.423f, Types.FLOAT);
    ps.setObject(6, -1323.0, Types.DOUBLE);
    ps.setObject(7, "abc", Types.VARCHAR);
    ps.execute();

    ArgumentCaptor<TSExecuteStatementReq> argument =
        ArgumentCaptor.forClass(TSExecuteStatementReq.class);
    verify(client).executeStatementV2(argument.capture());
    assertEquals(
        "INSERT INTO root.ln.wf01.wt02(time,a,b,c,d,e,f) VALUES(2020-01-01T10:10:10,false,123,123234345,123.423,-1323.0,'abc')",
        argument.getValue().getStatement());
  }
}
