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
import org.apache.iotdb.service.rpc.thrift.ServerProperties;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;
import org.apache.iotdb.service.rpc.thrift.TSGetTimeZoneResp;
import org.apache.iotdb.service.rpc.thrift.TSPrepareReq;
import org.apache.iotdb.service.rpc.thrift.TSSetTimeZoneReq;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class IoTDBConnectionTest {

  @Mock private IClientRPCService.Iface client;

  private IoTDBConnection connection = new IoTDBConnection();
  private TSStatus successStatus = RpcUtils.SUCCESS_STATUS;
  private long sessionId;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @After
  public void tearDown() {}

  @Test
  public void testSetTimeZone() throws IoTDBSQLException, TException {
    openConnection(connection);
    String timeZone = "Asia/Shanghai";
    when(client.setTimeZone(any(TSSetTimeZoneReq.class))).thenReturn(new TSStatus(successStatus));
    connection.setClient(client);
    connection.setTimeZone(timeZone);
    assertEquals(connection.getTimeZone(), timeZone);
  }

  @Test
  public void testGetTimeZone() throws TException {
    String timeZone = ZoneId.systemDefault().toString();
    sessionId = connection.getSessionId();
    when(client.getTimeZone(sessionId)).thenReturn(new TSGetTimeZoneResp(successStatus, timeZone));
    connection.setClient(client);
    assertEquals(connection.getTimeZone(), timeZone);
  }

  @Test
  public void testSetTimeZoneByClientInfo() throws TException, SQLClientInfoException {
    openConnection(connection);
    String timeZone = "+07:00";
    assertNotEquals(connection.getTimeZone(), timeZone);
    when(client.setTimeZone(any(TSSetTimeZoneReq.class))).thenReturn(new TSStatus(successStatus));
    connection.setClient(client);
    connection.setClientInfo("time_zone", timeZone);
    assertEquals(connection.getTimeZone(), timeZone);
  }

  @Test
  public void testSetTimeZoneRejectsInvalidZoneBeforeRpc() throws TException {
    openConnection(connection);
    connection.setClient(client);

    assertThrows(IoTDBSQLException.class, () -> connection.setTimeZone("invalid-zone"));
    verify(client, never()).setTimeZone(any(TSSetTimeZoneReq.class));
  }

  @Test
  public void testSetClientInfoWrapsInvalidTimeZone() {
    openConnection(connection);
    SQLClientInfoException exception =
        assertThrows(
            SQLClientInfoException.class,
            () -> connection.setClientInfo("time_zone", "invalid-zone"));

    assertTrue(exception.getCause() instanceof IoTDBSQLException);
  }

  @Test
  public void testSetClientInfoRejectsNullNameWithoutNpe() {
    openConnection(connection);
    assertThrows(SQLClientInfoException.class, () -> connection.setClientInfo(null, "value"));
  }

  @Test
  public void testTableCatalogAndSchemaRejectNull() {
    IoTDBConnection tableConnection =
        new IoTDBConnection() {
          @Override
          public String getSqlDialect() {
            return Constant.TABLE_DIALECT;
          }
        };
    openConnection(tableConnection);

    assertThrows(SQLException.class, () -> tableConnection.setCatalog(null));
    assertThrows(SQLException.class, () -> tableConnection.setSchema(null));
  }

  @Test
  public void testTableCatalogAndSchemaCloseUseStatements() throws Exception {
    IoTDBConnection tableConnection =
        new IoTDBConnection() {
          @Override
          public String getSqlDialect() {
            return Constant.TABLE_DIALECT;
          }
        };
    openConnection(tableConnection);
    tableConnection.setClient(client);
    TSExecuteStatementResp resp = mock(TSExecuteStatementResp.class);
    when(client.requestStatementId(anyLong())).thenReturn(1L, 2L);
    when(client.executeStatementV2(any(TSExecuteStatementReq.class))).thenReturn(resp);
    when(resp.getStatus()).thenReturn(successStatus);
    when(client.closeOperation(any())).thenReturn(successStatus);

    tableConnection.setSchema("root");
    tableConnection.setCatalog("root2");

    verify(client, times(2)).closeOperation(any());
  }

  @Test
  public void testPrepareStatementRejectsNullSqlBeforeRequestingStatementId() throws Exception {
    openConnection(connection);
    connection.setClient(client);

    assertThrows(SQLException.class, () -> connection.prepareStatement(null));

    verify(client, never()).requestStatementId(anyLong());
    verify(client, never()).closeOperation(any());
  }

  @Test
  public void testTablePrepareStatementRejectsNullSqlBeforeRequestingStatementId()
      throws Exception {
    IoTDBConnection tableConnection =
        new IoTDBConnection() {
          @Override
          public String getSqlDialect() {
            return Constant.TABLE_DIALECT;
          }
        };
    openConnection(tableConnection);
    tableConnection.setClient(client);

    assertThrows(SQLException.class, () -> tableConnection.prepareStatement(null));

    verify(client, never()).requestStatementId(anyLong());
    verify(client, never()).prepareStatement(any(TSPrepareReq.class));
    verify(client, never()).closeOperation(any());
  }

  @Test
  public void testGetServerProperties() throws TException {
    openConnection(connection);
    final String version = "v0.1";
    @SuppressWarnings("serial")
    final List<String> supportedAggregationTime =
        new ArrayList<String>() {
          {
            add("max_time");
            add("min_time");
          }
        };
    final String timestampPrecision = "ms";
    new ServerProperties();
    when(client.getProperties())
        .thenReturn(new ServerProperties(version, supportedAggregationTime, timestampPrecision, 1));
    connection.setClient(client);
    assertEquals(connection.getServerProperties().getVersion(), version);
    for (int i = 0; i < supportedAggregationTime.size(); i++) {
      assertEquals(
          connection.getServerProperties().getSupportedTimeAggregationOperations().get(i),
          supportedAggregationTime.get(i));
    }
    assertEquals(connection.getServerProperties().getTimestampPrecision(), timestampPrecision);
  }

  @Test
  public void setTimeoutTest() throws SQLException {
    openConnection(connection);
    connection.setQueryTimeout(60);
    Assert.assertEquals(60, connection.getQueryTimeout());
  }

  @Test
  public void testStandardConnectionStateMethods() throws Exception {
    openConnection(connection);

    assertFalse(connection.getAutoCommit());
    connection.setAutoCommit(true);
    assertTrue(connection.getAutoCommit());
    assertEquals("Apache IoTDB", connection.getCatalog());
    assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, connection.getHoldability());
    assertEquals(Connection.TRANSACTION_NONE, connection.getTransactionIsolation());
    assertFalse(connection.isReadOnly());
    connection.setReadOnly(false);
    assertTrue(connection.isValid(0));
  }

  @Test(expected = SQLException.class)
  public void testIsValidRejectsNegativeTimeout() throws SQLException {
    connection.isValid(-1);
  }

  @Test
  public void testWrapperMethods() throws Exception {
    openConnection(connection);

    assertTrue(connection.isWrapperFor(IoTDBConnection.class));
    assertTrue(connection.isWrapperFor(Connection.class));
    assertFalse(connection.isWrapperFor(String.class));
    assertFalse(connection.isWrapperFor(null));
    assertSame(connection, connection.unwrap(IoTDBConnection.class));
    assertSame(connection, connection.unwrap(Connection.class));
  }

  @Test(expected = SQLException.class)
  public void testUnwrapRejectsUnsupportedClass() throws Exception {
    openConnection(connection);

    connection.unwrap(String.class);
  }

  @Test
  public void testClosedConnectionRejectsOperations() throws SQLException {
    assertTrue(connection.isClosed());
    assertFalse(connection.isValid(0));

    assertThrows(SQLException.class, () -> connection.isWrapperFor(Connection.class));
    assertThrows(SQLException.class, () -> connection.unwrap(Connection.class));
    assertThrows(SQLException.class, () -> connection.abort(null));
    assertThrows(SQLException.class, () -> connection.clearWarnings());
    assertThrows(SQLException.class, () -> connection.commit());
    assertThrows(SQLException.class, () -> connection.createArrayOf("TEXT", new Object[0]));
    assertThrows(SQLException.class, () -> connection.createBlob());
    assertThrows(SQLException.class, () -> connection.createClob());
    assertThrows(SQLException.class, () -> connection.createNClob());
    assertThrows(SQLException.class, () -> connection.createSQLXML());
    assertThrows(SQLException.class, () -> connection.createStatement());
    assertThrows(
        SQLException.class,
        () -> connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));
    assertThrows(
        SQLException.class,
        () ->
            connection.createStatement(
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY,
                ResultSet.HOLD_CURSORS_OVER_COMMIT));
    assertThrows(SQLException.class, () -> connection.createStruct("TEXT", new Object[0]));
    assertThrows(SQLException.class, () -> connection.prepareStatement("SELECT ?"));
    assertThrows(SQLException.class, () -> connection.prepareStatement("SELECT ?", 0));
    assertThrows(SQLException.class, () -> connection.prepareStatement("SELECT ?", new int[0]));
    assertThrows(SQLException.class, () -> connection.prepareStatement("SELECT ?", new String[0]));
    assertThrows(
        SQLException.class,
        () ->
            connection.prepareStatement(
                "SELECT ?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));
    assertThrows(
        SQLException.class,
        () ->
            connection.prepareStatement(
                "SELECT ?",
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY,
                ResultSet.HOLD_CURSORS_OVER_COMMIT));
    assertThrows(SQLException.class, () -> connection.prepareCall("CALL x"));
    assertThrows(
        SQLException.class,
        () ->
            connection.prepareCall(
                "CALL x", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));
    assertThrows(
        SQLException.class,
        () ->
            connection.prepareCall(
                "CALL x",
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY,
                ResultSet.HOLD_CURSORS_OVER_COMMIT));
    assertThrows(SQLException.class, () -> connection.getAutoCommit());
    assertThrows(SQLException.class, () -> connection.setAutoCommit(true));
    assertThrows(SQLException.class, () -> connection.getCatalog());
    assertThrows(SQLException.class, () -> connection.setCatalog("root"));
    assertThrows(SQLException.class, () -> connection.getClientInfo());
    assertThrows(SQLException.class, () -> connection.getClientInfo("time_zone"));
    SQLClientInfoException clientInfoException =
        assertThrows(
            SQLClientInfoException.class, () -> connection.setClientInfo("time_zone", "+07:00"));
    assertTrue(clientInfoException.getMessage().contains("connection has been closed"));
    clientInfoException =
        assertThrows(
            SQLClientInfoException.class, () -> connection.setClientInfo(new Properties()));
    assertTrue(clientInfoException.getMessage().contains("connection has been closed"));
    assertThrows(SQLException.class, () -> connection.getHoldability());
    assertThrows(
        SQLException.class, () -> connection.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT));
    assertThrows(SQLException.class, () -> connection.getMetaData());
    assertThrows(SQLException.class, () -> connection.getNetworkTimeout());
    assertThrows(SQLException.class, () -> connection.getSchema());
    assertThrows(SQLException.class, () -> connection.setSchema("root"));
    assertThrows(SQLException.class, () -> connection.getTransactionIsolation());
    assertThrows(
        SQLException.class, () -> connection.setTransactionIsolation(Connection.TRANSACTION_NONE));
    assertThrows(SQLException.class, () -> connection.getTypeMap());
    assertThrows(SQLException.class, () -> connection.setTypeMap(null));
    assertThrows(SQLException.class, () -> connection.getWarnings());
    assertThrows(SQLException.class, () -> connection.isReadOnly());
    assertThrows(SQLException.class, () -> connection.setReadOnly(false));
    assertThrows(SQLException.class, () -> connection.nativeSQL("SELECT 1"));
    assertThrows(SQLException.class, () -> connection.releaseSavepoint(null));
    assertThrows(SQLException.class, () -> connection.rollback());
    assertThrows(SQLException.class, () -> connection.rollback((Savepoint) null));
    assertThrows(SQLException.class, () -> connection.setNetworkTimeout(null, 0));
    assertThrows(SQLException.class, () -> connection.getQueryTimeout());
    assertThrows(SQLException.class, () -> connection.setQueryTimeout(60));
    assertThrows(SQLException.class, () -> connection.setSavepoint());
    assertThrows(SQLException.class, () -> connection.setSavepoint("s"));
    assertThrows(IoTDBSQLException.class, () -> connection.setTimeZone("+07:00"));
    assertThrows(TException.class, () -> connection.getServerProperties());
  }

  @Test
  public void testClosedConnectionDoesNotReconnect() {
    TTransport transport = org.mockito.Mockito.mock(TTransport.class);
    setTransport(connection, transport);

    assertFalse(connection.reconnect());
    verify(transport, never()).close();
  }

  @Test
  public void testCloseClosesCreatedStatements() throws Exception {
    openConnection(connection);
    connection.setClient(client);
    when(client.requestStatementId(anyLong())).thenReturn(1L, 2L);
    when(client.closeOperation(any())).thenReturn(successStatus);
    when(client.closeSession(any())).thenReturn(successStatus);

    Statement statement = connection.createStatement();
    PreparedStatement preparedStatement = connection.prepareStatement("SELECT ?");

    connection.close();

    assertTrue(connection.isClosed());
    assertTrue(statement.isClosed());
    assertTrue(preparedStatement.isClosed());
    verify(client, times(2)).closeOperation(any());
    verify(client).closeSession(any());
  }

  @Test
  public void testCloseClosesCurrentResultSetFromCreatedStatement() throws Exception {
    openConnection(connection);
    connection.setClient(client);
    when(client.requestStatementId(anyLong())).thenReturn(1L);
    when(client.closeOperation(any())).thenReturn(successStatus);
    when(client.closeSession(any())).thenReturn(successStatus);

    IoTDBStatement statement = (IoTDBStatement) connection.createStatement();
    ResultSet resultSet = mock(ResultSet.class);
    statement.resultSet = resultSet;

    connection.close();

    assertTrue(statement.isClosed());
    verify(resultSet).close();
    verify(client).closeOperation(any());
    verify(client).closeSession(any());
  }

  @Test
  public void testClosedStatementIsUnregisteredFromConnection() throws Exception {
    openConnection(connection);
    connection.setClient(client);
    when(client.requestStatementId(anyLong())).thenReturn(1L);
    when(client.closeOperation(any())).thenReturn(successStatus);
    when(client.closeSession(any())).thenReturn(successStatus);

    Statement statement = connection.createStatement();
    statement.close();

    connection.close();

    assertTrue(statement.isClosed());
    verify(client, times(1)).closeOperation(any());
    verify(client).closeSession(any());
  }

  private void openConnection(IoTDBConnection target) {
    try {
      Field isClosedField = IoTDBConnection.class.getDeclaredField("isClosed");
      isClosedField.setAccessible(true);
      isClosedField.setBoolean(target, false);
    } catch (ReflectiveOperationException e) {
      throw new AssertionError(e);
    }
  }

  private void setTransport(IoTDBConnection target, TTransport transport) {
    try {
      Field transportField = IoTDBConnection.class.getDeclaredField("transport");
      transportField.setAccessible(true);
      transportField.set(target, transport);
    } catch (ReflectiveOperationException e) {
      throw new AssertionError(e);
    }
  }
}
