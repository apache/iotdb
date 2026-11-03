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
import org.apache.iotdb.jdbc.i18n.JdbcMessages;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.service.rpc.thrift.IClientRPCService;
import org.apache.iotdb.service.rpc.thrift.ServerProperties;
import org.apache.iotdb.service.rpc.thrift.TSGetTimeZoneResp;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionResp;
import org.apache.iotdb.service.rpc.thrift.TSProtocolVersion;
import org.apache.iotdb.service.rpc.thrift.TSSetTimeZoneReq;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class IoTDBConnectionTest {

  @Mock private IClientRPCService.Iface client;
  @Mock private TTransport transport;

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
    String timeZone = "+07:00";
    assertNotEquals(connection.getTimeZone(), timeZone);
    when(client.setTimeZone(any(TSSetTimeZoneReq.class))).thenReturn(new TSStatus(successStatus));
    connection.setClient(client);
    connection.setClientInfo("time_zone", timeZone);
    assertEquals(connection.getTimeZone(), timeZone);
  }

  @Test
  public void testGetServerProperties() throws TException {
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
    connection.setQueryTimeout(60);
    Assert.assertEquals(60, connection.getQueryTimeout());
  }

  @Test
  public void testReadOnlyIsAcceptedAsHint() throws SQLException {
    Assert.assertFalse(connection.isReadOnly());
    connection.setReadOnly(true);
    Assert.assertTrue(connection.isReadOnly());
    connection.setReadOnly(false);
    Assert.assertFalse(connection.isReadOnly());
  }

  @Test
  public void testNewConnectionIsInAutoCommitMode() {
    Assert.assertTrue(new IoTDBConnection().getAutoCommit());
  }

  @Test
  public void testCatalogIsNullInTableDialect() throws SQLException {
    IoTDBConnection tableConnection = spy(new IoTDBConnection());
    doReturn(Constant.TABLE_DIALECT).when(tableConnection).getSqlDialect();
    Assert.assertNull(tableConnection.getCatalog());

    // The tree model keeps its catalog, which its DatabaseMetaData compares against
    Assert.assertEquals("Apache IoTDB", connection.getCatalog());
  }

  @Test
  public void testSetCatalogToNullIsIgnoredInTableDialect() throws SQLException {
    IoTDBConnection tableConnection = spy(new IoTDBConnection());
    doReturn(Constant.TABLE_DIALECT).when(tableConnection).getSqlDialect();

    tableConnection.setCatalog(null);

    verify(tableConnection, never()).prepareStatement(anyString());
  }

  @Test
  public void testOpenSessionRecognizesLegacyProtocolDiagnostic() throws Exception {
    TException cause =
        new TException(
            "Required field 'client_protocol' was not present! Struct: TSOpenSessionReq(...)");

    SQLException exception = failOpenSession(cause);

    assertEquals(
        String.format(
            JdbcMessages
                .EXCEPTION_CAN_NOT_ESTABLISH_CONNECTION_ARG_YOU_MAY_TRY_CONNECT_OLD_8FC3703E,
            connection.getParams().getJdbcUriString(),
            cause.getMessage()),
        exception.getMessage());
  }

  @Test
  public void testOpenSessionPreservesOtherMissingFieldDiagnostic() throws Exception {
    TException cause =
        new TException("Required field 'username' was not present! Struct: TSOpenSessionReq(...)");

    SQLException exception = failOpenSession(cause);

    assertEquals(
        String.format(
            JdbcMessages.EXCEPTION_CAN_NOT_ESTABLISH_CONNECTION_ARG_ARG_D7246055,
            connection.getParams().getJdbcUriString(),
            cause.getMessage()),
        exception.getMessage());
  }

  @Test
  public void testOpenSessionPreservesMessageLessTransportException() throws Exception {
    TException cause = new TTransportException(TTransportException.END_OF_FILE);

    SQLException exception = failOpenSession(cause);

    assertEquals(
        String.format(
            JdbcMessages.EXCEPTION_CAN_NOT_ESTABLISH_CONNECTION_ARG_ARG_D7246055,
            connection.getParams().getJdbcUriString(),
            cause.getMessage()),
        exception.getMessage());
  }

  @Test
  public void testOpenSessionRecognizesEnglishPasswordExpiration() throws Exception {
    String expirationDate = passwordExpirationDate(1);
    assertOpenSessionWarning(
        "Login successfully. Your password will expire at " + expirationDate, expirationDate);
  }

  @Test
  public void testOpenSessionRecognizesChinesePasswordExpiration() throws Exception {
    String expirationDate = passwordExpirationDate(1);
    assertOpenSessionWarning("密码将于以下时间过期：" + expirationDate, expirationDate);
  }

  @Test
  public void testOpenSessionDoesNotWarnForDistantPasswordExpiration() throws Exception {
    assertOpenSessionWarning("Your password will expire at " + passwordExpirationDate(30), null);
  }

  @Test
  public void testOpenSessionWithoutPasswordExpiration() throws Exception {
    assertOpenSessionWarning("Login successfully", null);
  }

  @Test
  public void testOpenSessionWithoutStatusMessage() throws Exception {
    assertOpenSessionWarning(null, null);
  }

  private String passwordExpirationDate(int daysUntilExpiration) {
    return LocalDateTime.now()
        .plusDays(daysUntilExpiration)
        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
  }

  private void assertOpenSessionWarning(String statusMessage, String expirationDate)
      throws Exception {
    Method openSession = prepareOpenSession();
    TSOpenSessionResp response =
        new TSOpenSessionResp(
                new TSStatus(successStatus).setMessage(statusMessage),
                TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3)
            .setSessionId(1L);
    when(client.openSession(any(TSOpenSessionReq.class))).thenReturn(response);
    Logger logger = (Logger) LoggerFactory.getLogger(IoTDBConnection.class);
    Level originalLevel = logger.getLevel();
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.start();
    logger.addAppender(appender);
    logger.setLevel(Level.WARN);
    try {
      openSession.invoke(connection);

      Assert.assertFalse(connection.isClosed());
      assertEquals(1L, connection.getSessionId());
      verify(transport, never()).close();
      if (expirationDate == null) {
        assertTrue(appender.list.isEmpty());
      } else {
        assertEquals(1, appender.list.size());
        ILoggingEvent warning = appender.list.get(0);
        assertEquals(Level.WARN, warning.getLevel());
        assertEquals(
            JdbcMessages.LOG_ARG_ARG_PLEASE_CHANGE_IT_TIME_VIA_ALTER_USER_STATEMENT_6B67087C,
            warning.getMessage());
        Assert.assertArrayEquals(
            new Object[] {
              JdbcMessages.EXCEPTION_YOUR_PASSWORD_WILL_EXPIRE_AT_49098220, expirationDate
            },
            warning.getArgumentArray());
      }
    } finally {
      logger.detachAppender(appender);
      appender.stop();
      logger.setLevel(originalLevel);
    }
  }

  private Method prepareOpenSession() throws Exception {
    IoTDBConnectionParams params = Utils.parseUrl("jdbc:iotdb://127.0.0.1:6667/", new Properties());
    Field paramsField = IoTDBConnection.class.getDeclaredField("params");
    paramsField.setAccessible(true);
    paramsField.set(connection, params);
    Field transportField = IoTDBConnection.class.getDeclaredField("transport");
    transportField.setAccessible(true);
    transportField.set(connection, transport);
    connection.setClient(client);
    Method openSession = IoTDBConnection.class.getDeclaredMethod("openSession");
    openSession.setAccessible(true);
    return openSession;
  }

  private SQLException failOpenSession(TException cause) throws Exception {
    Method openSession = prepareOpenSession();
    when(client.openSession(any(TSOpenSessionReq.class))).thenThrow(cause);

    InvocationTargetException invocationException =
        assertThrows(InvocationTargetException.class, () -> openSession.invoke(connection));

    assertTrue(invocationException.getCause() instanceof SQLException);
    SQLException exception = (SQLException) invocationException.getCause();
    assertSame(cause, exception.getCause());
    verify(transport).close();
    return exception;
  }
}
