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
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.rpc.stmt.PreparedParameterSerde;
import org.apache.iotdb.service.rpc.thrift.IClientRPCService.Iface;
import org.apache.iotdb.service.rpc.thrift.TSCancelOperationReq;
import org.apache.iotdb.service.rpc.thrift.TSExecutePreparedReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;
import org.apache.iotdb.service.rpc.thrift.TSPrepareReq;
import org.apache.iotdb.service.rpc.thrift.TSPrepareResp;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class IoTDBTablePreparedStatementTest {

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
    when(connection.getSqlDialect()).thenReturn("table");
    when(execStatementResp.getStatus()).thenReturn(Status_SUCCESS);
    when(execStatementResp.getQueryId()).thenReturn(queryId);

    // Mock for prepareStatement - dynamically calculate parameter count from SQL
    when(client.prepareStatement(any(TSPrepareReq.class)))
        .thenAnswer(
            new Answer<TSPrepareResp>() {
              @Override
              public TSPrepareResp answer(InvocationOnMock invocation) throws Throwable {
                TSPrepareReq req = invocation.getArgument(0);
                String sql = req.getSql();
                int paramCount = countQuestionMarks(sql);

                TSPrepareResp resp = new TSPrepareResp();
                resp.setStatus(Status_SUCCESS);
                resp.setParameterCount(paramCount);
                return resp;
              }
            });

    // Mock for executePreparedStatement
    when(client.executePreparedStatement(any(TSExecutePreparedReq.class)))
        .thenReturn(execStatementResp);
  }

  /** Count the number of '?' placeholders in a SQL string, ignoring those inside quotes */
  private int countQuestionMarks(String sql) {
    int count = 0;
    boolean inSingleQuote = false;
    boolean inDoubleQuote = false;

    for (int i = 0; i < sql.length(); i++) {
      char c = sql.charAt(i);

      if (c == '\'' && !inDoubleQuote) {
        // Check for escaped quote
        if (i + 1 < sql.length() && sql.charAt(i + 1) == '\'') {
          i++; // Skip escaped quote
        } else {
          inSingleQuote = !inSingleQuote;
        }
      } else if (c == '"' && !inSingleQuote) {
        inDoubleQuote = !inDoubleQuote;
      } else if (c == '?' && !inSingleQuote && !inDoubleQuote) {
        count++;
      }
    }

    return count;
  }

  // ========== Table Model SQL Injection Prevention Tests ==========

  @SuppressWarnings("resource")
  @Test
  public void testTableModelLoginInjectionWithComment() throws Exception {
    // Login interface SQL injection attack 1: Using -- comments to bypass password checks
    String sql = "SELECT * FROM users WHERE username = ? AND password = ?";
    IoTDBTablePreparedStatement ps =
        new IoTDBTablePreparedStatement(connection, client, sessionId, sql, zoneId);
    ps.setString(1, "admin' --");
    ps.setString(2, "password");
    ps.execute();

    ArgumentCaptor<TSExecutePreparedReq> argument =
        ArgumentCaptor.forClass(TSExecutePreparedReq.class);
    verify(client).executePreparedStatement(argument.capture());
    // SQL injection is prevented by using prepared statements with parameterized queries
    assertTrue(argument.getValue().getParameters() != null);
  }

  @SuppressWarnings("resource")
  @Test
  public void testTableModelLoginInjectionWithORCondition() throws Exception {
    // Login interface SQL injection attack 2: Bypassing authentication by using 'OR '1'='1
    String sql = "SELECT * FROM users WHERE username = ? AND password = ?";
    IoTDBTablePreparedStatement ps =
        new IoTDBTablePreparedStatement(connection, client, sessionId, sql, zoneId);
    ps.setString(1, "admin");
    ps.setString(2, "' OR '1'='1");
    ps.execute();

    ArgumentCaptor<TSExecutePreparedReq> argument =
        ArgumentCaptor.forClass(TSExecutePreparedReq.class);
    verify(client).executePreparedStatement(argument.capture());
    // SQL injection is prevented by using prepared statements with parameterized queries
    assertTrue(argument.getValue().getParameters() != null);
  }

  @SuppressWarnings("resource")
  @Test
  public void testTableModelQueryWithMultipleInjectionVectors() throws Exception {
    String sql = "SELECT * FROM users WHERE email = ?";
    IoTDBTablePreparedStatement ps =
        new IoTDBTablePreparedStatement(connection, client, sessionId, sql, zoneId);
    ps.setString(1, "'; DROP TABLE users;");
    ps.execute();

    ArgumentCaptor<TSExecutePreparedReq> argument =
        ArgumentCaptor.forClass(TSExecutePreparedReq.class);
    verify(client).executePreparedStatement(argument.capture());
    // SQL injection is prevented by using prepared statements with parameterized queries
    assertTrue(argument.getValue().getParameters() != null);
  }

  @SuppressWarnings("resource")
  @Test
  public void testTableModelString1() throws Exception {
    String sql = "SELECT * FROM users WHERE password = ?";
    IoTDBTablePreparedStatement ps =
        new IoTDBTablePreparedStatement(connection, client, sessionId, sql, zoneId);
    ps.setString(1, "a'b");
    ps.execute();

    ArgumentCaptor<TSExecutePreparedReq> argument =
        ArgumentCaptor.forClass(TSExecutePreparedReq.class);
    verify(client).executePreparedStatement(argument.capture());
    assertTrue(argument.getValue().getParameters() != null);
  }

  @SuppressWarnings("resource")
  @Test
  public void testTableModelString2() throws Exception {
    String sql = "SELECT * FROM users WHERE password = ?";
    IoTDBTablePreparedStatement ps =
        new IoTDBTablePreparedStatement(connection, client, sessionId, sql, zoneId);
    ps.setString(1, "a\'b");
    ps.execute();

    ArgumentCaptor<TSExecutePreparedReq> argument =
        ArgumentCaptor.forClass(TSExecutePreparedReq.class);
    verify(client).executePreparedStatement(argument.capture());
    assertTrue(argument.getValue().getParameters() != null);
  }

  @SuppressWarnings("resource")
  @Test
  public void testTableModelString3() throws Exception {
    String sql = "SELECT * FROM users WHERE password = ?";
    IoTDBTablePreparedStatement ps =
        new IoTDBTablePreparedStatement(connection, client, sessionId, sql, zoneId);
    ps.setString(1, "a\\'b");
    ps.execute();

    ArgumentCaptor<TSExecutePreparedReq> argument =
        ArgumentCaptor.forClass(TSExecutePreparedReq.class);
    verify(client).executePreparedStatement(argument.capture());
    assertTrue(argument.getValue().getParameters() != null);
  }

  @SuppressWarnings("resource")
  @Test
  public void testTableModelString4() throws Exception {
    String sql = "SELECT * FROM users WHERE password = ?";
    IoTDBTablePreparedStatement ps =
        new IoTDBTablePreparedStatement(connection, client, sessionId, sql, zoneId);
    ps.setString(1, "a\\\'b");
    ps.execute();

    ArgumentCaptor<TSExecutePreparedReq> argument =
        ArgumentCaptor.forClass(TSExecutePreparedReq.class);
    verify(client).executePreparedStatement(argument.capture());
    assertTrue(argument.getValue().getParameters() != null);
  }

  @SuppressWarnings("resource")
  @Test
  public void testTableModelStringWithNull() throws Exception {
    String sql = "SELECT * FROM users WHERE email = ?";
    IoTDBTablePreparedStatement ps =
        new IoTDBTablePreparedStatement(connection, client, sessionId, sql, zoneId);
    ps.setString(1, null);
    ps.execute();

    ArgumentCaptor<TSExecutePreparedReq> argument =
        ArgumentCaptor.forClass(TSExecutePreparedReq.class);
    verify(client).executePreparedStatement(argument.capture());
    assertTrue(argument.getValue().getParameters() != null);
  }

  // ========== Cancel, fetch size and timestamp precision ==========

  @SuppressWarnings("resource")
  @Test
  public void testCancelTargetsQueryOfLatestPreparedExecution() throws Exception {
    when(client.executePreparedStatement(any(TSExecutePreparedReq.class)))
        .thenReturn(preparedQueryResp(42L), preparedQueryResp(43L));
    when(client.cancelOperation(any(TSCancelOperationReq.class)))
        .thenReturn(RpcUtils.SUCCESS_STATUS);
    IoTDBTablePreparedStatement ps =
        new IoTDBTablePreparedStatement(
            connection, client, sessionId, "SELECT * FROM t WHERE s1 = ?", zoneId);
    ps.setInt(1, 1);

    ps.execute();
    ps.cancel();
    ps.execute();
    ps.cancel();

    ArgumentCaptor<TSCancelOperationReq> argument =
        ArgumentCaptor.forClass(TSCancelOperationReq.class);
    verify(client, times(2)).cancelOperation(argument.capture());
    assertEquals(
        Arrays.asList(42L, 43L),
        argument.getAllValues().stream()
            .map(TSCancelOperationReq::getQueryId)
            .collect(Collectors.toList()));
    assertEquals(sessionId, argument.getValue().getSessionId());
  }

  @SuppressWarnings("resource")
  @Test
  public void testPreparedExecutionSendsFetchSize() throws Exception {
    IoTDBTablePreparedStatement ps =
        new IoTDBTablePreparedStatement(
            connection, client, sessionId, "SELECT * FROM t WHERE s1 = ?", zoneId);
    ps.setInt(1, 1);
    ps.setFetchSize(123);

    ps.execute();

    TSExecutePreparedReq req = lastPreparedExecution();
    assertTrue(req.isSetFetchSize());
    assertEquals(123, req.getFetchSize());
  }

  @SuppressWarnings("resource")
  @Test
  public void testPreparedExecutionFetchSizeIsCappedByMaxRows() throws Exception {
    IoTDBTablePreparedStatement ps =
        new IoTDBTablePreparedStatement(
            connection, client, sessionId, "SELECT * FROM t WHERE s1 = ?", zoneId);
    ps.setInt(1, 1);
    ps.setFetchSize(1000);
    ps.setMaxRows(10);

    ps.execute();

    assertEquals(10, lastPreparedExecution().getFetchSize());
  }

  @Test
  public void testSetTimestampUsesServerTimePrecision() throws Exception {
    Timestamp timestamp = new Timestamp(1_700_000_000_123L);
    timestamp.setNanos(123_456_789);
    assertEquals(1_700_000_000_123L, preparedTimestampValue(1_000, timestamp));
    assertEquals(1_700_000_000_123_456L, preparedTimestampValue(1_000_000, timestamp));
    assertEquals(1_700_000_000_123_456_789L, preparedTimestampValue(1_000_000_000, timestamp));

    // 1.5 seconds before the epoch: the whole-second part must be floored, not truncated.
    Timestamp beforeEpoch = new Timestamp(-1_500L);
    assertEquals(-1_500L, preparedTimestampValue(1_000, beforeEpoch));
    assertEquals(-1_500_000L, preparedTimestampValue(1_000_000, beforeEpoch));
    assertEquals(-1_500_000_000L, preparedTimestampValue(1_000_000_000, beforeEpoch));
  }

  @SuppressWarnings("resource")
  @Test
  public void testSetTimestampRejectsValueBeyondServerPrecisionRange() throws Exception {
    when(connection.getTimeFactor()).thenReturn(1_000_000_000);
    IoTDBTablePreparedStatement ps =
        new IoTDBTablePreparedStatement(
            connection, client, sessionId, "SELECT * FROM t WHERE time = ?", zoneId);

    // Nanosecond precision can only represent instants up to the year 2262.
    Timestamp farFuture = Timestamp.valueOf("2300-01-01 00:00:00");
    assertThrows(SQLException.class, () -> ps.setTimestamp(1, farFuture));

    // ...and down to 1677-09-21T00:12:43.145224192Z, which is Long.MIN_VALUE nanoseconds.
    Timestamp beforeLowerBound = Timestamp.from(Instant.parse("1677-09-21T00:12:43.145224191Z"));
    assertThrows(SQLException.class, () -> ps.setTimestamp(1, beforeLowerBound));
  }

  @Test
  public void testSetTimestampAcceptsNanosecondLowerBound() throws Exception {
    assertEquals(
        Long.MIN_VALUE + 1,
        preparedTimestampValue(
            1_000_000_000, Timestamp.from(Instant.parse("1677-09-21T00:12:43.145224193Z"))));
    assertEquals(
        Long.MIN_VALUE,
        preparedTimestampValue(
            1_000_000_000, Timestamp.from(Instant.parse("1677-09-21T00:12:43.145224192Z"))));
  }

  @SuppressWarnings("resource")
  @Test
  public void testSetTimestampInNonQueryStatementUsesServerTimePrecision() throws Exception {
    when(connection.getTimeFactor()).thenReturn(1_000_000);
    when(client.executeUpdateStatement(any(TSExecuteStatementReq.class)))
        .thenReturn(new TSExecuteStatementResp(RpcUtils.SUCCESS_STATUS));
    IoTDBTablePreparedStatement ps =
        new IoTDBTablePreparedStatement(
            connection, client, sessionId, "INSERT INTO t(time, s1) VALUES(?, 1)", zoneId);
    Timestamp timestamp = new Timestamp(1_700_000_000_123L);
    timestamp.setNanos(123_456_789);
    ps.setTimestamp(1, timestamp);

    ps.executeUpdate();

    ArgumentCaptor<TSExecuteStatementReq> argument =
        ArgumentCaptor.forClass(TSExecuteStatementReq.class);
    verify(client).executeUpdateStatement(argument.capture());
    assertEquals(
        "INSERT INTO t(time, s1) VALUES(1700000000123456, 1)", argument.getValue().getStatement());
  }

  // ========== LocalDate, client-side SQL completion and NaN/Infinity ==========

  @Test
  public void testSetObjectAcceptsLocalDateInClientCompletedSql() throws Exception {
    LocalDate date = LocalDate.of(2024, 1, 15);
    String expected = "INSERT INTO t(time, d) VALUES(1, '2024-01-15')";
    String sql = "INSERT INTO t(time, d) VALUES(1, ?)";
    assertEquals(expected, clientCompletedSql(sql, ps -> ps.setObject(1, date)));
    assertEquals(expected, clientCompletedSql(sql, ps -> ps.setObject(1, date, Types.DATE)));
    assertEquals(expected, clientCompletedSql(sql, ps -> ps.setObject(1, date, Types.DATE, 0)));
  }

  @SuppressWarnings("resource")
  @Test
  public void testSetObjectBindsLocalDateLikeSetDate() throws Exception {
    IoTDBTablePreparedStatement ps =
        new IoTDBTablePreparedStatement(
            connection, client, sessionId, "SELECT * FROM t WHERE d = ? AND e = ?", zoneId);
    ps.setObject(1, LocalDate.of(2024, 1, 15));
    ps.setDate(2, java.sql.Date.valueOf("2024-01-15"));
    ps.execute();

    List<PreparedParameterSerde.DeserializedParam> parameters = lastPreparedParameters();
    assertEquals("2024-01-15", parameters.get(0).value);
    assertEquals(parameters.get(1).type, parameters.get(0).type);
    assertEquals(parameters.get(1).value, parameters.get(0).value);
  }

  @Test
  public void testQuotedIdentifiersHideQuestionMarksAndQuotes() throws Exception {
    assertEquals(
        "INSERT INTO \"my?table\"(time, s1) VALUES(1, 'a')",
        clientCompletedSql("INSERT INTO \"my?table\"(time, s1) VALUES(?, ?)", this::bindRow));
    assertEquals(
        "INSERT INTO \"it's\"(time, s1) VALUES(1, 'a')",
        clientCompletedSql("INSERT INTO \"it's\"(time, s1) VALUES(?, ?)", this::bindRow));
    assertEquals(
        "INSERT INTO `a?b`(time, s1) VALUES(1, 'a')",
        clientCompletedSql("INSERT INTO `a?b`(time, s1) VALUES(?, ?)", this::bindRow));
  }

  @Test
  public void testStringLiteralsEscapeQuotesOnlyByDoublingThem() throws Exception {
    // A backslash is an ordinary character, so 'C:\' ends at its second quote
    assertEquals(
        "INSERT INTO t(time, s1, s2) VALUES(1, 'C:\\', 'a')",
        clientCompletedSql("INSERT INTO t(time, s1, s2) VALUES(?, 'C:\\', ?)", this::bindRow));
    assertEquals(
        "INSERT INTO t(time, s1, s2) VALUES(1, 'it''s ?', 'a')",
        clientCompletedSql("INSERT INTO t(time, s1, s2) VALUES(?, 'it''s ?', ?)", this::bindRow));
  }

  @Test
  public void testCommentsHideQuestionMarks() throws Exception {
    assertEquals(
        "INSERT INTO t(time, s1) VALUES(1, 'a') -- why?\n",
        clientCompletedSql("INSERT INTO t(time, s1) VALUES(?, ?) -- why?\n", this::bindRow));
    assertEquals(
        "INSERT /* ? */ INTO t(time, s1) VALUES(1, 'a')",
        clientCompletedSql("INSERT /* ? */ INTO t(time, s1) VALUES(?, ?)", this::bindRow));
  }

  @Test
  public void testLineCommentEndsAtCarriageReturnOrLineFeed() throws Exception {
    // As in the lexer's SIMPLE_COMMENT rule, a lone CR ends the comment just like LF or CRLF
    assertEquals(
        "INSERT -- comment\rINTO t(time, s1) VALUES(1, 'a')\n",
        clientCompletedSql("INSERT -- comment\rINTO t(time, s1) VALUES(?, ?)\n", this::bindRow));
    assertEquals(
        "INSERT -- comment\r\nINTO t(time, s1) VALUES(1, 'a')",
        clientCompletedSql("INSERT -- comment\r\nINTO t(time, s1) VALUES(?, ?)", this::bindRow));
  }

  @Test
  public void testNaNIsSentAsStringInClientCompletedSql() throws Exception {
    // The server has no NaN literal but stores the string 'NaN' as NaN in FLOAT/DOUBLE columns
    assertEquals(
        "INSERT INTO t(time, d, f) VALUES(1, 'NaN', 'NaN')",
        clientCompletedSql(
            "INSERT INTO t(time, d, f) VALUES(1, ?, ?)",
            ps -> {
              ps.setDouble(1, Double.NaN);
              ps.setFloat(2, Float.NaN);
            }));
  }

  @SuppressWarnings("resource")
  @Test
  public void testInfinityIsRejectedInClientCompletedSql() throws Exception {
    IoTDBTablePreparedStatement ps =
        new IoTDBTablePreparedStatement(
            connection, client, sessionId, "INSERT INTO t(time, d, f) VALUES(1, ?, ?)", zoneId);
    assertThrows(SQLException.class, () -> ps.setDouble(1, Double.POSITIVE_INFINITY));
    assertThrows(SQLException.class, () -> ps.setFloat(2, Float.NEGATIVE_INFINITY));
  }

  @SuppressWarnings("resource")
  @Test
  public void testInfinityIsBoundAsBinaryInServerPreparedQuery() throws Exception {
    IoTDBTablePreparedStatement ps =
        new IoTDBTablePreparedStatement(
            connection, client, sessionId, "SELECT * FROM t WHERE d < ?", zoneId);
    ps.setDouble(1, Double.POSITIVE_INFINITY);
    ps.execute();

    assertEquals(Double.POSITIVE_INFINITY, lastPreparedParameters().get(0).value);
  }

  @FunctionalInterface
  private interface ParameterBinder {
    void bind(IoTDBTablePreparedStatement ps) throws SQLException;
  }

  private void bindRow(IoTDBTablePreparedStatement ps) throws SQLException {
    ps.setLong(1, 1);
    ps.setString(2, "a");
  }

  /** Runs a statement that the driver completes on the client and returns the SQL it sends. */
  @SuppressWarnings("resource")
  private String clientCompletedSql(String sql, ParameterBinder binder) throws Exception {
    when(client.executeUpdateStatement(any(TSExecuteStatementReq.class)))
        .thenReturn(new TSExecuteStatementResp(RpcUtils.SUCCESS_STATUS));
    IoTDBTablePreparedStatement ps =
        new IoTDBTablePreparedStatement(connection, client, sessionId, sql, zoneId);
    binder.bind(ps);
    ps.executeUpdate();

    ArgumentCaptor<TSExecuteStatementReq> argument =
        ArgumentCaptor.forClass(TSExecuteStatementReq.class);
    verify(client, atLeastOnce()).executeUpdateStatement(argument.capture());
    return argument.getValue().getStatement();
  }

  private List<PreparedParameterSerde.DeserializedParam> lastPreparedParameters() throws Exception {
    return PreparedParameterSerde.deserialize(
        ByteBuffer.wrap(lastPreparedExecution().getParameters()));
  }

  private static TSExecuteStatementResp preparedQueryResp(long queryId) {
    TSExecuteStatementResp resp = new TSExecuteStatementResp(RpcUtils.SUCCESS_STATUS);
    resp.setQueryId(queryId);
    return resp;
  }

  private TSExecutePreparedReq lastPreparedExecution() throws Exception {
    ArgumentCaptor<TSExecutePreparedReq> argument =
        ArgumentCaptor.forClass(TSExecutePreparedReq.class);
    verify(client, atLeastOnce()).executePreparedStatement(argument.capture());
    return argument.getValue();
  }

  /** Binds {@code timestamp} under the given server time factor and returns the value sent. */
  @SuppressWarnings("resource")
  private long preparedTimestampValue(int timeFactor, Timestamp timestamp) throws Exception {
    when(connection.getTimeFactor()).thenReturn(timeFactor);
    IoTDBTablePreparedStatement ps =
        new IoTDBTablePreparedStatement(
            connection, client, sessionId, "SELECT * FROM t WHERE time = ?", zoneId);
    ps.setTimestamp(1, timestamp);
    ps.execute();

    List<PreparedParameterSerde.DeserializedParam> parameters =
        PreparedParameterSerde.deserialize(
            ByteBuffer.wrap(lastPreparedExecution().getParameters()));
    assertEquals(1, parameters.size());
    return (Long) parameters.get(0).value;
  }
}
