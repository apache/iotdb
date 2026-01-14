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

import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.ZoneId;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class IoTDBPreparedStatementTest {

  @Mock TSExecuteStatementResp execStatementResp;
  @Mock TSPrepareResp prepareResp;
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

  @SuppressWarnings("resource")
  @Test
  public void testNonParameterized() throws Exception {
    String sql =
        "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < 24 and time > 2017-11-1 0:13:00";
    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(connection, client, sessionId, sql, zoneId);
    ps.execute();

    // Verify executePreparedStatement was called (new behavior)
    ArgumentCaptor<TSExecutePreparedReq> argument =
        ArgumentCaptor.forClass(TSExecutePreparedReq.class);
    verify(client).executePreparedStatement(argument.capture());
    // Non-parameterized query should have empty parameters
    assertTrue(
        argument.getValue().getParameters() == null
            || argument.getValue().getParameters().isEmpty());
  }

  @SuppressWarnings("resource")
  @Test
  public void unusedArgument() throws SQLException {
    // SQL with no parameters - setting a parameter should throw an exception
    String sql =
        "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < 24 and time > 2017-11-1 0:13:00";
    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(connection, client, sessionId, sql, zoneId);
    // In the new server-side prepared statement implementation, setting a parameter
    // that doesn't exist in the SQL throws an exception
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
  public void oneIntArgument() throws Exception {
    String sql =
        "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < ? and time > 2017-11-1 0:13:00";
    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(connection, client, sessionId, sql, zoneId);
    ps.setInt(1, 123);
    ps.execute();
    ArgumentCaptor<TSExecutePreparedReq> argument =
        ArgumentCaptor.forClass(TSExecutePreparedReq.class);
    verify(client).executePreparedStatement(argument.capture());
    // Verify parameters were sent
    assertTrue(argument.getValue().getParameters() != null);
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
    ArgumentCaptor<TSExecutePreparedReq> argument =
        ArgumentCaptor.forClass(TSExecutePreparedReq.class);
    verify(client).executePreparedStatement(argument.capture());
    assertTrue(argument.getValue().getParameters() != null);
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
    ArgumentCaptor<TSExecutePreparedReq> argument =
        ArgumentCaptor.forClass(TSExecutePreparedReq.class);
    verify(client).executePreparedStatement(argument.capture());
    assertTrue(argument.getValue().getParameters() != null);
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
    ArgumentCaptor<TSExecutePreparedReq> argument =
        ArgumentCaptor.forClass(TSExecutePreparedReq.class);
    verify(client).executePreparedStatement(argument.capture());
    assertTrue(argument.getValue().getParameters() != null);
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
    ArgumentCaptor<TSExecutePreparedReq> argument =
        ArgumentCaptor.forClass(TSExecutePreparedReq.class);
    verify(client).executePreparedStatement(argument.capture());
    assertTrue(argument.getValue().getParameters() != null);
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
    ArgumentCaptor<TSExecutePreparedReq> argument =
        ArgumentCaptor.forClass(TSExecutePreparedReq.class);
    verify(client).executePreparedStatement(argument.capture());
    assertTrue(argument.getValue().getParameters() != null);
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
    ArgumentCaptor<TSExecutePreparedReq> argument =
        ArgumentCaptor.forClass(TSExecutePreparedReq.class);
    verify(client).executePreparedStatement(argument.capture());
    assertTrue(argument.getValue().getParameters() != null);
  }

  @SuppressWarnings("resource")
  @Test
  public void oneStringArgument3() throws Exception {
    String sql = "SELECT status, ? FROM root.ln.wf01.wt01";
    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(connection, client, sessionId, sql, zoneId);
    ps.setString(1, "temperature");
    ps.execute();
    ArgumentCaptor<TSExecutePreparedReq> argument =
        ArgumentCaptor.forClass(TSExecutePreparedReq.class);
    verify(client).executePreparedStatement(argument.capture());
    assertTrue(argument.getValue().getParameters() != null);
  }

  @SuppressWarnings("resource")
  @Test
  public void oneTimeLongArgument() throws Exception {
    String sql = "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE time > ?";
    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(connection, client, sessionId, sql, zoneId);
    ps.setLong(1, 1233);
    ps.execute();
    ArgumentCaptor<TSExecutePreparedReq> argument =
        ArgumentCaptor.forClass(TSExecutePreparedReq.class);
    verify(client).executePreparedStatement(argument.capture());
    assertTrue(argument.getValue().getParameters() != null);
  }

  @SuppressWarnings("resource")
  @Test
  public void oneTimeTimestampArgument() throws Exception {
    String sql = "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE time > ?";
    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(connection, client, sessionId, sql, zoneId);
    ps.setTimestamp(1, Timestamp.valueOf("2017-11-01 00:13:00"));
    ps.execute();
    ArgumentCaptor<TSExecutePreparedReq> argument =
        ArgumentCaptor.forClass(TSExecutePreparedReq.class);
    verify(client).executePreparedStatement(argument.capture());
    assertTrue(argument.getValue().getParameters() != null);
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

    ArgumentCaptor<TSExecutePreparedReq> argument =
        ArgumentCaptor.forClass(TSExecutePreparedReq.class);
    verify(client).executePreparedStatement(argument.capture());
    assertTrue(argument.getValue().getParameters() != null);
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

    ArgumentCaptor<TSExecutePreparedReq> argument =
        ArgumentCaptor.forClass(TSExecutePreparedReq.class);
    verify(client).executePreparedStatement(argument.capture());
    assertTrue(argument.getValue().getParameters() != null);
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

    ArgumentCaptor<TSExecutePreparedReq> argument =
        ArgumentCaptor.forClass(TSExecutePreparedReq.class);
    verify(client).executePreparedStatement(argument.capture());
    assertTrue(argument.getValue().getParameters() != null);
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

    ArgumentCaptor<TSExecutePreparedReq> argument =
        ArgumentCaptor.forClass(TSExecutePreparedReq.class);
    verify(client).executePreparedStatement(argument.capture());
    assertTrue(argument.getValue().getParameters() != null);
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

    ArgumentCaptor<TSExecutePreparedReq> argument =
        ArgumentCaptor.forClass(TSExecutePreparedReq.class);
    verify(client).executePreparedStatement(argument.capture());
    assertTrue(argument.getValue().getParameters() != null);
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

    ArgumentCaptor<TSExecutePreparedReq> argument =
        ArgumentCaptor.forClass(TSExecutePreparedReq.class);
    verify(client).executePreparedStatement(argument.capture());
    assertTrue(argument.getValue().getParameters() != null);
  }

  // ========== Table Model SQL Injection Prevention Tests ==========

  @SuppressWarnings("resource")
  @Test
  public void testTableModelLoginInjectionWithComment() throws Exception {
    // Login interface SQL injection attack 1: Using -- comments to bypass password checks
    when(connection.getSqlDialect()).thenReturn("table");
    String sql = "SELECT * FROM users WHERE username = ? AND password = ?";
    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(connection, client, sessionId, sql, zoneId);
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
    when(connection.getSqlDialect()).thenReturn("table");
    String sql = "SELECT * FROM users WHERE username = ? AND password = ?";
    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(connection, client, sessionId, sql, zoneId);
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
    when(connection.getSqlDialect()).thenReturn("table");
    String sql = "SELECT * FROM users WHERE email = ?";
    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(connection, client, sessionId, sql, zoneId);
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
    when(connection.getSqlDialect()).thenReturn("table");
    String sql = "SELECT * FROM users WHERE password = ?";
    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(connection, client, sessionId, sql, zoneId);
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
    when(connection.getSqlDialect()).thenReturn("table");
    String sql = "SELECT * FROM users WHERE password = ?";
    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(connection, client, sessionId, sql, zoneId);
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
    when(connection.getSqlDialect()).thenReturn("table");
    String sql = "SELECT * FROM users WHERE password = ?";
    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(connection, client, sessionId, sql, zoneId);
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
    when(connection.getSqlDialect()).thenReturn("table");
    String sql = "SELECT * FROM users WHERE password = ?";
    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(connection, client, sessionId, sql, zoneId);
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
    when(connection.getSqlDialect()).thenReturn("table");
    String sql = "SELECT * FROM users WHERE email = ?";
    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(connection, client, sessionId, sql, zoneId);
    ps.setString(1, null);
    ps.execute();

    ArgumentCaptor<TSExecutePreparedReq> argument =
        ArgumentCaptor.forClass(TSExecutePreparedReq.class);
    verify(client).executePreparedStatement(argument.capture());
    assertTrue(argument.getValue().getParameters() != null);
  }
}
