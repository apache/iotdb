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
import org.apache.iotdb.rpc.stmt.PreparedParameterSerde;
import org.apache.iotdb.rpc.stmt.PreparedParameterSerde.DeserializedParam;
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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.ZoneId;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
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
    when(client.executeStatementV2(any(TSExecuteStatementReq.class))).thenReturn(execStatementResp);
    when(client.deallocatePreparedStatement(any())).thenReturn(Status_SUCCESS);
    when(client.closeOperation(any())).thenReturn(Status_SUCCESS);
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
  public void testParameterMetadataWrapperMethods() throws Exception {
    IoTDBTablePreparedStatement ps =
        new IoTDBTablePreparedStatement(connection, client, sessionId, "SELECT ?", zoneId);
    ParameterMetaData metadata = ps.getParameterMetaData();

    assertTrue(metadata.isWrapperFor(ParameterMetaData.class));
    assertFalse(metadata.isWrapperFor(String.class));
    assertFalse(metadata.isWrapperFor(null));
    assertSame(metadata, metadata.unwrap(ParameterMetaData.class));
    assertThrows(SQLException.class, () -> metadata.unwrap(String.class));
  }

  @SuppressWarnings("resource")
  @Test
  public void testParameterMetadataRejectsInvalidIndex() throws Exception {
    IoTDBTablePreparedStatement ps =
        new IoTDBTablePreparedStatement(connection, client, sessionId, "SELECT ?", zoneId);
    ParameterMetaData metadata = ps.getParameterMetaData();

    assertEquals(1, metadata.getParameterCount());
    assertThrows(SQLException.class, () -> metadata.getParameterType(0));
    assertThrows(SQLException.class, () -> metadata.isSigned(2));
    assertThrows(SQLException.class, () -> ps.setInt(0, 1));
    assertThrows(SQLException.class, () -> ps.setInt(2, 1));

    ps.setInt(1, 1);

    assertEquals(Types.INTEGER, metadata.getParameterType(1));
    assertTrue(metadata.isSigned(1));
  }

  @SuppressWarnings("resource")
  @Test
  public void testClosedTablePreparedStatementRejectsParameterOperations() throws Exception {
    IoTDBTablePreparedStatement ps =
        new IoTDBTablePreparedStatement(connection, client, sessionId, "SELECT ?", zoneId);
    ParameterMetaData metadata = ps.getParameterMetaData();

    ps.close();

    assertTrue(ps.isClosed());
    assertThrows(SQLException.class, () -> ps.clearParameters());
    assertThrows(SQLException.class, () -> ps.getMetaData());
    assertThrows(SQLException.class, () -> ps.setInt(1, 1));
    assertThrows(SQLException.class, () -> ps.setString(1, "x"));
    assertThrows(
        SQLException.class,
        () -> ps.setBinaryStream(1, new ByteArrayInputStream(new byte[] {1}), 1));
    assertThrows(SQLException.class, () -> ps.getParameterMetaData());
    assertThrows(SQLException.class, () -> metadata.getParameterCount());
    assertThrows(SQLException.class, () -> metadata.getParameterType(1));
  }

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

  @SuppressWarnings("resource")
  @Test
  public void testTableModelPreparedNullSettersSerializeNulls() throws Exception {
    String sql = "SELECT * FROM users WHERE a=? AND b=? AND c=? AND d=? AND e=? AND f=?";
    IoTDBTablePreparedStatement ps =
        new IoTDBTablePreparedStatement(connection, client, sessionId, sql, zoneId);

    ps.setObject(1, null);
    ps.setBytes(2, null);
    ps.setDate(3, (Date) null);
    ps.setTime(4, (Time) null);
    ps.setTimestamp(5, (Timestamp) null);
    ps.setBinaryStream(6, (InputStream) null, 0);
    ps.execute();

    ArgumentCaptor<TSExecutePreparedReq> argument =
        ArgumentCaptor.forClass(TSExecutePreparedReq.class);
    verify(client).executePreparedStatement(argument.capture());
    List<DeserializedParam> parameters =
        PreparedParameterSerde.deserialize(ByteBuffer.wrap(argument.getValue().getParameters()));

    assertEquals(6, parameters.size());
    for (DeserializedParam parameter : parameters) {
      assertTrue(parameter.isNull());
    }
  }

  @SuppressWarnings("resource")
  @Test
  public void testTableModelSetBinaryStreamRejectsNegativeLength() throws Exception {
    IoTDBTablePreparedStatement ps =
        new IoTDBTablePreparedStatement(connection, client, sessionId, "SELECT ?", zoneId);

    assertThrows(
        SQLException.class, () -> ps.setBinaryStream(1, new ByteArrayInputStream(new byte[0]), -1));
  }

  @SuppressWarnings("resource")
  @Test
  public void testTableModelClientSideNullStringUsesSqlNullLiteral() throws Exception {
    String sql = "INSERT INTO users(time,email) VALUES(1, ?)";
    IoTDBTablePreparedStatement ps =
        new IoTDBTablePreparedStatement(connection, client, sessionId, sql, zoneId);

    assertEquals(1, ps.getParameterMetaData().getParameterCount());
    assertThrows(SQLException.class, () -> ps.setString(0, null));
    assertThrows(SQLException.class, () -> ps.setString(2, null));

    ps.setString(1, null);
    ps.execute();

    ArgumentCaptor<TSExecuteStatementReq> argument =
        ArgumentCaptor.forClass(TSExecuteStatementReq.class);
    verify(client).executeStatementV2(argument.capture());
    assertEquals(
        "INSERT INTO users(time,email) VALUES(1, NULL)", argument.getValue().getStatement());
  }
}
