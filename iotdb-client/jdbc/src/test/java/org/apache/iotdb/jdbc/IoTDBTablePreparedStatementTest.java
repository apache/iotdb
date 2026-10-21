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
import org.apache.iotdb.service.rpc.thrift.TSCloseOperationReq;
import org.apache.iotdb.service.rpc.thrift.TSExecutePreparedReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;
import org.apache.iotdb.service.rpc.thrift.TSPrepareReq;
import org.apache.iotdb.service.rpc.thrift.TSPrepareResp;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.TsBlockSerde;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.sql.ResultSet;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
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
    when(connection.getTimeFactor()).thenReturn(1_000);
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

    when(client.closeOperation(any(TSCloseOperationReq.class))).thenReturn(Status_SUCCESS);
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

  // ========== Table Model result column mapping tests ==========

  private static TsBlockBuilder int32BlockBuilder(int columnCount) {
    TsBlockBuilder builder = new TsBlockBuilder(Collections.nCopies(columnCount, TSDataType.INT32));
    builder.getTimeColumnBuilder().writeLong(1000L);
    builder.declarePosition();
    return builder;
  }

  private TSExecuteStatementResp buildQueryResp(
      List<String> columns, List<Integer> mapping, TsBlockBuilder blockBuilder) throws IOException {
    TSExecuteStatementResp resp = new TSExecuteStatementResp();
    resp.setStatus(Status_SUCCESS);
    resp.setQueryId(1L);
    resp.setColumns(columns);
    List<String> dataTypeList = new ArrayList<>();
    for (int i = 0; i < columns.size(); i++) {
      dataTypeList.add(TSDataType.INT32.name());
    }
    resp.setDataTypeList(dataTypeList);
    resp.setIgnoreTimeStamp(true);
    resp.setTableModel(true);
    if (mapping != null) {
      resp.setColumnIndex2TsBlockColumnIndexList(mapping);
    }
    resp.setQueryResult(
        Collections.singletonList(new TsBlockSerde().serialize(blockBuilder.build())));
    resp.setMoreData(false);
    return resp;
  }

  @SuppressWarnings("resource")
  @Test
  public void testTableModelPreparedStatementReorderedColumns() throws Exception {
    TsBlockBuilder blockBuilder = int32BlockBuilder(2);
    blockBuilder.getColumnBuilder(0).writeInt(10); // physical column 0
    blockBuilder.getColumnBuilder(1).writeInt(20); // physical column 1

    when(client.executePreparedStatement(any(TSExecutePreparedReq.class)))
        .thenReturn(buildQueryResp(Arrays.asList("b", "a"), Arrays.asList(1, 0), blockBuilder));

    IoTDBTablePreparedStatement ps =
        new IoTDBTablePreparedStatement(
            connection, client, sessionId, "SELECT b, a FROM t WHERE a > ?", zoneId);
    ps.setInt(1, 0);
    try (ResultSet rs = ps.executeQuery()) {
      assertTrue(rs.next());
      assertEquals(20, rs.getInt(1));
      assertEquals(10, rs.getInt(2));
      assertEquals(20, rs.getInt("b"));
      assertEquals(10, rs.getInt("a"));
    }
  }

  @SuppressWarnings("resource")
  @Test
  public void testTableModelPreparedStatementRepeatedProjection() throws Exception {
    TsBlockBuilder blockBuilder = int32BlockBuilder(1);
    blockBuilder.getColumnBuilder(0).writeInt(10); // physical column 0, referenced twice

    when(client.executePreparedStatement(any(TSExecutePreparedReq.class)))
        .thenReturn(buildQueryResp(Arrays.asList("x", "y"), Arrays.asList(0, 0), blockBuilder));

    IoTDBTablePreparedStatement ps =
        new IoTDBTablePreparedStatement(
            connection, client, sessionId, "SELECT a AS x, a AS y FROM t WHERE a > ?", zoneId);
    ps.setInt(1, 0);
    try (ResultSet rs = ps.executeQuery()) {
      assertTrue(rs.next());
      assertEquals(10, rs.getInt(1));
      assertEquals(10, rs.getInt(2));
      assertEquals(10, rs.getInt("x"));
      assertEquals(10, rs.getInt("y"));
    }
  }

  @SuppressWarnings("resource")
  @Test
  public void testTableModelPreparedStatementIdentityMapping() throws Exception {
    TsBlockBuilder blockBuilder = int32BlockBuilder(2);
    blockBuilder.getColumnBuilder(0).writeInt(10);
    blockBuilder.getColumnBuilder(1).writeInt(20);

    when(client.executePreparedStatement(any(TSExecutePreparedReq.class)))
        .thenReturn(buildQueryResp(Arrays.asList("a", "b"), Arrays.asList(0, 1), blockBuilder));

    IoTDBTablePreparedStatement ps =
        new IoTDBTablePreparedStatement(
            connection, client, sessionId, "SELECT a, b FROM t WHERE a > ?", zoneId);
    ps.setInt(1, 0);
    try (ResultSet rs = ps.executeQuery()) {
      assertTrue(rs.next());
      assertEquals(10, rs.getInt(1));
      assertEquals(20, rs.getInt(2));
      assertEquals(10, rs.getInt("a"));
      assertEquals(20, rs.getInt("b"));
    }
  }

  @SuppressWarnings("resource")
  @Test
  public void testTableModelPreparedStatementMissingMappingFallsBack() throws Exception {
    TsBlockBuilder blockBuilder = int32BlockBuilder(2);
    blockBuilder.getColumnBuilder(0).writeInt(10);
    blockBuilder.getColumnBuilder(1).writeInt(20);

    // No columnIndex2TsBlockColumnIndexList: the client must keep its own derivation
    when(client.executePreparedStatement(any(TSExecutePreparedReq.class)))
        .thenReturn(buildQueryResp(Arrays.asList("a", "b"), null, blockBuilder));

    IoTDBTablePreparedStatement ps =
        new IoTDBTablePreparedStatement(
            connection, client, sessionId, "SELECT a, b FROM t WHERE a > ?", zoneId);
    ps.setInt(1, 0);
    try (ResultSet rs = ps.executeQuery()) {
      assertTrue(rs.next());
      assertEquals(10, rs.getInt(1));
      assertEquals(20, rs.getInt(2));
      assertEquals(10, rs.getInt("a"));
      assertEquals(20, rs.getInt("b"));
    }
  }
}
