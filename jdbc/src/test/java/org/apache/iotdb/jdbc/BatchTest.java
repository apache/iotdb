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
import org.apache.iotdb.service.rpc.thrift.IClientRPCService;
import org.apache.iotdb.service.rpc.thrift.TSExecuteBatchStatementReq;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.sql.BatchUpdateException;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

public class BatchTest {

  @Mock private IoTDBConnection connection;
  @Mock private IClientRPCService.Iface client;
  private long sessionId;
  @Mock private IoTDBStatement statement;
  private TSStatus errorStatus = RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR);
  private TSStatus resp;
  private ZoneId zoneID = ZoneId.systemDefault();

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    when(connection.createStatement())
        .thenReturn(new IoTDBStatement(connection, client, sessionId, zoneID, 0, 1L));
  }

  @After
  public void tearDown() {}

  @SuppressWarnings("serial")
  @Test
  public void testExecuteBatchSQL1() throws SQLException, TException {
    Statement statement = connection.createStatement();
    statement.addBatch("sql1");
    resp = new TSStatus();
    resp =
        RpcUtils.getStatus(
            Collections.singletonList(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS)));
    when(client.executeBatchStatement(any(TSExecuteBatchStatementReq.class))).thenReturn(resp);
    int[] result = statement.executeBatch();
    assertEquals(1, result.length);

    List<TSStatus> resExpected =
        new ArrayList<TSStatus>() {
          {
            add(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
            add(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
            add(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
            add(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
            add(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
            add(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
            add(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
            add(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
            add(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
          }
        };
    resp.setSubStatus(resExpected);

    statement.clearBatch();
    statement.addBatch("CREATE DATABASE root.ln.wf01.wt01");
    statement.addBatch(
        "CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN");
    statement.addBatch(
        "CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE=FLOAT, ENCODING=RLE");
    statement.addBatch(
        "insert into root.ln.wf01.wt01(timestamp,status) values(1509465600000,true)");
    statement.addBatch(
        "insert into root.ln.wf01.wt01(timestamp,status) values(1509465660000,true)");
    statement.addBatch(
        "insert into root.ln.wf01.wt01(timestamp,status) vvvvvv(1509465720000,false)");
    statement.addBatch(
        "insert into root.ln.wf01.wt01(timestamp,temperature) values(1509465600000,25.957603)");
    statement.addBatch(
        "insert into root.ln.wf01.wt01(timestamp,temperature) values(1509465660000,24.359503)");
    statement.addBatch(
        "insert into root.ln.wf01.wt01(timestamp,temperature) vvvvvv(1509465720000,20.092794)");
    result = statement.executeBatch();
    assertEquals(resp.getSubStatus().size(), result.length);
    for (int i = 0; i < resp.getSubStatus().size(); i++) {
      assertEquals(resExpected.get(i).code, result[i]);
    }
    statement.clearBatch();
  }

  @Test(expected = BatchUpdateException.class)
  public void testExecuteBatchSQL2() throws SQLException, TException {
    Statement statement = connection.createStatement();
    statement.addBatch("sql1");
    resp =
        RpcUtils.getStatus(
            Collections.singletonList(RpcUtils.getStatus(TSStatusCode.SQL_PARSE_ERROR)));

    when(client.executeBatchStatement(any(TSExecuteBatchStatementReq.class))).thenReturn(resp);
    statement.executeBatch();
  }

  @SuppressWarnings("serial")
  @Test
  public void testExecuteBatchSQL3() throws SQLException, TException {
    Statement statement = connection.createStatement();
    resp = RpcUtils.getStatus(Collections.singletonList(errorStatus));
    statement.addBatch("sql1");
    statement.addBatch("sql1");
    List<TSStatus> resExpected =
        new ArrayList<TSStatus>() {
          {
            add(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
            add(RpcUtils.getStatus(TSStatusCode.SQL_PARSE_ERROR));
          }
        };
    resp.setSubStatus(resExpected);
    when(client.executeBatchStatement(any(TSExecuteBatchStatementReq.class))).thenReturn(resp);
    try {
      statement.executeBatch();
    } catch (BatchUpdateException e) {
      int[] result = e.getUpdateCounts();
      for (int i = 0; i < resExpected.size(); i++) {
        assertEquals(resExpected.get(i).code, result[i]);
      }
      return;
    }
    fail();
  }
}
