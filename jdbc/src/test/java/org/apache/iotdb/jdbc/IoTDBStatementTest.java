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

import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.service.rpc.thrift.TSFetchMetadataReq;
import org.apache.iotdb.service.rpc.thrift.TSFetchMetadataResp;
import org.apache.iotdb.service.rpc.thrift.TSIService.Iface;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.sql.SQLException;
import java.time.ZoneId;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

public class IoTDBStatementTest {

  @Mock private IoTDBConnection connection;

  @Mock private Iface client;

  private long sessionId;

  @Mock private TSFetchMetadataResp fetchMetadataResp;

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
}
