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
import org.apache.iotdb.service.rpc.thrift.IClientRPCService.Iface;
import org.apache.iotdb.service.rpc.thrift.TSFetchMetadataReq;
import org.apache.iotdb.service.rpc.thrift.TSFetchMetadataResp;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneId;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
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
        .thenReturn(new IoTDBDatabaseMetadata(connection, client, sessionId, zoneID));
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
    IoTDBStatement stmt =
        new IoTDBStatement(
            connection, client, sessionId, fetchSize, zoneID, TSFileConfig.STRING_CHARSET, 0);
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
    IoTDBStatement statement =
        new IoTDBStatement(connection, client, sessionId, zoneID, TSFileConfig.STRING_CHARSET, 60);
    Assert.assertEquals(60, statement.getQueryTimeout());
    statement.setQueryTimeout(100);
    Assert.assertEquals(100, statement.getQueryTimeout());
  }

  @Test(expected = SQLException.class)
  public void testSetQueryTimeoutRejectsNegativeValue() throws SQLException {
    IoTDBStatement statement = new IoTDBStatement(connection, client, sessionId, zoneID);
    statement.setQueryTimeout(-1);
  }

  @Test
  public void testWrapperMethods() throws SQLException {
    IoTDBStatement statement = new IoTDBStatement(connection, client, sessionId, zoneID);

    assertTrue(statement.isWrapperFor(IoTDBStatement.class));
    assertTrue(statement.isWrapperFor(Statement.class));
    assertFalse(statement.isWrapperFor(String.class));
    assertFalse(statement.isWrapperFor(null));
    assertSame(statement, statement.unwrap(IoTDBStatement.class));
    assertSame(statement, statement.unwrap(Statement.class));
  }

  @Test
  public void testStandardResultSetHoldability() throws SQLException {
    IoTDBStatement statement = new IoTDBStatement(connection, client, sessionId, zoneID);

    assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, statement.getResultSetHoldability());
  }

  @Test
  public void testClosedStatementRejectsOperations() throws SQLException {
    IoTDBStatement statement = new IoTDBStatement(connection, client, sessionId, zoneID, 0, -1L);

    statement.close();

    assertTrue(statement.isClosed());
    assertThrows(SQLException.class, () -> statement.execute("select 1"));
    assertThrows(SQLException.class, () -> statement.executeQuery("select 1"));
    assertThrows(
        SQLException.class,
        () -> statement.executeUpdate("insert into root.sg.d(time,s) values(1,1)"));
    assertThrows(SQLException.class, () -> statement.executeBatch());
    assertThrows(SQLException.class, () -> statement.addBatch("select 1"));
    assertThrows(SQLException.class, () -> statement.clearBatch());
    assertThrows(SQLException.class, () -> statement.isWrapperFor(Statement.class));
    assertThrows(SQLException.class, () -> statement.unwrap(Statement.class));
    assertThrows(SQLException.class, () -> statement.getFetchSize());
    assertThrows(SQLException.class, () -> statement.getWarnings());
  }

  @Test(expected = SQLException.class)
  public void testUnwrapRejectsUnsupportedClass() throws SQLException {
    new IoTDBStatement(connection, client, sessionId, zoneID).unwrap(String.class);
  }
}
