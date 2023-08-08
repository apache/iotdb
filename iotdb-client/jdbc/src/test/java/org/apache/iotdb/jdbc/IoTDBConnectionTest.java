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
import org.apache.iotdb.service.rpc.thrift.TSGetTimeZoneResp;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionResp;
import org.apache.iotdb.service.rpc.thrift.TSSetTimeZoneReq;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

public class IoTDBConnectionTest {

  @Mock private IClientRPCService.Iface client;

  @Mock private IoTDBConnection connectionMock;

  @Mock private Properties properties;

  @Mock private TSOpenSessionResp tsOpenSessionResp;

  private IoTDBConnection connection = new IoTDBConnection();
  private TSStatus successStatus = RpcUtils.SUCCESS_STATUS;
  private long sessionId;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    connection = new IoTDBConnection();
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
  public void reconnect() {
    connection.reconnect();
  }

  @Test
  public void setTimeoutTest() throws SQLException {
    connection.setQueryTimeout(60);
    Assert.assertEquals(60, connection.getQueryTimeout());
  }

  @Test(expected = SQLException.class)
  public void isWrapperFor() throws SQLException {
    connection.isWrapperFor(String.class);
  }

  @Test(expected = SQLException.class)
  public void unwrap() throws SQLException {
    connection.unwrap(String.class);
  }

  @Test(expected = SQLException.class)
  public void abort() throws SQLException {
    connection.abort(Executors.newCachedThreadPool());
  }

  @Test(expected = SQLException.class)
  public void commit() throws SQLException {
    connection.commit();
  }

  @Test(expected = SQLException.class)
  public void createArrayOf() throws SQLException {
    connection.createArrayOf("1", new Object[] {});
  }

  @Test(expected = SQLException.class)
  public void createBlob() throws SQLException {
    connection.createBlob();
  }

  @Test(expected = SQLException.class)
  public void createClob() throws SQLException {
    connection.createClob();
  }

  @Test(expected = SQLException.class)
  public void createNClob() throws SQLException {
    connection.createNClob();
  }

  @Test(expected = SQLException.class)
  public void createSQLXML() throws SQLException {
    connection.createSQLXML();
  }

  @Test(expected = SQLException.class)
  public void createStatement1() throws SQLException {
    connection.close();
    connection.createStatement();
  }

  @Test(expected = SQLException.class)
  public void createStatement2() throws SQLException {
    connection.createStatement(1, 1, 1);
  }

  @Test(expected = SQLException.class)
  public void createStruct() throws SQLException {
    connection.createStruct("", new Object[] {});
  }

  @Test(expected = SQLException.class)
  public void setCatalog() throws SQLException {
    connection.setCatalog("");
  }

  @Test(expected = SQLException.class)
  public void getClientInfo1() throws SQLException {
    connection.getClientInfo();
  }

  @Test(expected = SQLException.class)
  public void setClientInfo() throws SQLClientInfoException {
    connection.setClientInfo(new Properties());
  }

  @Test(expected = SQLException.class)
  public void getClientInfo2() throws SQLException {
    connection.getClientInfo("");
  }

  @Test(expected = SQLException.class)
  public void setHoldability() throws SQLException {
    connection.setHoldability(1);
  }

  @Test(expected = SQLException.class)
  public void getMetaData() throws SQLException {
    connection.close();
    connection.getMetaData();
  }

  @Test(expected = SQLException.class)
  public void getSchema() throws SQLException {
    connection.getSchema();
  }

  @Test(expected = SQLException.class)
  public void setSchema() throws SQLException {
    connection.setSchema("");
  }

  @Test(expected = SQLException.class)
  public void setTransactionIsolation() throws SQLException {
    connection.setTransactionIsolation(1);
  }

  @Test(expected = SQLException.class)
  public void getTypeMap() throws SQLException {
    connection.getTypeMap();
  }

  @Test(expected = SQLException.class)
  public void setTypeMap() throws SQLException {
    connection.setTypeMap(new HashMap<>());
  }

  @Test(expected = SQLException.class)
  public void nativeSQL() throws SQLException {
    connection.nativeSQL("");
  }

  @Test(expected = SQLException.class)
  public void prepareCall1() throws SQLException {
    connection.prepareCall("");
  }

  @Test(expected = SQLException.class)
  public void prepareCall2() throws SQLException {
    connection.prepareCall("", 1, 1);
  }

  @Test(expected = SQLException.class)
  public void prepareCall3() throws SQLException {
    connection.prepareCall("", 1, 1, 1);
  }

  @Test(expected = SQLException.class)
  public void prepareStatement2() throws SQLException {
    connection.prepareStatement("", 1);
  }

  @Test(expected = SQLException.class)
  public void prepareStatement3() throws SQLException {
    connection.prepareStatement("", new int[] {});
  }

  @Test(expected = SQLException.class)
  public void prepareStatement4() throws SQLException {
    connection.prepareStatement("", new String[] {});
  }

  @Test(expected = SQLException.class)
  public void prepareStatement5() throws SQLException {
    connection.prepareStatement("", 1, 1);
  }

  @Test(expected = SQLException.class)
  public void prepareStatement6() throws SQLException {
    connection.prepareStatement("", 1, 1, 1);
  }

  @Test(expected = SQLException.class)
  public void releaseSavepoint() throws SQLException {
    connection.releaseSavepoint(null);
  }

  @Test(expected = TTransportException.class)
  public void create() throws SQLException, TException {
    when(properties.get("user")).thenReturn("root");
    when(connectionMock.getClient()).thenReturn(client);
    when(connectionMock.getClient().openSession(any())).thenReturn(tsOpenSessionResp);
    connectionMock = new IoTDBConnection("jdbc:iotdb://127.0.0.1:6667", properties);
  }
}
