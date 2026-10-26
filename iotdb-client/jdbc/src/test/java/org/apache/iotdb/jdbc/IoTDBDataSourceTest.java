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
import org.apache.iotdb.rpc.TElasticFramedTransport;
import org.apache.iotdb.rpc.TimeoutChangeableTransport;
import org.apache.iotdb.service.rpc.thrift.IClientRPCService;
import org.apache.iotdb.service.rpc.thrift.TSCloseSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionResp;
import org.apache.iotdb.service.rpc.thrift.TSProtocolVersion;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.junit.Test;

import javax.sql.DataSource;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IoTDBDataSourceTest {

  @Test
  public void testDataSourceExplicitUrlTakesPrecedenceOverStandardProperties() {
    IoTDBDataSource dataSource = new IoTDBDataSource();

    dataSource.setUrl("jdbc:iotdb://explicit:6667/root.explicit");
    dataSource.setServerName("127.0.0.1");
    dataSource.setPortNumber(6688);
    dataSource.setDatabaseName("root.sg");

    assertEquals("jdbc:iotdb://explicit:6667/root.explicit", dataSource.getConnectionUrl());
  }

  @Test
  public void testDataSourceAllowsClearingUserAndPassword() {
    IoTDBDataSource dataSource = new IoTDBDataSource();

    dataSource.setUser("root");
    dataSource.setPassword("root");
    dataSource.setUser(null);
    dataSource.setPassword(null);

    assertNull(dataSource.getUser());
    assertNull(dataSource.getPassword());
  }

  @Test
  public void testDataSourceConstructorAllowsNullPort() {
    IoTDBDataSource dataSource =
        new IoTDBDataSource("jdbc:iotdb://localhost:6667", null, null, null);

    assertEquals(Integer.valueOf(6667), dataSource.getPort());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDataSourceRejectsInvalidDirectPort() {
    new IoTDBDataSource().setPortNumber(65536);
  }

  @Test
  public void testDataSourceWrapperMethods() throws SQLException {
    IoTDBDataSource dataSource = new IoTDBDataSource();

    assertTrue(dataSource.isWrapperFor(IoTDBDataSource.class));
    assertTrue(dataSource.isWrapperFor(DataSource.class));
    assertFalse(dataSource.isWrapperFor(String.class));
    assertFalse(dataSource.isWrapperFor(null));
    assertSame(dataSource, dataSource.unwrap(IoTDBDataSource.class));
    assertSame(dataSource, dataSource.unwrap(DataSource.class));
  }

  @Test(expected = SQLException.class)
  public void testDataSourceUnwrapRejectsUnsupportedClass() throws SQLException {
    new IoTDBDataSource().unwrap(String.class);
  }

  @Test(expected = SQLFeatureNotSupportedException.class)
  public void testDataSourceParentLoggerIsUnsupported() throws SQLException {
    new IoTDBDataSource().getParentLogger();
  }

  @Test
  public void testDataSourceStoresLogWriterAndLoginTimeout() throws SQLException {
    IoTDBDataSource dataSource = new IoTDBDataSource();
    PrintWriter logWriter = new PrintWriter(new StringWriter());

    dataSource.setLogWriter(logWriter);
    dataSource.setLoginTimeout(10);

    assertSame(logWriter, dataSource.getLogWriter());
    assertEquals(10, dataSource.getLoginTimeout());
  }

  @Test
  public void testLoginTimeoutDoesNotSetQueryReadTimeout() throws Exception {
    assertNetworkTimeoutAfterLogin(null, 0);
  }

  @Test
  public void testLoginTimeoutPreservesExplicitNetworkTimeout() throws Exception {
    assertNetworkTimeoutAfterLogin("5000", 5000);
  }

  private void assertNetworkTimeoutAfterLogin(String configuredTimeout, int expectedTimeout)
      throws Exception {
    IClientRPCService.Iface client = mock(IClientRPCService.Iface.class);
    TSStatus status = new TSStatus(RpcUtils.SUCCESS_STATUS);
    status.setMessage("");
    TSOpenSessionResp response =
        new TSOpenSessionResp(status, TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3);
    response.setSessionId(1L);
    when(client.openSession(any(TSOpenSessionReq.class))).thenReturn(response);
    when(client.closeSession(any(TSCloseSessionReq.class))).thenReturn(status);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
      server.setSoTimeout(5000);
      Future<Void> serverResult =
          executor.submit(
              () -> {
                try (Socket socket = server.accept()) {
                  socket.setSoTimeout(5000);
                  try (TTransport transport =
                      new TElasticFramedTransport(
                          new TSocket(socket),
                          RpcUtils.THRIFT_DEFAULT_BUF_CAPACITY,
                          RpcUtils.THRIFT_FRAME_MAX_SIZE,
                          true)) {
                    IClientRPCService.Processor<IClientRPCService.Iface> processor =
                        new IClientRPCService.Processor<>(client);
                    TBinaryProtocol protocol = new TBinaryProtocol(transport);
                    processor.process(protocol, protocol);
                    processor.process(protocol, protocol);
                  }
                }
                return null;
              });
      IoTDBDataSource dataSource = new IoTDBDataSource();
      dataSource.setServerName(server.getInetAddress().getHostAddress());
      dataSource.setPortNumber(server.getLocalPort());
      dataSource.setLoginTimeout(2);
      if (configuredTimeout != null) {
        dataSource.setConnectionProperty(Config.NETWORK_TIMEOUT, configuredTimeout);
      }
      try (IoTDBConnection connection = (IoTDBConnection) dataSource.getConnection()) {
        assertEquals(expectedTimeout, connection.getNetworkTimeout());
        Field transportField = IoTDBConnection.class.getDeclaredField("transport");
        transportField.setAccessible(true);
        TimeoutChangeableTransport transport =
            (TimeoutChangeableTransport) transportField.get(connection);
        assertEquals(expectedTimeout, transport.getTimeOut());
      }
      serverResult.get(10, TimeUnit.SECONDS);
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  @Test(expected = SQLException.class)
  public void testDataSourceRejectsNegativeLoginTimeout() throws SQLException {
    new IoTDBDataSource().setLoginTimeout(-1);
  }

  @Test(expected = SQLException.class)
  public void testDataSourceConnectionWithCredentialsThrowsInvalidUrl() throws SQLException {
    IoTDBDataSource dataSource = new IoTDBDataSource();

    dataSource.setUrl("jdbc:iotdb://test");
    dataSource.getConnection("root", "root");
  }
}
