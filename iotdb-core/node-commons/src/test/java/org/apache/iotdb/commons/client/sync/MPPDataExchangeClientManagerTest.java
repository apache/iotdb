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

package org.apache.iotdb.commons.client.sync;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.QueryTimeoutException;

import org.apache.thrift.transport.TTransportException;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class MPPDataExchangeClientManagerTest {
  @Test
  public void exposesActiveConnectionSetupWorkersUntilManagerCloses() throws Exception {
    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    ObjectName name =
        new ObjectName(
            IoTDBConstant.IOTDB_THREADPOOL_JMX_NAME,
            IoTDBConstant.JMX_TYPE,
            ThreadName.MPP_DATA_EXCHANGE_CONNECTION_SETUP.getName());
    MPPDataExchangeClientManager manager = new MPPDataExchangeClientManager();
    try {
      int active =
          manager.prepareConnection(
              () -> (Integer) server.getAttribute(name, "ActiveCount"), 5_000);
      assertEquals(1, active);
    } finally {
      manager.close();
    }
    assertFalse(server.isRegistered(name));
  }

  @Test
  public void reusedConnectionUsesEachBorrowersTimeout() throws Exception {
    MPPDataExchangeClientManager manager = new MPPDataExchangeClientManager();
    int configured = CommonDescriptor.getInstance().getConfig().getDnConnectionTimeoutInMS();
    try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
      TEndPoint endpoint =
          new TEndPoint(server.getInetAddress().getHostAddress(), server.getLocalPort());
      SyncDataNodeMPPDataExchangeServiceClient first;
      try (SyncDataNodeMPPDataExchangeServiceClient client = manager.borrowClient(endpoint)) {
        first = client;
        assertEquals(configured, client.getTimeout());
      }
      try (Socket peer = server.accept()) {
        try (SyncDataNodeMPPDataExchangeServiceClient shortQuery =
            manager.borrowClient(endpoint, System.currentTimeMillis() + 800)) {
          assertSame(first, shortQuery);
          assertTrue(shortQuery.getTimeout() > 0 && shortQuery.getTimeout() <= 800);
        }
        try (SyncDataNodeMPPDataExchangeServiceClient legacy = manager.borrowClient(endpoint)) {
          assertSame(first, legacy);
          assertEquals(configured, legacy.getTimeout());
        }
        try (SyncDataNodeMPPDataExchangeServiceClient longQuery =
            manager.borrowClient(endpoint, System.currentTimeMillis() + 20_000)) {
          assertSame(first, longQuery);
          assertTrue(longQuery.getTimeout() > 800 && longQuery.getTimeout() <= 20_000);
        }
        try (SyncDataNodeMPPDataExchangeServiceClient capped =
            manager.borrowClient(endpoint, System.currentTimeMillis() + 2L * configured)) {
          assertEquals(configured, capped.getTimeout());
        }
      }
    } finally {
      manager.close();
    }
  }

  @Test(timeout = 10_000)
  public void exhaustedPoolHonorsBothQueryAndPoolWaitLimits() throws Exception {
    MPPDataExchangeClientManager manager = new MPPDataExchangeClientManager();
    manager.getPool().setMaxTotalPerKey(1);
    try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
      TEndPoint endpoint =
          new TEndPoint(server.getInetAddress().getHostAddress(), server.getLocalPort());
      try (SyncDataNodeMPPDataExchangeServiceClient held = manager.borrowClient(endpoint);
          Socket peer = server.accept()) {
        long deadline = System.currentTimeMillis() + 150;
        assertThrows(ClientManagerException.class, () -> manager.borrowClient(endpoint, deadline));
        assertTrue(System.currentTimeMillis() >= deadline - 1);
        manager.getPool().setMaxWait(Duration.ofMillis(30));
        assertThrows(
            ClientManagerException.class,
            () -> manager.borrowClient(endpoint, System.currentTimeMillis() + 20_000));
        assertEquals(1, manager.getPool().getNumActive());
      }
      assertEquals(1, manager.getPool().getNumIdle());
    } finally {
      manager.close();
    }
  }

  @Test(timeout = 5_000)
  public void stalledConnectionSetupReleasesBorrower() throws Exception {
    MPPDataExchangeClientManager manager = new MPPDataExchangeClientManager();
    CountDownLatch release = new CountDownLatch(1);
    try {
      TTransportException failure =
          assertThrows(
              TTransportException.class,
              () ->
                  manager.prepareConnection(
                      () -> {
                        release.await();
                        return null;
                      },
                      80));
      assertEquals(TTransportException.TIMED_OUT, failure.getType());
      assertEquals("ready", manager.prepareConnection(() -> "ready", 1_000));
    } finally {
      release.countDown();
      manager.close();
    }
  }

  @Test
  public void connectUsesRemainingBudgetAndClosesFailedSocket() throws Exception {
    SyncDataNodeMPPDataExchangeServiceClient client =
        new SyncDataNodeMPPDataExchangeServiceClient(
            new ThriftClientProperty.Builder().setConnectionTimeoutMs(60_000).build(),
            new TEndPoint("127.0.0.1", 10740),
            null);
    Socket socket = mock(Socket.class);
    doThrow(new SocketTimeoutException()).when(socket).connect(any(SocketAddress.class), anyInt());
    long deadline = System.currentTimeMillis() - 3_200 + 4_000;
    assertThrows(SocketTimeoutException.class, () -> client.connect(socket, deadline));
    ArgumentCaptor<Integer> timeout = ArgumentCaptor.forClass(Integer.class);
    verify(socket).connect(any(SocketAddress.class), timeout.capture());
    assertTrue(timeout.getValue() > 0 && timeout.getValue() <= 800);
    verify(socket).close();

    Socket expired = mock(Socket.class);
    assertThrows(
        QueryTimeoutException.class, () -> client.connect(expired, System.currentTimeMillis() - 1));
    verify(expired, never()).connect(any(SocketAddress.class), anyInt());
    verify(expired).close();
  }
}
