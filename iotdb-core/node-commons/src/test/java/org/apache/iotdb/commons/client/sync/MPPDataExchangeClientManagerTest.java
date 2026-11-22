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
import org.apache.iotdb.commons.client.ThriftClient;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.QueryTimeoutException;
import org.apache.iotdb.rpc.RpcSslUtils;
import org.apache.iotdb.rpc.TElasticFramedTransport;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class MPPDataExchangeClientManagerTest {
  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private ObjectName setupPoolName() throws Exception {
    return new ObjectName(
        IoTDBConstant.IOTDB_THREADPOOL_JMX_NAME,
        IoTDBConstant.JMX_TYPE,
        ThreadName.MPP_DATA_EXCHANGE_CONNECTION_SETUP.getName());
  }

  @Test
  public void exposesActiveConnectionSetupWorkersUntilManagerCloses() throws Exception {
    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    ObjectName name = setupPoolName();
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
            manager.borrowClient(endpoint, System.currentTimeMillis() + 5_000)) {
          assertSame(first, shortQuery);
          assertTrue(shortQuery.getTimeout() > 0 && shortQuery.getTimeout() <= 5_000);
        }
        try (SyncDataNodeMPPDataExchangeServiceClient legacy = manager.borrowClient(endpoint)) {
          assertSame(first, legacy);
          assertEquals(configured, legacy.getTimeout());
        }
        try (SyncDataNodeMPPDataExchangeServiceClient longQuery =
            manager.borrowClient(endpoint, System.currentTimeMillis() + 20_000)) {
          assertSame(first, longQuery);
          assertTrue(longQuery.getTimeout() > 5_000 && longQuery.getTimeout() <= 20_000);
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
        long start = System.nanoTime();
        long deadline = System.currentTimeMillis() + 150;
        assertThrows(ClientManagerException.class, () -> manager.borrowClient(endpoint, deadline));
        assertTrue(System.nanoTime() - start >= MILLISECONDS.toNanos(140));
        ClientManagerException expired =
            assertThrows(
                ClientManagerException.class,
                () -> manager.borrowClient(endpoint, System.currentTimeMillis() - 1));
        assertTrue(expired.getCause() instanceof QueryTimeoutException);
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

  @Test
  public void borrowPreservesQueryTimeoutCauseDuringPreparation() {
    QueryTimeoutException expired = new QueryTimeoutException();
    MPPDataExchangeClientManager manager =
        new MPPDataExchangeClientManager() {
          @Override
          <T> T prepareConnection(Callable<T> prepare, int timeoutMs) {
            throw expired;
          }
        };
    try {
      TEndPoint endpoint = new TEndPoint("localhost", 1);
      ClientManagerException failure =
          assertThrows(
              ClientManagerException.class, () -> manager.borrowClient(endpoint, Long.MAX_VALUE));
      assertSame(expired, failure.getCause());
      assertEquals(0, manager.getPool().getNumActive());
      assertEquals(0, manager.getPool().getNumIdle());
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
  public void closingPoolReportsQueuedSetupCancellationAsTransportFailure() throws Exception {
    MPPDataExchangeClientManager manager = new MPPDataExchangeClientManager();
    ExecutorService callers = Executors.newFixedThreadPool(5);
    CountDownLatch started = new CountDownLatch(4);
    CountDownLatch release = new CountDownLatch(1);
    try {
      for (int i = 0; i < 4; i++) {
        callers.submit(
            () ->
                manager.prepareConnection(
                    () -> {
                      started.countDown();
                      release.await();
                      return null;
                    },
                    0));
      }
      assertTrue(started.await(5, SECONDS));
      Future<String> queued = callers.submit(() -> manager.prepareConnection(() -> "queued", 0));
      ObjectName name = setupPoolName();
      MBeanServer server = ManagementFactory.getPlatformMBeanServer();
      await()
          .atMost(5, SECONDS)
          .until(() -> Integer.valueOf(1).equals(server.getAttribute(name, "QueueLength")));
      manager.close();
      Throwable failure =
          assertThrows(ExecutionException.class, () -> queued.get(5, SECONDS)).getCause();
      assertTrue(failure instanceof TTransportException);
      assertTrue(failure.getCause() instanceof CancellationException);
    } finally {
      release.countDown();
      callers.shutdownNow();
      manager.close();
    }
  }

  @Test
  public void onlyPeerFailuresClearOtherConnectionsEvenOnInterruptedWorker() {
    TEndPoint endpoint = new TEndPoint("127.0.0.1", 10740);
    MPPDataExchangeClientManager manager = mock(MPPDataExchangeClientManager.class);
    SyncDataNodeMPPDataExchangeServiceClient client =
        new SyncDataNodeMPPDataExchangeServiceClient(
            new ThriftClientProperty.Builder().setPrintLogWhenEncounterException(false).build(),
            endpoint,
            manager);
    ThriftClient.resolveException(new TException(new ClosedByInterruptException()), client);
    verify(manager, never()).clear(endpoint);
    Thread.currentThread().interrupt();
    try {
      ThriftClient.resolveException(new TException(new SocketException("Broken pipe")), client);
      verify(manager).clear(endpoint);
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  public void tlsConnectionVerifiesPeerOnLoopback() throws Exception {
    String password = "test-only";
    Path store = temporaryFolder.getRoot().toPath().resolve("loopback.p12");
    Path output = temporaryFolder.newFile("keytool.log").toPath();
    Path keytool =
        Path.of(
            System.getProperty("java.home"),
            "bin",
            File.separatorChar == '\\' ? "keytool.exe" : "keytool");
    Process keys =
        new ProcessBuilder(
                keytool.toString(),
                "-genkeypair",
                "-alias",
                "loopback",
                "-keyalg",
                "EC",
                "-dname",
                "CN=localhost",
                "-ext",
                "SAN=ip:127.0.0.1",
                "-validity",
                "2",
                "-storetype",
                "PKCS12",
                "-keystore",
                store.toString(),
                "-storepass",
                password)
            .redirectErrorStream(true)
            .redirectOutput(output.toFile())
            .start();
    try {
      assertTrue(keys.waitFor(30, SECONDS));
      assertEquals(Files.readString(output), 0, keys.exitValue());
    } finally {
      keys.destroyForcibly();
    }
    CommonConfig config = CommonDescriptor.getInstance().getConfig();
    boolean previousSsl = config.isEnableInternalSSL();
    String previousTrustStore = config.getTrustStorePath();
    String previousTrustPassword = config.getTrustStorePwd();
    SSLContext tls =
        RpcSslUtils.createSSLContext(store.toString(), password, store.toString(), password);
    MPPDataExchangeClientManager manager = new MPPDataExchangeClientManager();
    ExecutorService peerThread = Executors.newSingleThreadExecutor();
    try (SSLServerSocket server =
        (SSLServerSocket)
            tls.getServerSocketFactory()
                .createServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))) {
      config.setEnableInternalSSL(true);
      config.setTrustStorePath(store.toString());
      config.setTrustStorePwd(password);
      server.setSoTimeout(10_000);
      Future<?> peerHandshake =
          peerThread.submit(
              () -> {
                try (SSLSocket peer = (SSLSocket) server.accept()) {
                  peer.setSoTimeout(10_000);
                  peer.startHandshake();
                }
                return null;
              });
      try (SyncDataNodeMPPDataExchangeServiceClient client =
          manager.borrowClient(
              new TEndPoint("127.0.0.1", server.getLocalPort()),
              System.currentTimeMillis() + 10_000)) {
        TSocket transport =
            (TSocket)
                ((TElasticFramedTransport) client.getInputProtocol().getTransport()).getSocket();
        SSLSocket socket = (SSLSocket) transport.getSocket();
        assertEquals("HTTPS", socket.getSSLParameters().getEndpointIdentificationAlgorithm());
        socket.startHandshake();
        assertTrue(socket.getSession().getPeerCertificates().length > 0);
        client.invalidate();
      }
      peerHandshake.get(10, SECONDS);
    } finally {
      config.setEnableInternalSSL(previousSsl);
      config.setTrustStorePath(previousTrustStore);
      config.setTrustStorePwd(previousTrustPassword);
      manager.close();
      peerThread.shutdownNow();
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
    IOException closeFailure = new IOException("failed to close");
    doThrow(closeFailure).when(expired).close();
    QueryTimeoutException timeoutFailure =
        assertThrows(
            QueryTimeoutException.class,
            () -> client.connect(expired, System.currentTimeMillis() - 1));
    assertArrayEquals(new Throwable[] {closeFailure}, timeoutFailure.getSuppressed());
    verify(expired, never()).connect(any(SocketAddress.class), anyInt());
    verify(expired).close();

    SyncDataNodeMPPDataExchangeServiceClient unlimited =
        new SyncDataNodeMPPDataExchangeServiceClient(
            new ThriftClientProperty.Builder().setConnectionTimeoutMs(-1).build(),
            new TEndPoint("127.0.0.1", 10740),
            null);
    Socket failed = mock(Socket.class);
    AssertionError connectFailure = new AssertionError("connect failed");
    doThrow(connectFailure).when(failed).connect(any(SocketAddress.class), eq(0));
    doThrow(closeFailure).when(failed).close();
    assertSame(
        connectFailure,
        assertThrows(AssertionError.class, () -> unlimited.connect(failed, Long.MAX_VALUE)));
    assertArrayEquals(new Throwable[] {closeFailure}, connectFailure.getSuppressed());
  }
}
