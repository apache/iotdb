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
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.QueryTimeoutException;
import org.apache.iotdb.mpp.rpc.thrift.TFetchDeviceEntrySegmentReq;
import org.apache.iotdb.rpc.DeepCopyRpcTransportFactory;
import org.apache.iotdb.rpc.RpcSslUtils;
import org.apache.iotdb.rpc.TElasticFramedTransport;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.security.KeyStore;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class MPPDataExchangeClientTlsTest {
  @ClassRule public static final TemporaryFolder folder = new TemporaryFolder();
  private static final String PASSWORD = "test-only";
  private static Path store;
  private static SSLContext serverContext;
  private final MPPDataExchangeClientManager clients = new MPPDataExchangeClientManager();
  private final ExecutorService peerThread = Executors.newSingleThreadExecutor();
  private final CommonConfig config = CommonDescriptor.getInstance().getConfig();
  private boolean previousSsl;
  private String previousTrustPath;
  private String previousTrustPassword;

  @BeforeClass
  public static void createStores() throws Exception {
    store = folder.getRoot().toPath().resolve("loopback.p12");
    Path output = folder.newFile("keytool.log").toPath();
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
                "CN=mpp-test",
                "-ext",
                "SAN=dns:localhost",
                "-validity",
                "2",
                "-storetype",
                "PKCS12",
                "-keystore",
                store.toString(),
                "-storepass",
                PASSWORD)
            .redirectErrorStream(true)
            .redirectOutput(output.toFile())
            .start();
    try {
      assertTrue(keys.waitFor(30, SECONDS));
      assertEquals(Files.readString(output), 0, keys.exitValue());
    } finally {
      keys.destroyForcibly();
    }
    serverContext =
        RpcSslUtils.createSSLContext(store.toString(), PASSWORD, store.toString(), PASSWORD);
  }

  @Before
  public void enableTls() {
    previousSsl = config.isEnableInternalSSL();
    previousTrustPath = config.getTrustStorePath();
    previousTrustPassword = config.getTrustStorePwd();
    config.setEnableInternalSSL(true);
    config.setTrustStorePath(store.toString());
    config.setTrustStorePwd(PASSWORD);
  }

  @After
  public void cleanUp() {
    config.setEnableInternalSSL(previousSsl);
    config.setTrustStorePath(previousTrustPath);
    config.setTrustStorePwd(previousTrustPassword);
    clients.close();
    peerThread.shutdownNow();
  }

  private SSLServerSocket server() throws IOException {
    SSLServerSocket server =
        (SSLServerSocket)
            serverContext
                .getServerSocketFactory()
                .createServerSocket(0, 4, InetAddress.getByName("localhost"));
    server.setSoTimeout(10_000);
    return server;
  }

  @Test
  public void bothTransportsAcceptMatchingHostname() throws Exception {
    checkPeerIdentity(true, true);
    checkPeerIdentity(false, true);
  }

  @Test
  public void bothTransportsRejectTrustedCertificateWithoutIpSan() throws Exception {
    checkPeerIdentity(true, false);
    checkPeerIdentity(false, false);
  }

  private void checkPeerIdentity(boolean thrift, boolean matches) throws Exception {
    try (SSLServerSocket listener = server()) {
      String host = matches ? "localhost" : listener.getInetAddress().getHostAddress();
      int port = listener.getLocalPort();
      Future<?> handshake =
          peerThread.submit(
              () -> {
                try (SSLSocket peer = (SSLSocket) listener.accept()) {
                  peer.setSoTimeout(10_000);
                  peer.startHandshake();
                } catch (IOException e) {
                  if (matches) {
                    throw e;
                  }
                }
                return null;
              });
      if (thrift) {
        // Original MPP transport, also used by the other internal TLS clients.
        try (TTransport transport =
            DeepCopyRpcTransportFactory.INSTANCE.getTransport(
                host, port, 10_000, store.toString(), PASSWORD, "", "", config.getSslProtocol())) {
          checkHandshake(transport, matches);
        }
      } else {
        try (SyncDataNodeMPPDataExchangeServiceClient client =
            clients.borrowClient(new TEndPoint(host, port), System.currentTimeMillis() + 10_000)) {
          try {
            checkHandshake(client.getInputProtocol().getTransport(), matches);
          } finally {
            client.invalidate();
          }
        }
      }
      handshake.get(10, SECONDS);
    }
  }

  private static void checkHandshake(TTransport transport, boolean matches) throws IOException {
    TSocket thriftSocket = (TSocket) ((TElasticFramedTransport) transport).getSocket();
    SSLSocket socket = (SSLSocket) thriftSocket.getSocket();
    assertEquals("HTTPS", socket.getSSLParameters().getEndpointIdentificationAlgorithm());
    if (matches) {
      socket.startHandshake();
      assertTrue(socket.getSession().getPeerCertificates().length > 0);
    } else {
      SSLHandshakeException failure =
          assertThrows(SSLHandshakeException.class, socket::startHandshake);
      assertTrue(failure.getCause() instanceof CertificateException);
    }
  }

  @Test
  public void interruptedTlsReadLeavesOtherPooledClientsUsable() throws Exception {
    try (SSLServerSocket listener = server()) {
      TEndPoint endpoint = new TEndPoint("localhost", listener.getLocalPort());
      AtomicReference<Thread> owner = new AtomicReference<>();
      Future<Boolean> interrupted =
          peerThread.submit(
              () -> {
                owner.set(Thread.currentThread());
                try (SyncDataNodeMPPDataExchangeServiceClient client =
                    clients.borrowClient(endpoint)) {
                  assertThrows(
                      TException.class,
                      () ->
                          client.fetchDeviceEntrySegment(
                              new TFetchDeviceEntrySegmentReq("q", "p", 0)));
                  return Thread.currentThread().isInterrupted();
                }
              });
      try (SSLSocket peer = (SSLSocket) listener.accept()) {
        peer.setSoTimeout(10_000);
        assertTrue(peer.getInputStream().read() >= 0);
        SyncDataNodeMPPDataExchangeServiceClient other;
        try (SyncDataNodeMPPDataExchangeServiceClient borrowed = clients.borrowClient(endpoint)) {
          other = borrowed;
        }
        try (Socket otherPeer = listener.accept()) {
          owner.get().interrupt();
          assertTrue(interrupted.get(10, SECONDS));
          assertEquals(0, clients.getPool().getNumActive());
          assertEquals(1, clients.getPool().getNumIdle());
          try (SyncDataNodeMPPDataExchangeServiceClient reused = clients.borrowClient(endpoint)) {
            assertSame(other, reused);
          }
        }
      }
    }
  }

  @Test
  public void expiredBudgetSkipsLocalStoreAccess() throws Exception {
    config.setTrustStorePath(folder.getRoot().toPath().resolve("missing.p12").toString());
    SyncDataNodeMPPDataExchangeServiceClient client =
        new SyncDataNodeMPPDataExchangeServiceClient(
            new ThriftClientProperty.Builder().build(),
            new TEndPoint("localhost", 1),
            clients,
            false);
    assertThrows(QueryTimeoutException.class, () -> client.prepare(System.currentTimeMillis() - 1));
  }

  @Test
  public void concurrentColdConnectionsShareTheFactory() throws Exception {
    Path copy = folder.getRoot().toPath().resolve("reconnect.p12");
    Files.copy(store, copy);
    config.setTrustStorePath(copy.toString());
    ExecutorService borrowers = Executors.newFixedThreadPool(8);
    CountDownLatch start = new CountDownLatch(1);
    try {
      List<Future<SSLSocketFactory>> factories = new ArrayList<>();
      for (int i = 0; i < 8; i++) {
        factories.add(
            borrowers.submit(
                () -> {
                  start.await();
                  return SyncDataNodeMPPDataExchangeServiceClient.sslSocketFactory();
                }));
      }
      start.countDown();
      SSLSocketFactory first = factories.get(0).get(5, SECONDS);
      for (Future<SSLSocketFactory> factory : factories) {
        assertSame(first, factory.get(5, SECONDS));
      }
    } finally {
      start.countDown();
      borrowers.shutdownNow();
    }
  }

  @Test
  public void sslFactoryReloadsRotatedStoresAndRetriesFailedLoads() throws Exception {
    String password = PASSWORD;
    Path store = folder.getRoot().toPath().resolve("rotating-trust.p12");
    KeyStore trust = KeyStore.getInstance("PKCS12");
    trust.load(null, null);
    try (OutputStream output = Files.newOutputStream(store)) {
      trust.store(output, password.toCharArray());
    }
    byte[] validStore = Files.readAllBytes(store);
    config.setTrustStorePath(store.toString());
    config.setTrustStorePwd(password);
    SSLSocketFactory first = SyncDataNodeMPPDataExchangeServiceClient.sslSocketFactory();
    assertSame(first, SyncDataNodeMPPDataExchangeServiceClient.sslSocketFactory());
    FileTime nextVersion =
        FileTime.fromMillis(Files.getLastModifiedTime(store).toMillis() + 10_000);
    Files.setLastModifiedTime(store, nextVersion);
    SSLSocketFactory rotated = SyncDataNodeMPPDataExchangeServiceClient.sslSocketFactory();
    assertNotSame(first, rotated);
    assertSame(rotated, SyncDataNodeMPPDataExchangeServiceClient.sslSocketFactory());

    Files.write(store, new byte[validStore.length]);
    FileTime damagedVersion = FileTime.fromMillis(nextVersion.toMillis() + 10_000);
    Files.setLastModifiedTime(store, damagedVersion);
    assertThrows(IOException.class, SyncDataNodeMPPDataExchangeServiceClient::sslSocketFactory);
    Files.write(store, validStore);
    Files.setLastModifiedTime(store, damagedVersion);
    SSLSocketFactory repaired = SyncDataNodeMPPDataExchangeServiceClient.sslSocketFactory();
    assertNotSame(rotated, repaired);
    assertSame(repaired, SyncDataNodeMPPDataExchangeServiceClient.sslSocketFactory());
  }
}
