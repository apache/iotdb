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

package org.apache.iotdb.commons.client;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.ConfigRegionId;

import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public class AbstractConfigNodeClientConnectionTest {

  private static final int CLIENT_COUNT = 100;
  private static final int RECONNECT_ROUNDS = 3;
  private static final int TIMEOUT_SECONDS = 30;

  @Test(timeout = 60_000)
  public void testCloseReturnsConnectionToPoolUntilManagerCloses() throws Exception {
    CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();
    boolean internalSslEnabled = commonConfig.isEnableInternalSSL();
    commonConfig.setEnableInternalSSL(false);
    try (SocketAcceptor server = new SocketAcceptor()) {
      AbstractConfigNodeInfo nodeInfo = mock(AbstractConfigNodeInfo.class);
      List<TEndPoint> nodes = Collections.singletonList(server.endpoint());
      when(nodeInfo.getLatestConfigNodes()).thenReturn(nodes);
      ClientManager<ConfigRegionId, TestConfigNodeClient> manager = newManager(nodes, nodeInfo);
      TestConfigNodeClient client = null;
      try {
        client = manager.borrowClient(AbstractConfigNodeInfo.CONFIG_REGION_ID);
        TTransport transport = client.getTransport();
        List<Socket> sockets = server.awaitConnections(1);
        assertTrue(transport.isOpen());
        assertEquals(1, manager.getPool().getNumActive());

        client.close();
        assertTrue(transport.isOpen());
        assertEquals(0, manager.getPool().getNumActive());
        assertEquals(1, manager.getPool().getNumIdle());

        try (TestConfigNodeClient borrowed =
            manager.borrowClient(AbstractConfigNodeInfo.CONFIG_REGION_ID)) {
          assertSame(client, borrowed);
          assertSame(transport, borrowed.getTransport());
          assertEquals(1, manager.getPool().getCreatedCount());
          assertEquals(1, manager.getPool().getNumActive());
          assertEquals(0, manager.getPool().getNumIdle());
        }
        manager.close();
        assertFalse(transport.isOpen());
        assertEquals(0, manager.getPool().getNumActive());
        assertEquals(0, manager.getPool().getNumIdle());
        assertPeerConnectionsClosed(sockets);
      } finally {
        manager.close();
        if (client != null) {
          client.invalidate();
        }
      }
    } finally {
      commonConfig.setEnableInternalSSL(internalSslEnabled);
    }
  }

  /** Exercises 100 clients with real TCP transports, not 100 complete DataNode processes. */
  @Test(timeout = 120_000)
  public void testConcurrentReconnectReleasesPreviousConnections() throws Exception {
    CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();
    boolean internalSslEnabled = commonConfig.isEnableInternalSSL();
    commonConfig.setEnableInternalSSL(false);
    ExecutorService workers = Executors.newFixedThreadPool(CLIENT_COUNT);
    List<TestConfigNodeClient> clients = new ArrayList<>();
    List<TTransport> allTransports = new ArrayList<>();
    try (SocketAcceptor first = new SocketAcceptor();
        SocketAcceptor second = new SocketAcceptor()) {
      AtomicReference<List<TEndPoint>> latestNodes =
          new AtomicReference<>(Collections.singletonList(first.endpoint()));
      AbstractConfigNodeInfo nodeInfo = mock(AbstractConfigNodeInfo.class);
      when(nodeInfo.getLatestConfigNodes())
          .thenAnswer(invocation -> new ArrayList<>(latestNodes.get()));

      for (int i = 0; i < CLIENT_COUNT; i++) {
        TestConfigNodeClient client =
            mock(
                TestConfigNodeClient.class,
                withSettings()
                    .useConstructor(latestNodes.get(), nodeInfo)
                    .defaultAnswer(CALLS_REAL_METHODS));
        clients.add(client);
        allTransports.add(client.getTransport());
      }
      List<Socket> previousSockets = first.awaitConnections(CLIENT_COUNT);
      assertOpenTransportCount(allTransports, CLIENT_COUNT);

      for (int round = 0; round < RECONNECT_ROUNDS; round++) {
        SocketAcceptor target = round % 2 == 0 ? second : first;
        latestNodes.set(Collections.singletonList(target.endpoint()));
        CountDownLatch ready = new CountDownLatch(CLIENT_COUNT);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> reconnects = new ArrayList<>();
        try {
          for (TestConfigNodeClient client : clients) {
            reconnects.add(
                workers.submit(
                    () -> {
                      ready.countDown();
                      assertTrue(start.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
                      TTransport previousTransport = client.getTransport();
                      client.configLeader = null;
                      // Use real fallback selection and real TCP connection replacement.
                      client.connectAndSync();
                      assertEquals(target.endpoint(), client.configNode);
                      assertNotSame(previousTransport, client.getTransport());
                      assertFalse(previousTransport.isOpen());
                      assertTrue(client.getTransport().isOpen());
                      return null;
                    }));
          }
          assertTrue(ready.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        } finally {
          start.countDown();
        }
        for (Future<?> reconnect : reconnects) {
          reconnect.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
        for (TestConfigNodeClient client : clients) {
          allTransports.add(client.getTransport());
        }
        assertOpenTransportCount(allTransports, CLIENT_COUNT);
        assertPeerConnectionsClosed(previousSockets);
        previousSockets = target.awaitConnections(CLIENT_COUNT);
      }

      clients.forEach(TestConfigNodeClient::invalidate);
      assertOpenTransportCount(allTransports, 0);
      assertPeerConnectionsClosed(previousSockets);
      assertEquals(CLIENT_COUNT * (RECONNECT_ROUNDS + 1), allTransports.size());
    } finally {
      workers.shutdownNow();
      try {
        assertTrue(workers.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS));
      } finally {
        clients.forEach(TestConfigNodeClient::invalidate);
        commonConfig.setEnableInternalSSL(internalSslEnabled);
      }
    }
  }

  private static void assertOpenTransportCount(List<TTransport> transports, int expected) {
    assertEquals(expected, transports.stream().filter(TTransport::isOpen).count());
  }

  private static ClientManager<ConfigRegionId, TestConfigNodeClient> newManager(
      List<TEndPoint> nodes, AbstractConfigNodeInfo nodeInfo) {
    return new ClientManager<>(
        manager ->
            new GenericKeyedObjectPool<>(
                new BaseKeyedPooledObjectFactory<ConfigRegionId, TestConfigNodeClient>() {
                  @Override
                  public TestConfigNodeClient create(ConfigRegionId key) {
                    return mock(
                        TestConfigNodeClient.class,
                        withSettings()
                            .useConstructor(nodes, nodeInfo, manager)
                            .defaultAnswer(CALLS_REAL_METHODS));
                  }

                  @Override
                  public PooledObject<TestConfigNodeClient> wrap(TestConfigNodeClient client) {
                    return new DefaultPooledObject<>(client);
                  }

                  @Override
                  public void destroyObject(
                      ConfigRegionId key, PooledObject<TestConfigNodeClient> client) {
                    client.getObject().invalidate();
                  }
                }));
  }

  private static void assertPeerConnectionsClosed(List<Socket> sockets) throws IOException {
    for (Socket socket : sockets) {
      // connect() sends no RPC payload: EOF proves the client closed the actual TCP connection.
      socket.setSoTimeout((int) TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
      assertEquals(-1, socket.getInputStream().read());
      socket.close();
    }
  }

  private static final class SocketAcceptor implements AutoCloseable {

    private final ServerSocket server;
    private final ExecutorService acceptor = Executors.newSingleThreadExecutor();
    private final BlockingQueue<Socket> connections = new LinkedBlockingQueue<>();
    private final List<Socket> allConnections = Collections.synchronizedList(new ArrayList<>());
    private final Future<?> accepting;

    private SocketAcceptor() throws IOException {
      server = new ServerSocket(0, CLIENT_COUNT * 2, InetAddress.getByName("127.0.0.1"));
      accepting =
          acceptor.submit(
              () -> {
                while (!server.isClosed()) {
                  try {
                    Socket socket = server.accept();
                    allConnections.add(socket);
                    connections.add(socket);
                  } catch (SocketException e) {
                    if (!server.isClosed()) {
                      throw e;
                    }
                  }
                }
                return null;
              });
    }

    private TEndPoint endpoint() {
      return new TEndPoint("127.0.0.1", server.getLocalPort());
    }

    private List<Socket> awaitConnections(int count) throws InterruptedException {
      List<Socket> result = new ArrayList<>();
      for (int i = 0; i < count; i++) {
        Socket socket = connections.poll(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertNotNull(socket);
        result.add(socket);
      }
      return result;
    }

    @Override
    public void close() throws Exception {
      try {
        server.close();
        accepting.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      } finally {
        acceptor.shutdownNow();
        try {
          assertTrue(acceptor.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        } finally {
          for (Socket socket : allConnections) {
            socket.close();
          }
        }
      }
    }
  }

  abstract static class TestConfigNodeClient
      extends AbstractConfigNodeClient<TestConfigNodeClient> {

    private final AbstractConfigNodeInfo nodeInfo;

    TestConfigNodeClient(List<TEndPoint> nodes, AbstractConfigNodeInfo nodeInfo) throws TException {
      this(nodes, nodeInfo, null);
    }

    TestConfigNodeClient(
        List<TEndPoint> nodes,
        AbstractConfigNodeInfo nodeInfo,
        ClientManager<ConfigRegionId, TestConfigNodeClient> manager)
        throws TException {
      super(
          nodes,
          new ThriftClientProperty.Builder()
              .setConnectionTimeoutMs((int) TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS))
              .build(),
          manager);
      this.nodeInfo = nodeInfo;
    }

    @Override
    protected String getNodeTypeName() {
      return "DataNode";
    }

    @Override
    protected TEndPoint getAddressAndPort() {
      return new TEndPoint("127.0.0.1", 0);
    }

    @Override
    protected AbstractConfigNodeInfo getConfigNodeInfo() {
      return nodeInfo;
    }
  }
}
