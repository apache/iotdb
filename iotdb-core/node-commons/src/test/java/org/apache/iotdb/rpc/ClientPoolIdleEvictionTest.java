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

package org.apache.iotdb.rpc;

import org.apache.iotdb.commons.client.property.ClientPoolProperty;

import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class ClientPoolIdleEvictionTest {

  private static final int INITIAL_BUFFER_SIZE = 32;
  private static final byte[] PAYLOAD = new byte[4096];

  private final boolean compression;

  public ClientPoolIdleEvictionTest(boolean compression) {
    this.compression = compression;
  }

  @Parameterized.Parameters(name = "compression={0}")
  public static Iterable<Object[]> parameters() {
    return Arrays.asList(new Object[][] {{false}, {true}});
  }

  @Before
  public void setUp() {
    Arrays.fill(PAYLOAD, (byte) 13);
  }

  @Test
  public void testOneRunEvictsAllExpiredClientsAcrossNodes() throws Exception {
    try (GenericKeyedObjectPool<Integer, TElasticFramedTransport> pool =
        createPool(100, -1, null)) {
      TElasticFramedTransport active = pool.borrowObject(0);
      try {
        List<TElasticFramedTransport> idleClients = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
          idleClients.add(pool.borrowObject(i % 2));
        }
        for (int i = 0; i < idleClients.size(); i++) {
          pool.returnObject(i % 2, idleClients.get(i));
        }

        assertEquals(10, pool.getNumIdle());

        // Wait for the idle timeout with the background evictor disabled, then run one sweep.
        waitForIdleTimeout();
        pool.evict();

        assertEquals(0, pool.getNumIdle());
        assertEquals(10, pool.getDestroyedCount());
        assertEquals(1, pool.getNumActive());

        // Eviction must preserve a borrowed client's frame and allow a new client to connect.
        byte[] actual = new byte[PAYLOAD.length];
        active.readAll(actual, 0, actual.length);
        assertArrayEquals(PAYLOAD, actual);
        TElasticFramedTransport replacement = pool.borrowObject(2);
        pool.returnObject(2, replacement);
      } finally {
        pool.returnObject(0, active);
      }
    }
  }

  @Test
  public void testBackgroundEvictionClosesIdleClients() throws Exception {
    try (GenericKeyedObjectPool<Integer, TElasticFramedTransport> pool =
        createPool(100, 20, null)) {
      TElasticFramedTransport client = pool.borrowObject(0);
      pool.returnObject(0, client);

      await()
          .atMost(5, TimeUnit.SECONDS)
          .pollInterval(Duration.ofMillis(10))
          .until(() -> pool.getNumIdle() == 0 && pool.getDestroyedCount() == 1);
      assertEquals(1, pool.getDestroyedCount());
    }
  }

  @Test
  public void testUnexpiredClientRemainsReusable() throws Exception {
    try (GenericKeyedObjectPool<Integer, TElasticFramedTransport> pool =
        createPool(TimeUnit.MINUTES.toMillis(1), -1, null)) {
      TElasticFramedTransport client = pool.borrowObject(0);
      pool.returnObject(0, client);

      pool.evict();
      assertEquals(1, pool.getNumIdle());
      assertEquals(0, pool.getDestroyedCount());

      TElasticFramedTransport reused = pool.borrowObject(0);
      try {
        assertSame(client, reused);
        byte[] actual = new byte[PAYLOAD.length];
        reused.readAll(actual, 0, actual.length);
        assertArrayEquals(PAYLOAD, actual);
      } finally {
        pool.returnObject(0, reused);
      }
    }
  }

  @Test(timeout = 20000)
  public void testEvictionClosesServerConnections() throws Exception {
    try (ServerSocket socket = new ServerSocket()) {
      socket.bind(new InetSocketAddress("127.0.0.1", 0));
      ExecutorService workers = Executors.newFixedThreadPool(2);
      List<TTransport> serverConnections = new CopyOnWriteArrayList<>();
      TServer server =
          new TThreadPoolServer(
              new TThreadPoolServer.Args(new TServerSocket(socket))
                  .executorService(workers)
                  .transportFactory(
                      new TTransportFactory() {
                        @Override
                        public TTransport getTransport(TTransport transport) {
                          serverConnections.add(transport);
                          return createTransport(transport);
                        }
                      })
                  .protocolFactory(new TBinaryProtocol.Factory())
                  .processor(
                      (input, output) -> {
                        output.writeBinary(input.readBinary());
                        output.getTransport().flush();
                      }));
      Thread serverThread = new Thread(server::serve, "client-pool-idle-eviction-test-server");
      serverThread.setDaemon(true);
      serverThread.start();

      try (GenericKeyedObjectPool<Integer, TElasticFramedTransport> pool =
          createPool(100, -1, socket.getLocalPort())) {
        TElasticFramedTransport first = pool.borrowObject(0);
        try {
          echo(first, PAYLOAD);
        } finally {
          pool.returnObject(0, first);
        }
        assertFalse(serverConnections.isEmpty());

        waitForIdleTimeout();
        pool.evict();
        assertFalse(first.isOpen());
        // The server must observe EOF and close its side of the connection as well.
        await()
            .atMost(5, TimeUnit.SECONDS)
            .pollInterval(Duration.ofMillis(10))
            .until(() -> serverConnections.stream().noneMatch(TTransport::isOpen));

        TElasticFramedTransport replacement = pool.borrowObject(1);
        try {
          echo(replacement, new byte[] {1, 2, 3, 4});
        } finally {
          pool.returnObject(1, replacement);
        }
      } finally {
        server.stop();
        workers.shutdownNow();
        assertTrue(workers.awaitTermination(5, TimeUnit.SECONDS));
        serverThread.join(5000);
        assertFalse(serverThread.isAlive());
      }
    }
  }

  private static void echo(TElasticFramedTransport transport, byte[] payload) throws Exception {
    TProtocol protocol = new TBinaryProtocol(transport);
    protocol.writeBinary(ByteBuffer.wrap(payload));
    transport.flush();
    assertEquals(ByteBuffer.wrap(payload), protocol.readBinary());
  }

  private static void waitForIdleTimeout() {
    long returnedAt = System.nanoTime();
    await()
        .atMost(5, TimeUnit.SECONDS)
        .pollInterval(Duration.ofMillis(10))
        .until(() -> System.nanoTime() - returnedAt > TimeUnit.MILLISECONDS.toNanos(200));
  }

  private GenericKeyedObjectPool<Integer, TElasticFramedTransport> createPool(
      long idleTimeoutMs, long evictionIntervalMs, Integer serverPort) {
    return new GenericKeyedObjectPool<>(
        new BaseKeyedPooledObjectFactory<Integer, TElasticFramedTransport>() {
          @Override
          public TElasticFramedTransport create(Integer node) throws Exception {
            TElasticFramedTransport transport =
                createTransport(
                    serverPort == null
                        ? new TMemoryBuffer(0)
                        : new TSocket("127.0.0.1", serverPort, 5000));
            try {
              if (serverPort == null) {
                transport.write(PAYLOAD, 0, PAYLOAD.length);
                transport.flush();
              } else {
                transport.open();
              }
              return transport;
            } catch (Exception e) {
              transport.close();
              throw e;
            }
          }

          @Override
          public PooledObject<TElasticFramedTransport> wrap(TElasticFramedTransport transport) {
            return new DefaultPooledObject<>(transport);
          }

          @Override
          public void destroyObject(Integer node, PooledObject<TElasticFramedTransport> transport) {
            transport.getObject().close();
          }
        },
        new ClientPoolProperty.Builder<TElasticFramedTransport>()
            .setMaxClientNumForEachNode(16)
            .setMaxIdleClientNumForEachNode(16)
            .setMinIdleTimeForClient(idleTimeoutMs)
            .setTimeBetweenEvictionRuns(evictionIntervalMs)
            .build()
            .getConfig());
  }

  private TElasticFramedTransport createTransport(TTransport underlying) {
    return compression
        ? new TSnappyElasticFramedTransport(underlying, INITIAL_BUFFER_SIZE, 65536, true)
        : new TElasticFramedTransport(underlying, INITIAL_BUFFER_SIZE, 65536, true);
  }
}
