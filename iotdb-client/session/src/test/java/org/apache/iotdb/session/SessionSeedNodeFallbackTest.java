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

package org.apache.iotdb.session;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.isession.INodeSupplier;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.pool.SessionDataSetWrapper;
import org.apache.iotdb.rpc.DeepCopyRpcTransportFactory;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.rpc.UrlUtils;
import org.apache.iotdb.service.rpc.thrift.IClientRPCService;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionResp;
import org.apache.iotdb.service.rpc.thrift.TSProtocolVersion;
import org.apache.iotdb.session.pool.SessionPool;

import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.TsBlockSerde;
import org.apache.tsfile.utils.BytesUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import java.net.ServerSocket;
import java.net.Socket;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Exercise discovery and reconnection over real Thrift connections with controlled availability.
 */
public class SessionSeedNodeFallbackTest {
  private final List<TestNode> nodes = new ArrayList<>();
  private final List<TEndPoint> attempts = new CopyOnWriteArrayList<>();
  private final List<TEndPoint> queries = new CopyOnWriteArrayList<>();
  private final List<Runnable> refreshes = new ArrayList<>();
  private final List<SessionConnection> connections = new ArrayList<>();
  private volatile List<TEndPoint> advertised;
  private ScheduledExecutorService executor;
  private NodesSupplier supplier;

  @Before
  public void setUp() throws Exception {
    for (int i = 0; i < 3; i++) {
      nodes.add(new TestNode());
    }
    advertised = endpoints(0, 1, 2);
    executor = mock(ScheduledExecutorService.class);
    doAnswer(
            invocation -> {
              refreshes.add(invocation.getArgument(0));
              return null;
            })
        .when(executor)
        .execute(any(Runnable.class));
    supplier = createSupplier(endpoints(0, 1, 2));
  }

  @After
  public void tearDown() throws Exception {
    for (SessionConnection connection : connections) {
      connection.forceCloseTransport();
    }
    if (supplier != null) {
      supplier.close();
    }
    for (TestNode node : nodes) {
      node.close();
    }
  }

  @Test
  public void testBackgroundRefreshRecoversFromStaleSnapshot() {
    nodes.get(0).online = false;
    advertised = endpoints(1, 2);
    supplier.run();
    assertEquals(advertised, supplier.get());

    // Node 1 has finished upgrading, but both cached candidates are now unavailable.
    nodes.get(0).online = true;
    nodes.get(1).online = false;
    nodes.get(2).online = false;
    advertised = endpoints(0);
    attempts.clear();
    supplier.run();
    assertEquals(endpoints(2, 0), attempts);
    assertEquals(endpoints(0), supplier.get());
    assertEquals(endpoints(0, 1, 2), supplier.getSeedNodes());
  }

  @Test
  public void testBusinessReconnectFallsBackAndRequestsRefresh() throws Exception {
    advertised = endpoints(1, 2);
    supplier.run();
    nodes.get(1).online = false;
    nodes.get(2).online = false;
    advertised = endpoints(0);
    attempts.clear();
    SessionConnection connection = newConnection();
    assertTrue(reconnect(connection));
    assertEquals(nodes.get(0).endpoint, connection.getEndPoint());
    assertEquals(3, attempts.size());
    assertEquals(nodes.get(0).endpoint, attempts.get(2));
    assertEquals(1, refreshes.size());
    refreshes.remove(0).run();
    assertEquals(endpoints(0), supplier.get());
  }

  @Test
  public void testDiscoveredNodeIsPreferredToHealthySeed() throws Exception {
    advertised = endpoints(1);
    supplier.run();
    attempts.clear();
    assertTrue(reconnect(newConnection()));
    assertEquals(endpoints(1), attempts);
    assertTrue(refreshes.isEmpty());
    for (int i = 0; i < 6; i++) {
      assertEquals(nodes.get(1).endpoint, supplier.getQueryEndPoint().get());
    }
  }

  @Test
  public void testEmptyDiscoveryKeepsPreviousSnapshot() {
    advertised = endpoints(1, 2);
    supplier.run();
    attempts.clear();
    advertised = Collections.emptyList();
    supplier.run();
    assertEquals(endpoints(1, 2), supplier.get());
    assertEquals(nodes.get(1).endpoint, supplier.getQueryEndPoint().get());
    assertEquals(nodes.get(2).endpoint, supplier.getQueryEndPoint().get());
    assertEquals(endpoints(0, 1, 2), supplier.getSeedNodes());
    assertTrue(attempts.isEmpty());
  }

  @Test
  public void testWildcardDiscoveryKeepsSeedsAndLastSnapshot() {
    advertised = Arrays.asList(new TEndPoint("0.0.0.0", 6667), new TEndPoint("::", 6667));
    supplier.run();
    assertEquals(endpoints(0, 1, 2), supplier.get());
    List<TEndPoint> selected = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      selected.add(supplier.getQueryEndPoint().get());
    }
    assertEquals(new HashSet<>(endpoints(0, 1, 2)), new HashSet<>(selected));

    advertised = endpoints(1, 2);
    supplier.run();
    advertised = Collections.singletonList(new TEndPoint("0.0.0.0", 6667));
    supplier.run();
    assertEquals(endpoints(1, 2), supplier.get());
  }

  @Test
  public void testQueryStatusFailureMovesDiscoveryToAnotherNodeNextRound() {
    assertQueryFailureMovesDiscovery(false);
  }

  @Test
  public void testQueryApplicationFailureMovesDiscoveryToAnotherNodeNextRound() {
    assertQueryFailureMovesDiscovery(true);
  }

  private void assertQueryFailureMovesDiscovery(boolean applicationError) {
    supplier.run();
    nodes.get(0).failQuery = true;
    nodes.get(0).queryApplicationError = applicationError;
    advertised = endpoints(1, 2);
    queries.clear();
    attempts.clear();
    supplier.run();
    assertEquals(endpoints(0), queries);
    assertTrue(attempts.isEmpty());
    assertEquals(endpoints(0, 1, 2), supplier.get());

    // Node 0 is still reachable but cannot query the ConfigNode. Do not pin discovery to it.
    supplier.run();
    assertEquals(endpoints(0, 1), queries);
    assertEquals(endpoints(1), attempts);
    assertEquals(endpoints(1, 2), supplier.get());

    advertised = endpoints(2);
    supplier.run();
    assertEquals(endpoints(2), supplier.get());
    assertEquals(endpoints(0, 1, 1), queries);
    assertEquals(endpoints(1), attempts);
  }

  @Test
  public void testClusterQueryStatusFailuresAreBounded() {
    assertClusterQueryFailuresAreBounded(false);
  }

  @Test
  public void testClusterQueryApplicationFailuresAreBounded() {
    assertClusterQueryFailuresAreBounded(true);
  }

  private void assertClusterQueryFailuresAreBounded(boolean applicationError) {
    nodes.forEach(
        node -> {
          node.failQuery = true;
          node.queryApplicationError = applicationError;
        });
    for (int i = 0; i < 6; i++) {
      supplier.run();
      // One new session and one query per refresh, even when every node returns an error.
      assertEquals(i + 1, attempts.size());
      assertEquals(i + 1, queries.size());
      assertEquals(nodes.get(i % 3).endpoint, queries.get(i));
      assertEquals(endpoints(0, 1, 2), supplier.get());
    }
    assertEquals(queries, attempts);

    // The first two candidates still fail. Discovery must eventually reach the recovered third.
    nodes.get(2).failQuery = false;
    advertised = endpoints(2);
    for (int i = 0; i < 3; i++) {
      supplier.run();
      assertEquals(7 + i, attempts.size());
      assertEquals(7 + i, queries.size());
    }
    assertEquals(endpoints(2), supplier.get());
    supplier.run();
    assertEquals(9, attempts.size());
    assertEquals(10, queries.size());
    assertEquals(nodes.get(2).endpoint, queries.get(9));
  }

  @Test
  public void testDiscoveryQueryFailuresEventuallyReachRecoverySeed() {
    advertised = endpoints(0, 1);
    supplier.run();
    nodes.get(0).failQuery = true;
    nodes.get(1).failQuery = true;
    advertised = endpoints(2);
    queries.clear();
    attempts.clear();
    supplier.run();
    assertEquals(endpoints(0), queries);
    assertTrue(attempts.isEmpty());
    assertEquals(endpoints(0, 1), supplier.get());

    supplier.run();
    assertEquals(endpoints(0, 1), queries);
    assertEquals(endpoints(1), attempts);
    assertEquals(endpoints(0, 1), supplier.get());

    supplier.run();
    assertEquals(endpoints(0, 1, 2), queries);
    assertEquals(endpoints(1, 2), attempts);
    assertEquals(endpoints(2), supplier.get());
    assertEquals(endpoints(0, 1, 2), supplier.getSeedNodes());
  }

  @Test
  public void testAllUnavailableRetriesAreBoundedAndDeduplicated() throws Exception {
    supplier.close();
    supplier = createSupplier(endpoints(0, 0, 1, 2));
    advertised = endpoints(1);
    supplier.run();
    nodes.forEach(node -> node.online = false);
    attempts.clear();
    assertFalse(reconnect(newConnection()));
    assertEquals(SessionConfig.RETRY_NUM * 3, attempts.size());
    for (int i = 0; i < SessionConfig.RETRY_NUM; i++) {
      List<TEndPoint> round = attempts.subList(i * 3, (i + 1) * 3);
      assertEquals(nodes.get(1).endpoint, round.get(0));
      assertEquals(new HashSet<>(endpoints(0, 1, 2)), new HashSet<>(round));
    }
    assertTrue(refreshes.isEmpty());
    assertEquals(endpoints(0, 1, 2), supplier.getSeedNodes());
  }

  @Test
  public void testSeedSnapshotIsIndependentOfCallerAndDiscovery() {
    supplier.close();
    List<TEndPoint> seeds = endpoints(0, 1);
    supplier = createSupplier(seeds);
    seeds.get(0).setPort(1);
    seeds.clear();
    advertised = endpoints(2);
    supplier.run();
    assertEquals(endpoints(0, 1), supplier.getSeedNodes());
    assertEquals(endpoints(2), supplier.get());
  }

  @Test
  public void testBackgroundRefreshDoesNotContactSeedsWhileCurrentNodeWorks() {
    advertised = endpoints(1);
    supplier.run();
    attempts.clear();
    supplier.run();
    assertTrue(attempts.isEmpty());
    assertEquals(endpoints(1), supplier.get());
  }

  @Test
  public void testNodeUrlsSessionReopenAfterFailover() throws Exception {
    assertSessionReopenAfterFailover(false);
  }

  @Test
  public void testSingleHostSessionReopenAfterFailover() throws Exception {
    assertSessionReopenAfterFailover(true);
  }

  @Test
  public void testStaticNodeUrlsSurviveReopenAndSecondFailover() throws Exception {
    nodes.get(1).online = false;
    try (Session session =
        new Session.Builder()
            .nodeUrls(Arrays.asList(url(0), url(1), url(0)))
            .enableAutoFetch(false)
            .build()) {
      session.open(false, 1000);
      assertEquals(nodes.get(0).endpoint, session.defaultEndPoint);
      nodes.get(0).online = false;
      nodes.get(1).online = true;
      assertTrue(reconnect(session.defaultSessionConnection));
      assertEquals(nodes.get(1).endpoint, session.defaultEndPoint);
      session.close();

      session.open(false, 1000);
      INodeSupplier reopened = Whitebox.getInternalState(session, "availableNodes");
      assertEquals(endpoints(1, 0), reopened.get());
      nodes.get(1).online = false;
      nodes.get(0).online = true;
      assertTrue(reconnect(session.defaultSessionConnection));
      assertEquals(nodes.get(0).endpoint, session.defaultEndPoint);
      advertised = endpoints(0);
      try (SessionDataSet result = session.executeQueryStatement("SHOW AVAILABLE URLS")) {
        assertTrue(result.hasNext());
        result.next();
        assertFalse(result.hasNext());
      }
    }
  }

  private void assertSessionReopenAfterFailover(boolean singleHost) throws Exception {
    List<String> urls = new ArrayList<>(Collections.singletonList(url(0)));
    Session session =
        singleHost
            ? new Session.Builder()
                .host(nodes.get(0).endpoint.getIp())
                .port(nodes.get(0).endpoint.getPort())
                .build()
            : new Session.Builder().nodeUrls(urls).build();
    urls.clear();
    advertised = endpoints(0, 1);
    try {
      session.open(false, 1000);
      NodesSupplier first = Whitebox.getInternalState(session, "availableNodes");
      awaitSnapshot(first, endpoints(0, 1));
      nodes.get(0).online = false;
      advertised = endpoints(1);
      awaitSnapshot(first, endpoints(1));
      SessionConnection connection = Whitebox.getInternalState(session, "defaultSessionConnection");
      assertTrue(reconnect(connection));
      assertEquals(nodes.get(1).endpoint, connection.getEndPoint());
      session.close();

      attempts.clear();
      session.open(false, 1000);
      NodesSupplier reopened = Whitebox.getInternalState(session, "availableNodes");
      assertEquals(endpoints(0), reopened.getSeedNodes());
      assertEquals(endpoints(1), reopened.get());
      awaitSnapshot(reopened, endpoints(1));
      for (int i = 0; i < 10; i++) {
        try (SessionDataSet result = session.executeQueryStatement("SHOW AVAILABLE URLS")) {
          assertTrue(result.hasNext());
          result.next();
          assertFalse(result.hasNext());
        }
      }
      assertFalse(attempts.contains(nodes.get(0).endpoint));
    } finally {
      session.close();
    }
  }

  private void awaitSnapshot(NodesSupplier current, List<TEndPoint> expected) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (!expected.equals(current.get()) && System.nanoTime() < deadline) {
      current.run();
      TimeUnit.MILLISECONDS.sleep(10);
    }
    assertEquals(expected, current.get());
  }

  @Test
  public void testPoolListConstructorCopiesUrlsForFutureSessions() throws Exception {
    List<String> urls = new ArrayList<>(Collections.singletonList(url(0)));
    SessionPool pool = new SessionPool(urls, "root", "root", 1);
    try {
      urls.clear();
      try (SessionDataSetWrapper result = pool.executeQueryStatement("SHOW AVAILABLE URLS")) {
        assertTrue(result.hasNext());
      }
      NodesSupplier current = Whitebox.getInternalState(pool, "availableNodes");
      assertEquals(endpoints(0), current.getSeedNodes());
    } finally {
      pool.close();
    }
  }

  @Test
  public void testCloseAbortsBlockedSessionOpenAndDoesNotTryRemainingCandidates() throws Exception {
    supplier.close();
    ExecutorService tasks = Executors.newCachedThreadPool();
    Socket peer = null;
    try (ServerSocket listener = new ServerSocket(0)) {
      List<TEndPoint> seeds = new ArrayList<>();
      seeds.add(new TEndPoint("127.0.0.1", listener.getLocalPort()));
      seeds.addAll(endpoints(1, 2));
      supplier = createSupplier(seeds, 0);
      Future<Socket> accepted = tasks.submit(listener::accept);
      Future<?> refresh = tasks.submit(supplier::run);
      peer = accepted.get(5, TimeUnit.SECONDS);
      peer.setSoTimeout(5000);
      assertTrue(peer.getInputStream().read() >= 0);
      // Timeout 0 means the peer can stall openSession forever unless close aborts the socket.
      tasks.submit(supplier::close).get(5, TimeUnit.SECONDS);
      refresh.get(5, TimeUnit.SECONDS);
      assertTrue(attempts.isEmpty());
      assertNull(Whitebox.getInternalState(supplier, "client"));
      assertNull(Whitebox.getInternalState(supplier, "activeClient"));
      supplier.run();
      assertTrue(attempts.isEmpty());
      while (peer.getInputStream().read() != -1) {
        // Drain the already-written request before observing socket closure.
      }
    } finally {
      if (peer != null) {
        peer.close();
      }
      tasks.shutdownNow();
      assertTrue(tasks.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  public void testCloseAbortsBlockedDiscoveryQuery() throws Exception {
    advertised = endpoints(0);
    supplier.close();
    supplier = createSupplier(endpoints(0, 1, 2), 0);
    supplier.run();
    TestNode node = nodes.get(0);
    node.queryEntered = new CountDownLatch(1);
    node.queryRelease = new CountDownLatch(1);
    ExecutorService tasks = Executors.newCachedThreadPool();
    attempts.clear();
    try {
      Future<?> refresh = tasks.submit(supplier::run);
      assertTrue(node.queryEntered.await(5, TimeUnit.SECONDS));
      tasks.submit(supplier::close).get(5, TimeUnit.SECONDS);
      refresh.get(5, TimeUnit.SECONDS);
      assertTrue(attempts.isEmpty());
      assertNull(Whitebox.getInternalState(supplier, "client"));
      assertNull(Whitebox.getInternalState(supplier, "activeClient"));
    } finally {
      node.queryRelease.countDown();
      tasks.shutdownNow();
      assertTrue(tasks.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  public void testQuerySelectionDuringEmptyDiscoveryRefresh() throws Exception {
    advertised = endpoints(0);
    supplier.run();
    ExecutorService reader = Executors.newSingleThreadExecutor();
    AtomicBoolean finished = new AtomicBoolean();
    CountDownLatch started = new CountDownLatch(1);
    Future<?> selection =
        reader.submit(
            () -> {
              started.countDown();
              while (!finished.get()) {
                supplier
                    .getQueryEndPoint()
                    .ifPresent(endpoint -> assertEquals(nodes.get(0).endpoint, endpoint));
                assertTrue(supplier.getQueryEndPoint().isPresent());
              }
            });
    try {
      assertTrue(started.await(5, TimeUnit.SECONDS));
      for (int i = 0; i < 100; i++) {
        advertised = i % 2 == 0 ? Collections.emptyList() : endpoints(0);
        supplier.run();
      }
    } finally {
      finished.set(true);
      reader.shutdownNow();
      assertTrue(reader.awaitTermination(5, TimeUnit.SECONDS));
    }
    selection.get(5, TimeUnit.SECONDS);
  }

  @Test
  public void testRefreshRequestsAreCoalescedAndIgnoredAfterClose() {
    supplier.requestRefresh();
    supplier.requestRefresh();
    assertEquals(1, refreshes.size());
    refreshes.remove(0).run();
    supplier.requestRefresh();
    assertEquals(1, refreshes.size());
    supplier.close();
    refreshes.remove(0).run();
    supplier.requestRefresh();
    assertTrue(refreshes.isEmpty());
  }

  private NodesSupplier createSupplier(List<TEndPoint> seeds) {
    return createSupplier(seeds, 1000);
  }

  private NodesSupplier createSupplier(List<TEndPoint> seeds, int timeout) {
    return NodesSupplier.createNodeSupplier(
        seeds,
        executor,
        "root",
        "root",
        ZoneId.of("UTC"),
        SessionConfig.DEFAULT_INITIAL_BUFFER_CAPACITY,
        SessionConfig.DEFAULT_MAX_FRAME_SIZE,
        timeout,
        false,
        null,
        null,
        null,
        null,
        "TLS",
        false,
        "V_1_0");
  }

  private SessionConnection newConnection() {
    SessionConnection connection = new SessionConnection("tree");
    Session session = new Session.Builder().nodeUrls(Collections.singletonList(url(0))).build();
    session.connectionTimeoutInMs = 1000;
    Whitebox.setInternalState(connection, "session", session);
    Whitebox.setInternalState(connection, "zoneId", ZoneId.of("UTC"));
    Whitebox.setInternalState(connection, "transport", mock(TTransport.class));
    Whitebox.setInternalState(connection, "availableNodes", supplier);
    connections.add(connection);
    return connection;
  }

  private boolean reconnect(SessionConnection connection) throws Exception {
    return Whitebox.invokeMethod(connection, "reconnect");
  }

  private List<TEndPoint> endpoints(int... indices) {
    List<TEndPoint> result = new ArrayList<>();
    for (int index : indices) {
      result.add(nodes.get(index).endpoint.deepCopy());
    }
    return result;
  }

  private String url(int index) {
    return UrlUtils.convertTEndPointIpv4AndIpv6Url(nodes.get(index).endpoint);
  }

  private TSStatus success() {
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  private class TestNode implements AutoCloseable {
    private final TEndPoint endpoint;
    private final TServerSocket socket;
    private final TThreadPoolServer server;
    private final ExecutorService workers = Executors.newCachedThreadPool();
    private final Thread thread;
    private volatile boolean online = true;
    private volatile boolean failQuery;
    private volatile boolean queryApplicationError;
    private volatile CountDownLatch queryEntered;
    private volatile CountDownLatch queryRelease;
    private final ThreadLocal<TTransport> activeTransport = new ThreadLocal<>();

    private TestNode() throws Exception {
      ServerSocket listener = new ServerSocket(0);
      endpoint = new TEndPoint("127.0.0.1", listener.getLocalPort());
      socket = new TServerSocket(listener);
      IClientRPCService.Iface handler = mock(IClientRPCService.Iface.class);
      when(handler.openSession(any()))
          .thenAnswer(
              invocation -> {
                attempts.add(endpoint);
                checkOnline();
                return new TSOpenSessionResp(success(), TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3)
                    .setSessionId(1);
              });
      when(handler.requestStatementId(anyLong())).thenReturn(1L);
      when(handler.closeSession(any())).thenReturn(success());
      when(handler.closeOperation(any())).thenReturn(success());
      when(handler.executeQueryStatementV2(any()))
          .thenAnswer(
              invocation -> {
                checkOnline();
                queries.add(endpoint);
                if (queryEntered != null) {
                  queryEntered.countDown();
                  queryRelease.await();
                }
                if (failQuery) {
                  if (queryApplicationError) {
                    throw new TApplicationException(
                        TApplicationException.INTERNAL_ERROR, "Discovery query failed");
                  }
                  return new TSExecuteStatementResp(
                      new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
                          .setMessage("Discovery query failed"));
                }
                TsBlockBuilder block =
                    new TsBlockBuilder(Arrays.asList(TSDataType.TEXT, TSDataType.INT32));
                for (TEndPoint address : advertised) {
                  block.getTimeColumnBuilder().writeLong(0);
                  block.getColumnBuilder(0).writeBinary(BytesUtils.valueOf(address.getIp()));
                  block.getColumnBuilder(1).writeInt(address.getPort());
                  block.declarePosition();
                }
                return new TSExecuteStatementResp(success())
                    .setQueryId(1)
                    .setColumns(Arrays.asList("RpcAddress", "RpcPort"))
                    .setDataTypeList(Arrays.asList("TEXT", "INT32"))
                    .setQueryResult(
                        advertised.isEmpty()
                            ? Collections.emptyList()
                            : Collections.singletonList(
                                new TsBlockSerde().serialize(block.build())))
                    .setIgnoreTimeStamp(true)
                    .setMoreData(false);
              });
      server =
          new TThreadPoolServer(
              new TThreadPoolServer.Args(socket)
                  .processor(new IClientRPCService.Processor<>(handler))
                  .protocolFactory(new TBinaryProtocol.Factory())
                  .transportFactory(DeepCopyRpcTransportFactory.INSTANCE)
                  .executorService(workers));
      server.setServerEventHandler(
          new TServerEventHandler() {
            @Override
            public void preServe() {}

            @Override
            public ServerContext createContext(TProtocol input, TProtocol output) {
              return mock(ServerContext.class);
            }

            @Override
            public void processContext(ServerContext context, TTransport input, TTransport output) {
              activeTransport.set(input);
            }

            @Override
            public void deleteContext(ServerContext context, TProtocol input, TProtocol output) {
              activeTransport.remove();
            }
          });
      CountDownLatch ready = new CountDownLatch(1);
      thread =
          new Thread(
              () -> {
                ready.countDown();
                server.serve();
              },
              "seed-fallback-test-server");
      thread.setDaemon(true);
      thread.start();
      assertTrue(ready.await(5, TimeUnit.SECONDS));
    }

    private void checkOnline() throws TException {
      if (!online) {
        // A stopped node breaks the transport; a reachable peer returning an RPC error is
        // different.
        TTransport current = activeTransport.get();
        if (current != null) {
          current.close();
        }
        throw new TException("Node is restarting");
      }
    }

    @Override
    public void close() throws Exception {
      server.stop();
      socket.close();
      workers.shutdownNow();
      thread.join(5000);
      assertTrue(workers.awaitTermination(5, TimeUnit.SECONDS));
    }
  }
}
