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

package org.apache.iotdb.session.it;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.pool.SessionDataSetWrapper;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.EnvUtils;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.UrlUtils;
import org.apache.iotdb.session.NodesSupplier;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.pool.SessionPool;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(IoTDBTestRunner.class)
@Category(ClusterIT.class)
public class SessionSeedNodeFallbackIT {
  private static final int RECOVERY_CONNECTION_TIMEOUT_MS = 30_000;

  @Before
  public void setUp() {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setSchemaReplicationFactor(3)
        .setDataReplicationFactor(3)
        .setDataRegionGroupExtensionPolicy("CUSTOM")
        .setDefaultDataRegionGroupNumPerDatabase(1)
        .setTimePartitionInterval(1000)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS);
    EnvFactory.getEnv().initClusterEnvironment(1, 3);
  }

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testOriginalWriteClientsRecoverCachedRoutesAfterLeaderCrash() throws Exception {
    String[] devices = {"root.route_first.d", "root.route_second.d"};
    Map<String, Map<Long, String>> expected = new HashMap<>();
    for (String device : devices) {
      expected.put(device, new TreeMap<>());
    }
    // Create Regions before choosing the business seed so its address cannot accidentally be
    // the leader whose cached write connection we need to break.
    try (Session setup =
        new Session.Builder()
            .host(endpoint(2).getIp())
            .port(endpoint(2).getPort())
            .maxRetryCount(0)
            .build()) {
      setup.open(false, RECOVERY_CONNECTION_TIMEOUT_MS);
      for (String device : devices) {
        setup.executeNonQueryStatement(
            "CREATE DATABASE " + device.substring(0, device.lastIndexOf('.')));
        setup.executeNonQueryStatement("CREATE TIMESERIES " + device + ".n INT64");
        setup.executeNonQueryStatement("CREATE TIMESERIES " + device + ".t TEXT");
        setup.executeNonQueryStatement(
            "INSERT INTO "
                + device
                + "(time,n,t) VALUES(0,"
                + routeNumber(device, 0, devices)
                + ",'"
                + device
                + "-0')");
        expected.get(device).put(0L, device + "-0");
      }
    }
    Map<String, TEndPoint> initialLeaders = awaitWriteLeaders(devices, null, 2);
    int seedIndex = initialLeaders.get(devices[0]).equals(endpoint(0)) ? 1 : 0;
    TEndPoint seed = endpoint(seedIndex);
    // The seed stays alive; the test specifically crashes a cached write leader.
    SessionPool pool =
        new SessionPool.Builder()
            .host(seed.getIp())
            .port(seed.getPort())
            .maxSize(1)
            .connectionTimeoutInMs(RECOVERY_CONNECTION_TIMEOUT_MS)
            .waitToGetSessionTimeoutInMs(RECOVERY_CONNECTION_TIMEOUT_MS)
            .maxRetryCount(0)
            .build();
    try (Session session =
        new Session.Builder().host(seed.getIp()).port(seed.getPort()).maxRetryCount(0).build()) {
      session.open(false, RECOVERY_CONNECTION_TIMEOUT_MS);
      writeRoutePhase(session, pool, devices, 0, expected);
      // Repeat identical keys to ensure both clients have received the routing recommendations.
      writeRoutePhase(session, pool, devices, 0, expected);
      Map<String, TEndPoint> leaders = awaitWriteLeaders(devices, null, seedIndex);
      Map<String, TEndPoint> sessionRoutes = writeRoutes(session);
      Map<String, TEndPoint> poolRoutes = writeRoutes(pool);
      TEndPoint victim =
          Arrays.stream(devices)
              .map(device -> sessionRoutes.get(device))
              .filter(route -> route != null && !route.equals(seed))
              .filter(
                  route ->
                      Arrays.stream(devices)
                          .anyMatch(
                              device ->
                                  route.equals(leaders.get(device))
                                      && route.equals(poolRoutes.get(device))))
              .findFirst()
              .orElseThrow(
                  () ->
                      new AssertionError(
                          "No shared cached leader: session="
                              + sessionRoutes
                              + ", pool="
                              + poolRoutes
                              + ", leaders="
                              + leaders));
      int victimIndex = -1;
      for (int index = 0; index < 3; index++) {
        if (endpoint(index).equals(victim)) {
          victimIndex = index;
        }
      }
      assertTrue(victimIndex >= 0);
      EnvFactory.getEnv().getDataNodeWrapper(victimIndex).stopForcibly();
      try {
        awaitStatus(victimIndex, NodeStatus.Unknown);
        awaitWriteLeaders(devices, victim, seedIndex);
        // Fixed keys make an outer retry after a transport failure safe; only returned success
        // enters the ledger. The same Session and size-one pool must recover their old routes.
        writeRoutePhase(session, pool, devices, 1, expected);
        assertRouteData(devices, expected, session);
        assertEquals(0, pool.currentOccupiedSize());
        assertEquals(1, pool.currentAvailableSize());
      } finally {
        EnvFactory.getEnv().startDataNode(victimIndex);
      }
      awaitStatus(victimIndex, NodeStatus.Running);
      awaitWriteLeaders(devices, null, seedIndex);
      writeRoutePhase(session, pool, devices, 2, expected);
      session.executeNonQueryStatement("FLUSH");
      assertRouteData(devices, expected, session);
      assertEquals(0, pool.currentOccupiedSize());
      // The environment checks every DataNode, including the recovered old leader.
      try (Connection connection = EnvFactory.getEnv().getConnection();
          Statement statement = connection.createStatement()) {
        for (String device : devices) {
          try (ResultSet result = statement.executeQuery("SELECT n,t FROM " + device)) {
            for (Map.Entry<Long, String> row : expected.get(device).entrySet()) {
              assertTrue(result.next());
              assertEquals((long) row.getKey(), result.getLong(1));
              assertEquals(routeNumber(device, row.getKey(), devices), result.getLong(2));
              assertEquals(row.getValue(), result.getString(3));
            }
            assertFalse(result.next());
          }
        }
      }
    } finally {
      pool.close();
    }
  }

  @SuppressWarnings("unchecked")
  private Map<String, TEndPoint> writeRoutes(Object client) throws Exception {
    Field field = client.getClass().getDeclaredField("deviceIdToEndpoint");
    field.setAccessible(true);
    return new HashMap<>((Map<String, TEndPoint>) field.get(client));
  }

  private Map<String, TEndPoint> awaitWriteLeaders(
      String[] devices, TEndPoint excluded, int probeIndex) {
    Map<String, TEndPoint> leaders = new HashMap<>();
    Awaitility.await()
        .atMost(90, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              leaders.clear();
              Map<String, Integer> runningReplicas = new HashMap<>();
              Map<String, Integer> regionIds = new HashMap<>();
              try (Session probe =
                  new Session.Builder()
                      .host(endpoint(probeIndex).getIp())
                      .port(endpoint(probeIndex).getPort())
                      .maxRetryCount(0)
                      .build()) {
                probe.open(false, RECOVERY_CONNECTION_TIMEOUT_MS);
                try (SessionDataSet result = probe.executeQueryStatement("SHOW REGIONS")) {
                  SessionDataSet.DataIterator rows = result.iterator();
                  while (rows.next()) {
                    String device = rows.getString("Database") + ".d";
                    if (!Arrays.asList(devices).contains(device)
                        || !"DataRegion".equals(rows.getString("Type"))) {
                      continue;
                    }
                    int id = rows.getInt("RegionId");
                    Integer previous = regionIds.put(device, id);
                    assertTrue(
                        "Expected one DataRegion per database", previous == null || previous == id);
                    TEndPoint replica =
                        new TEndPoint(rows.getString("RpcAddress"), rows.getInt("RpcPort"));
                    if ("Running".equals(rows.getString("Status")) && !replica.equals(excluded)) {
                      runningReplicas.merge(device, 1, Integer::sum);
                      if ("Leader".equals(rows.getString("Role"))) {
                        leaders.put(device, replica);
                      }
                    }
                  }
                }
              }
              assertEquals(2, new HashSet<>(regionIds.values()).size());
              assertEquals(2, leaders.size());
              for (String device : devices) {
                assertEquals(
                    excluded == null ? 3 : 2, (int) runningReplicas.getOrDefault(device, 0));
              }
            });
    return new HashMap<>(leaders);
  }

  private void writeRoutePhase(
      Session session,
      SessionPool pool,
      String[] devices,
      int phase,
      Map<String, Map<Long, String>> expected)
      throws Exception {
    for (int writer = 0; writer < 2; writer++) {
      final int client = writer;
      Awaitility.await()
          .atMost(90, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                Map<String, Tablet> batch = new HashMap<>();
                for (String device : devices) {
                  Tablet tablet =
                      new Tablet(
                          device,
                          Arrays.asList(
                              new MeasurementSchema("n", TSDataType.INT64),
                              new MeasurementSchema("t", TSDataType.TEXT)),
                          2);
                  for (int row = 0; row < 2; row++) {
                    long time = phase * 3000L + client * 100L + (row == 0 ? 1001 : 1);
                    tablet.addTimestamp(row, time);
                    tablet.addValue("n", row, routeNumber(device, time, devices));
                    tablet.addValue("t", row, row == 0 ? device + "-" + time : null);
                  }
                  batch.put(device, tablet);
                }
                try {
                  if (client == 0) {
                    session.insertTablets(batch);
                  } else {
                    pool.insertTablets(batch);
                  }
                } catch (IoTDBConnectionException | StatementExecutionException e) {
                  // Leader/Running heartbeats can precede write readiness. Both transport and
                  // server-side failures must retry these same idempotent keys until success.
                  throw new AssertionError("Original writer has not recovered its cached route", e);
                }
              });
      for (String device : devices) {
        for (int row = 0; row < 2; row++) {
          long time = phase * 3000L + writer * 100L + (row == 0 ? 1001 : 1);
          expected.get(device).put(time, row == 0 ? device + "-" + time : null);
        }
      }
    }
  }

  private static long routeNumber(String device, long time, String[] devices) {
    return (device.equals(devices[0]) ? 0 : 100_000L) + time;
  }

  private void assertRouteData(
      String[] devices, Map<String, Map<Long, String>> expected, Session session) throws Exception {
    for (String device : devices) {
      try (SessionDataSet result = session.executeQueryStatement("SELECT n,t FROM " + device)) {
        for (Map.Entry<Long, String> row : expected.get(device).entrySet()) {
          assertTrue(result.hasNext());
          RowRecord actual = result.next();
          assertEquals((long) row.getKey(), actual.getTimestamp());
          assertEquals(
              routeNumber(device, row.getKey(), devices), actual.getFields().get(0).getLongV());
          assertEquals(
              row.getValue(),
              actual.getFields().get(1).getDataType() == null
                  ? null
                  : actual.getFields().get(1).getStringValue());
        }
        assertFalse(result.hasNext());
      }
    }
  }

  @Test
  public void testQueriesDuringRollingRestart() throws Exception {
    SessionPool pool =
        new SessionPool.Builder().nodeUrls(urls()).maxSize(1).connectionTimeoutInMs(1000).build();
    Session session = new Session.Builder().nodeUrls(urls()).build();
    try {
      // Initial Ratis region creation can exceed one second; do not retry its DDL prematurely.
      session.open(false, 30_000);
      session.executeNonQueryStatement("CREATE DATABASE root.seed_fallback");
      session.executeNonQueryStatement(
          "CREATE TIMESERIES root.seed_fallback.d.s WITH DATATYPE=INT32, ENCODING=RLE");
      session.executeNonQueryStatement("INSERT INTO root.seed_fallback.d(time,s) VALUES(1,42)");
      assertQuery(session, pool);
      NodesSupplier sessionSupplier = supplier(session);
      NodesSupplier poolSupplier = supplier(pool);
      Set<TEndPoint> runningNodes =
          new HashSet<>(Arrays.asList(endpoint(0), endpoint(1), endpoint(2)));
      for (int index = 0; index < 3; index++) {
        EnvFactory.getEnv().shutdownDataNode(index);
        awaitStatus(index, EnvUtils.getNodeStatusAfterLocalStop());
        runningNodes.remove(endpoint(index));
        awaitSnapshot(sessionSupplier, runningNodes);
        awaitSnapshot(poolSupplier, runningNodes);
        assertQuery(session, pool);

        EnvFactory.getEnv().startDataNode(index);
        awaitStatus(index, NodeStatus.Running);
        runningNodes.add(endpoint(index));
        awaitSnapshot(sessionSupplier, runningNodes);
        awaitSnapshot(poolSupplier, runningNodes);
        assertQuery(session, pool);
      }
    } finally {
      try {
        session.close();
      } catch (IoTDBConnectionException e) {
        // Cached connections may still belong to a stopped process. Only cleanup tolerates this;
        // all query and discovery assertions above must succeed without ignoring failures.
      } finally {
        pool.close();
      }
    }
  }

  @Test
  public void testBusinessAndBackgroundRecoveryThroughOmittedSeed() throws Exception {
    EnvFactory.getEnv().shutdownDataNode(0);
    awaitStatus(0, EnvUtils.getNodeStatusAfterLocalStop());
    // Control refresh timing so neither supplier learns that node 0 recovered before failover.
    ScheduledThreadPoolExecutor executor =
        new ScheduledThreadPoolExecutor(1) {
          @Override
          public ScheduledFuture<?> scheduleAtFixedRate(
              Runnable command, long initialDelay, long period, TimeUnit unit) {
            return super.scheduleAtFixedRate(command, 1, 1, TimeUnit.DAYS);
          }
        };
    // Node 2 keeps a Ratis majority alive for login queries, but is unknown to these clients until
    // discovery recovers. Initialize a controlled stale snapshot without running discovery first.
    List<TEndPoint> discovered = Collections.singletonList(endpoint(1));
    List<TEndPoint> seeds = Arrays.asList(endpoint(0), endpoint(1));
    NodesSupplier business = createSupplier(executor, discovered, seeds);
    NodesSupplier background = createSupplier(executor, discovered, seeds);
    List<String> seedUrls =
        seeds.stream().map(UrlUtils::convertTEndPointIpv4AndIpv6Url).collect(Collectors.toList());
    try (Session session =
        new Session.Builder().nodeUrls(seedUrls).enableAutoFetch(false).build()) {
      assertEquals(discovered, business.get());
      assertEquals(discovered, background.get());
      session.open(false, RECOVERY_CONNECTION_TIMEOUT_MS, new ConcurrentHashMap<>(), business);
      assertEquals(endpoint(1), connectedEndpoint(session));

      EnvFactory.getEnv().startDataNode(0);
      awaitStatus(0, NodeStatus.Running);
      awaitLogin(0);
      EnvFactory.getEnv().getDataNodeWrapper(1).stopForcibly();
      awaitStatus(1, NodeStatus.Unknown);
      assertEquals(discovered, business.get());
      assertEquals(discovered, background.get());

      // Every discovered endpoint is dead. Node 0 is the only reachable initial seed.
      Set<TEndPoint> recoveredNodes = new HashSet<>(Arrays.asList(endpoint(0), endpoint(2)));
      try (SessionDataSet result = session.executeQueryStatement("SHOW AVAILABLE URLS")) {
        assertEquals(recoveredNodes, readEndpoints(result));
      }
      assertEquals(endpoint(0), connectedEndpoint(session));
      awaitSnapshot(background, recoveredNodes);
      assertEquals(seeds, background.getSeedNodes());
      // Wait for the immediate refresh requested by business reconnection, not a 60-second poll.
      executor.submit(() -> {}).get(60, TimeUnit.SECONDS);
      assertEquals(recoveredNodes, new HashSet<>(business.get()));
      assertEquals(seeds, business.getSeedNodes());
    } finally {
      business.close();
      background.close();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  private void assertQuery(Session session, SessionPool pool) throws Exception {
    String sql = "SELECT s FROM root.seed_fallback.d";
    try (SessionDataSet result = session.executeQueryStatement(sql)) {
      assertTrue(result.hasNext());
      assertEquals(42, result.next().getFields().get(0).getIntV());
      assertFalse(result.hasNext());
    }
    try (SessionDataSetWrapper result = pool.executeQueryStatement(sql)) {
      assertTrue(result.hasNext());
      assertEquals(42, result.next().getFields().get(0).getIntV());
      assertFalse(result.hasNext());
    }
  }

  private NodesSupplier supplier(Object owner) throws Exception {
    Field field = owner.getClass().getDeclaredField("availableNodes");
    field.setAccessible(true);
    return (NodesSupplier) field.get(owner);
  }

  private TEndPoint connectedEndpoint(Session session) throws Exception {
    Field field = Session.class.getDeclaredField("defaultEndPoint");
    field.setAccessible(true);
    return (TEndPoint) field.get(session);
  }

  private NodesSupplier createSupplier(
      ScheduledThreadPoolExecutor executor, List<TEndPoint> discovered, List<TEndPoint> seeds) {
    return NodesSupplier.createNodeSupplier(
        discovered,
        seeds,
        executor,
        SessionConfig.DEFAULT_USER,
        SessionConfig.DEFAULT_PASSWORD,
        ZoneId.of("UTC"),
        SessionConfig.DEFAULT_INITIAL_BUFFER_CAPACITY,
        SessionConfig.DEFAULT_MAX_FRAME_SIZE,
        RECOVERY_CONNECTION_TIMEOUT_MS,
        false,
        null,
        null,
        null,
        null,
        "TLS",
        false,
        "V_1_0");
  }

  private Set<TEndPoint> readEndpoints(SessionDataSet dataSet) throws Exception {
    Set<TEndPoint> result = new HashSet<>();
    SessionDataSet.DataIterator iterator = dataSet.iterator();
    while (iterator.next()) {
      result.add(new TEndPoint(iterator.getString("RpcAddress"), iterator.getInt("RpcPort")));
    }
    return result;
  }

  private TEndPoint endpoint(int index) {
    return new TEndPoint(
        EnvFactory.getEnv().getDataNodeWrapper(index).getIp(),
        EnvFactory.getEnv().getDataNodeWrapper(index).getPort());
  }

  private List<String> urls() {
    return Arrays.asList(endpoint(0), endpoint(1), endpoint(2)).stream()
        .map(UrlUtils::convertTEndPointIpv4AndIpv6Url)
        .collect(Collectors.toCollection(ArrayList::new));
  }

  private void awaitStatus(int index, NodeStatus status) {
    EnvFactory.getEnv()
        .ensureNodeStatus(
            Collections.singletonList(EnvFactory.getEnv().getDataNodeWrapper(index)),
            Collections.singletonList(status));
  }

  private void awaitLogin(int index) {
    TEndPoint recovered = endpoint(index);
    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(100, TimeUnit.MILLISECONDS)
        .untilAsserted(
            () -> {
              // Probe the recovered seed independently so the tested snapshots remain stale.
              try (Session probe =
                  new Session.Builder()
                      .host(recovered.getIp())
                      .port(recovered.getPort())
                      .enableAutoFetch(false)
                      .build()) {
                probe.open(false, RECOVERY_CONNECTION_TIMEOUT_MS);
                try (SessionDataSet result = probe.executeQueryStatement("SHOW AVAILABLE URLS")) {
                  assertTrue(readEndpoints(result).contains(recovered));
                }
              } catch (IoTDBConnectionException | StatementExecutionException e) {
                throw new AssertionError("Seed is not ready for session queries: " + recovered, e);
              }
            });
  }

  private void awaitSnapshot(NodesSupplier current, Set<TEndPoint> expected) {
    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .pollInterval(100, TimeUnit.MILLISECONDS)
        .untilAsserted(
            () -> {
              // run() can return immediately while a scheduled refresh is still in flight.
              current.run();
              assertEquals(expected, new HashSet<>(current.get()));
            });
  }
}
