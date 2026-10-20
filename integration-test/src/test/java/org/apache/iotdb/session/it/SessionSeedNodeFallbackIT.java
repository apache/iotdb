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
import org.apache.iotdb.rpc.UrlUtils;
import org.apache.iotdb.session.NodesSupplier;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.pool.SessionPool;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Field;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
  @Before
  public void setUp() {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setSchemaReplicationFactor(3)
        .setDataReplicationFactor(3)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS);
    EnvFactory.getEnv().initClusterEnvironment(1, 3);
  }

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
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
    NodesSupplier business = createSupplier(executor);
    NodesSupplier background = createSupplier(executor);
    try (Session session = new Session.Builder().nodeUrls(urls()).enableAutoFetch(false).build()) {
      Set<TEndPoint> discovered = new HashSet<>(Arrays.asList(endpoint(1), endpoint(2)));
      awaitSnapshot(business, discovered);
      awaitSnapshot(background, discovered);
      session.open(false, 1000, new ConcurrentHashMap<>(), business);

      EnvFactory.getEnv().startDataNode(0);
      awaitStatus(0, NodeStatus.Running);
      EnvFactory.getEnv().getDataNodeWrapper(1).stopForcibly();
      awaitStatus(1, NodeStatus.Unknown);
      EnvFactory.getEnv().shutdownDataNode(2);
      awaitStatus(2, EnvUtils.getNodeStatusAfterLocalStop());
      assertFalse(business.get().contains(endpoint(0)));
      assertFalse(background.get().contains(endpoint(0)));

      // Both discovered endpoints are dead. Only the initial seed omitted by discovery is live.
      try (SessionDataSet result = session.executeQueryStatement("SHOW AVAILABLE URLS")) {
        assertEquals(Collections.singleton(endpoint(0)), readEndpoints(result));
      }
      awaitSnapshot(background, Collections.singleton(endpoint(0)));
      assertEquals(3, background.getSeedNodes().size());
      // Wait for the immediate refresh requested by business reconnection, not a 60-second poll.
      executor.submit(() -> {}).get(10, TimeUnit.SECONDS);
      assertEquals(Collections.singletonList(endpoint(0)), business.get());
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

  private NodesSupplier createSupplier(ScheduledThreadPoolExecutor executor) {
    List<TEndPoint> seeds = Arrays.asList(endpoint(0), endpoint(1), endpoint(2));
    return NodesSupplier.createNodeSupplier(
        seeds,
        executor,
        "root",
        "root",
        ZoneId.of("UTC"),
        SessionConfig.DEFAULT_INITIAL_BUFFER_CAPACITY,
        SessionConfig.DEFAULT_MAX_FRAME_SIZE,
        1000,
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
