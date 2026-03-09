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

package org.apache.iotdb.db.it.iotconsensusv2;

import org.apache.iotdb.confignode.it.regionmigration.IoTDBRegionOperationReliabilityITFramework;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.apache.tsfile.utils.Pair;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.iotdb.util.MagicUtils.makeItCloseQuietly;

/**
 * Abstract base for IoTConsensusV2 3C3D integration tests. Subclasses specify batch or stream mode.
 *
 * <p>Verifies that a 3C3D cluster with IoTConsensusV2 can: 1. Start successfully 2. Write data 3.
 * Execute flush on cluster 4. Query and verify data was written successfully
 *
 * <p>Additionally tests replica consistency: after stopping the leader DataNode, the follower
 * should be elected as new leader and serve the same data.
 */
public abstract class IoTDBIoTConsensusV23C3DBasicITBase
    extends IoTDBRegionOperationReliabilityITFramework {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBIoTConsensusV23C3DBasicITBase.class);

  protected static final int CONFIG_NODE_NUM = 3;
  protected static final int DATA_NODE_NUM = 3;
  protected static final int DATA_REPLICATION_FACTOR = 2;
  protected static final int SCHEMA_REPLICATION_FACTOR = 3;

  /** Timeout in seconds for 3C3D cluster init. */
  protected static final int CLUSTER_INIT_TIMEOUT_SECONDS = 300;

  protected static final String INSERTION1 =
      "INSERT INTO root.sg.d1(timestamp,speed,temperature) values(100, 1, 2)";
  protected static final String INSERTION2 =
      "INSERT INTO root.sg.d1(timestamp,speed,temperature) values(101, 3, 4)";
  protected static final String INSERTION3 =
      "INSERT INTO root.sg.d1(timestamp,speed,temperature) values(102, 5, 6)";
  protected static final String FLUSH_COMMAND = "flush on cluster";
  protected static final String COUNT_QUERY = "select count(*) from root.sg.**";
  protected static final String SELECT_ALL_QUERY = "select speed, temperature from root.sg.d1";

  /**
   * Returns IoTConsensusV2 mode: {@link ConsensusFactory#IOT_CONSENSUS_V2_BATCH_MODE} or {@link
   * ConsensusFactory#IOT_CONSENSUS_V2_STREAM_MODE}.
   */
  protected abstract String getIoTConsensusV2Mode();

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setDataReplicationFactor(DATA_REPLICATION_FACTOR)
        .setSchemaReplicationFactor(SCHEMA_REPLICATION_FACTOR)
        .setIoTConsensusV2Mode(getIoTConsensusV2Mode());

    EnvFactory.getEnv()
        .getConfig()
        .getDataNodeConfig()
        .setMetricReporterType(Collections.singletonList("PROMETHEUS"));

    EnvFactory.getEnv()
        .initClusterEnvironment(CONFIG_NODE_NUM, DATA_NODE_NUM, CLUSTER_INIT_TIMEOUT_SECONDS);
  }

  public void test3C3DWriteFlushAndQuery() throws Exception {
    try (Connection connection = makeItCloseQuietly(EnvFactory.getEnv().getConnection());
        Statement statement = makeItCloseQuietly(connection.createStatement())) {

      LOGGER.info("Writing data to 3C3D cluster (mode: {})...", getIoTConsensusV2Mode());
      statement.execute(INSERTION1);
      statement.execute(INSERTION2);
      statement.execute(INSERTION3);

      LOGGER.info("Executing flush on cluster...");
      statement.execute(FLUSH_COMMAND);

      verifyDataConsistency(statement);

      LOGGER.info("3C3D IoTConsensusV2 {} basic test passed", getIoTConsensusV2Mode());
    }
  }

  /**
   * Test replica consistency: with replication factor 2, stop the leader DataNode and verify the
   * follower serves the same data.
   */
  public void testReplicaConsistencyAfterLeaderStop() throws Exception {
    try (Connection connection = makeItCloseQuietly(EnvFactory.getEnv().getConnection());
        Statement statement = makeItCloseQuietly(connection.createStatement())) {

      LOGGER.info("Writing data to 3C3D cluster (mode: {})...", getIoTConsensusV2Mode());
      statement.execute(INSERTION1);
      statement.execute(INSERTION2);
      statement.execute(INSERTION3);
      statement.execute(FLUSH_COMMAND);

      verifyDataConsistency(statement);

      Map<Integer, Pair<Integer, Set<Integer>>> dataRegionMap =
          getDataRegionMapWithLeader(statement);

      int targetRegionId = -1;
      int leaderDataNodeId = -1;
      int followerDataNodeId = -1;
      for (Map.Entry<Integer, Pair<Integer, Set<Integer>>> entry : dataRegionMap.entrySet()) {
        Pair<Integer, Set<Integer>> leaderAndReplicas = entry.getValue();
        if (leaderAndReplicas.getRight().size() > 1
            && leaderAndReplicas.getRight().size() <= DATA_REPLICATION_FACTOR
            && leaderAndReplicas.getLeft() > 0) {
          targetRegionId = entry.getKey();
          leaderDataNodeId = leaderAndReplicas.getLeft();
          final int lambdaLeaderDataNodeId = leaderDataNodeId;
          followerDataNodeId =
              leaderAndReplicas.getRight().stream()
                  .filter(i -> i != lambdaLeaderDataNodeId)
                  .findAny()
                  .orElse(-1);
          break;
        }
      }

      Assert.assertTrue(
          "Should find a data region with leader for root.sg",
          targetRegionId > 0 && leaderDataNodeId > 0 && followerDataNodeId > 0);

      DataNodeWrapper leaderNode =
          EnvFactory.getEnv()
              .dataNodeIdToWrapper(leaderDataNodeId)
              .orElseThrow(() -> new AssertionError("DataNode not found in cluster"));

      waitForReplicationComplete(leaderNode);

      LOGGER.info(
          "Stopping leader DataNode {} (region {}) for replica consistency test",
          leaderDataNodeId,
          targetRegionId);

      leaderNode.stopForcibly();
      Assert.assertFalse("Leader should be stopped", leaderNode.isAlive());

      DataNodeWrapper followerNode =
          EnvFactory.getEnv()
              .dataNodeIdToWrapper(followerDataNodeId)
              .orElseThrow(() -> new AssertionError("Follower DataNode not found in cluster"));
      LOGGER.info(
          "Waiting for follower DataNode {} to be elected as new leader and verifying replica consistency...",
          followerDataNodeId);
      Awaitility.await()
          .pollDelay(2, TimeUnit.SECONDS)
          .atMost(2, TimeUnit.MINUTES)
          .untilAsserted(
              () -> {
                try (Connection followerConn =
                        makeItCloseQuietly(
                            EnvFactory.getEnv()
                                .getConnection(
                                    followerNode,
                                    SessionConfig.DEFAULT_USER,
                                    SessionConfig.DEFAULT_PASSWORD,
                                    BaseEnv.TREE_SQL_DIALECT));
                    Statement followerStmt = makeItCloseQuietly(followerConn.createStatement())) {
                  verifyDataConsistency(followerStmt);
                }
              });

      LOGGER.info(
          "Replica consistency verified: follower has same data as former leader after failover");
    }
  }

  private static final Pattern SYNC_LAG_PATTERN =
      Pattern.compile("pipe_consensus\\{[^}]*type=\"syncLag\"[^}]*}\\s+(\\S+)");

  /**
   * Wait until all consensus pipe syncLag metrics on the given leader DataNode reach 0, meaning
   * replication is fully caught up. Queries the leader's Prometheus metrics endpoint periodically.
   */
  protected void waitForReplicationComplete(DataNodeWrapper leaderNode) {
    final long timeoutSeconds = 120;
    final String metricsUrl =
        "http://" + leaderNode.getIp() + ":" + leaderNode.getMetricPort() + "/metrics";
    LOGGER.info(
        "Waiting for consensus pipe syncLag to reach 0 on leader DataNode (url: {}, timeout: {}s)...",
        metricsUrl,
        timeoutSeconds);
    Awaitility.await()
        .pollInterval(500, TimeUnit.MILLISECONDS)
        .atMost(timeoutSeconds, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              String metricsContent = EnvFactory.getEnv().getUrlContent(metricsUrl, null);
              Assert.assertNotNull(
                  "Failed to fetch metrics from leader DataNode at " + metricsUrl, metricsContent);
              Matcher matcher = SYNC_LAG_PATTERN.matcher(metricsContent);
              boolean found = false;
              while (matcher.find()) {
                found = true;
                double syncLag = Double.parseDouble(matcher.group(1));
                LOGGER.debug("Found syncLag metric value: {}", syncLag);
                Assert.assertEquals(
                    "Consensus pipe syncLag should be 0.0 but was " + syncLag, 0.0, syncLag, 0.001);
              }
              Assert.assertTrue(
                  "No pipe_consensus syncLag metric found in leader DataNode metrics", found);
            });
    LOGGER.info("All consensus pipe syncLag == 0 on leader, replication is complete");
  }

  protected void verifyDataConsistency(Statement statement) throws Exception {
    LOGGER.info("Querying data to verify write success...");
    try (ResultSet countResult = statement.executeQuery(COUNT_QUERY)) {
      Assert.assertTrue("Count query should return results", countResult.next());

      int columnCount = countResult.getMetaData().getColumnCount();
      long totalCount = 0;
      for (int i = 1; i <= columnCount; i++) {
        totalCount += parseLongFromString(countResult.getString(i));
      }
      Assert.assertEquals(
          "Expected 6 total data points (3 timestamps x 2 measurements)", 6, totalCount);
    }

    int rowCount = 0;
    try (ResultSet selectResult = statement.executeQuery(SELECT_ALL_QUERY)) {
      while (selectResult.next()) {
        rowCount++;
        long timestamp = parseLongFromString(selectResult.getString(1));
        long speed = parseLongFromString(selectResult.getString(2));
        long temperature = parseLongFromString(selectResult.getString(3));
        if (timestamp == 100) {
          Assert.assertEquals(1, speed);
          Assert.assertEquals(2, temperature);
        } else if (timestamp == 101) {
          Assert.assertEquals(3, speed);
          Assert.assertEquals(4, temperature);
        } else if (timestamp == 102) {
          Assert.assertEquals(5, speed);
          Assert.assertEquals(6, temperature);
        }
      }
    }
    Assert.assertEquals("Expected 3 rows from select *", 3, rowCount);
  }

  /** Parse long from IoTDB result string (handles both "1" and "1.0" formats). */
  protected static long parseLongFromString(String s) {
    if (s == null || s.isEmpty()) {
      return 0;
    }
    try {
      return Long.parseLong(s);
    } catch (NumberFormatException e) {
      return (long) Double.parseDouble(s);
    }
  }
}
