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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.it.regionmigration.IoTDBRegionOperationReliabilityITFramework;
import org.apache.iotdb.confignode.rpc.thrift.TTriggerRegionConsistencyRepairReq;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.modification.v1.ModificationFileV1;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.utils.Pair;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

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
  protected static final String DELETE_SPEED_UP_TO_101 =
      "DELETE FROM root.sg.d1.speed WHERE time <= 101";
  protected static final String COUNT_AFTER_DELETE_QUERY =
      "select count(speed), count(temperature) from root.sg.d1";
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

      insertAndFlushTestData(statement);
      verifyDataConsistency(statement);

      LOGGER.info("3C3D IoTConsensusV2 {} basic test passed", getIoTConsensusV2Mode());
    }
  }

  /**
   * Test that a follower can observe the same logical view after the leader reports replication
   * catch-up.
   */
  public void testFollowerCanReadConsistentDataAfterCatchUp() throws Exception {
    try (Connection connection = makeItCloseQuietly(EnvFactory.getEnv().getConnection());
        Statement statement = makeItCloseQuietly(connection.createStatement())) {

      insertAndFlushTestData(statement);
      verifyDataConsistency(statement);

      RegionReplicaSelection regionReplicaSelection = selectReplicatedDataRegion(statement);
      waitForReplicationComplete(regionReplicaSelection.leaderNode);

      LOGGER.info(
          "Verifying logical view from follower DataNode {} for region {} after catch-up",
          regionReplicaSelection.followerDataNodeId,
          regionReplicaSelection.regionId);
      verifyDataConsistencyOnNode(regionReplicaSelection.followerNode);
    }
  }

  /**
   * Test replica consistency: with replication factor 2, stop the leader DataNode and verify the
   * follower serves the same data.
   */
  public void testReplicaConsistencyAfterLeaderStop() throws Exception {
    RegionReplicaSelection regionReplicaSelection;
    try (Connection connection = makeItCloseQuietly(EnvFactory.getEnv().getConnection());
        Statement statement = makeItCloseQuietly(connection.createStatement())) {

      insertAndFlushTestData(statement);
      verifyDataConsistency(statement);

      regionReplicaSelection = selectReplicatedDataRegion(statement);
      waitForReplicationComplete(regionReplicaSelection.leaderNode);
    }

    LOGGER.info(
        "Stopping leader DataNode {} (region {}) for replica consistency test",
        regionReplicaSelection.leaderDataNodeId,
        regionReplicaSelection.regionId);

    regionReplicaSelection.leaderNode.stopForcibly();
    Assert.assertFalse("Leader should be stopped", regionReplicaSelection.leaderNode.isAlive());

    LOGGER.info(
        "Waiting for follower DataNode {} to be elected as new leader and verifying replica consistency...",
        regionReplicaSelection.followerDataNodeId);
    Awaitility.await()
        .pollDelay(2, TimeUnit.SECONDS)
        .atMost(2, TimeUnit.MINUTES)
        .untilAsserted(() -> verifyDataConsistencyOnNode(regionReplicaSelection.followerNode));

    LOGGER.info(
        "Replica consistency verified: follower has same data as former leader after failover");
  }

  /**
   * Test replica consistency for a delete path: after deletion is replicated, stopping the leader
   * must not change the surviving logical view.
   */
  public void testReplicaConsistencyAfterDeleteAndLeaderStop() throws Exception {
    RegionReplicaSelection regionReplicaSelection;
    try (Connection connection = makeItCloseQuietly(EnvFactory.getEnv().getConnection());
        Statement statement = makeItCloseQuietly(connection.createStatement())) {

      insertAndFlushTestData(statement);
      regionReplicaSelection = selectReplicatedDataRegion(statement);

      LOGGER.info(
          "Deleting replicated data on leader DataNode {} for region {}",
          regionReplicaSelection.leaderDataNodeId,
          regionReplicaSelection.regionId);
      statement.execute(DELETE_SPEED_UP_TO_101);
      statement.execute(FLUSH_COMMAND);

      verifyPostDeleteConsistency(statement);
      waitForReplicationComplete(regionReplicaSelection.leaderNode);
      verifyPostDeleteConsistencyOnNode(regionReplicaSelection.followerNode);
    }

    LOGGER.info(
        "Stopping leader DataNode {} after replicated delete",
        regionReplicaSelection.leaderDataNodeId);
    regionReplicaSelection.leaderNode.stopForcibly();
    Assert.assertFalse("Leader should be stopped", regionReplicaSelection.leaderNode.isAlive());

    Awaitility.await()
        .pollDelay(2, TimeUnit.SECONDS)
        .atMost(2, TimeUnit.MINUTES)
        .untilAsserted(() -> verifyPostDeleteConsistencyOnNode(regionReplicaSelection.followerNode));

    LOGGER.info(
        "Replica consistency verified after delete and failover on follower DataNode {}",
        regionReplicaSelection.followerDataNodeId);
  }

  /**
   * Simulate a follower missing a sealed TsFile, trigger replica consistency repair through
   * ConfigNode, and verify the repaired follower still serves the correct data after the leader is
   * stopped.
   */
  public void testReplicaConsistencyRepairAfterFollowerLosesSealedTsFile() throws Exception {
    RegionReplicaSelection regionReplicaSelection;
    Path deletedTsFile;

    try (Connection connection = makeItCloseQuietly(EnvFactory.getEnv().getConnection());
        Statement statement = makeItCloseQuietly(connection.createStatement())) {
      insertAndFlushTestData(statement);
      verifyDataConsistency(statement);

      regionReplicaSelection = selectReplicatedDataRegion(statement);
      waitForReplicationComplete(regionReplicaSelection.leaderNode);
      deletedTsFile =
          findLatestSealedTsFile(regionReplicaSelection.followerNode, regionReplicaSelection.regionId);
    }

    LOGGER.info(
        "Stopping follower DataNode {} and deleting sealed TsFile {} for region {}",
        regionReplicaSelection.followerDataNodeId,
        deletedTsFile,
        regionReplicaSelection.regionId);
    regionReplicaSelection.followerNode.stopForcibly();
    Assert.assertFalse("Follower should be stopped", regionReplicaSelection.followerNode.isAlive());
    deleteTsFileArtifacts(deletedTsFile);

    regionReplicaSelection.followerNode.start();
    Awaitility.await()
        .pollDelay(2, TimeUnit.SECONDS)
        .atMost(2, TimeUnit.MINUTES)
        .untilAsserted(() -> assertDataInconsistentOnNode(regionReplicaSelection.followerNode));

    triggerRegionConsistencyRepair(regionReplicaSelection.regionId);

    Awaitility.await()
        .pollDelay(2, TimeUnit.SECONDS)
        .atMost(2, TimeUnit.MINUTES)
        .untilAsserted(() -> verifyDataConsistencyOnNode(regionReplicaSelection.followerNode));

    LOGGER.info(
        "Stopping leader DataNode {} after repair to verify repaired follower serves local data",
        regionReplicaSelection.leaderDataNodeId);
    regionReplicaSelection.leaderNode.stopForcibly();
    Assert.assertFalse("Leader should be stopped", regionReplicaSelection.leaderNode.isAlive());

    Awaitility.await()
        .pollDelay(2, TimeUnit.SECONDS)
        .atMost(2, TimeUnit.MINUTES)
        .untilAsserted(() -> verifyDataConsistencyOnNode(regionReplicaSelection.followerNode));
  }

  protected void insertAndFlushTestData(Statement statement) throws Exception {
    LOGGER.info("Writing data to 3C3D cluster (mode: {})...", getIoTConsensusV2Mode());
    statement.execute(INSERTION1);
    statement.execute(INSERTION2);
    statement.execute(INSERTION3);

    LOGGER.info("Executing flush on cluster...");
    statement.execute(FLUSH_COMMAND);
  }

  protected RegionReplicaSelection selectReplicatedDataRegion(Statement statement) throws Exception {
    Map<Integer, Pair<Integer, Set<Integer>>> dataRegionMap = getDataRegionMapWithLeader(statement);

    for (Map.Entry<Integer, Pair<Integer, Set<Integer>>> entry : dataRegionMap.entrySet()) {
      Pair<Integer, Set<Integer>> leaderAndReplicas = entry.getValue();
      if (leaderAndReplicas.getLeft() <= 0 || leaderAndReplicas.getRight().size() <= 1) {
        continue;
      }

      int leaderDataNodeId = leaderAndReplicas.getLeft();
      int followerDataNodeId =
          leaderAndReplicas.getRight().stream()
              .filter(dataNodeId -> dataNodeId != leaderDataNodeId)
              .findFirst()
              .orElse(-1);
      if (followerDataNodeId <= 0) {
        continue;
      }

      DataNodeWrapper leaderNode =
          EnvFactory.getEnv()
              .dataNodeIdToWrapper(leaderDataNodeId)
              .orElseThrow(() -> new AssertionError("Leader DataNode not found in cluster"));
      DataNodeWrapper followerNode =
          EnvFactory.getEnv()
              .dataNodeIdToWrapper(followerDataNodeId)
              .orElseThrow(() -> new AssertionError("Follower DataNode not found in cluster"));
      return new RegionReplicaSelection(
          entry.getKey(),
          leaderDataNodeId,
          followerDataNodeId,
          leaderNode,
          followerNode);
    }

    Assert.fail("Should find a replicated data region with a leader for root.sg");
    throw new AssertionError("unreachable");
  }

  protected void verifyDataConsistencyOnNode(DataNodeWrapper targetNode) throws Exception {
    try (Connection targetConnection =
            makeItCloseQuietly(
                EnvFactory.getEnv()
                    .getConnection(
                        targetNode,
                        SessionConfig.DEFAULT_USER,
                        SessionConfig.DEFAULT_PASSWORD,
                        BaseEnv.TREE_SQL_DIALECT));
        Statement targetStatement = makeItCloseQuietly(targetConnection.createStatement())) {
      verifyDataConsistency(targetStatement);
    }
  }

  protected void verifyPostDeleteConsistencyOnNode(DataNodeWrapper targetNode) throws Exception {
    try (Connection targetConnection =
            makeItCloseQuietly(
                EnvFactory.getEnv()
                    .getConnection(
                        targetNode,
                        SessionConfig.DEFAULT_USER,
                        SessionConfig.DEFAULT_PASSWORD,
                        BaseEnv.TREE_SQL_DIALECT));
        Statement targetStatement = makeItCloseQuietly(targetConnection.createStatement())) {
      verifyPostDeleteConsistency(targetStatement);
    }
  }

  private static final Pattern SYNC_LAG_PATTERN =
      Pattern.compile("iot_consensus_v2\\{[^}]*type=\"syncLag\"[^}]*}\\s+(\\S+)");

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
                  "No iot_consensus_v2 syncLag metric found in leader DataNode metrics", found);
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

  protected void verifyPostDeleteConsistency(Statement statement) throws Exception {
    LOGGER.info("Querying data to verify replicated delete success...");
    try (ResultSet countResult = statement.executeQuery(COUNT_AFTER_DELETE_QUERY)) {
      Assert.assertTrue("Delete count query should return results", countResult.next());
      Assert.assertEquals(
          "Expected only one surviving speed value after delete",
          1,
          parseLongFromString(countResult.getString(1)));
      Assert.assertEquals(
          "Expected all temperature values to remain after delete",
          3,
          parseLongFromString(countResult.getString(2)));
    }

    int rowCount = 0;
    try (ResultSet selectResult = statement.executeQuery(SELECT_ALL_QUERY)) {
      while (selectResult.next()) {
        rowCount++;
        long timestamp = parseLongFromString(selectResult.getString(1));
        String speed = selectResult.getString(2);
        long temperature = parseLongFromString(selectResult.getString(3));
        if (timestamp == 100) {
          assertNullValue(speed);
          Assert.assertEquals(2, temperature);
        } else if (timestamp == 101) {
          assertNullValue(speed);
          Assert.assertEquals(4, temperature);
        } else if (timestamp == 102) {
          Assert.assertEquals(5, parseLongFromString(speed));
          Assert.assertEquals(6, temperature);
        } else {
          Assert.fail("Unexpected timestamp after delete: " + timestamp);
        }
      }
    }
    Assert.assertEquals("Expected 3 logical rows from select after delete", 3, rowCount);
  }

  protected void assertDataInconsistentOnNode(DataNodeWrapper targetNode) throws Exception {
    try (Connection targetConnection =
            makeItCloseQuietly(
                EnvFactory.getEnv()
                    .getConnection(
                        targetNode,
                        SessionConfig.DEFAULT_USER,
                        SessionConfig.DEFAULT_PASSWORD,
                        BaseEnv.TREE_SQL_DIALECT));
        Statement targetStatement = makeItCloseQuietly(targetConnection.createStatement())) {
      try {
        verifyDataConsistency(targetStatement);
        Assert.fail("Expected inconsistent data on DataNode " + targetNode.getId());
      } catch (AssertionError expected) {
        LOGGER.info("Observed expected inconsistency on DataNode {}", targetNode.getId());
      }
    }
  }

  protected static void assertNullValue(String value) {
    Assert.assertTrue(
        "Expected deleted value to be null, but was " + value,
        value == null || "null".equalsIgnoreCase(value));
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

  protected static final class RegionReplicaSelection {
    private final int regionId;
    private final int leaderDataNodeId;
    private final int followerDataNodeId;
    private final DataNodeWrapper leaderNode;
    private final DataNodeWrapper followerNode;

    private RegionReplicaSelection(
        int regionId,
        int leaderDataNodeId,
        int followerDataNodeId,
        DataNodeWrapper leaderNode,
        DataNodeWrapper followerNode) {
      this.regionId = regionId;
      this.leaderDataNodeId = leaderDataNodeId;
      this.followerDataNodeId = followerDataNodeId;
      this.leaderNode = leaderNode;
      this.followerNode = followerNode;
    }
  }

  private Path findLatestSealedTsFile(DataNodeWrapper dataNodeWrapper, int regionId)
      throws Exception {
    try (Stream<Path> tsFiles = Files.walk(Paths.get(dataNodeWrapper.getDataPath()))) {
      Optional<Path> candidate =
          tsFiles
              .filter(Files::isRegularFile)
              .filter(path -> path.getFileName().toString().endsWith(".tsfile"))
              .filter(path -> path.toString().contains(File.separator + "root.sg" + File.separator))
              .filter(path -> belongsToRegion(path, regionId))
              .max((left, right) -> Long.compare(left.toFile().lastModified(), right.toFile().lastModified()));
      if (candidate.isPresent()) {
        return candidate.get();
      }
    }
    throw new AssertionError("No sealed TsFile found for region " + regionId);
  }

  private boolean belongsToRegion(Path tsFile, int regionId) {
    Path timePartitionDir = tsFile.getParent();
    Path regionDir = timePartitionDir == null ? null : timePartitionDir.getParent();
    return regionDir != null && String.valueOf(regionId).equals(regionDir.getFileName().toString());
  }

  private void deleteTsFileArtifacts(Path tsFile) throws Exception {
    Files.deleteIfExists(tsFile);
    Files.deleteIfExists(Paths.get(tsFile.toString() + TsFileResource.RESOURCE_SUFFIX));
    Files.deleteIfExists(Paths.get(tsFile.toString() + ModificationFile.FILE_SUFFIX));
    Files.deleteIfExists(Paths.get(tsFile.toString() + ModificationFile.COMPACTION_FILE_SUFFIX));
    Files.deleteIfExists(Paths.get(tsFile.toString() + ModificationFileV1.FILE_SUFFIX));
    Files.deleteIfExists(
        Paths.get(tsFile.toString() + ModificationFileV1.COMPACTION_FILE_SUFFIX));
  }

  private void triggerRegionConsistencyRepair(int regionId) throws Exception {
    TConsensusGroupId consensusGroupId = new TConsensusGroupId(TConsensusGroupType.DataRegion, regionId);
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      TSStatus status =
          client.triggerRegionConsistencyRepair(
              new TTriggerRegionConsistencyRepairReq(consensusGroupId));
      Assert.assertEquals(
          "Replica consistency repair should succeed",
          200,
          status.getCode());
    }
  }
}
