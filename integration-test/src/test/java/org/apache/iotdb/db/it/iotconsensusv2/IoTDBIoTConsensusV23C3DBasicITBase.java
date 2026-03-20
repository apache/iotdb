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
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.ClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.confignode.it.regionmigration.IoTDBRegionOperationReliabilityITFramework;
import org.apache.iotdb.confignode.rpc.thrift.TTriggerRegionConsistencyRepairReq;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.modification.v1.ModificationFileV1;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.mpp.rpc.thrift.TGetConsistencyEligibilityReq;
import org.apache.iotdb.mpp.rpc.thrift.TGetConsistencyEligibilityResp;
import org.apache.iotdb.mpp.rpc.thrift.TPartitionConsistencyEligibility;

import org.apache.tsfile.utils.Pair;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
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

  private static final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient>
      DATA_NODE_INTERNAL_CLIENT_MANAGER =
          new IClientManager.Factory<TEndPoint, SyncDataNodeInternalServiceClient>()
              .createClientManager(
                  new ClientPoolFactory.SyncDataNodeInternalServiceClientPoolFactory());

  /** Timeout in seconds for 3C3D cluster init. */
  protected static final int CLUSTER_INIT_TIMEOUT_SECONDS = 300;

  protected static final long TIME_PARTITION_INTERVAL = 100L;
  protected static final long CONSISTENCY_CHECK_INITIAL_DELAY_MS = 1_000L;
  protected static final long CONSISTENCY_CHECK_INTERVAL_MS = 1_000L;

  protected static final String INSERTION1 =
      "INSERT INTO root.sg.d1(timestamp,speed,temperature) values(100, 1, 2)";
  protected static final String INSERTION2 =
      "INSERT INTO root.sg.d1(timestamp,speed,temperature) values(101, 3, 4)";
  protected static final String INSERTION3 =
      "INSERT INTO root.sg.d1(timestamp,speed,temperature) values(102, 5, 6)";
  protected static final String FLUSH_COMMAND = "flush on cluster";
  protected static final String LOCAL_FLUSH_COMMAND = "flush on local";
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
        .setTimePartitionInterval(TIME_PARTITION_INTERVAL)
        .setIoTConsensusV2Mode(getIoTConsensusV2Mode());

    EnvFactory.getEnv()
        .getConfig()
        .getDataNodeConfig()
        .setMetricReporterType(Collections.singletonList("PROMETHEUS"));

    EnvFactory.getEnv()
        .getConfig()
        .getConfigNodeConfig()
        .setConsistencyCheckSchedulerInitialDelayInMs(CONSISTENCY_CHECK_INITIAL_DELAY_MS)
        .setConsistencyCheckSchedulerIntervalInMs(CONSISTENCY_CHECK_INTERVAL_MS);

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
      waitForReplicationComplete(
          regionReplicaSelection.leaderNode, regionReplicaSelection.regionId);

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
      waitForReplicationComplete(
          regionReplicaSelection.leaderNode, regionReplicaSelection.regionId);
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
      waitForReplicationComplete(
          regionReplicaSelection.leaderNode, regionReplicaSelection.regionId);
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
        .untilAsserted(
            () -> verifyPostDeleteConsistencyOnNode(regionReplicaSelection.followerNode));

    LOGGER.info(
        "Replica consistency verified after delete and failover on follower DataNode {}",
        regionReplicaSelection.followerDataNodeId);
  }

  /**
   * Background consistency check should skip hot partitions before flush and only verify them after
   * they become cold and safe.
   */
  public void testBackgroundConsistencyCheckOnlyRunsOnColdPartitions() throws Exception {
    RegionReplicaSelection regionReplicaSelection;
    long partitionId = timePartitionId(100L);

    try (Connection connection = makeItCloseQuietly(EnvFactory.getEnv().getConnection());
        Statement statement = makeItCloseQuietly(connection.createStatement())) {
      insertPartitionData(statement, 100L);
      regionReplicaSelection = selectReplicatedDataRegion(statement);

      TimeUnit.MILLISECONDS.sleep(
          CONSISTENCY_CHECK_INITIAL_DELAY_MS + CONSISTENCY_CHECK_INTERVAL_MS * 2);
      assertRepairProgressEmpty(regionReplicaSelection.regionId);

      statement.execute(FLUSH_COMMAND);
      waitForReplicationComplete(
          regionReplicaSelection.leaderNode, regionReplicaSelection.regionId);
    }

    RepairProgressRow row =
        waitForCheckState(regionReplicaSelection.regionId, partitionId, "VERIFIED");
    Assert.assertEquals("IDLE", row.repairState);
  }

  /**
   * Restarting a follower should allow the background checker to rebuild its logical snapshot and
   * keep the partition verified after the node rejoins.
   */
  public void testBackgroundConsistencyCheckRebuildsLogicalSnapshotAfterFollowerRestart()
      throws Exception {
    RegionReplicaSelection regionReplicaSelection;
    long partitionId = timePartitionId(100L);

    try (Connection connection = makeItCloseQuietly(EnvFactory.getEnv().getConnection());
        Statement statement = makeItCloseQuietly(connection.createStatement())) {
      insertAndFlushTestData(statement);
      regionReplicaSelection = selectReplicatedDataRegion(statement);
      waitForReplicationComplete(
          regionReplicaSelection.leaderNode, regionReplicaSelection.regionId);
      waitForCheckState(regionReplicaSelection.regionId, partitionId, "VERIFIED");
    }

    RepairProgressRow previousRow =
        getRepairProgressRow(regionReplicaSelection.regionId, partitionId);
    Assert.assertNotNull(previousRow);

    regionReplicaSelection.followerNode.stopForcibly();
    Assert.assertFalse("Follower should be stopped", regionReplicaSelection.followerNode.isAlive());

    regionReplicaSelection.followerNode.start();
    waitForNodeConnectionReady(regionReplicaSelection.followerNode);
    waitForProgressRefresh(regionReplicaSelection.regionId, partitionId, previousRow.lastCheckedAt);
    RepairProgressRow refreshedRow =
        waitForCheckState(regionReplicaSelection.regionId, partitionId, "VERIFIED");
    Assert.assertEquals("READY", refreshedRow.snapshotState);
    Assert.assertTrue(refreshedRow.snapshotEpoch >= previousRow.snapshotEpoch);
  }

  /**
   * Background check must not advance progress while the leader reports non-zero sync lag. Once the
   * lagging follower catches up again, the same partition can be checked and verified in a new
   * round.
   */
  public void testBackgroundConsistencyCheckWaitsForSyncLagToClear() throws Exception {
    RegionReplicaSelection regionReplicaSelection;
    long partitionId = timePartitionId(100L);
    RepairProgressRow baselineRow;

    try (Connection connection = makeItCloseQuietly(EnvFactory.getEnv().getConnection());
        Statement statement = makeItCloseQuietly(connection.createStatement())) {
      insertAndFlushTestData(statement);
      regionReplicaSelection = selectReplicatedDataRegion(statement);
      waitForReplicationComplete(
          regionReplicaSelection.leaderNode, regionReplicaSelection.regionId);
      baselineRow = waitForCheckState(regionReplicaSelection.regionId, partitionId, "VERIFIED");
    }

    regionReplicaSelection.followerNode.stopForcibly();
    Assert.assertFalse("Follower should be stopped", regionReplicaSelection.followerNode.isAlive());

    try (Connection connection =
            makeItCloseQuietly(
                EnvFactory.getEnv()
                    .getConnection(
                        regionReplicaSelection.leaderNode,
                        SessionConfig.DEFAULT_USER,
                        SessionConfig.DEFAULT_PASSWORD,
                        BaseEnv.TREE_SQL_DIALECT));
        Statement statement = makeItCloseQuietly(connection.createStatement())) {
      insertPartitionData(statement, 103L);
      if (shouldFlushLaggingWritesBeforeFollowerRestart()) {
        statement.execute(LOCAL_FLUSH_COMMAND);
      }
    }

    waitForReplicationLag(regionReplicaSelection.leaderNode, regionReplicaSelection.regionId);
    assertDataPointCountOnNode(regionReplicaSelection.leaderNode, 12L);

    TimeUnit.MILLISECONDS.sleep(CONSISTENCY_CHECK_INTERVAL_MS * 3);
    RepairProgressRow laggingRow =
        getRepairProgressRow(
            regionReplicaSelection.leaderNode, regionReplicaSelection.regionId, partitionId);
    Assert.assertNotNull(laggingRow);
    Assert.assertEquals(
        "Background checker should keep the last verified result while syncLag > 0",
        "VERIFIED",
        laggingRow.checkState);
    Assert.assertEquals(
        "Background checker should not advance last_checked_at while syncLag > 0",
        baselineRow.lastCheckedAt,
        laggingRow.lastCheckedAt);
    Assert.assertEquals(
        "Background checker should not persist a new mutation epoch while syncLag > 0",
        baselineRow.partitionMutationEpoch,
        laggingRow.partitionMutationEpoch);
    Assert.assertEquals(
        "Background checker should not persist a new snapshot epoch while syncLag > 0",
        baselineRow.snapshotEpoch,
        laggingRow.snapshotEpoch);

    regionReplicaSelection.followerNode.start();
    waitForNodeConnectionReady(regionReplicaSelection.followerNode);
    waitForReplicationComplete(regionReplicaSelection.leaderNode, regionReplicaSelection.regionId);
    if (!shouldFlushLaggingWritesBeforeFollowerRestart()) {
      localFlushOnNode(regionReplicaSelection.leaderNode);
      waitForReplicationComplete(
          regionReplicaSelection.leaderNode, regionReplicaSelection.regionId);
    }
    waitForProgressRefresh(regionReplicaSelection.regionId, partitionId, baselineRow.lastCheckedAt);
    RepairProgressRow refreshedRow =
        waitForCheckState(regionReplicaSelection.regionId, partitionId, "VERIFIED");
    Assert.assertTrue(
        "Expected a new mutation epoch after the lagging writes are replicated",
        refreshedRow.partitionMutationEpoch > baselineRow.partitionMutationEpoch);
    Assert.assertTrue(
        "Expected a rebuilt snapshot epoch after sync lag is cleared",
        refreshedRow.snapshotEpoch > baselineRow.snapshotEpoch);
    assertDataPointCountOnNode(regionReplicaSelection.followerNode, 12L);
  }

  /**
   * Verified progress should remain queryable after the current ConfigNode leader is restarted.
   * This validates that progress is durably persisted rather than kept only in the live leader's
   * memory.
   */
  public void testRepairProgressSurvivesConfigNodeLeaderRestart() throws Exception {
    RegionReplicaSelection regionReplicaSelection;
    long partitionId = timePartitionId(100L);
    RepairProgressRow baselineRow;

    try (Connection connection = makeItCloseQuietly(EnvFactory.getEnv().getConnection());
        Statement statement = makeItCloseQuietly(connection.createStatement())) {
      insertAndFlushTestData(statement);
      regionReplicaSelection = selectReplicatedDataRegion(statement);
      waitForReplicationComplete(
          regionReplicaSelection.leaderNode, regionReplicaSelection.regionId);
      baselineRow = waitForCheckState(regionReplicaSelection.regionId, partitionId, "VERIFIED");
    }

    int leaderConfigNodeIndex = EnvFactory.getEnv().getLeaderConfigNodeIndex();
    EnvFactory.getEnv().getConfigNodeWrapperList().get(leaderConfigNodeIndex).stopForcibly();

    Awaitility.await()
        .pollDelay(1, TimeUnit.SECONDS)
        .pollInterval(1, TimeUnit.SECONDS)
        .atMost(2, TimeUnit.MINUTES)
        .untilAsserted(
            () -> {
              RepairProgressRow row =
                  getRepairProgressRow(regionReplicaSelection.regionId, partitionId);
              Assert.assertNotNull(row);
              Assert.assertEquals("VERIFIED", row.checkState);
              Assert.assertTrue(row.lastCheckedAt >= baselineRow.lastCheckedAt);
              Assert.assertTrue(row.snapshotEpoch >= baselineRow.snapshotEpoch);
            });

    EnvFactory.getEnv().getConfigNodeWrapperList().get(leaderConfigNodeIndex).start();

    Awaitility.await()
        .pollDelay(1, TimeUnit.SECONDS)
        .pollInterval(1, TimeUnit.SECONDS)
        .atMost(2, TimeUnit.MINUTES)
        .untilAsserted(
            () -> {
              RepairProgressRow row =
                  getRepairProgressRow(regionReplicaSelection.regionId, partitionId);
              Assert.assertNotNull(row);
              Assert.assertEquals("VERIFIED", row.checkState);
              Assert.assertTrue(row.lastCheckedAt >= baselineRow.lastCheckedAt);
              Assert.assertTrue(row.snapshotEpoch >= baselineRow.snapshotEpoch);
            });
  }

  /**
   * Background check should only mark mismatches. Manual repair should then consume only the
   * mismatched partition scope and restore the follower.
   */
  public void testReplicaConsistencyRepairAfterFollowerLosesSealedTsFile() throws Exception {
    RegionReplicaSelection regionReplicaSelection;
    long firstPartitionId = timePartitionId(100L);
    long secondPartitionId = timePartitionId(200L);
    Path deletedTsFile;

    try (Connection connection = makeItCloseQuietly(EnvFactory.getEnv().getConnection());
        Statement statement = makeItCloseQuietly(connection.createStatement())) {
      insertAndFlushPartitionData(statement, 100L);
      insertAndFlushPartitionData(statement, 200L);

      regionReplicaSelection = selectReplicatedDataRegion(statement);
      waitForReplicationComplete(
          regionReplicaSelection.leaderNode, regionReplicaSelection.regionId);
      waitForCheckState(regionReplicaSelection.regionId, firstPartitionId, "VERIFIED");
      waitForCheckState(regionReplicaSelection.regionId, secondPartitionId, "VERIFIED");
      deletedTsFile =
          findLatestSealedTsFile(
              regionReplicaSelection.followerNode,
              regionReplicaSelection.regionId,
              secondPartitionId);
    }

    regionReplicaSelection.followerNode.stopForcibly();
    Assert.assertFalse("Follower should be stopped", regionReplicaSelection.followerNode.isAlive());
    deleteTsFileArtifacts(deletedTsFile);

    regionReplicaSelection.followerNode.start();
    waitForNodeConnectionReady(regionReplicaSelection.followerNode);
    waitForCheckState(regionReplicaSelection.regionId, secondPartitionId, "MISMATCH");
    assertPartitionViewMismatch(regionReplicaSelection);
    assertDataPointCountOnNode(regionReplicaSelection.followerNode, 6L);

    RepairProgressRow firstPartition =
        getRepairProgressRow(regionReplicaSelection.regionId, firstPartitionId);
    RepairProgressRow secondPartition =
        getRepairProgressRow(regionReplicaSelection.regionId, secondPartitionId);
    Assert.assertNotNull(firstPartition);
    Assert.assertNotNull(secondPartition);
    Assert.assertEquals("VERIFIED", firstPartition.checkState);
    Assert.assertEquals("MISMATCH", secondPartition.checkState);

    TimeUnit.MILLISECONDS.sleep(CONSISTENCY_CHECK_INTERVAL_MS * 3);
    Assert.assertEquals(
        "Background checker should stay check-only and not auto-repair mismatches",
        "MISMATCH",
        getRepairProgressRow(regionReplicaSelection.regionId, secondPartitionId).checkState);
    assertDataPointCountOnNode(regionReplicaSelection.followerNode, 6L);

    triggerRegionConsistencyRepair(regionReplicaSelection.regionId);

    waitForCheckState(regionReplicaSelection.regionId, secondPartitionId, "VERIFIED");
    assertPartitionViewMatched(regionReplicaSelection);
    assertDataPointCountOnNode(regionReplicaSelection.followerNode, 12L);

    LOGGER.info(
        "Stopping leader DataNode {} after repair to verify repaired follower serves local data",
        regionReplicaSelection.leaderDataNodeId);
    regionReplicaSelection.leaderNode.stopForcibly();
    Assert.assertFalse("Leader should be stopped", regionReplicaSelection.leaderNode.isAlive());

    Awaitility.await()
        .pollDelay(2, TimeUnit.SECONDS)
        .atMost(2, TimeUnit.MINUTES)
        .untilAsserted(() -> assertDataPointCountOnNode(regionReplicaSelection.followerNode, 12L));
  }

  protected void insertAndFlushTestData(Statement statement) throws Exception {
    insertAndFlushPartitionData(statement, 100L);
  }

  protected void insertAndFlushPartitionData(Statement statement, long baseTimestamp)
      throws Exception {
    insertPartitionData(statement, baseTimestamp);
    statement.execute(FLUSH_COMMAND);
  }

  protected void insertPartitionData(Statement statement, long baseTimestamp) throws Exception {
    LOGGER.info(
        "Writing partition-scoped data at baseTimestamp={} to 3C3D cluster (mode: {})...",
        baseTimestamp,
        getIoTConsensusV2Mode());
    statement.execute(insertSql(baseTimestamp, 1L, 2L));
    statement.execute(insertSql(baseTimestamp + 1, 3L, 4L));
    statement.execute(insertSql(baseTimestamp + 2, 5L, 6L));
  }

  protected boolean shouldFlushLaggingWritesBeforeFollowerRestart() {
    return ConsensusFactory.IOT_CONSENSUS_V2_BATCH_MODE.equals(getIoTConsensusV2Mode());
  }

  protected void localFlushOnNode(DataNodeWrapper targetNode) throws Exception {
    try (Connection connection =
            makeItCloseQuietly(
                EnvFactory.getEnv()
                    .getConnection(
                        targetNode,
                        SessionConfig.DEFAULT_USER,
                        SessionConfig.DEFAULT_PASSWORD,
                        BaseEnv.TREE_SQL_DIALECT));
        Statement statement = makeItCloseQuietly(connection.createStatement())) {
      statement.execute(LOCAL_FLUSH_COMMAND);
    }
  }

  protected RegionReplicaSelection selectReplicatedDataRegion(Statement statement)
      throws Exception {
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
          entry.getKey(), leaderDataNodeId, followerDataNodeId, leaderNode, followerNode);
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

  /**
   * Wait until the target region's eligibility reports syncLag == 0, matching the production
   * scheduler's gating logic.
   */
  protected void waitForReplicationComplete(DataNodeWrapper leaderNode, int regionId) {
    final long timeoutSeconds = 120;
    LOGGER.info(
        "Waiting for region {} syncLag to reach 0 on leader DataNode {} (timeout: {}s)...",
        regionId,
        leaderNode.getId(),
        timeoutSeconds);
    Awaitility.await()
        .pollInterval(500, TimeUnit.MILLISECONDS)
        .atMost(timeoutSeconds, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              long syncLag = getConsistencyEligibility(leaderNode, regionId).getSyncLag();
              LOGGER.debug(
                  "Observed region {} syncLag={} on leader DataNode {}",
                  regionId,
                  syncLag,
                  leaderNode.getId());
              Assert.assertEquals(
                  "Region " + regionId + " syncLag should be 0 but was " + syncLag, 0L, syncLag);
            });
    LOGGER.info("Region {} syncLag == 0 on leader, replication is complete", regionId);
  }

  protected void waitForReplicationLag(DataNodeWrapper leaderNode, int regionId) {
    final long timeoutSeconds = 60;
    LOGGER.info(
        "Waiting for region {} syncLag to become positive on leader DataNode {} (timeout: {}s)...",
        regionId,
        leaderNode.getId(),
        timeoutSeconds);
    Awaitility.await()
        .pollInterval(500, TimeUnit.MILLISECONDS)
        .atMost(timeoutSeconds, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              long syncLag = getConsistencyEligibility(leaderNode, regionId).getSyncLag();
              LOGGER.debug(
                  "Observed region {} syncLag={} while waiting for lag on leader DataNode {}",
                  regionId,
                  syncLag,
                  leaderNode.getId());
              Assert.assertTrue(
                  "Expected region " + regionId + " syncLag > 0 while follower is lagging",
                  syncLag > 0L);
            });
    LOGGER.info("Observed region {} syncLag > 0 on leader while follower is lagging", regionId);
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

  protected void verifyDataPointCount(Statement statement, long expectedTotalCount)
      throws Exception {
    try (ResultSet countResult = statement.executeQuery(COUNT_QUERY)) {
      Assert.assertTrue("Count query should return results", countResult.next());

      int columnCount = countResult.getMetaData().getColumnCount();
      long totalCount = 0;
      for (int i = 1; i <= columnCount; i++) {
        totalCount += parseLongFromString(countResult.getString(i));
      }
      Assert.assertEquals("Unexpected total data point count", expectedTotalCount, totalCount);
    }
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

  protected void assertDataPointCountOnNode(DataNodeWrapper targetNode, long expectedTotalCount)
      throws Exception {
    try (Connection targetConnection =
            makeItCloseQuietly(
                EnvFactory.getEnv()
                    .getConnection(
                        targetNode,
                        SessionConfig.DEFAULT_USER,
                        SessionConfig.DEFAULT_PASSWORD,
                        BaseEnv.TREE_SQL_DIALECT));
        Statement targetStatement = makeItCloseQuietly(targetConnection.createStatement())) {
      verifyDataPointCount(targetStatement, expectedTotalCount);
    }
  }

  protected void waitForNodeConnectionReady(DataNodeWrapper targetNode) {
    Awaitility.await()
        .pollDelay(1, TimeUnit.SECONDS)
        .pollInterval(1, TimeUnit.SECONDS)
        .atMost(2, TimeUnit.MINUTES)
        .untilAsserted(
            () -> {
              try (Connection ignored =
                  makeItCloseQuietly(
                      EnvFactory.getEnv()
                          .getConnection(
                              targetNode,
                              SessionConfig.DEFAULT_USER,
                              SessionConfig.DEFAULT_PASSWORD,
                              BaseEnv.TREE_SQL_DIALECT))) {
                Assert.assertNotNull(
                    "Expected a JDBC connection for DataNode " + targetNode.getId(), ignored);
              } catch (SQLException e) {
                throw new AssertionError(
                    "DataNode " + targetNode.getId() + " is not accepting JDBC connections yet", e);
              }
            });
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
    return findLatestSealedTsFile(dataNodeWrapper, regionId, null);
  }

  private Path findLatestSealedTsFile(
      DataNodeWrapper dataNodeWrapper, int regionId, Long timePartitionId) throws Exception {
    try (Stream<Path> tsFiles = Files.walk(Paths.get(dataNodeWrapper.getDataPath()))) {
      Optional<Path> candidate =
          tsFiles
              .filter(Files::isRegularFile)
              .filter(path -> path.getFileName().toString().endsWith(".tsfile"))
              .filter(path -> path.toString().contains(File.separator + "root.sg" + File.separator))
              .filter(path -> belongsToRegion(path, regionId))
              .filter(
                  path -> timePartitionId == null || belongsToTimePartition(path, timePartitionId))
              .max(
                  (left, right) ->
                      Long.compare(left.toFile().lastModified(), right.toFile().lastModified()));
      if (candidate.isPresent()) {
        return candidate.get();
      }
    }
    throw new AssertionError(
        "No sealed TsFile found for region "
            + regionId
            + (timePartitionId == null ? "" : (" partition " + timePartitionId)));
  }

  private boolean belongsToRegion(Path tsFile, int regionId) {
    Path timePartitionDir = tsFile.getParent();
    Path regionDir = timePartitionDir == null ? null : timePartitionDir.getParent();
    return regionDir != null && String.valueOf(regionId).equals(regionDir.getFileName().toString());
  }

  private boolean belongsToTimePartition(Path tsFile, long timePartitionId) {
    Path timePartitionDir = tsFile.getParent();
    return timePartitionDir != null
        && String.valueOf(timePartitionId).equals(timePartitionDir.getFileName().toString());
  }

  private void deleteTsFileArtifacts(Path tsFile) throws Exception {
    Files.deleteIfExists(tsFile);
    Files.deleteIfExists(Paths.get(tsFile.toString() + TsFileResource.RESOURCE_SUFFIX));
    Files.deleteIfExists(Paths.get(tsFile.toString() + ModificationFile.FILE_SUFFIX));
    Files.deleteIfExists(Paths.get(tsFile.toString() + ModificationFile.COMPACTION_FILE_SUFFIX));
    Files.deleteIfExists(Paths.get(tsFile.toString() + ModificationFileV1.FILE_SUFFIX));
    Files.deleteIfExists(Paths.get(tsFile.toString() + ModificationFileV1.COMPACTION_FILE_SUFFIX));
  }

  private void triggerRegionConsistencyRepair(int regionId) throws Exception {
    TConsensusGroupId consensusGroupId =
        new TConsensusGroupId(TConsensusGroupType.DataRegion, regionId);
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      TSStatus status =
          client.triggerRegionConsistencyRepair(
              new TTriggerRegionConsistencyRepairReq(consensusGroupId));
      Assert.assertEquals("Replica consistency repair should succeed", 200, status.getCode());
    }
  }

  private void assertPartitionViewMismatch(RegionReplicaSelection regionReplicaSelection)
      throws Exception {
    Assert.assertNotEquals(
        "Expected leader and follower logical snapshot roots to differ before repair",
        partitionSnapshotSignature(
            getConsistencyEligibility(
                regionReplicaSelection.leaderNode, regionReplicaSelection.regionId)),
        partitionSnapshotSignature(
            getConsistencyEligibility(
                regionReplicaSelection.followerNode, regionReplicaSelection.regionId)));
  }

  private void assertPartitionViewMatched(RegionReplicaSelection regionReplicaSelection)
      throws Exception {
    Assert.assertEquals(
        "Expected leader and follower logical snapshot roots to match after repair",
        partitionSnapshotSignature(
            getConsistencyEligibility(
                regionReplicaSelection.leaderNode, regionReplicaSelection.regionId)),
        partitionSnapshotSignature(
            getConsistencyEligibility(
                regionReplicaSelection.followerNode, regionReplicaSelection.regionId)));
  }

  private TGetConsistencyEligibilityResp getConsistencyEligibility(
      DataNodeWrapper dataNodeWrapper, int regionId) throws Exception {
    try (SyncDataNodeInternalServiceClient client =
        DATA_NODE_INTERNAL_CLIENT_MANAGER.borrowClient(
            new TEndPoint(
                dataNodeWrapper.getInternalAddress(), dataNodeWrapper.getInternalPort()))) {
      TGetConsistencyEligibilityResp response =
          client.getConsistencyEligibility(
              new TGetConsistencyEligibilityReq(
                  new TConsensusGroupId(TConsensusGroupType.DataRegion, regionId)));
      Assert.assertEquals(
          "Consistency eligibility RPC should succeed on DataNode " + dataNodeWrapper.getId(),
          200,
          response.getStatus().getCode());
      return response;
    }
  }

  private List<String> partitionSnapshotSignature(
      TGetConsistencyEligibilityResp eligibilityResponse) {
    if (eligibilityResponse == null || !eligibilityResponse.isSetPartitions()) {
      return Collections.emptyList();
    }

    List<String> partitionSignatures = new ArrayList<>();
    for (TPartitionConsistencyEligibility partition : eligibilityResponse.getPartitions()) {
      partitionSignatures.add(
          partition.getTimePartitionId()
              + "|mutation="
              + partition.getPartitionMutationEpoch()
              + "|snapshot="
              + partition.getSnapshotEpoch()
              + "|state="
              + partition.getSnapshotState()
              + "|live="
              + partition.getLiveRootXorHash()
              + ":"
              + partition.getLiveRootAddHash()
              + "|tombstone="
              + partition.getTombstoneRootXorHash()
              + ":"
              + partition.getTombstoneRootAddHash());
    }
    Collections.sort(partitionSignatures);
    return partitionSignatures;
  }

  private void assertRepairProgressEmpty(int regionId) throws Exception {
    Assert.assertTrue(
        "Expected no repair progress rows yet for region " + regionId,
        getRepairProgressRows(regionId).isEmpty());
  }

  private RepairProgressRow waitForCheckState(int regionId, long timePartitionId, String checkState)
      throws Exception {
    Awaitility.await()
        .pollDelay(1, TimeUnit.SECONDS)
        .pollInterval(1, TimeUnit.SECONDS)
        .atMost(2, TimeUnit.MINUTES)
        .untilAsserted(
            () -> {
              RepairProgressRow row = getRepairProgressRow(regionId, timePartitionId);
              Assert.assertNotNull(
                  "Expected repair progress row for region "
                      + regionId
                      + " partition "
                      + timePartitionId,
                  row);
              Assert.assertEquals(checkState, row.checkState);
            });
    return getRepairProgressRow(regionId, timePartitionId);
  }

  private void waitForProgressRefresh(int regionId, long timePartitionId, long previousCheckedAt)
      throws Exception {
    Awaitility.await()
        .pollDelay(1, TimeUnit.SECONDS)
        .pollInterval(1, TimeUnit.SECONDS)
        .atMost(2, TimeUnit.MINUTES)
        .untilAsserted(
            () -> {
              RepairProgressRow row = getRepairProgressRow(regionId, timePartitionId);
              Assert.assertNotNull(row);
              Assert.assertTrue(
                  "Expected last_checked_at to advance after a new background round",
                  row.lastCheckedAt > previousCheckedAt);
            });
  }

  private RepairProgressRow getRepairProgressRow(int regionId, long timePartitionId)
      throws Exception {
    return getRepairProgressRows(regionId).stream()
        .filter(row -> row.timePartition == timePartitionId)
        .findFirst()
        .orElse(null);
  }

  private RepairProgressRow getRepairProgressRow(
      DataNodeWrapper targetNode, int regionId, long timePartitionId) throws Exception {
    return getRepairProgressRows(targetNode, regionId).stream()
        .filter(row -> row.timePartition == timePartitionId)
        .findFirst()
        .orElse(null);
  }

  private List<RepairProgressRow> getRepairProgressRows(int regionId) throws Exception {
    try (Connection connection =
            makeItCloseQuietly(EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT));
        Statement statement = makeItCloseQuietly(connection.createStatement());
        ResultSet resultSet =
            statement.executeQuery(
                "select region_id, time_partition, check_state, repair_state, last_checked_at, "
                    + "last_safe_watermark, partition_mutation_epoch, snapshot_epoch, snapshot_state, "
                    + "last_mismatch_at, mismatch_scope_ref, mismatch_leaf_count, repair_epoch, "
                    + "last_error_code, last_error_message from information_schema.repair_progress "
                    + "where region_id = "
                    + regionId
                    + " order by time_partition")) {
      List<RepairProgressRow> rows = new ArrayList<>();
      while (resultSet.next()) {
        rows.add(
            new RepairProgressRow(
                resultSet.getInt(1),
                resultSet.getLong(2),
                resultSet.getString(3),
                resultSet.getString(4),
                resultSet.getLong(5),
                resultSet.getLong(6),
                resultSet.getLong(7),
                resultSet.getLong(8),
                resultSet.getString(9),
                resultSet.getLong(10),
                resultSet.getString(11),
                resultSet.getInt(12),
                resultSet.getString(13),
                resultSet.getString(14),
                resultSet.getString(15)));
      }
      rows.sort(Comparator.comparingLong(row -> row.timePartition));
      return rows;
    }
  }

  private List<RepairProgressRow> getRepairProgressRows(DataNodeWrapper targetNode, int regionId)
      throws Exception {
    try (Connection connection =
            makeItCloseQuietly(
                EnvFactory.getEnv()
                    .getConnection(
                        targetNode,
                        SessionConfig.DEFAULT_USER,
                        SessionConfig.DEFAULT_PASSWORD,
                        BaseEnv.TABLE_SQL_DIALECT));
        Statement statement = makeItCloseQuietly(connection.createStatement());
        ResultSet resultSet =
            statement.executeQuery(
                "select region_id, time_partition, check_state, repair_state, last_checked_at, "
                    + "last_safe_watermark, partition_mutation_epoch, snapshot_epoch, snapshot_state, "
                    + "last_mismatch_at, mismatch_scope_ref, mismatch_leaf_count, repair_epoch, "
                    + "last_error_code, last_error_message from information_schema.repair_progress "
                    + "where region_id = "
                    + regionId
                    + " order by time_partition")) {
      List<RepairProgressRow> rows = new ArrayList<>();
      while (resultSet.next()) {
        rows.add(
            new RepairProgressRow(
                resultSet.getInt(1),
                resultSet.getLong(2),
                resultSet.getString(3),
                resultSet.getString(4),
                resultSet.getLong(5),
                resultSet.getLong(6),
                resultSet.getLong(7),
                resultSet.getLong(8),
                resultSet.getString(9),
                resultSet.getLong(10),
                resultSet.getString(11),
                resultSet.getInt(12),
                resultSet.getString(13),
                resultSet.getString(14),
                resultSet.getString(15)));
      }
      return rows;
    }
  }

  private long timePartitionId(long timestamp) {
    return Math.floorDiv(timestamp, TIME_PARTITION_INTERVAL);
  }

  private String insertSql(long timestamp, long speed, long temperature) {
    return String.format(
        "INSERT INTO root.sg.d1(timestamp,speed,temperature) values(%d, %d, %d)",
        timestamp, speed, temperature);
  }

  private static final class RepairProgressRow {
    private final int regionId;
    private final long timePartition;
    private final String checkState;
    private final String repairState;
    private final long lastCheckedAt;
    private final long lastSafeWatermark;
    private final long partitionMutationEpoch;
    private final long snapshotEpoch;
    private final String snapshotState;
    private final long lastMismatchAt;
    private final String mismatchScopeRef;
    private final int mismatchLeafCount;
    private final String repairEpoch;
    private final String lastErrorCode;
    private final String lastErrorMessage;

    private RepairProgressRow(
        int regionId,
        long timePartition,
        String checkState,
        String repairState,
        long lastCheckedAt,
        long lastSafeWatermark,
        long partitionMutationEpoch,
        long snapshotEpoch,
        String snapshotState,
        long lastMismatchAt,
        String mismatchScopeRef,
        int mismatchLeafCount,
        String repairEpoch,
        String lastErrorCode,
        String lastErrorMessage) {
      this.regionId = regionId;
      this.timePartition = timePartition;
      this.checkState = checkState;
      this.repairState = repairState;
      this.lastCheckedAt = lastCheckedAt;
      this.lastSafeWatermark = lastSafeWatermark;
      this.partitionMutationEpoch = partitionMutationEpoch;
      this.snapshotEpoch = snapshotEpoch;
      this.snapshotState = snapshotState;
      this.lastMismatchAt = lastMismatchAt;
      this.mismatchScopeRef = mismatchScopeRef;
      this.mismatchLeafCount = mismatchLeafCount;
      this.repairEpoch = repairEpoch;
      this.lastErrorCode = lastErrorCode;
      this.lastErrorMessage = lastErrorMessage;
    }
  }
}
