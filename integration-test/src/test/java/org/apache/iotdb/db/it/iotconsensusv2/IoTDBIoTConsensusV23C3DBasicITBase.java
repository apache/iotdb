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
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.record.Tablet;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
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
      "INSERT INTO root.sg.d1(timestamp, speed, temperature, power) VALUES (100, 1, 2, 3)";
  protected static final String INSERTION2 =
      "INSERT INTO root.sg.d1(timestamp, speed, temperature, power) VALUES (101, 4, 5, 6)";
  protected static final String INSERTION3 =
      "INSERT INTO root.sg.d1(timestamp, speed, temperature, power) VALUES (102, 7, 8, 9)";
  protected static final String FLUSH_COMMAND = "flush on cluster";
  protected static final String COUNT_QUERY = "select count(*) from root.sg.**";
  protected static final String SELECT_ALL_QUERY =
      "select speed, temperature, power from root.sg.d1";
  protected static final String DELETE_TIMESERIES_SPEED = "DELETE TIMESERIES root.sg.d1.speed";
  protected static final String SHOW_TIMESERIES_D1 = "SHOW TIMESERIES root.sg.d1.*";
  protected static final String SELECT_SURVIVING_QUERY =
      "SELECT temperature, power FROM root.sg.d1";
  protected static final String OBJECT_DATABASE = "object_db";
  protected static final String OBJECT_TABLE = "object_table";
  protected static final long OBJECT_TIMESTAMP = 1L;

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

  /**
   * Test that DELETE TIMESERIES is properly replicated to all DataNode replicas via IoTConsensusV2.
   *
   * <p>This test reproduces the scenario from the historical deletion replication bug: when a
   * timeseries is deleted after data insertion (with some unflushed data), the deletion event must
   * be consistently replicated to all replicas. After waiting for replication to complete, stopping
   * each DataNode in turn should show the same schema on all surviving nodes.
   *
   * <p>Scenario:
   *
   * <ol>
   *   <li>Insert data into root.sg.d1 with 3 measurements (speed, temperature, power), flush
   *   <li>Insert more data (unflushed to create WAL-only entries)
   *   <li>DELETE TIMESERIES root.sg.d1.speed
   *   <li>Flush again to persist deletion
   *   <li>Wait for replication to complete on all DataNodes
   *   <li>Verify that every DataNode independently shows the same timeseries (speed is gone)
   * </ol>
   */
  public void testDeleteTimeSeriesReplicaConsistency() throws Exception {
    try (Connection connection = makeItCloseQuietly(EnvFactory.getEnv().getConnection());
        Statement statement = makeItCloseQuietly(connection.createStatement())) {

      // Step 1: Insert data with 3 measurements and flush
      LOGGER.info(
          "Step 1: Inserting data with 3 measurements and flushing (mode: {})...",
          getIoTConsensusV2Mode());
      statement.execute(INSERTION1);
      statement.execute(INSERTION2);
      statement.execute(FLUSH_COMMAND);

      // Step 2: Insert more data without flush (creates WAL-only entries)
      LOGGER.info("Step 2: Inserting more data without flush (WAL-only entries)...");
      statement.execute(INSERTION3);

      // Step 3: Delete one timeseries
      LOGGER.info("Step 3: Deleting timeseries root.sg.d1.speed...");
      statement.execute(DELETE_TIMESERIES_SPEED);

      // Step 4: Flush again to persist the deletion
      LOGGER.info("Step 4: Flushing to persist deletion...");
      statement.execute(FLUSH_COMMAND);

      // Verify on the current connection: speed should be gone, 2 timeseries remain
      verifyTimeSeriesAfterDelete(statement, "via initial connection");

      // Step 5: Wait for replication to complete on data region leaders
      LOGGER.info("Step 5: Waiting for replication to complete on data region leaders...");
      Map<Integer, Pair<Integer, Set<Integer>>> dataRegionMap =
          getDataRegionMapWithLeader(statement);
      Set<Integer> leaderNodeIds = new HashSet<>();
      for (Pair<Integer, Set<Integer>> leaderAndReplicas : dataRegionMap.values()) {
        if (leaderAndReplicas.getLeft() > 0) {
          leaderNodeIds.add(leaderAndReplicas.getLeft());
        }
      }
      for (int leaderNodeId : leaderNodeIds) {
        EnvFactory.getEnv()
            .dataNodeIdToWrapper(leaderNodeId)
            .ifPresent(this::waitForReplicationComplete);
      }

      // Step 6: Verify schema consistency on each DataNode independently
      LOGGER.info("Step 6: Verifying schema consistency on each DataNode independently...");
      List<DataNodeWrapper> dataNodeWrappers = EnvFactory.getEnv().getDataNodeWrapperList();
      for (DataNodeWrapper wrapper : dataNodeWrappers) {
        String nodeDescription = "DataNode " + wrapper.getIp() + ":" + wrapper.getPort();
        LOGGER.info("Verifying schema on {}", nodeDescription);
        Awaitility.await()
            .atMost(60, TimeUnit.SECONDS)
            .untilAsserted(
                () -> {
                  try (Connection nodeConn =
                          makeItCloseQuietly(
                              EnvFactory.getEnv()
                                  .getConnection(
                                      wrapper,
                                      SessionConfig.DEFAULT_USER,
                                      SessionConfig.DEFAULT_PASSWORD,
                                      BaseEnv.TREE_SQL_DIALECT));
                      Statement nodeStmt = makeItCloseQuietly(nodeConn.createStatement())) {
                    verifyTimeSeriesAfterDelete(nodeStmt, nodeDescription);
                  }
                });
      }

      // Step 7: Stop each DataNode one by one and verify remaining nodes still consistent
      LOGGER.info(
          "Step 7: Stopping each DataNode in turn and verifying remaining nodes show consistent schema...");
      for (DataNodeWrapper stoppedNode : dataNodeWrappers) {
        String stoppedDesc = "DataNode " + stoppedNode.getIp() + ":" + stoppedNode.getPort();
        LOGGER.info("Stopping {}", stoppedDesc);
        stoppedNode.stopForcibly();
        Assert.assertFalse(stoppedDesc + " should be stopped", stoppedNode.isAlive());

        try {
          // Verify schema on each surviving node
          for (DataNodeWrapper aliveNode : dataNodeWrappers) {
            if (aliveNode == stoppedNode) {
              continue;
            }
            String aliveDesc = "DataNode " + aliveNode.getIp() + ":" + aliveNode.getPort();
            Awaitility.await()
                .pollDelay(1, TimeUnit.SECONDS)
                .atMost(90, TimeUnit.SECONDS)
                .untilAsserted(
                    () -> {
                      try (Connection aliveConn =
                              makeItCloseQuietly(
                                  EnvFactory.getEnv()
                                      .getConnection(
                                          aliveNode,
                                          SessionConfig.DEFAULT_USER,
                                          SessionConfig.DEFAULT_PASSWORD,
                                          BaseEnv.TREE_SQL_DIALECT));
                          Statement aliveStmt = makeItCloseQuietly(aliveConn.createStatement())) {
                        verifyTimeSeriesAfterDelete(
                            aliveStmt, aliveDesc + " (while " + stoppedDesc + " is down)");
                      }
                    });
          }
        } finally {
          // Restart the stopped node before moving to the next iteration
          LOGGER.info("Restarting {}", stoppedDesc);
          stoppedNode.start();
          // Wait for the restarted node to rejoin
          Awaitility.await()
              .atMost(120, TimeUnit.SECONDS)
              .pollInterval(2, TimeUnit.SECONDS)
              .until(stoppedNode::isAlive);
        }
      }

      LOGGER.info(
          "DELETE TIMESERIES replica consistency test passed for mode: {}",
          getIoTConsensusV2Mode());
    }
  }

  /**
   * Test that OBJECT values written as object-file pieces are replicated to every IoTConsensusV2
   * replica, and that a follower can still serve the object metadata after the former leader stops.
   */
  public void testObjectReplicaConsistency() throws Exception {
    final byte[] objectBytes = readObjectExampleBytes();

    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("CREATE DATABASE IF NOT EXISTS " + OBJECT_DATABASE);
      session.executeNonQueryStatement("USE " + OBJECT_DATABASE);
      session.executeNonQueryStatement(
          "CREATE TABLE "
              + OBJECT_TABLE
              + " (region_id STRING TAG, plant_id STRING TAG, device_id STRING TAG, "
              + "temperature FLOAT FIELD, file OBJECT FIELD)");
      insertObjectTablet(session, objectBytes);
      session.executeNonQueryStatement("FLUSH");
    }

    try (Connection tableConnection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement tableStatement = tableConnection.createStatement()) {
      tableStatement.execute("USE " + OBJECT_DATABASE);
      verifyObjectLength(tableStatement, "initial table connection", objectBytes.length);

      final Map<Integer, Pair<Integer, Set<Integer>>> dataRegionMap =
          waitForDataRegionMap(tableStatement);
      final Set<Integer> leaderNodeIds = new HashSet<>();
      for (final Pair<Integer, Set<Integer>> leaderAndReplicas : dataRegionMap.values()) {
        if (leaderAndReplicas.getLeft() > 0) {
          leaderNodeIds.add(leaderAndReplicas.getLeft());
        }
      }
      for (final int leaderNodeId : leaderNodeIds) {
        EnvFactory.getEnv()
            .dataNodeIdToWrapper(leaderNodeId)
            .ifPresent(this::waitForReplicationComplete);
      }

      final ObjectRegionSelection targetRegion = waitForObjectRegion(dataRegionMap, objectBytes);
      final int targetRegionId = targetRegion.regionId;
      final int leaderDataNodeId = targetRegion.leaderDataNodeId;
      final int followerDataNodeId = targetRegion.followerDataNodeId;
      final Set<Integer> targetReplicaIds = targetRegion.replicaDataNodeIds;
      verifyObjectFileOnReplicas(targetRegionId, targetReplicaIds, objectBytes);

      final DataNodeWrapper leaderNode =
          EnvFactory.getEnv()
              .dataNodeIdToWrapper(leaderDataNodeId)
              .orElseThrow(() -> new AssertionError("Leader DataNode not found"));
      final DataNodeWrapper followerNode =
          EnvFactory.getEnv()
              .dataNodeIdToWrapper(followerDataNodeId)
              .orElseThrow(() -> new AssertionError("Follower DataNode not found"));

      LOGGER.info(
          "Stopping object-region leader DataNode {} (region {})",
          leaderDataNodeId,
          targetRegionId);
      leaderNode.stopForcibly();
      Assert.assertFalse("Leader should be stopped", leaderNode.isAlive());

      Awaitility.await()
          .pollDelay(2, TimeUnit.SECONDS)
          .atMost(2, TimeUnit.MINUTES)
          .untilAsserted(
              () -> {
                try (Connection followerConnection =
                        EnvFactory.getEnv()
                            .getConnection(
                                followerNode,
                                SessionConfig.DEFAULT_USER,
                                SessionConfig.DEFAULT_PASSWORD,
                                BaseEnv.TABLE_SQL_DIALECT);
                    Statement followerStatement = followerConnection.createStatement()) {
                  followerStatement.execute("USE " + OBJECT_DATABASE);
                  verifyObjectLength(
                      followerStatement, "follower after object leader stop", objectBytes.length);
                }
              });
    }
  }

  private Map<Integer, Pair<Integer, Set<Integer>>> waitForDataRegionMap(
      final Statement statement) {
    final AtomicReference<Map<Integer, Pair<Integer, Set<Integer>>>> dataRegionMapReference =
        new AtomicReference<>(Collections.emptyMap());
    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              final Map<Integer, Pair<Integer, Set<Integer>>> dataRegionMap =
                  getDataRegionMapWithLeader(statement);
              Assert.assertFalse(
                  "Expected at least one non-system DataRegion", dataRegionMap.isEmpty());
              dataRegionMapReference.set(dataRegionMap);
            });
    return dataRegionMapReference.get();
  }

  private ObjectRegionSelection waitForObjectRegion(
      final Map<Integer, Pair<Integer, Set<Integer>>> dataRegionMap, final byte[] objectBytes) {
    final AtomicReference<ObjectRegionSelection> targetRegionReference = new AtomicReference<>();
    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              final ObjectRegionSelection targetRegion =
                  selectObjectRegion(dataRegionMap, objectBytes);
              Assert.assertNotNull(
                  "Should find a replicated data region containing the object file. "
                      + describeObjectFiles(dataRegionMap, objectBytes),
                  targetRegion);
              targetRegionReference.set(targetRegion);
            });
    return targetRegionReference.get();
  }

  private ObjectRegionSelection selectObjectRegion(
      final Map<Integer, Pair<Integer, Set<Integer>>> dataRegionMap, final byte[] objectBytes)
      throws IOException {
    for (final Map.Entry<Integer, Pair<Integer, Set<Integer>>> entry : dataRegionMap.entrySet()) {
      final Pair<Integer, Set<Integer>> leaderAndReplicas = entry.getValue();
      if (leaderAndReplicas.getRight().size() > 1
          && leaderAndReplicas.getRight().size() <= DATA_REPLICATION_FACTOR
          && leaderAndReplicas.getLeft() > 0
          && containsObjectFileInAnyReplica(
              entry.getKey(), leaderAndReplicas.getRight(), objectBytes)) {
        final int leaderDataNodeId = leaderAndReplicas.getLeft();
        final int followerDataNodeId =
            leaderAndReplicas.getRight().stream()
                .filter(i -> i != leaderDataNodeId)
                .findAny()
                .orElse(-1);
        if (followerDataNodeId > 0) {
          return new ObjectRegionSelection(
              entry.getKey(), leaderDataNodeId, followerDataNodeId, leaderAndReplicas.getRight());
        }
      }
    }
    return null;
  }

  private byte[] readObjectExampleBytes() throws IOException {
    final String testObject =
        System.getProperty("user.dir")
            + File.separator
            + "target"
            + File.separator
            + "test-classes"
            + File.separator
            + "object-example.pt";
    return Files.readAllBytes(Paths.get(testObject));
  }

  private void insertObjectTablet(final ITableSession session, final byte[] objectBytes)
      throws Exception {
    final Tablet tablet =
        new Tablet(
            OBJECT_TABLE,
            Arrays.asList("region_id", "plant_id", "device_id", "temperature", "file"),
            Arrays.asList(
                TSDataType.STRING,
                TSDataType.STRING,
                TSDataType.STRING,
                TSDataType.FLOAT,
                TSDataType.OBJECT),
            Arrays.asList(
                ColumnCategory.TAG,
                ColumnCategory.TAG,
                ColumnCategory.TAG,
                ColumnCategory.FIELD,
                ColumnCategory.FIELD),
            1);
    final int rowIndex = tablet.getRowSize();
    tablet.addTimestamp(rowIndex, OBJECT_TIMESTAMP);
    tablet.addValue(rowIndex, 0, "1");
    tablet.addValue(rowIndex, 1, "5");
    tablet.addValue(rowIndex, 2, "3");
    tablet.addValue(rowIndex, 3, 37.6F);
    tablet.addValue(rowIndex, 4, true, 0, objectBytes);
    session.insert(tablet);
  }

  private void verifyObjectLength(
      final Statement statement, final String context, final long expectedObjectLength)
      throws Exception {
    try (ResultSet resultSet =
        statement.executeQuery(
            "SELECT length(file) FROM "
                + OBJECT_TABLE
                + " WHERE time = "
                + OBJECT_TIMESTAMP
                + " AND region_id = '1' AND plant_id = '5' AND device_id = '3'")) {
      Assert.assertTrue("[" + context + "] OBJECT row should exist", resultSet.next());
      Assert.assertEquals(
          "[" + context + "] Unexpected OBJECT length",
          expectedObjectLength,
          parseLongFromString(resultSet.getString(1)));
      Assert.assertFalse("[" + context + "] Only one OBJECT row is expected", resultSet.next());
    }
  }

  private boolean containsObjectFileInAnyReplica(
      final int regionId, final Set<Integer> replicaIds, final byte[] expectedContent)
      throws IOException {
    for (final int dataNodeId : replicaIds) {
      final DataNodeWrapper wrapper =
          EnvFactory.getEnv()
              .dataNodeIdToWrapper(dataNodeId)
              .orElseThrow(() -> new AssertionError("DataNode not found: " + dataNodeId));
      if (containsObjectFile(wrapper, regionId, expectedContent)) {
        return true;
      }
    }
    return false;
  }

  private void verifyObjectFileOnReplicas(
      final int regionId, final Set<Integer> replicaIds, final byte[] expectedContent) {
    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              for (final int dataNodeId : replicaIds) {
                final DataNodeWrapper wrapper =
                    EnvFactory.getEnv()
                        .dataNodeIdToWrapper(dataNodeId)
                        .orElseThrow(() -> new AssertionError("DataNode not found: " + dataNodeId));
                Assert.assertTrue(
                    "Expected object file on DataNode "
                        + dataNodeId
                        + " for region "
                        + regionId
                        + ". "
                        + describeObjectDir(
                            new File(wrapper.getDataNodeObjectDir(), String.valueOf(regionId)),
                            expectedContent),
                    containsObjectFile(wrapper, regionId, expectedContent));
              }
            });
  }

  private boolean containsObjectFile(
      final DataNodeWrapper wrapper, final int regionId, final byte[] expectedContent)
      throws IOException {
    final File objectRegionDir = new File(wrapper.getDataNodeObjectDir(), String.valueOf(regionId));
    return containsObjectFile(objectRegionDir, expectedContent);
  }

  private boolean containsObjectFile(final File file, final byte[] expectedContent)
      throws IOException {
    if (!file.exists()) {
      return false;
    }
    if (file.isFile()) {
      return file.getName().endsWith(".bin")
          && (expectedContent == null
              || Arrays.equals(expectedContent, Files.readAllBytes(file.toPath())));
    }
    final File[] children = file.listFiles();
    if (children == null) {
      return false;
    }
    for (final File child : children) {
      if (containsObjectFile(child, expectedContent)) {
        return true;
      }
    }
    return false;
  }

  private String describeObjectFiles(
      final Map<Integer, Pair<Integer, Set<Integer>>> dataRegionMap, final byte[] expectedContent)
      throws IOException {
    final StringBuilder builder = new StringBuilder("Object files by region:");
    for (final Map.Entry<Integer, Pair<Integer, Set<Integer>>> entry : dataRegionMap.entrySet()) {
      builder.append(" region ").append(entry.getKey()).append(" ->");
      for (final int dataNodeId : entry.getValue().getRight()) {
        final DataNodeWrapper wrapper =
            EnvFactory.getEnv()
                .dataNodeIdToWrapper(dataNodeId)
                .orElseThrow(() -> new AssertionError("DataNode not found: " + dataNodeId));
        builder
            .append(" DataNode ")
            .append(dataNodeId)
            .append(": ")
            .append(
                describeObjectDir(
                    new File(wrapper.getDataNodeObjectDir(), String.valueOf(entry.getKey())),
                    expectedContent));
      }
    }
    return builder.toString();
  }

  private String describeObjectDir(final File file, final byte[] expectedContent)
      throws IOException {
    if (!file.exists()) {
      return "missing(" + file.getPath() + ")";
    }
    if (file.isFile()) {
      if (!file.getName().endsWith(".bin")) {
        return "non-object-file(" + file.getPath() + ")";
      }
      final byte[] content = Files.readAllBytes(file.toPath());
      return file.getPath()
          + "[length="
          + content.length
          + ", match="
          + Arrays.equals(expectedContent, content)
          + "]";
    }
    final File[] children = file.listFiles();
    if (children == null || children.length == 0) {
      return "empty(" + file.getPath() + ")";
    }
    final StringBuilder builder = new StringBuilder();
    for (final File child : children) {
      final String childDescription = describeObjectDir(child, expectedContent);
      if (childDescription.contains(".bin") || childDescription.startsWith("missing")) {
        if (builder.length() > 0) {
          builder.append(", ");
        }
        builder.append(childDescription);
      }
    }
    return builder.length() == 0 ? "no-bin-under(" + file.getPath() + ")" : builder.toString();
  }

  private static class ObjectRegionSelection {

    private final int regionId;
    private final int leaderDataNodeId;
    private final int followerDataNodeId;
    private final Set<Integer> replicaDataNodeIds;

    private ObjectRegionSelection(
        final int regionId,
        final int leaderDataNodeId,
        final int followerDataNodeId,
        final Set<Integer> replicaDataNodeIds) {
      this.regionId = regionId;
      this.leaderDataNodeId = leaderDataNodeId;
      this.followerDataNodeId = followerDataNodeId;
      this.replicaDataNodeIds = replicaDataNodeIds;
    }
  }

  /**
   * Verify that after deleting root.sg.d1.speed, only temperature and power timeseries remain, and
   * that data queries do not return the deleted timeseries.
   */
  private void verifyTimeSeriesAfterDelete(Statement statement, String context) throws Exception {
    // Verify via SHOW TIMESERIES: speed should be gone, only temperature and power remain
    Set<String> timeseries = new HashSet<>();
    try (ResultSet resultSet = statement.executeQuery(SHOW_TIMESERIES_D1)) {
      while (resultSet.next()) {
        timeseries.add(resultSet.getString("Timeseries"));
      }
    }
    LOGGER.info("[{}] SHOW TIMESERIES result: {}", context, timeseries);
    Assert.assertEquals(
        "[" + context + "] Expected exactly 2 timeseries after delete (temperature, power)",
        2,
        timeseries.size());
    Assert.assertFalse(
        "[" + context + "] root.sg.d1.speed should have been deleted",
        timeseries.contains("root.sg.d1.speed"));
    Assert.assertTrue(
        "[" + context + "] root.sg.d1.temperature should still exist",
        timeseries.contains("root.sg.d1.temperature"));
    Assert.assertTrue(
        "[" + context + "] root.sg.d1.power should still exist",
        timeseries.contains("root.sg.d1.power"));

    // Verify via SELECT: only temperature and power columns should return data
    try (ResultSet selectResult = statement.executeQuery(SELECT_SURVIVING_QUERY)) {
      int rowCount = 0;
      while (selectResult.next()) {
        rowCount++;
      }
      // After delete, remaining data depends on whether unflushed data for the deleted
      // timeseries was also cleaned up. We mainly verify that the query doesn't fail
      // and that some rows are returned for the surviving measurements.
      Assert.assertTrue(
          "[" + context + "] Expected at least 1 row from SELECT on surviving timeseries",
          rowCount >= 1);
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
          "Expected 9 total data points (3 timestamps x 3 measurements)", 9, totalCount);
    }

    int rowCount = 0;
    try (ResultSet selectResult = statement.executeQuery(SELECT_ALL_QUERY)) {
      while (selectResult.next()) {
        rowCount++;
        long timestamp = parseLongFromString(selectResult.getString(1));
        long speed = parseLongFromString(selectResult.getString(2));
        long temperature = parseLongFromString(selectResult.getString(3));
        long power = parseLongFromString(selectResult.getString(4));
        if (timestamp == 100) {
          Assert.assertEquals(1, speed);
          Assert.assertEquals(2, temperature);
          Assert.assertEquals(3, power);
        } else if (timestamp == 101) {
          Assert.assertEquals(4, speed);
          Assert.assertEquals(5, temperature);
          Assert.assertEquals(6, power);
        } else if (timestamp == 102) {
          Assert.assertEquals(7, speed);
          Assert.assertEquals(8, temperature);
          Assert.assertEquals(9, power);
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
