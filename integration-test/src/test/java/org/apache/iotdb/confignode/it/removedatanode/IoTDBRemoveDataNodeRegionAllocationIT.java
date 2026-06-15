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

package org.apache.iotdb.confignode.it.removedatanode;

import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.confignode.it.utils.ConfigNodeTestUtils;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.exception.InconsistentDataException;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.iotdb.confignode.it.regionmigration.IoTDBRegionOperationReliabilityITFramework.getAllRegionMap;
import static org.apache.iotdb.confignode.it.regionmigration.IoTDBRegionOperationReliabilityITFramework.getDataRegionMap;
import static org.apache.iotdb.confignode.it.removedatanode.IoTDBRemoveDataNodeUtils.awaitUntilSuccess;
import static org.apache.iotdb.confignode.it.removedatanode.IoTDBRemoveDataNodeUtils.generateRemoveString;
import static org.apache.iotdb.confignode.it.removedatanode.IoTDBRemoveDataNodeUtils.stopDataNodes;
import static org.apache.iotdb.util.MagicUtils.makeItCloseQuietly;

/**
 * Regression test for the bug where, after a {@code remove datanode} has been submitted, the
 * ConfigNode still allocated brand-new Region replicas onto the DataNode that was being removed
 * (typically a node that had been {@code kill -9}'d and was therefore reported as {@code Unknown}).
 * The stranded replica could never be created on the dead node, so the removal hung forever and the
 * target DataNode never disappeared from {@code show datanodes}.
 *
 * <p>The test kills one DataNode, submits the removal, and — while the removal is still in progress
 * — forces a fresh Region allocation by creating a new database and writing to it. It then asserts
 * that none of the <em>newly allocated</em> Regions were placed on the DataNode being removed, and
 * that the removal eventually completes.
 *
 * <p>Note: we must compare against a snapshot of the pre-existing Region ids rather than asserting
 * "no Region anywhere references the removing DataNode". The removing node legitimately keeps
 * hosting its own pre-existing Regions until each one finishes migrating away (the new replica is
 * added first and the old one is dropped last), so those Regions still list the removing node
 * during the window. Only freshly created Region groups are expected to exclude it.
 */
@Category({ClusterIT.class})
@RunWith(IoTDBTestRunner.class)
public class IoTDBRemoveDataNodeRegionAllocationIT {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBRemoveDataNodeRegionAllocationIT.class);

  private static final String SHOW_DATANODES = "show datanodes";

  private static final String DEFAULT_SCHEMA_REGION_GROUP_EXTENSION_POLICY = "CUSTOM";
  private static final String DEFAULT_DATA_REGION_GROUP_EXTENSION_POLICY = "CUSTOM";

  @Before
  public void setUp() {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionGroupExtensionPolicy(DEFAULT_SCHEMA_REGION_GROUP_EXTENSION_POLICY)
        .setDataRegionGroupExtensionPolicy(DEFAULT_DATA_REGION_GROUP_EXTENSION_POLICY);
  }

  @After
  public void tearDown() throws InterruptedException {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void newRegionMustNotBeAllocatedOnRemovingDataNodeTest() throws Exception {
    final int configNodeNum = 1;
    final int dataNodeNum = 4;
    final int dataReplicationFactor = 2;
    // Schema regions use Ratis, so migrating a schema replica off the killed node needs a majority
    // of the *old* peers to stay alive. With schemaReplicationFactor=2, killing one of the two
    // replica holders breaks Ratis quorum, the schema-region migration fails, and the whole
    // RemoveDataNodesProcedure rolls back without ever finishing. Use 3 (as the proven
    // IoTDBRemoveUnknownDataNodeIT#successTest does) so one kill still leaves a quorum and the
    // removal can actually complete.
    final int schemaReplicationFactor = 3;
    // Place a few DataRegions per DataNode so the node being removed actually owns regions
    // that have to be migrated, which keeps the RemoveDataNodesProcedure in progress long
    // enough for us to race a new allocation against it.
    final int dataRegionPerDataNode = 2;

    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setSchemaReplicationFactor(schemaReplicationFactor)
        .setDataReplicationFactor(dataReplicationFactor)
        .setDefaultDataRegionGroupNumPerDatabase(
            dataRegionPerDataNode * dataNodeNum / dataReplicationFactor);
    EnvFactory.getEnv().initClusterEnvironment(configNodeNum, dataNodeNum);

    final int removeDataNodeId;
    final List<TDataNodeLocation> removeDataNodeLocations = new ArrayList<>();

    try (final Connection connection = makeItCloseQuietly(EnvFactory.getEnv().getConnection());
        final Statement statement = makeItCloseQuietly(connection.createStatement());
        final SyncConfigNodeIServiceClient client =
            (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {

      // Seed the cluster with data so that DataRegions are spread across all DataNodes.
      ConfigNodeTestUtils.insertTreeModelData(statement);

      final Map<Integer, Set<Integer>> dataRegionMap = getDataRegionMap(statement);
      Assert.assertFalse("Expected some DataRegions to exist", dataRegionMap.isEmpty());

      // Pick a DataNode that currently hosts at least one DataRegion as the removal target.
      removeDataNodeId =
          dataRegionMap.values().stream()
              .flatMap(Set::stream)
              .findAny()
              .orElseThrow(() -> new AssertionError("No DataNode hosts a DataRegion"));
      LOGGER.info("Selected DataNode {} to remove.", removeDataNodeId);

      removeDataNodeLocations.addAll(
          client.getDataNodeConfiguration(-1).getDataNodeConfigurationMap().values().stream()
              .map(TDataNodeConfiguration::getLocation)
              .filter(location -> location.getDataNodeId() == removeDataNodeId)
              .collect(Collectors.toList()));
      Assert.assertEquals(1, removeDataNodeLocations.size());

      // kill -9 the target DataNode so that it becomes Unknown (this is the exact condition under
      // which the failure detector overrides the Removing status back to Unknown).
      final List<DataNodeWrapper> removeDataNodeWrappers =
          List.of(EnvFactory.getEnv().dataNodeIdToWrapper(removeDataNodeId).get());
      stopDataNodes(removeDataNodeWrappers);
      LOGGER.info("DataNode {} is stopped.", removeDataNodeId);
    } catch (InconsistentDataException e) {
      LOGGER.error("Unexpected error during setup:", e);
      throw e;
    }

    // Pick a DataNode that survives the removal; all SQL after the kill must be pinned to it.
    // EnvFactory.getConnection() (no args) fans every read out to *all* DataNodes, including the
    // killed one, which would fail with "Connection Error" and surface as InconsistentDataException
    // rather than the behaviour we want to test.
    final int killedDataNodePort =
        EnvFactory.getEnv().dataNodeIdToWrapper(removeDataNodeId).get().getPort();
    final DataNodeWrapper survivingDataNode =
        EnvFactory.getEnv().getDataNodeWrapperList().stream()
            .filter(wrapper -> wrapper.getPort() != killedDataNodePort)
            .findAny()
            .orElseThrow(() -> new AssertionError("No surviving DataNode to connect to"));

    // Re-establish a connection (pinned to the surviving DataNode) after the DataNode was killed.
    try (final Connection connection =
            makeItCloseQuietly(EnvFactory.getEnv().getConnection(survivingDataNode));
        final Statement statement = makeItCloseQuietly(connection.createStatement());
        final SyncConfigNodeIServiceClient client =
            (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      final AtomicReference<SyncConfigNodeIServiceClient> clientRef = new AtomicReference<>(client);

      // Wait until the killed DataNode is reported Unknown, then submit the removal.
      awaitDataNodeStatus(statement, removeDataNodeId, "Unknown");

      // Snapshot the Region ids that already exist; region migration only moves replicas of
      // these existing groups (it never mints new Region ids), so any Region id appearing after
      // this point belongs to the allocation we are about to force.
      final Set<Integer> preExistingRegionIds = new HashSet<>(getAllRegionMap(statement).keySet());

      final String removeDataNodeSQL = generateRemoveString(Set.of(removeDataNodeId));
      LOGGER.info("Submitting: {}", removeDataNodeSQL);
      statement.execute(removeDataNodeSQL);
      LOGGER.info("Remove DataNode {} submitted.", removeDataNodeId);

      // The removal is asynchronous: the SQL returns once the procedure is submitted, while the
      // actual region migration off the (dead) node keeps it in progress. Confirm it is in progress
      // before we force a new allocation against it.
      Assert.assertTrue(
          "Removal completed before we could force a new allocation; cannot exercise the bug",
          isRemovalInProgress(clientRef, removeDataNodeLocations));

      // While the removal is in progress, force a fresh Region allocation by creating a new
      // database and writing to it. Before the fix, the allocator could still choose the removing
      // (Unknown) DataNode as a replica holder for these new regions.
      try (final Connection probeConnection =
              makeItCloseQuietly(EnvFactory.getEnv().getConnection(survivingDataNode));
          final Statement probeStatement = makeItCloseQuietly(probeConnection.createStatement())) {
        for (int i = 0; i < 64; i++) {
          probeStatement.addBatch(
              String.format(
                  "INSERT INTO root.alloc_probe.d%d(timestamp,speed) values(%d, %d)", i, i, i));
        }
        probeStatement.executeBatch();
        LOGGER.info("Forced new Region allocation via root.alloc_probe.");

        // The core assertion: none of the newly allocated Regions may land on the removing
        // DataNode.
        assertNewRegionsExcludeDataNode(probeStatement, preExistingRegionIds, removeDataNodeId);
      }

      // The removal must still be able to complete; the original bug left it stuck forever.
      awaitUntilSuccess(clientRef, removeDataNodeLocations);
      LOGGER.info("Remove DataNode {} completed.", removeDataNodeId);

      // Final guard: after the node is gone, nothing references it any more.
      assertNoRegionOnDataNode(statement, removeDataNodeId);
      assertDataNodeAbsent(statement, removeDataNodeId);
    } catch (InconsistentDataException e) {
      LOGGER.error("Unexpected error:", e);
      throw e;
    }
  }

  private static boolean isRemovalInProgress(
      final AtomicReference<SyncConfigNodeIServiceClient> clientRef,
      final List<TDataNodeLocation> removeDataNodeLocations) {
    try {
      final List<TDataNodeLocation> remaining =
          clientRef
              .get()
              .getDataNodeConfiguration(-1)
              .getDataNodeConfigurationMap()
              .values()
              .stream()
              .map(TDataNodeConfiguration::getLocation)
              .collect(Collectors.toList());
      return removeDataNodeLocations.stream().anyMatch(remaining::contains);
    } catch (Exception e) {
      LOGGER.warn("Failed to query DataNode configuration", e);
      return false;
    }
  }

  private static void awaitDataNodeStatus(
      final Statement statement, final int dataNodeId, final String expectedStatus) {
    Awaitility.await()
        .atMost(2, TimeUnit.MINUTES)
        .pollDelay(1, TimeUnit.SECONDS)
        .until(
            () -> {
              try (final ResultSet result = statement.executeQuery(SHOW_DATANODES)) {
                while (result.next()) {
                  if (result.getInt(ColumnHeaderConstant.NODE_ID) == dataNodeId) {
                    return expectedStatus.equalsIgnoreCase(
                        result.getString(ColumnHeaderConstant.STATUS));
                  }
                }
              }
              return false;
            });
  }

  /**
   * Wait until the forced allocation has produced at least one new Region (a Region id not present
   * in {@code preExistingRegionIds}), then assert that none of those new Regions has a replica on
   * {@code dataNodeId}.
   */
  private static void assertNewRegionsExcludeDataNode(
      final Statement statement, final Set<Integer> preExistingRegionIds, final int dataNodeId) {
    final AtomicReference<Map<Integer, Set<Integer>>> newRegionsRef = new AtomicReference<>();
    Awaitility.await()
        .atMost(1, TimeUnit.MINUTES)
        .pollDelay(1, TimeUnit.SECONDS)
        .until(
            () -> {
              final Map<Integer, Set<Integer>> newRegions =
                  getAllRegionMap(statement).entrySet().stream()
                      .filter(entry -> !preExistingRegionIds.contains(entry.getKey()))
                      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
              newRegionsRef.set(newRegions);
              return !newRegions.isEmpty();
            });

    final Map<Integer, Set<Integer>> newRegions = newRegionsRef.get();
    final Set<Integer> offendingRegions =
        newRegions.entrySet().stream()
            .filter(entry -> entry.getValue().contains(dataNodeId))
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
    Assert.assertTrue(
        String.format(
            "Newly allocated Region(s) %s were placed on DataNode %d which is being removed; "
                + "new Region map: %s",
            offendingRegions, dataNodeId, newRegions),
        offendingRegions.isEmpty());
  }

  private static void assertNoRegionOnDataNode(final Statement statement, final int dataNodeId)
      throws Exception {
    final Map<Integer, Set<Integer>> allRegionMap = getAllRegionMap(statement);
    final Set<Integer> offendingRegions =
        allRegionMap.entrySet().stream()
            .filter(entry -> entry.getValue().contains(dataNodeId))
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
    Assert.assertTrue(
        String.format(
            "Region(s) %s still reference removed DataNode %d; full map: %s",
            offendingRegions, dataNodeId, allRegionMap),
        offendingRegions.isEmpty());
  }

  private static void assertDataNodeAbsent(final Statement statement, final int dataNodeId)
      throws Exception {
    try (final ResultSet result = statement.executeQuery(SHOW_DATANODES)) {
      while (result.next()) {
        Assert.assertNotEquals(
            "DataNode " + dataNodeId + " should have been removed",
            dataNodeId,
            result.getInt(ColumnHeaderConstant.NODE_ID));
      }
    }
  }
}
