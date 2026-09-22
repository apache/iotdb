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

package org.apache.iotdb.confignode.it.partition;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.cluster.RegionStatus;
import org.apache.iotdb.commons.partition.executor.hash.BKDRHashExecutor;
import org.apache.iotdb.confignode.it.utils.ConfigNodeTestUtils;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchemaResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetDatabaseReq;
import org.apache.iotdb.confignode.rpc.thrift.TRegionInfo;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionResp;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.commons.schema.SchemaConstant.ALL_MATCH_SCOPE_BINARY;
import static org.apache.iotdb.confignode.it.utils.ConfigNodeTestUtils.generatePatternTreeBuffer;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBProactiveRegionGroupExtensionIT {

  private static final String DATABASE = "root.proactive";
  private static final int SERIES_SLOT_NUM = 32;
  private static final int MAX_REGION_GROUP_NUM = 4;
  private static final long TIME_PARTITION_INTERVAL = 10;
  private static final BKDRHashExecutor PARTITION_EXECUTOR = new BKDRHashExecutor(SERIES_SLOT_NUM);

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testMinimumGrowthCountsOnlyDistinctPendingSlots() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.SIMPLE_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.SIMPLE_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.SIMPLE_CONSENSUS)
        .setSchemaReplicationFactor(1)
        .setDataReplicationFactor(1)
        .setSchemaRegionGroupExtensionPolicy("PROACTIVE")
        .setDataRegionGroupExtensionPolicy("PROACTIVE")
        .setSchemaRegionPerDataNode(4)
        .setDataRegionPerDataNode(4)
        .setTimePartitionInterval(TIME_PARTITION_INTERVAL);
    EnvFactory.getEnv().initClusterEnvironment(1, 1);
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      assertSuccess(
          client.setDatabase(
              new TDatabaseSchema(DATABASE)
                  .setMinSchemaRegionGroupNum(4)
                  .setMinDataRegionGroupNum(4)));
      Map<String, List<TSeriesPartitionSlot>> schemaRequest = new HashMap<>();
      schemaRequest.put(
          DATABASE, Arrays.asList(new TSeriesPartitionSlot(0), new TSeriesPartitionSlot(0)));
      assertSuccess(client.getOrCreateSchemaPartitionTableWithSlots(schemaRequest).getStatus());
      assertRegionGroups(client, TConsensusGroupType.SchemaRegion, 1, 1);

      createDataPartitions(client, 0, 2, 0, 1);
      assertRegionGroups(client, TConsensusGroupType.DataRegion, 2, 1);
      // Slots 0 and 1 already have this time partition; only slot 2 contributes to minimum growth.
      createDataPartitions(client, 0, 3, 0, 1);
      assertRegionGroups(client, TConsensusGroupType.DataRegion, 3, 1);
      createDataPartitions(client, 0, 3, 0, 1);
      assertRegionGroups(client, TConsensusGroupType.DataRegion, 3, 1);
      // A genuinely new time partition for an existing series slot still grows toward the minimum.
      createDataPartitions(client, 2, 3, 1, 2);
      assertRegionGroups(client, TConsensusGroupType.DataRegion, 4, 1);
    }
  }

  @Test
  public void testSingleConfigNodeAndDataNode() throws Exception {
    checkProactiveExtension(
        1, 1, ConsensusFactory.SIMPLE_CONSENSUS, ConsensusFactory.SIMPLE_CONSENSUS);
  }

  @Test
  public void testTwoConfigNodesAndDataNodes() throws Exception {
    checkProactiveExtension(
        2, 2, ConsensusFactory.RATIS_CONSENSUS, ConsensusFactory.RATIS_CONSENSUS);
  }

  @Test
  public void testThreeConfigNodesAndDataNodes() throws Exception {
    checkProactiveExtension(
        3, 3, ConsensusFactory.RATIS_CONSENSUS, ConsensusFactory.RATIS_CONSENSUS);
  }

  @Test
  public void testSingleConfigNodeAndThreeDataNodes() throws Exception {
    checkProactiveExtension(1, 3, ConsensusFactory.RATIS_CONSENSUS, ConsensusFactory.IOT_CONSENSUS);
  }

  @Test
  public void testThreeConfigNodesAndSingleDataNode() throws Exception {
    checkProactiveExtension(
        3, 1, ConsensusFactory.SIMPLE_CONSENSUS, ConsensusFactory.SIMPLE_CONSENSUS);
  }

  private void checkProactiveExtension(
      int configNodeCount, int dataNodeCount, String schemaConsensus, String dataConsensus)
      throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setConfigNodeConsensusProtocolClass(
            configNodeCount == 1 && dataNodeCount == 1
                ? ConsensusFactory.SIMPLE_CONSENSUS
                : ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(schemaConsensus)
        .setDataRegionConsensusProtocolClass(dataConsensus)
        .setSchemaReplicationFactor(dataNodeCount)
        .setDataReplicationFactor(dataNodeCount)
        .setSchemaRegionGroupExtensionPolicy("PROACTIVE")
        .setDataRegionGroupExtensionPolicy("PROACTIVE")
        .setSchemaRegionPerDataNode(MAX_REGION_GROUP_NUM)
        .setDataRegionPerDataNode(MAX_REGION_GROUP_NUM)
        .setSeriesSlotNum(SERIES_SLOT_NUM)
        .setSeriesPartitionExecutorClass(BKDRHashExecutor.class.getName())
        .setTimePartitionInterval(TIME_PARTITION_INTERVAL);
    EnvFactory.getEnv().initClusterEnvironment(configNodeCount, dataNodeCount);

    String[] paths = new String[MAX_REGION_GROUP_NUM + 2];
    for (int slot = 0; slot < paths.length; slot++) {
      paths[slot] = findPathInSlot(slot, null);
    }

    Set<TConsensusGroupId> schemaGroups;
    Set<TConsensusGroupId> dataGroups;
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      assertSuccess(
          client.setDatabase(
              new TDatabaseSchema(DATABASE)
                  .setMinSchemaRegionGroupNum(2)
                  .setMinDataRegionGroupNum(2)));
      assertMaxRegionGroupNum(client);
      assertRegionGroups(client, TConsensusGroupType.SchemaRegion, 0, dataNodeCount);
      assertRegionGroups(client, TConsensusGroupType.DataRegion, 0, dataNodeCount);

      // A minimum of two is reached gradually, adding one group per pending schema slot.
      for (int slot = 0; slot < 3; slot++) {
        createSchemaPartitions(client, paths[slot]);
        assertRegionGroups(client, TConsensusGroupType.SchemaRegion, slot + 1, dataNodeCount);
      }
      // Measurements, repeated requests and another device hashing to the same slot do not grow.
      createSchemaPartitions(client, paths[0], paths[0] + "2", findPathInSlot(0, paths[0]));
      schemaGroups = assertRegionGroups(client, TConsensusGroupType.SchemaRegion, 3, dataNodeCount);
      assertRegionGroups(client, TConsensusGroupType.DataRegion, 0, dataNodeCount);

      // Activating schema slots must not count as activating data slots.
      createDataPartitions(client, 0, 1, 0, 1);
      assertRegionGroups(client, TConsensusGroupType.DataRegion, 1, dataNodeCount);
      createDataPartitions(client, 0, 1, 0, 1);
      // Already assigned partitions do not trigger growth toward the minimum.
      assertRegionGroups(client, TConsensusGroupType.DataRegion, 1, dataNodeCount);
      createDataPartitions(client, 0, 1, 1, 4);
      // New times for the existing series slot complete the minimum, just as with AUTO.
      assertRegionGroups(client, TConsensusGroupType.DataRegion, 2, dataNodeCount);
      createDataPartitions(client, 0, 2, 4, 5);
      assertRegionGroups(client, TConsensusGroupType.DataRegion, 2, dataNodeCount);
      // New time partitions for two existing slots are still only two activated series slots.
      createDataPartitions(client, 0, 2, 5, 8);
      dataGroups = assertRegionGroups(client, TConsensusGroupType.DataRegion, 2, dataNodeCount);
    }

    int oldLeader = EnvFactory.getEnv().getLeaderConfigNodeIndex();
    EnvFactory.getEnv().shutdownConfigNode(oldLeader);
    if (configNodeCount < 3) {
      // A two-member ConfigNode group needs both members for quorum.
      EnvFactory.getEnv().startConfigNode(oldLeader);
    } else {
      Assert.assertNotEquals(oldLeader, EnvFactory.getEnv().getLeaderConfigNodeIndex());
    }

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      // Restart / leader election preserves the groups, counts and resource-derived cap.
      assertMaxRegionGroupNum(client);
      Assert.assertEquals(
          schemaGroups,
          assertRegionGroups(client, TConsensusGroupType.SchemaRegion, 3, dataNodeCount));
      Assert.assertEquals(
          dataGroups, assertRegionGroups(client, TConsensusGroupType.DataRegion, 2, dataNodeCount));
      // Leader discovery precedes the heartbeat statistics that make recovered groups available.
      awaitRunningRegions(client, (schemaGroups.size() + dataGroups.size()) * dataNodeCount);
      createSchemaPartitions(client, paths[0], paths[1], paths[2]);
      createDataPartitions(client, 0, 2, 8, 9);
      assertRegionGroups(client, TConsensusGroupType.SchemaRegion, 3, dataNodeCount);
      assertRegionGroups(client, TConsensusGroupType.DataRegion, 2, dataNodeCount);

      // Batched requests mix existing and new slots, then reach and stay at AUTO's same cap.
      createDataPartitions(client, 0, 3, 9, 10);
      assertRegionGroups(client, TConsensusGroupType.DataRegion, 3, dataNodeCount);
      createSchemaPartitions(client, paths);
      createDataPartitions(client, 0, paths.length, 10, 11);
      assertRegionGroups(
          client, TConsensusGroupType.SchemaRegion, MAX_REGION_GROUP_NUM, dataNodeCount);
      assertRegionGroups(
          client, TConsensusGroupType.DataRegion, MAX_REGION_GROUP_NUM, dataNodeCount);
      createSchemaPartitions(client, findPathInSlot(paths.length, null));
      createDataPartitions(client, 0, paths.length + 1, 11, 12);
      assertRegionGroups(
          client, TConsensusGroupType.SchemaRegion, MAX_REGION_GROUP_NUM, dataNodeCount);
      assertRegionGroups(
          client, TConsensusGroupType.DataRegion, MAX_REGION_GROUP_NUM, dataNodeCount);
    }
  }

  private static void awaitRunningRegions(
      SyncConfigNodeIServiceClient client, int expectedReplicaCount) throws Exception {
    TShowRegionResp response = null;
    for (int retry = 0; retry < 30; retry++) {
      response = client.showRegion(new TShowRegionReq());
      assertSuccess(response.getStatus());
      int replicaCount = 0;
      int runningReplicaCount = 0;
      for (TRegionInfo region : response.getRegionInfoList()) {
        if (DATABASE.equals(region.getDatabase())) {
          replicaCount++;
          if (RegionStatus.Running.getStatus().equals(region.getStatus())) {
            runningReplicaCount++;
          }
        }
      }
      if (replicaCount == expectedReplicaCount && runningReplicaCount == expectedReplicaCount) {
        return;
      }
      TimeUnit.SECONDS.sleep(1);
    }
    Assert.fail("Regions did not become Running after ConfigNode recovery: " + response);
  }

  private static String findPathInSlot(int slot, String excludedPath) {
    for (int device = 0; device < SERIES_SLOT_NUM * 100; device++) {
      String devicePath = DATABASE + ".d" + device;
      String path = devicePath + ".s";
      if (!path.equals(excludedPath)
          && PARTITION_EXECUTOR.getSeriesPartitionSlot(devicePath).getSlotId() == slot) {
        return path;
      }
    }
    throw new AssertionError("No device found in series slot " + slot);
  }

  private static void createSchemaPartitions(SyncConfigNodeIServiceClient client, String... paths)
      throws Exception {
    TSchemaPartitionTableResp response =
        client.getOrCreateSchemaPartitionTable(
            new TSchemaPartitionReq(generatePatternTreeBuffer(paths)));
    assertSuccess(response.getStatus());
    Set<TSeriesPartitionSlot> expectedSlots = new HashSet<>();
    for (String path : paths) {
      expectedSlots.add(
          PARTITION_EXECUTOR.getSeriesPartitionSlot(path.substring(0, path.lastIndexOf('.'))));
    }
    Assert.assertEquals(expectedSlots, response.getSchemaPartitionTable().get(DATABASE).keySet());
  }

  private static void createDataPartitions(
      SyncConfigNodeIServiceClient client, int slotStart, int slotEnd, int timeStart, int timeEnd)
      throws Exception {
    TDataPartitionTableResp response =
        client.getOrCreateDataPartitionTable(
            new TDataPartitionReq(
                ConfigNodeTestUtils.constructPartitionSlotsMap(
                    DATABASE, slotStart, slotEnd, timeStart, timeEnd, TIME_PARTITION_INTERVAL)));
    assertSuccess(response.getStatus());
    ConfigNodeTestUtils.checkDataPartitionTable(
        DATABASE,
        slotStart,
        slotEnd,
        timeStart,
        timeEnd,
        TIME_PARTITION_INTERVAL,
        response.getDataPartitionTable());
  }

  private static void assertMaxRegionGroupNum(SyncConfigNodeIServiceClient client)
      throws Exception {
    TDatabaseSchemaResp response =
        client.getMatchedDatabaseSchemas(
            new TGetDatabaseReq(Arrays.asList("root", "proactive"), ALL_MATCH_SCOPE_BINARY));
    assertSuccess(response.getStatus());
    TDatabaseSchema schema = response.getDatabaseSchemaMap().get(DATABASE);
    // One database: per-node quota * DataNode count / replication factor = 4, as with AUTO.
    Assert.assertEquals(MAX_REGION_GROUP_NUM, schema.getMaxSchemaRegionGroupNum());
    Assert.assertEquals(MAX_REGION_GROUP_NUM, schema.getMaxDataRegionGroupNum());
  }

  private static Set<TConsensusGroupId> assertRegionGroups(
      SyncConfigNodeIServiceClient client,
      TConsensusGroupType type,
      int expectedGroupCount,
      int replicationFactor)
      throws Exception {
    TShowRegionResp response = client.showRegion(new TShowRegionReq().setConsensusGroupType(type));
    assertSuccess(response.getStatus());
    Map<TConsensusGroupId, Set<Integer>> replicas = new HashMap<>();
    int replicaCount = 0;
    for (TRegionInfo region : response.getRegionInfoList()) {
      if (DATABASE.equals(region.getDatabase())) {
        replicas
            .computeIfAbsent(region.getConsensusGroupId(), ignored -> new HashSet<>())
            .add(region.getDataNodeId());
        replicaCount++;
      }
    }
    Assert.assertEquals(type.toString(), expectedGroupCount, replicas.size());
    Assert.assertEquals(expectedGroupCount * replicationFactor, replicaCount);
    replicas.values().forEach(nodes -> Assert.assertEquals(replicationFactor, nodes.size()));
    return replicas.keySet();
  }

  private static void assertSuccess(TSStatus status) {
    Assert.assertEquals(
        status.getMessage(), TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
  }
}
