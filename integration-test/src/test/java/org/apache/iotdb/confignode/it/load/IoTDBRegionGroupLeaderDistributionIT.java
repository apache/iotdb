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

package org.apache.iotdb.confignode.it.load;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.cluster.RegionRoleType;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.confignode.rpc.thrift.TRegionInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionResp;
import org.apache.iotdb.confignode.rpc.thrift.TTimeSlotList;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.EnvUtils;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBRegionGroupLeaderDistributionIT {
  private static final String TEST_SCHEMA_REGION_CONSENSUS_PROTOCOL_CLASS =
      ConsensusFactory.RATIS_CONSENSUS;
  private static final String TEST_DATA_REGION_CONSENSUS_PROTOCOL_CLASS =
      ConsensusFactory.IOT_CONSENSUS;
  private static final int TEST_REPLICATION_FACTOR = 3;

  private static final String DATABASE = "root.db";
  private static final int TEST_DATA_NODE_NUM = 3;

  private static final int RETRY_NUM = 50;

  @Before
  public void setUp() {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setEnableAutoLeaderBalanceForRatisConsensus(true)
        .setSchemaRegionConsensusProtocolClass(TEST_SCHEMA_REGION_CONSENSUS_PROTOCOL_CLASS)
        .setDataRegionConsensusProtocolClass(TEST_DATA_REGION_CONSENSUS_PROTOCOL_CLASS)
        .setSchemaReplicationFactor(TEST_REPLICATION_FACTOR)
        .setDataReplicationFactor(TEST_REPLICATION_FACTOR);
    EnvFactory.getEnv().initClusterEnvironment(1, TEST_DATA_NODE_NUM);
  }

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testBasicLeaderDistribution() throws Exception {
    TSStatus status;
    final int databaseNum = 3;
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      // Set Databases
      for (int i = 0; i < databaseNum; i++) {
        status = client.setDatabase(new TDatabaseSchema(DATABASE + i));
        Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      }

      // Create a DataRegionGroup for each Database through getOrCreateDataPartition
      for (int i = 0; i < databaseNum; i++) {
        Map<TSeriesPartitionSlot, TTimeSlotList> seriesSlotMap = new HashMap<>();
        seriesSlotMap.put(
            new TSeriesPartitionSlot(1),
            new TTimeSlotList()
                .setTimePartitionSlots(Collections.singletonList(new TTimePartitionSlot(100))));
        Map<String, Map<TSeriesPartitionSlot, TTimeSlotList>> sgSlotsMap = new HashMap<>();
        sgSlotsMap.put(DATABASE + i, seriesSlotMap);
        TDataPartitionTableResp dataPartitionTableResp =
            client.getOrCreateDataPartitionTable(new TDataPartitionReq(sgSlotsMap));
        Assert.assertEquals(
            TSStatusCode.SUCCESS_STATUS.getStatusCode(),
            dataPartitionTableResp.getStatus().getCode());
      }

      // The RegionGroup leaders should be spread evenly across the DataNodes. This must poll:
      // the RegionGroups have just been created, so their leaders are still being elected and the
      // leader balancer is still migrating them while this method samples.
      awaitBalancedLeaderDistribution(client, TEST_DATA_NODE_NUM);
    }
  }

  @Test
  public void testCFDWithUnknownStatus() throws Exception {
    TSStatus status;
    final int databaseNum = 6;
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      for (int i = 0; i < databaseNum; i++) {
        // Set Databases
        status = client.setDatabase(new TDatabaseSchema(DATABASE + i));
        Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

        // Create a DataRegionGroup for each Database
        Map<TSeriesPartitionSlot, TTimeSlotList> seriesSlotMap = new HashMap<>();
        seriesSlotMap.put(
            new TSeriesPartitionSlot(1),
            new TTimeSlotList()
                .setTimePartitionSlots(Collections.singletonList(new TTimePartitionSlot(100))));
        Map<String, Map<TSeriesPartitionSlot, TTimeSlotList>> sgSlotsMap = new HashMap<>();
        sgSlotsMap.put(DATABASE + i, seriesSlotMap);
        TDataPartitionTableResp dataPartitionTableResp =
            client.getOrCreateDataPartitionTable(new TDataPartitionReq(sgSlotsMap));
        Assert.assertEquals(
            TSStatusCode.SUCCESS_STATUS.getStatusCode(),
            dataPartitionTableResp.getStatus().getCode());
      }

      // All DataNodes should share the RegionGroup leaders evenly.
      awaitBalancedLeaderDistribution(client, TEST_DATA_NODE_NUM);

      // Shutdown a DataNode
      EnvFactory.getEnv().shutdownDataNode(0);
      EnvFactory.getEnv()
          .ensureNodeStatus(
              Collections.singletonList(EnvFactory.getEnv().getDataNodeWrapper(0)),
              Collections.singletonList(EnvUtils.getNodeStatusAfterLocalStop()));

      // Only the two Running DataNodes keep RegionGroup leaders, and they share them evenly.
      awaitBalancedLeaderDistribution(client, TEST_DATA_NODE_NUM - 1);
    }
  }

  @Test
  public void testCFDWithReadOnlyStatus() throws Exception {
    TSStatus status;
    final int databaseNum = 3;
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {

      for (int i = 0; i < databaseNum; i++) {
        // Set Databases
        status = client.setDatabase(new TDatabaseSchema(DATABASE + i));
        Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

        // Create a DataRegionGroup for each Database
        Map<TSeriesPartitionSlot, TTimeSlotList> seriesSlotMap = new HashMap<>();
        seriesSlotMap.put(
            new TSeriesPartitionSlot(1),
            new TTimeSlotList()
                .setTimePartitionSlots(Collections.singletonList(new TTimePartitionSlot(100))));
        Map<String, Map<TSeriesPartitionSlot, TTimeSlotList>> sgSlotsMap = new HashMap<>();
        sgSlotsMap.put(DATABASE + i, seriesSlotMap);
        TDataPartitionTableResp dataPartitionTableResp =
            client.getOrCreateDataPartitionTable(new TDataPartitionReq(sgSlotsMap));
        Assert.assertEquals(
            TSStatusCode.SUCCESS_STATUS.getStatusCode(),
            dataPartitionTableResp.getStatus().getCode());
      }

      // All DataNodes should share the RegionGroup leaders evenly.
      awaitBalancedLeaderDistribution(client, TEST_DATA_NODE_NUM);
    }

    try (Connection connection =
            EnvFactory.getEnv()
                .getConnectionWithSpecifiedDataNode(EnvFactory.getEnv().getDataNodeWrapper(0));
        Statement statement = connection.createStatement()) {
      statement.execute("SET SYSTEM TO READONLY ON LOCAL");
    }

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {

      // Make sure there exist one ReadOnly DataNode
      EnvFactory.getEnv()
          .ensureNodeStatus(
              Collections.singletonList(EnvFactory.getEnv().getDataNodeWrapper(0)),
              Collections.singletonList(NodeStatus.ReadOnly));

      // Only Running DataNodes may hold leaders after the ReadOnly transition.
      awaitBalancedLeaderDistribution(client, TEST_DATA_NODE_NUM - 1);
    }
  }

  /**
   * Checks the same population as the DataRegion leader balancer: all databases, including internal
   * databases. SchemaRegion leaders are balanced separately and must not be mixed into these
   * counts.
   */
  private static void awaitBalancedLeaderDistribution(
      final SyncConfigNodeIServiceClient client, final int expectedDataNodeNum) {
    Awaitility.await()
        .atMost(RETRY_NUM, TimeUnit.SECONDS)
        .pollInterval(1, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              TShowRegionResp response =
                  client.showRegion(
                      new TShowRegionReq().setConsensusGroupType(TConsensusGroupType.DataRegion));
              Assert.assertEquals(
                  TSStatusCode.SUCCESS_STATUS.getStatusCode(), response.getStatus().getCode());
              Set<Integer> regionIds = new HashSet<>();
              Map<Integer, Integer> leadersPerRegion = new TreeMap<>();
              Map<Integer, Integer> leadersPerNode = new TreeMap<>();
              for (TRegionInfo region : response.getRegionInfoList()) {
                regionIds.add(region.getConsensusGroupId().getId());
                if (RegionRoleType.Leader.getRoleType().equals(region.getRoleType())) {
                  Assert.assertEquals(NodeStatus.Running.getStatus(), region.getStatus());
                  leadersPerRegion.merge(region.getConsensusGroupId().getId(), 1, Integer::sum);
                  leadersPerNode.merge(region.getDataNodeId(), 1, Integer::sum);
                }
              }
              // A partial heartbeat result must not look balanced just because leaders are missing.
              Assert.assertFalse("No DataRegionGroups were reported", regionIds.isEmpty());
              Assert.assertEquals(
                  "Some DataRegionGroups have no leader", regionIds, leadersPerRegion.keySet());
              for (int leaderCount : leadersPerRegion.values()) {
                Assert.assertEquals(
                    "Each DataRegionGroup must have exactly one leader", 1, leaderCount);
              }
              Assert.assertEquals(
                  leadersPerNode.toString(), expectedDataNodeNum, leadersPerNode.size());
              Assert.assertTrue(
                  leadersPerNode.toString(),
                  Collections.max(leadersPerNode.values())
                          - Collections.min(leadersPerNode.values())
                      <= 1);
            });
  }
}
