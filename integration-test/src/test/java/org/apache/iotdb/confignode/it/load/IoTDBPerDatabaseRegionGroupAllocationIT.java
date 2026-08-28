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
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.confignode.it.utils.ConfigNodeTestUtils;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.confignode.rpc.thrift.TRegionInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionResp;
import org.apache.iotdb.confignode.rpc.thrift.TTimeSlotList;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Verifies that DataRegion allocators distribute each database's replicas evenly across DataNodes.
 * Regression for the case where {@code PartiteGraphPlacementRegionGroupAllocator} ignored {@code
 * databaseAllocatedRegionGroups} and ended up with one DataNode holding many replicas of one
 * database and few of another.
 */
@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBPerDatabaseRegionGroupAllocationIT {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBPerDatabaseRegionGroupAllocationIT.class);

  private static final int TEST_DATA_NODE_NUM = 4;
  private static final int TEST_REPLICATION_FACTOR = 2;
  private static final int TEST_DATABASE_NUM = 2;
  private static final int TEST_DATA_REGION_GROUP_NUM_PER_DATABASE = 4;
  private static final String DATABASE_PREFIX = "root.db";
  private static final long TEST_TIME_PARTITION_INTERVAL = 604_800_000L;

  private void initCluster(String regionGroupAllocatePolicy) {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setRegionGroupAllocatePolicy(regionGroupAllocatePolicy)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setDataReplicationFactor(TEST_REPLICATION_FACTOR)
        .setDataRegionGroupExtensionPolicy("CUSTOM")
        .setDefaultDataRegionGroupNumPerDatabase(TEST_DATA_REGION_GROUP_NUM_PER_DATABASE);
    EnvFactory.getEnv().initClusterEnvironment(1, TEST_DATA_NODE_NUM);
  }

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testPgpPolicyPerDbReplicaBalance() throws Exception {
    initCluster("PGP");
    runPerDbBalanceCheck("PGP");
  }

  @Test
  public void testGcrPolicyPerDbReplicaBalance() throws Exception {
    initCluster("GCR");
    runPerDbBalanceCheck("GCR");
  }

  @Test
  public void testGreedyPolicyPerDbReplicaBalance() throws Exception {
    initCluster("GREEDY");
    runPerDbBalanceCheck("GREEDY");
  }

  private void runPerDbBalanceCheck(String policy) throws Exception {
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      // Create databases and trigger DataRegion materialization
      for (int i = 0; i < TEST_DATABASE_NUM; i++) {
        String currentDatabase = DATABASE_PREFIX + i;
        TSStatus status = client.setDatabase(new TDatabaseSchema(currentDatabase));
        Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

        Map<String, Map<TSeriesPartitionSlot, TTimeSlotList>> partitionSlotsMap =
            ConfigNodeTestUtils.constructPartitionSlotsMap(
                currentDatabase,
                0,
                TEST_DATA_REGION_GROUP_NUM_PER_DATABASE,
                0,
                1,
                TEST_TIME_PARTITION_INTERVAL);
        TDataPartitionTableResp dataPartitionTableResp =
            client.getOrCreateDataPartitionTable(new TDataPartitionReq(partitionSlotsMap));
        Assert.assertEquals(
            "Failed to create DataPartitions for "
                + currentDatabase
                + ": "
                + dataPartitionTableResp.getStatus(),
            TSStatusCode.SUCCESS_STATUS.getStatusCode(),
            dataPartitionTableResp.getStatus().getCode());
      }

      // Collect DataRegion replicas grouped by database and DataNode
      TShowRegionResp showRegionResp = client.showRegion(new TShowRegionReq());
      List<TRegionInfo> dataRegionInfoList =
          showRegionResp.getRegionInfoList().stream()
              .filter(r -> !r.database.startsWith(SystemConstant.SYSTEM_DATABASE))
              .filter(r -> !r.database.startsWith(SystemConstant.AUDIT_DATABASE))
              // Only consider DataRegion since SchemaRegion is allocated by a separate path
              .filter(r -> r.getConsensusGroupId().getType() == TConsensusGroupType.DataRegion)
              .collect(Collectors.toList());

      Map<String, Map<Integer, Integer>> perDbReplicaCount = new TreeMap<>();
      Map<Integer, Integer> globalReplicaCount = new TreeMap<>();
      for (TRegionInfo info : dataRegionInfoList) {
        perDbReplicaCount
            .computeIfAbsent(info.getDatabase(), k -> new TreeMap<>())
            .merge(info.getDataNodeId(), 1, Integer::sum);
        globalReplicaCount.merge(info.getDataNodeId(), 1, Integer::sum);
      }

      int expectedPerDbPerDn =
          TEST_DATA_REGION_GROUP_NUM_PER_DATABASE * TEST_REPLICATION_FACTOR / TEST_DATA_NODE_NUM;
      int expectedGlobalPerDn =
          TEST_DATABASE_NUM
              * TEST_DATA_REGION_GROUP_NUM_PER_DATABASE
              * TEST_REPLICATION_FACTOR
              / TEST_DATA_NODE_NUM;

      for (int i = 0; i < TEST_DATABASE_NUM; i++) {
        String currentDatabase = DATABASE_PREFIX + i;
        Map<Integer, Integer> dnReplicaCount = perDbReplicaCount.get(currentDatabase);
        Assert.assertNotNull("No DataRegion replicas found for " + currentDatabase, dnReplicaCount);
        LOGGER.info("[{}] db {} replicas per DN: {}", policy, currentDatabase, dnReplicaCount);
        Assert.assertEquals(
            "policy=" + policy + " db=" + currentDatabase + " should cover all DataNodes",
            TEST_DATA_NODE_NUM,
            dnReplicaCount.size());
        for (Map.Entry<Integer, Integer> entry : dnReplicaCount.entrySet()) {
          Assert.assertEquals(
              "policy=" + policy + " db=" + currentDatabase + " dn=" + entry.getKey(),
              expectedPerDbPerDn,
              (int) entry.getValue());
        }
      }

      LOGGER.info("[{}] global replicas per DN: {}", policy, globalReplicaCount);
      Assert.assertEquals(
          "policy=" + policy + " global allocation should cover all DataNodes",
          TEST_DATA_NODE_NUM,
          globalReplicaCount.size());
      for (Map.Entry<Integer, Integer> entry : globalReplicaCount.entrySet()) {
        Assert.assertEquals(
            "policy=" + policy + " global dn=" + entry.getKey(),
            expectedGlobalPerDn,
            (int) entry.getValue());
      }
    }
  }
}
