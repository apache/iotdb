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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.cluster.RegionRoleType;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
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
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBClusterRegionLeaderBalancingIT {
  private static final String testSchemaRegionConsensusProtocolClass =
      ConsensusFactory.RATIS_CONSENSUS;
  private static final String testDataRegionConsensusProtocolClass = ConsensusFactory.IOT_CONSENSUS;
  private static final int testReplicationFactor = 3;

  private static final String sg = "root.sg";
  private final int testDataNodeNum = 3;

  @Before
  public void setUp() {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setEnableAutoLeaderBalanceForRatisConsensus(true)
        .setEnableAutoLeaderBalanceForIoTConsensus(true)
        .setSchemaRegionConsensusProtocolClass(testSchemaRegionConsensusProtocolClass)
        .setDataRegionConsensusProtocolClass(testDataRegionConsensusProtocolClass)
        .setSchemaReplicationFactor(testReplicationFactor)
        .setDataReplicationFactor(testReplicationFactor);
    EnvFactory.getEnv().initClusterEnvironment(1, 3);
  }

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testGreedyLeaderDistribution() throws Exception {

    TSStatus status;
    final int storageGroupNum = 3;
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {

      // Set StorageGroups
      for (int i = 0; i < storageGroupNum; i++) {
        status = client.setDatabase(new TDatabaseSchema(sg + i));
        Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      }

      // Create a DataRegionGroup for each StorageGroup through getOrCreateDataPartition
      for (int i = 0; i < storageGroupNum; i++) {
        Map<TSeriesPartitionSlot, TTimeSlotList> seriesSlotMap = new HashMap<>();
        seriesSlotMap.put(
            new TSeriesPartitionSlot(1),
            new TTimeSlotList()
                .setTimePartitionSlots(Collections.singletonList(new TTimePartitionSlot(100))));
        Map<String, Map<TSeriesPartitionSlot, TTimeSlotList>> sgSlotsMap = new HashMap<>();
        sgSlotsMap.put(sg + i, seriesSlotMap);
        TDataPartitionTableResp dataPartitionTableResp =
            client.getOrCreateDataPartitionTable(new TDataPartitionReq(sgSlotsMap));
        Assert.assertEquals(
            TSStatusCode.SUCCESS_STATUS.getStatusCode(),
            dataPartitionTableResp.getStatus().getCode());
      }

      // Check the number of Region-leader in each DataNode.
      Map<Integer, AtomicInteger> leaderCounter = new ConcurrentHashMap<>();
      TShowRegionResp showRegionResp = client.showRegion(new TShowRegionReq());
      showRegionResp
          .getRegionInfoList()
          .forEach(
              regionInfo -> {
                if (RegionRoleType.Leader.getRoleType().equals(regionInfo.getRoleType())) {
                  leaderCounter
                      .computeIfAbsent(regionInfo.getDataNodeId(), empty -> new AtomicInteger(0))
                      .getAndIncrement();
                }
              });
      // The number of Region-leader in each DataNode should be exactly 1
      Assert.assertEquals(testDataNodeNum, leaderCounter.size());
      leaderCounter.values().forEach(leaderCount -> Assert.assertEquals(1, leaderCount.get()));
    }
  }

  @Test
  public void testMCFLeaderDistribution() throws Exception {
    final int retryNum = 50;

    TSStatus status;
    final int storageGroupNum = 6;
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {

      for (int i = 0; i < storageGroupNum; i++) {
        // Set StorageGroups
        status = client.setDatabase(new TDatabaseSchema(sg + i));
        Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

        // Create a DataRegionGroup for each StorageGroup
        Map<TSeriesPartitionSlot, TTimeSlotList> seriesSlotMap = new HashMap<>();
        seriesSlotMap.put(
            new TSeriesPartitionSlot(1),
            new TTimeSlotList()
                .setTimePartitionSlots(Collections.singletonList(new TTimePartitionSlot(100))));
        Map<String, Map<TSeriesPartitionSlot, TTimeSlotList>> sgSlotsMap = new HashMap<>();
        sgSlotsMap.put(sg + i, seriesSlotMap);
        TDataPartitionTableResp dataPartitionTableResp =
            client.getOrCreateDataPartitionTable(new TDataPartitionReq(sgSlotsMap));
        Assert.assertEquals(
            TSStatusCode.SUCCESS_STATUS.getStatusCode(),
            dataPartitionTableResp.getStatus().getCode());
      }

      // Check leader distribution
      Map<Integer, AtomicInteger> leaderCounter = new ConcurrentHashMap<>();
      TShowRegionResp showRegionResp;
      boolean isDistributionBalanced = false;
      for (int retry = 0; retry < retryNum; retry++) {
        leaderCounter.clear();
        showRegionResp = client.showRegion(new TShowRegionReq());
        showRegionResp
            .getRegionInfoList()
            .forEach(
                regionInfo -> {
                  if (RegionRoleType.Leader.getRoleType().equals(regionInfo.getRoleType())) {
                    leaderCounter
                        .computeIfAbsent(regionInfo.getDataNodeId(), empty -> new AtomicInteger(0))
                        .getAndIncrement();
                  }
                });

        // All DataNodes have Region-leader
        isDistributionBalanced = leaderCounter.size() == testDataNodeNum;
        // Each DataNode has exactly 2 Region-leader
        for (AtomicInteger leaderCount : leaderCounter.values()) {
          if (leaderCount.get() != 2) {
            isDistributionBalanced = false;
          }
        }

        if (isDistributionBalanced) {
          break;
        } else {
          TimeUnit.SECONDS.sleep(1);
        }
      }
      Assert.assertTrue(isDistributionBalanced);

      // Shutdown a DataNode
      EnvFactory.getEnv().shutdownDataNode(0);
      EnvFactory.getEnv()
          .ensureNodeStatus(
              Collections.singletonList(EnvFactory.getEnv().getDataNodeWrapper(0)),
              Collections.singletonList(NodeStatus.Unknown));

      // Check leader distribution
      isDistributionBalanced = false;
      for (int retry = 0; retry < retryNum; retry++) {
        leaderCounter.clear();
        showRegionResp = client.showRegion(new TShowRegionReq());
        showRegionResp
            .getRegionInfoList()
            .forEach(
                regionInfo -> {
                  if (RegionRoleType.Leader.getRoleType().equals(regionInfo.getRoleType())) {
                    leaderCounter
                        .computeIfAbsent(regionInfo.getDataNodeId(), empty -> new AtomicInteger(0))
                        .getAndIncrement();
                  }
                });

        // Only Running DataNodes have Region-leader
        isDistributionBalanced = leaderCounter.size() == testDataNodeNum - 1;
        // Each Running DataNode has exactly 3 Region-leader
        for (AtomicInteger leaderCount : leaderCounter.values()) {
          if (leaderCount.get() != 3) {
            isDistributionBalanced = false;
          }
        }

        if (isDistributionBalanced) {
          break;
        } else {
          TimeUnit.SECONDS.sleep(1);
        }
      }
      Assert.assertTrue(isDistributionBalanced);
    }
  }
}
