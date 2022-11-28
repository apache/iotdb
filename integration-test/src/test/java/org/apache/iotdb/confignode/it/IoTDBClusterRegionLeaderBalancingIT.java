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
package org.apache.iotdb.confignode.it;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.cluster.RegionRoleType;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.confignode.it.utils.ConfigNodeTestUtils;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TSetStorageGroupReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowDataNodesResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionResp;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;
import org.apache.iotdb.confignode.rpc.thrift.TTimeSlotList;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.ConfigFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.env.BaseConfig;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBClusterRegionLeaderBalancingIT {

  private static final BaseConfig CONF = ConfigFactory.getConfig();

  protected static boolean originalEnableAutoLeaderBalanceForRatisConsensus;
  protected static boolean originalEnableAutoLeaderBalancerForIoTConsensus;

  protected static String originalSchemaRegionConsensusProtocolClass;
  private static final String testSchemaRegionConsensusProtocolClass =
      ConsensusFactory.RATIS_CONSENSUS;
  protected static String originalDataRegionConsensusProtocolClass;
  private static final String testDataRegionConsensusProtocolClass = ConsensusFactory.IOT_CONSENSUS;

  protected static int originalSchemaReplicationFactor;
  protected static int originalDataReplicationFactor;
  private static final int testReplicationFactor = 3;

  private static final String sg = "root.sg";

  @BeforeClass
  public static void setUp() {
    originalEnableAutoLeaderBalanceForRatisConsensus =
        CONF.isEnableAutoLeaderBalanceForRatisConsensus();
    CONF.setEnableAutoLeaderBalanceForRatisConsensus(true);
    originalEnableAutoLeaderBalancerForIoTConsensus =
        CONF.isEnableAutoLeaderBalanceForIoTConsensus();
    CONF.setEnableAutoLeaderBalanceForIoTConsensus(true);

    originalSchemaRegionConsensusProtocolClass = CONF.getSchemaRegionConsensusProtocolClass();
    CONF.setSchemaRegionConsensusProtocolClass(testSchemaRegionConsensusProtocolClass);

    originalDataRegionConsensusProtocolClass = CONF.getDataRegionConsensusProtocolClass();
    CONF.setDataRegionConsensusProtocolClass(testDataRegionConsensusProtocolClass);

    originalSchemaReplicationFactor = CONF.getSchemaReplicationFactor();
    originalDataReplicationFactor = CONF.getDataReplicationFactor();
    CONF.setSchemaReplicationFactor(testReplicationFactor);
    CONF.setDataReplicationFactor(testReplicationFactor);
  }

  @AfterClass
  public static void tearDown() {
    CONF.setEnableAutoLeaderBalanceForRatisConsensus(
        originalEnableAutoLeaderBalanceForRatisConsensus);
    CONF.setEnableAutoLeaderBalanceForIoTConsensus(originalEnableAutoLeaderBalancerForIoTConsensus);

    CONF.setSchemaRegionConsensusProtocolClass(originalSchemaRegionConsensusProtocolClass);
    CONF.setDataRegionConsensusProtocolClass(originalDataRegionConsensusProtocolClass);

    CONF.setSchemaReplicationFactor(originalSchemaReplicationFactor);
    CONF.setDataReplicationFactor(originalDataReplicationFactor);
  }

  @Test
  public void testGreedyLeaderDistribution() throws IOException, InterruptedException, TException {
    final int testConfigNodeNum = 1;
    final int testDataNodeNum = 3;
    EnvFactory.getEnv().initClusterEnvironment(testConfigNodeNum, testDataNodeNum);

    TSStatus status;
    final int storageGroupNum = 3;
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {

      // Set StorageGroups
      for (int i = 0; i < storageGroupNum; i++) {
        TSetStorageGroupReq setReq = new TSetStorageGroupReq(new TStorageGroupSchema(sg + i));
        status = client.setStorageGroup(setReq);
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
  public void testMCFLeaderDistribution()
      throws IOException, InterruptedException, TException, IllegalPathException {
    final int testConfigNodeNum = 1;
    final int testDataNodeNum = 3;
    final int retryNum = 100;
    EnvFactory.getEnv().initClusterEnvironment(testConfigNodeNum, testDataNodeNum);

    TSStatus status;
    final int storageGroupNum = 6;
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {

      for (int i = 0; i < storageGroupNum; i++) {
        // Set StorageGroups
        TSetStorageGroupReq setReq = new TSetStorageGroupReq(new TStorageGroupSchema(sg + i));
        status = client.setStorageGroup(setReq);
        Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

        // Create a SchemaRegionGroup for each StorageGroup
        TSchemaPartitionTableResp schemaPartitionTableResp =
            client.getOrCreateSchemaPartitionTable(
                new TSchemaPartitionReq(
                    ConfigNodeTestUtils.generatePatternTreeBuffer(
                        new String[] {sg + i + "." + "d"})));
        Assert.assertEquals(
            TSStatusCode.SUCCESS_STATUS.getStatusCode(),
            schemaPartitionTableResp.getStatus().getCode());

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
        // Each DataNode has exactly 4 Region-leader
        for (AtomicInteger leaderCount : leaderCounter.values()) {
          if (leaderCount.get() != 4) {
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
      boolean isDataNodeShutdown = false;
      EnvFactory.getEnv().shutdownDataNode(0);
      for (int retry = 0; retry < retryNum; retry++) {
        AtomicInteger runningCnt = new AtomicInteger(0);
        AtomicInteger unknownCnt = new AtomicInteger(0);
        TShowDataNodesResp showDataNodesResp = client.showDataNodes();
        showDataNodesResp
            .getDataNodesInfoList()
            .forEach(
                dataNodeInfo -> {
                  if (NodeStatus.Running.getStatus().equals(dataNodeInfo.getStatus())) {
                    runningCnt.getAndIncrement();
                  } else if (NodeStatus.Unknown.getStatus().equals(dataNodeInfo.getStatus())) {
                    unknownCnt.getAndIncrement();
                  }
                });
        if (runningCnt.get() == testDataNodeNum - 1 && unknownCnt.get() == 1) {
          isDataNodeShutdown = true;
          break;
        }

        TimeUnit.SECONDS.sleep(1);
      }
      Assert.assertTrue(isDataNodeShutdown);

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
        // Each Running DataNode has exactly 6 Region-leader
        for (AtomicInteger leaderCount : leaderCounter.values()) {
          if (leaderCount.get() != 6) {
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
