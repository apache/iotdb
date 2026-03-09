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
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
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

import java.sql.Connection;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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

  @Before
  public void setUp() {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setEnableAutoLeaderBalanceForRatisConsensus(true)
        .setEnableAutoLeaderBalanceForIoTConsensus(true)
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

      // Check the number of RegionGroup-leader in each DataNode.
      Map<Integer, Integer> leaderCounter = new TreeMap<>();
      TShowRegionResp showRegionResp = client.showRegion(new TShowRegionReq());
      showRegionResp
          .getRegionInfoList()
          .removeIf(r -> r.database.startsWith(SystemConstant.SYSTEM_DATABASE));
      showRegionResp
          .getRegionInfoList()
          .removeIf(r -> r.database.startsWith(SystemConstant.AUDIT_DATABASE));
      showRegionResp
          .getRegionInfoList()
          .forEach(
              regionInfo -> {
                if (RegionRoleType.Leader.getRoleType().equals(regionInfo.getRoleType())) {
                  leaderCounter.merge(regionInfo.getDataNodeId(), 1, Integer::sum);
                }
              });
      // The number of Region-leader in each DataNode should be exactly 1
      Assert.assertEquals(TEST_DATA_NODE_NUM, leaderCounter.size());
      leaderCounter.forEach((dataNodeId, leaderCount) -> Assert.assertEquals(1, (int) leaderCount));
    }
  }

  @Test
  public void testCFDWithUnknownStatus() throws Exception {
    final int retryNum = 50;
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

      // Check leader distribution
      Map<Integer, Integer> leaderCounter = new TreeMap<>();
      TShowRegionResp showRegionResp;
      boolean isDistributionBalanced = false;
      for (int retry = 0; retry < retryNum; retry++) {
        leaderCounter.clear();
        showRegionResp = client.showRegion(new TShowRegionReq());
        showRegionResp
            .getRegionInfoList()
            .removeIf(r -> r.database.startsWith(SystemConstant.SYSTEM_DATABASE));
        showRegionResp
            .getRegionInfoList()
            .removeIf(r -> r.database.startsWith(SystemConstant.AUDIT_DATABASE));
        showRegionResp
            .getRegionInfoList()
            .forEach(
                regionInfo -> {
                  if (RegionRoleType.Leader.getRoleType().equals(regionInfo.getRoleType())) {
                    leaderCounter.merge(regionInfo.getDataNodeId(), 1, Integer::sum);
                  }
                });

        // All DataNodes have Region-leader
        isDistributionBalanced = leaderCounter.size() == TEST_DATA_NODE_NUM;
        // Each DataNode has exactly 2 Region-leader
        for (Integer leaderCount : leaderCounter.values()) {
          if (leaderCount != databaseNum / TEST_DATA_NODE_NUM) {
            isDistributionBalanced = false;
            break;
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
            .removeIf(r -> r.database.startsWith(SystemConstant.SYSTEM_DATABASE));
        showRegionResp
            .getRegionInfoList()
            .removeIf(r -> r.database.startsWith(SystemConstant.AUDIT_DATABASE));
        showRegionResp
            .getRegionInfoList()
            .forEach(
                regionInfo -> {
                  if (RegionRoleType.Leader.getRoleType().equals(regionInfo.getRoleType())) {
                    leaderCounter.merge(regionInfo.getDataNodeId(), 1, Integer::sum);
                  }
                });

        // Only Running DataNodes have Region-leader
        isDistributionBalanced = leaderCounter.size() == TEST_DATA_NODE_NUM - 1;
        // Each Running DataNode has exactly 3 Region-leader
        for (Integer leaderCount : leaderCounter.values()) {
          if (leaderCount != databaseNum / (TEST_DATA_NODE_NUM - 1)) {
            isDistributionBalanced = false;
            break;
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

  @Test
  public void testCFDWithReadOnlyStatus() throws Exception {
    final int retryNum = 50;

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

      // Check leader distribution
      Map<Integer, Integer> leaderCounter = new ConcurrentHashMap<>();
      TShowRegionResp showRegionResp;
      boolean isDistributionBalanced = false;
      for (int retry = 0; retry < retryNum; retry++) {
        leaderCounter.clear();
        showRegionResp = client.showRegion(new TShowRegionReq());
        showRegionResp
            .getRegionInfoList()
            .removeIf(r -> r.database.startsWith(SystemConstant.SYSTEM_DATABASE));
        showRegionResp
            .getRegionInfoList()
            .removeIf(r -> r.database.startsWith(SystemConstant.AUDIT_DATABASE));
        showRegionResp
            .getRegionInfoList()
            .forEach(
                regionInfo -> {
                  if (RegionRoleType.Leader.getRoleType().equals(regionInfo.getRoleType())) {
                    leaderCounter.merge(regionInfo.getDataNodeId(), 1, Integer::sum);
                  }
                });

        // All DataNodes have Region-leader
        isDistributionBalanced = leaderCounter.size() == TEST_DATA_NODE_NUM;
        // Each DataNode has exactly 1 Region-leader
        for (Integer leaderCount : leaderCounter.values()) {
          if (leaderCount != databaseNum / TEST_DATA_NODE_NUM) {
            isDistributionBalanced = false;
            break;
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

      // Check leader distribution
      TShowRegionResp showRegionResp;
      AtomicBoolean isDistributionBalanced = new AtomicBoolean();
      for (int retry = 0; retry < retryNum; retry++) {
        isDistributionBalanced.set(true);
        showRegionResp = client.showRegion(new TShowRegionReq());
        showRegionResp
            .getRegionInfoList()
            .removeIf(r -> r.database.startsWith(SystemConstant.SYSTEM_DATABASE));
        showRegionResp
            .getRegionInfoList()
            .removeIf(r -> r.database.startsWith(SystemConstant.AUDIT_DATABASE));
        showRegionResp
            .getRegionInfoList()
            .forEach(
                regionInfo -> {
                  if (RegionRoleType.Leader.getRoleType().equals(regionInfo.getRoleType())
                      && NodeStatus.ReadOnly.getStatus().equals(regionInfo.getStatus())) {
                    // ReadOnly DataNode couldn't have Region-leader
                    isDistributionBalanced.set(false);
                  }
                });

        if (isDistributionBalanced.get()) {
          break;
        } else {
          TimeUnit.SECONDS.sleep(1);
        }
      }
      Assert.assertTrue(isDistributionBalanced.get());
    }
  }
}
