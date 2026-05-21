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
import org.apache.iotdb.common.rpc.thrift.TSetConfigurationReq;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.cluster.RegionRoleType;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.confignode.rpc.thrift.TRegionInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowClusterResp;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBRegionGroupLeaderBalanceWithWALBlockIT {

  private static final String TEST_SCHEMA_REGION_CONSENSUS_PROTOCOL_CLASS =
      ConsensusFactory.RATIS_CONSENSUS;
  private static final String TEST_DATA_REGION_CONSENSUS_PROTOCOL_CLASS =
      ConsensusFactory.IOT_CONSENSUS;
  private static final int TEST_REPLICATION_FACTOR = 3;
  private static final int TEST_DATA_NODE_NUM = 3;
  private static final int DATABASE_NUM = 3;
  private static final int RETRY_NUM = 60;

  private static final String DATABASE = "root.wal_block_db";
  private static final String WAL_THROTTLE_THRESHOLD_IN_BYTE = "wal_throttle_threshold_in_byte";
  private static final String WAL_BLOCKED_STATUS = NodeStatus.ReadOnly.getStatus() + "(WALBlocked)";

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
        .setDataReplicationFactor(TEST_REPLICATION_FACTOR)
        .setCheckPeriodWhenInsertBlocked(50)
        .setMaxWaitingTimeWhenInsertBlocked(2000);
    EnvFactory.getEnv().initClusterEnvironment(1, TEST_DATA_NODE_NUM);
  }

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testRegionLeaderBalanceWhenWalLongTermBlocked() throws Exception {
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      createDataRegionGroups(client);
      waitUntil(
          "all DataNodes have balanced DataRegion leaders",
          () -> isLeaderDistributionBalanced(client));

      TRegionInfo targetLeader = findAnyDataRegionLeader(client);
      triggerLongTermWalBlockingOnDataNode(client, targetLeader.getDataNodeId());

      waitUntil(
          "target leader DataNode becomes ReadOnly because of long-term WAL blocking",
          () ->
              WAL_BLOCKED_STATUS.equals(
                  getNodeStatusWithReason(client, targetLeader.getDataNodeId())));
      waitUntil(
          "Region leaders are moved away from ReadOnly DataNodes",
          () -> hasNoLeaderOnReadOnlyDataNode(client, targetLeader.getDataNodeId()));
    }
  }

  private void createDataRegionGroups(SyncConfigNodeIServiceClient client) throws Exception {
    for (int i = 0; i < DATABASE_NUM; i++) {
      TSStatus status = client.setDatabase(new TDatabaseSchema(DATABASE + i));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Map<TSeriesPartitionSlot, TTimeSlotList> seriesSlotMap = new HashMap<>();
      seriesSlotMap.put(
          new TSeriesPartitionSlot(1),
          new TTimeSlotList()
              .setTimePartitionSlots(Collections.singletonList(new TTimePartitionSlot(100))));
      Map<String, Map<TSeriesPartitionSlot, TTimeSlotList>> databaseSlotsMap = new HashMap<>();
      databaseSlotsMap.put(DATABASE + i, seriesSlotMap);

      TDataPartitionTableResp dataPartitionTableResp =
          client.getOrCreateDataPartitionTable(new TDataPartitionReq(databaseSlotsMap));
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          dataPartitionTableResp.getStatus().getCode());
    }
  }

  private boolean isLeaderDistributionBalanced(SyncConfigNodeIServiceClient client)
      throws Exception {
    Map<Integer, Integer> leaderCounter = new HashMap<>();
    for (TRegionInfo regionInfo : getUserDataRegionInfoList(client)) {
      if (RegionRoleType.Leader.getRoleType().equals(regionInfo.getRoleType())) {
        leaderCounter.merge(regionInfo.getDataNodeId(), 1, Integer::sum);
      }
    }
    if (leaderCounter.size() != TEST_DATA_NODE_NUM) {
      return false;
    }
    for (Integer leaderCount : leaderCounter.values()) {
      if (leaderCount != DATABASE_NUM / TEST_DATA_NODE_NUM) {
        return false;
      }
    }
    return true;
  }

  private TRegionInfo findAnyDataRegionLeader(SyncConfigNodeIServiceClient client)
      throws Exception {
    for (TRegionInfo regionInfo : getUserDataRegionInfoList(client)) {
      if (RegionRoleType.Leader.getRoleType().equals(regionInfo.getRoleType())) {
        return regionInfo;
      }
    }
    throw new AssertionError("DataRegion leader not found");
  }

  private void triggerLongTermWalBlockingOnDataNode(
      SyncConfigNodeIServiceClient client, int dataNodeId) throws Exception {
    Map<String, String> configItems = new HashMap<>();
    // The throttle threshold used by WALManager is 80% of this value, so 1 makes it 0 and
    // deterministically triggers long-term WAL blocking on the target DataNode heartbeat.
    configItems.put(WAL_THROTTLE_THRESHOLD_IN_BYTE, "1");
    TSStatus status = client.setConfiguration(new TSetConfigurationReq(configItems, dataNodeId));
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
  }

  private String getNodeStatusWithReason(SyncConfigNodeIServiceClient client, int dataNodeId)
      throws Exception {
    TShowClusterResp showClusterResp = client.showCluster();
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), showClusterResp.getStatus().getCode());
    return showClusterResp.getNodeStatus().get(dataNodeId);
  }

  private boolean hasNoLeaderOnReadOnlyDataNode(
      SyncConfigNodeIServiceClient client, int readOnlyDataNodeId) throws Exception {
    for (TRegionInfo regionInfo : getUserDataRegionInfoList(client)) {
      if (RegionRoleType.Leader.getRoleType().equals(regionInfo.getRoleType())) {
        if (regionInfo.getDataNodeId() == readOnlyDataNodeId
            || NodeStatus.ReadOnly.getStatus().equals(regionInfo.getStatus())) {
          return false;
        }
      }
    }
    return true;
  }

  private List<TRegionInfo> getUserDataRegionInfoList(SyncConfigNodeIServiceClient client)
      throws Exception {
    TShowRegionResp showRegionResp = client.showRegion(new TShowRegionReq());
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), showRegionResp.getStatus().getCode());

    List<TRegionInfo> result = new ArrayList<>();
    for (TRegionInfo regionInfo : showRegionResp.getRegionInfoList()) {
      if (TConsensusGroupType.DataRegion.equals(regionInfo.getConsensusGroupId().getType())
          && !regionInfo.getDatabase().startsWith(SystemConstant.SYSTEM_DATABASE)
          && !regionInfo.getDatabase().startsWith(SystemConstant.AUDIT_DATABASE)) {
        result.add(regionInfo);
      }
    }
    return result;
  }

  private void waitUntil(String condition, WaitCondition waitCondition) throws Exception {
    for (int retry = 0; retry < RETRY_NUM; retry++) {
      if (waitCondition.evaluate()) {
        return;
      }
      TimeUnit.SECONDS.sleep(1);
    }
    Assert.fail("Failed to wait until " + condition);
  }

  @FunctionalInterface
  private interface WaitCondition {
    boolean evaluate() throws Exception;
  }
}
