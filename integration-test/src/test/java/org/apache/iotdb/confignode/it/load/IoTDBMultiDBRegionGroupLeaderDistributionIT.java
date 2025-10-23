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
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.cluster.RegionRoleType;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.confignode.it.utils.ConfigNodeTestUtils;
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

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBMultiDBRegionGroupLeaderDistributionIT {

  private static final int TEST_DATA_NODE_NUM = 3;
  private static final int TEST_REPLICATION_FACTOR = 2;
  private static final String TEST_DATA_REGION_CONSENSUS_PROTOCOL_CLASS =
      ConsensusFactory.IOT_CONSENSUS;

  private static final String DATABASE = "root.db";
  private static final int TEST_DATABASE_NUM = 2;
  private static final int TEST_MIN_DATA_REGION_GROUP_NUM = 4;
  private static final long TEST_TIME_PARTITION_INTERVAL = 604800000;

  @Before
  public void setUp() {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setEnableAutoLeaderBalanceForIoTConsensus(true)
        .setDataReplicationFactor(TEST_REPLICATION_FACTOR)
        .setDataRegionConsensusProtocolClass(TEST_DATA_REGION_CONSENSUS_PROTOCOL_CLASS);
    EnvFactory.getEnv().initClusterEnvironment(1, TEST_DATA_NODE_NUM);
  }

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testMultiDatabaseLeaderDistribution()
      throws ClientManagerException, IOException, InterruptedException, TException {
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      for (int i = 0; i < TEST_DATABASE_NUM; i++) {
        String curDb = DATABASE + i;
        TSStatus status =
            client.setDatabase(
                new TDatabaseSchema(curDb)
                    .setMinDataRegionGroupNum(TEST_MIN_DATA_REGION_GROUP_NUM));
        Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
        // Insert DataPartitions to create DataRegionGroups
        Map<String, Map<TSeriesPartitionSlot, TTimeSlotList>> partitionSlotsMap =
            ConfigNodeTestUtils.constructPartitionSlotsMap(
                curDb, 0, 10, 0, 10, TEST_TIME_PARTITION_INTERVAL);
        TDataPartitionTableResp dataPartitionTableResp =
            client.getOrCreateDataPartitionTable(new TDataPartitionReq(partitionSlotsMap));
        Assert.assertEquals(
            TSStatusCode.SUCCESS_STATUS.getStatusCode(),
            dataPartitionTableResp.getStatus().getCode());
      }

      final int retryNum = 50;
      for (int retry = 0; retry < retryNum; retry++) {
        // Check the number of RegionGroup-leader in each DataNode.
        Map<Integer, Integer> dataNodeLeaderCounter = new TreeMap<>();
        Map<String, Map<Integer, Integer>> databaseLeaderCounter = new TreeMap<>();
        TShowRegionResp showRegionResp = client.showRegion(new TShowRegionReq());
        showRegionResp
            .getRegionInfoList()
            // Skip AUDIT database
            .removeIf(r -> r.database.startsWith(SystemConstant.AUDIT_DATABASE));
        showRegionResp
            .getRegionInfoList()
            .forEach(
                regionInfo -> {
                  if (RegionRoleType.Leader.getRoleType().equals(regionInfo.getRoleType())) {
                    dataNodeLeaderCounter.merge(regionInfo.getDataNodeId(), 1, Integer::sum);
                    databaseLeaderCounter
                        .computeIfAbsent(regionInfo.getDatabase(), k -> new TreeMap<>())
                        .merge(regionInfo.getDataNodeId(), 1, Integer::sum);
                  }
                });
        AtomicBoolean pass = new AtomicBoolean(true);
        // The number of Region-leader in each DataNode should be equal
        if (dataNodeLeaderCounter.size() != TEST_DATA_NODE_NUM) {
          pass.set(false);
        }
        if (dataNodeLeaderCounter.values().stream().max(Integer::compareTo).orElse(0)
                - dataNodeLeaderCounter.values().stream().min(Integer::compareTo).orElse(0)
            > 1) {
          pass.set(false);
        }
        // The number of Region-leader in each DataNode within the same Database should be equal
        if (TEST_DATABASE_NUM != databaseLeaderCounter.size()) {
          pass.set(false);
        }
        databaseLeaderCounter.forEach(
            (database, leaderCounter) -> {
              if (leaderCounter.values().stream().max(Integer::compareTo).orElse(0)
                      - leaderCounter.values().stream().min(Integer::compareTo).orElse(0)
                  > 1) {
                pass.set(false);
              }
            });
        if (pass.get()) {
          return;
        }
        TimeUnit.SECONDS.sleep(1);
      }
      Assert.fail("The leader distribution is not balanced after " + retryNum + " retries.");
    }
  }
}
