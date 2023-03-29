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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
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

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBPartitionInheritPolicyIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBPartitionInheritPolicyIT.class);
  private static final boolean testEnableDataPartitionInheritPolicy = true;
  private static final String testDataRegionConsensusProtocolClass =
      ConsensusFactory.RATIS_CONSENSUS;
  private static final int testReplicationFactor = 3;
  private static final long testTimePartitionInterval = 604800000;

  private static final String sg = "root.sg";
  private static final int storageGroupNum = 2;
  private static final int testSeriesPartitionSlotNum = 1000;
  private static final int seriesPartitionBatchSize = 10;
  private static final int testTimePartitionSlotsNum = 10;
  private static final int timePartitionBatchSize = 10;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setDataRegionConsensusProtocolClass(testDataRegionConsensusProtocolClass)
        .setEnableDataPartitionInheritPolicy(testEnableDataPartitionInheritPolicy)
        .setDataReplicationFactor(testReplicationFactor)
        .setTimePartitionInterval(testTimePartitionInterval)
        .setSeriesSlotNum(testSeriesPartitionSlotNum * 10);

    // Init 1C3D environment
    EnvFactory.getEnv().initClusterEnvironment(1, 3);

    // Set StorageGroups
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      for (int i = 0; i < storageGroupNum; i++) {
        TSStatus status = client.setDatabase(new TDatabaseSchema(sg + i));
        Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      }
    }
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testDataPartitionInheritPolicy() throws Exception {

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      TDataPartitionReq dataPartitionReq = new TDataPartitionReq();
      TDataPartitionTableResp dataPartitionTableResp;
      Map<String, Map<TSeriesPartitionSlot, TTimeSlotList>> partitionSlotsMap;

      for (int i = 0; i < storageGroupNum; i++) {
        String storageGroup = sg + i;
        for (int j = 0; j < testSeriesPartitionSlotNum; j += seriesPartitionBatchSize) {
          for (long k = 0; k < testTimePartitionSlotsNum; k += timePartitionBatchSize) {
            partitionSlotsMap =
                ConfigNodeTestUtils.constructPartitionSlotsMap(
                    storageGroup,
                    j,
                    j + seriesPartitionBatchSize,
                    k,
                    k + timePartitionBatchSize,
                    testTimePartitionInterval);

            // Let ConfigNode create DataPartition
            dataPartitionReq.setPartitionSlotsMap(partitionSlotsMap);
            for (int retry = 0; retry < 5; retry++) {
              // Build new Client since it's unstable
              try (SyncConfigNodeIServiceClient configNodeClient =
                  (SyncConfigNodeIServiceClient)
                      EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
                dataPartitionTableResp =
                    configNodeClient.getOrCreateDataPartitionTable(dataPartitionReq);
                if (dataPartitionTableResp != null) {
                  break;
                }
              } catch (Exception e) {
                // Retry sometimes in order to avoid request timeout
                LOGGER.error(e.getMessage());
                TimeUnit.SECONDS.sleep(1);
              }
            }
          }
        }
      }

      // Test DataPartition inherit policy
      TShowRegionResp showRegionResp = client.showRegion(new TShowRegionReq());
      showRegionResp
          .getRegionInfoList()
          .forEach(
              regionInfo -> {
                // All Timeslots belonging to the same SeriesSlot are allocated to the same
                // DataRegionGroup
                Assert.assertEquals(
                    regionInfo.getSeriesSlots() * testTimePartitionSlotsNum,
                    regionInfo.getTimeSlots());
              });
    }
  }
}
