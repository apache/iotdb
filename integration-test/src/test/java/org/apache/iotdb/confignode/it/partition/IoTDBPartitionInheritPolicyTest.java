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
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.it.utils.ConfigNodeTestUtils;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TSetStorageGroupReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionResp;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.ConfigFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBPartitionInheritPolicyTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBPartitionInheritPolicyTest.class);

  private static boolean originalEnableDataPartitionInheritPolicy;
  private static final boolean testEnableDataPartitionInheritPolicy = true;

  private static String originalDataRegionConsensusProtocolClass;
  private static final String testDataRegionConsensusProtocolClass =
      ConsensusFactory.RATIS_CONSENSUS;

  private static int originalDataReplicationFactor;
  private static final int testReplicationFactor = 3;

  private static long originalTimePartitionInterval;
  private static final long testTimePartitionInterval = 604800000;

  private static final String sg = "root.sg";
  private static final int storageGroupNum = 5;
  private static final int seriesPartitionSlotsNum = 10000;
  private static final int timePartitionSlotsNum = 10;

  @BeforeClass
  public static void setUp() throws Exception {
    originalDataRegionConsensusProtocolClass =
        ConfigFactory.getConfig().getDataRegionConsensusProtocolClass();
    ConfigFactory.getConfig()
        .setDataRegionConsensusProtocolClass(testDataRegionConsensusProtocolClass);

    originalEnableDataPartitionInheritPolicy =
        ConfigFactory.getConfig().isEnableDataPartitionInheritPolicy();
    ConfigFactory.getConfig()
        .setEnableDataPartitionInheritPolicy(testEnableDataPartitionInheritPolicy);

    originalDataReplicationFactor = ConfigFactory.getConfig().getDataReplicationFactor();
    ConfigFactory.getConfig().setDataReplicationFactor(testReplicationFactor);

    originalTimePartitionInterval = ConfigFactory.getConfig().getTimePartitionInterval();
    ConfigFactory.getConfig().setTimePartitionInterval(testTimePartitionInterval);

    EnvFactory.getEnv().initBeforeClass();

    // Set StorageGroups
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      for (int i = 0; i < storageGroupNum; i++) {
        TSetStorageGroupReq setReq = new TSetStorageGroupReq(new TStorageGroupSchema(sg + i));
        TSStatus status = client.setStorageGroup(setReq);
        Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      }
    }
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanAfterClass();

    ConfigFactory.getConfig()
        .setDataRegionConsensusProtocolClass(originalDataRegionConsensusProtocolClass);
    ConfigFactory.getConfig()
        .setEnableDataPartitionInheritPolicy(originalEnableDataPartitionInheritPolicy);
    ConfigFactory.getConfig().setDataReplicationFactor(originalDataReplicationFactor);
    ConfigFactory.getConfig().setTimePartitionInterval(originalTimePartitionInterval);
  }

  @Test
  public void testDataPartitionInheritPolicy()
      throws TException, IOException, InterruptedException {
    final int seriesPartitionBatchSize = 100;
    final int timePartitionBatchSize = 10;

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      TDataPartitionReq dataPartitionReq = new TDataPartitionReq();
      TDataPartitionTableResp dataPartitionTableResp;
      Map<String, Map<TSeriesPartitionSlot, List<TTimePartitionSlot>>> partitionSlotsMap;

      for (int i = 0; i < storageGroupNum; i++) {
        String storageGroup = sg + i;
        for (int j = 0; j < seriesPartitionSlotsNum; j += seriesPartitionBatchSize) {
          for (long k = 0; k < timePartitionSlotsNum; k += timePartitionBatchSize) {
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
                    regionInfo.getSeriesSlots() * timePartitionSlotsNum, regionInfo.getTimeSlots());
              });
    }
  }
}
