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
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.it.utils.ConfigNodeTestUtils;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBPartitionShuffleStrategyIT {

  private static final String testDataRegionConsensusProtocolClass =
      ConsensusFactory.RATIS_CONSENSUS;
  private static final int testReplicationFactor = 1;
  public static final String SHUFFLE = "SHUFFLE";
  private static final int testSeriesSlotNum = 1000;
  private static final long testTimePartitionInterval = 604800000;
  private static final double testDataRegionPerDataNode = 5.0;

  private static final String database = "root.database";
  private static final int testTimePartitionSlotsNum = 100;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setDataRegionConsensusProtocolClass(testDataRegionConsensusProtocolClass)
        .setDataReplicationFactor(testReplicationFactor)
        .setTimePartitionInterval(testTimePartitionInterval)
        .setSeriesSlotNum(testSeriesSlotNum)
        .setDataPartitionAllocationStrategy(SHUFFLE)
        .setDataRegionPerDataNode(testDataRegionPerDataNode);

    // Init 1C1D environment
    EnvFactory.getEnv().initClusterEnvironment(1, 1);

    // Set Database
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      TSStatus status = client.setDatabase(new TDatabaseSchema(database));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    }
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testDataPartitionShuffleStrategy() throws Exception {
    List<Integer> randomTimeSlotList = new ArrayList<>();
    for (int i = 0; i < testTimePartitionSlotsNum; i++) {
      randomTimeSlotList.add(i);
    }
    Collections.shuffle(randomTimeSlotList);
    for (int timeSlotId : randomTimeSlotList) {
      // To test the shuffle strategy, we merely need to use a random time slot order
      ConfigNodeTestUtils.getOrCreateDataPartitionWithRetry(
          database, 0, testSeriesSlotNum, timeSlotId, timeSlotId + 1, testTimePartitionInterval);
    }
    TDataPartitionTableResp dataPartitionTableResp;
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      dataPartitionTableResp =
          client.getDataPartitionTable(
              new TDataPartitionReq(
                  ConfigNodeTestUtils.constructPartitionSlotsMap(
                      database,
                      0,
                      testSeriesSlotNum,
                      0,
                      testTimePartitionSlotsNum,
                      testTimePartitionInterval)));
    }
    Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TConsensusGroupId>>>>
        partitionTable = dataPartitionTableResp.getDataPartitionTable();
    for (long currentStartTime = testTimePartitionInterval;
        currentStartTime < testTimePartitionInterval * testTimePartitionSlotsNum;
        currentStartTime += testTimePartitionInterval) {
      TTimePartitionSlot precedingTimeSlot =
          new TTimePartitionSlot(currentStartTime - testTimePartitionInterval);
      TTimePartitionSlot currentTimeSlot = new TTimePartitionSlot(currentStartTime);
      for (int seriesSlotId = 0; seriesSlotId < testSeriesSlotNum; seriesSlotId++) {
        TSeriesPartitionSlot seriesPartitionSlot = new TSeriesPartitionSlot(seriesSlotId);
        List<TConsensusGroupId> precedingRegionGroupIds =
            partitionTable.get(database).get(seriesPartitionSlot).get(precedingTimeSlot);
        List<TConsensusGroupId> currentRegionGroupIds =
            partitionTable.get(database).get(seriesPartitionSlot).get(currentTimeSlot);
        Assert.assertEquals(precedingRegionGroupIds.size(), currentRegionGroupIds.size());
        for (int i = 0; i < precedingRegionGroupIds.size(); i++) {
          // Ensure that the RegionGroupId is different in two adjacent TimePartitionSlots
          Assert.assertNotEquals(precedingRegionGroupIds.get(i), currentRegionGroupIds.get(i));
        }
      }
    }
  }
}
