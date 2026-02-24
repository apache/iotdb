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

import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBPartitionInheritStrategyIT {

  private static final String testDataRegionConsensusProtocolClass =
      ConsensusFactory.RATIS_CONSENSUS;
  private static final int testReplicationFactor = 1;
  private static final int testSeriesSlotNum = 1000;
  private static final long testTimePartitionInterval = 604800000;
  private static final double testDataRegionPerDataNode = 5.0;

  private static final String database = "root.database";
  private static final int seriesPartitionSlotBatchSize = 100;
  private static final int testTimePartitionSlotsNum = 100;
  private static final int timePartitionBatchSize = 10;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setDataRegionConsensusProtocolClass(testDataRegionConsensusProtocolClass)
        .setDataReplicationFactor(testReplicationFactor)
        .setTimePartitionInterval(testTimePartitionInterval)
        .setSeriesSlotNum(testSeriesSlotNum)
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
  public void testDataPartitionInheritStrategy() throws Exception {
    final long baseStartTime = 1000;
    Map<TSeriesPartitionSlot, TConsensusGroupId> dataAllotTable1 = new ConcurrentHashMap<>();

    // Test1: divide and inherit DataPartitions from scratch
    for (long timePartitionSlot = baseStartTime;
        timePartitionSlot < baseStartTime + testTimePartitionSlotsNum;
        timePartitionSlot++) {
      for (int seriesPartitionSlot = 0;
          seriesPartitionSlot < testSeriesSlotNum;
          seriesPartitionSlot += seriesPartitionSlotBatchSize) {
        ConfigNodeTestUtils.getOrCreateDataPartitionWithRetry(
            database,
            seriesPartitionSlot,
            seriesPartitionSlot + seriesPartitionSlotBatchSize,
            timePartitionSlot,
            timePartitionSlot + 1,
            testTimePartitionInterval);
      }
    }

    int mu = (int) (testSeriesSlotNum / testDataRegionPerDataNode);
    TDataPartitionTableResp dataPartitionTableResp =
        ConfigNodeTestUtils.getDataPartitionWithRetry(
            database,
            0,
            testSeriesSlotNum,
            baseStartTime,
            baseStartTime + testTimePartitionSlotsNum,
            testTimePartitionInterval);
    Assert.assertNotNull(dataPartitionTableResp);

    // All DataRegionGroups divide all SeriesSlots evenly
    final int expectedPartitionNum1 = mu * testTimePartitionSlotsNum;
    Map<TConsensusGroupId, Integer> counter =
        ConfigNodeTestUtils.countDataPartition(
            dataPartitionTableResp.getDataPartitionTable().get(database));
    counter.forEach((groupId, num) -> Assert.assertEquals(expectedPartitionNum1, num.intValue()));

    // Test DataPartition inherit policy
    dataPartitionTableResp
        .getDataPartitionTable()
        .get(database)
        .forEach(
            ((seriesPartitionSlot, timePartitionSlotMap) -> {
              // All Timeslots belonging to the same SeriesSlot are allocated to the same
              // DataRegionGroup
              TConsensusGroupId groupId =
                  timePartitionSlotMap
                      .get(new TTimePartitionSlot(baseStartTime * testTimePartitionInterval))
                      .get(0);
              timePartitionSlotMap.forEach(
                  (timePartitionSlot, groupIdList) ->
                      Assert.assertEquals(groupId, groupIdList.get(0)));
              dataAllotTable1.put(seriesPartitionSlot, groupId);
            }));

    // Register a new DataNode to extend DataRegionGroups
    EnvFactory.getEnv().registerNewDataNode(true);

    // Test2: divide and inherit DataPartitions after extension
    mu = (int) (testSeriesSlotNum / (testDataRegionPerDataNode * 2));
    dataPartitionTableResp =
        ConfigNodeTestUtils.getOrCreateDataPartitionWithRetry(
            database,
            0,
            testSeriesSlotNum,
            baseStartTime + testTimePartitionSlotsNum,
            baseStartTime + testTimePartitionSlotsNum + timePartitionBatchSize,
            testTimePartitionInterval);
    Assert.assertNotNull(dataPartitionTableResp);

    // All DataRegionGroups divide all SeriesSlots evenly
    counter =
        ConfigNodeTestUtils.countDataPartition(
            dataPartitionTableResp.getDataPartitionTable().get(database));
    final int expectedPartitionNum2 = mu * timePartitionBatchSize;
    counter.forEach((groupId, num) -> Assert.assertEquals(expectedPartitionNum2, num.intValue()));

    // Test DataPartition inherit policy
    AtomicInteger inheritedSeriesSlotNum = new AtomicInteger(0);
    Map<TSeriesPartitionSlot, TConsensusGroupId> dataAllotTable2 = new ConcurrentHashMap<>();
    dataPartitionTableResp
        .getDataPartitionTable()
        .get(database)
        .forEach(
            ((seriesPartitionSlot, timePartitionSlotMap) -> {
              // All Timeslots belonging to the same SeriesSlot are allocated to the same
              // DataRegionGroup
              TConsensusGroupId groupId =
                  timePartitionSlotMap
                      .get(
                          new TTimePartitionSlot(
                              (baseStartTime + testTimePartitionSlotsNum)
                                  * testTimePartitionInterval))
                      .get(0);
              timePartitionSlotMap.forEach(
                  (timePartitionSlot, groupIdList) ->
                      Assert.assertEquals(groupId, groupIdList.get(0)));

              if (dataAllotTable1.containsValue(groupId)) {
                // The DataRegionGroup has been inherited
                Assert.assertTrue(dataAllotTable1.containsKey(seriesPartitionSlot));
                Assert.assertEquals(dataAllotTable1.get(seriesPartitionSlot), groupId);
                inheritedSeriesSlotNum.incrementAndGet();
              }
              dataAllotTable2.put(seriesPartitionSlot, groupId);
            }));
    // Exactly half of the SeriesSlots are inherited
    Assert.assertEquals(testSeriesSlotNum / 2, inheritedSeriesSlotNum.get());

    // Test3: historical DataPartitions will inherit successor
    Random random = new Random();
    Set<Integer> allocatedSlots = new HashSet<>();
    for (int i = 0; i < 10; i++) {
      int slot = random.nextInt(testSeriesSlotNum);
      while (allocatedSlots.contains(slot)) {
        slot = random.nextInt(testSeriesSlotNum);
      }
      allocatedSlots.add(slot);
      dataPartitionTableResp =
          ConfigNodeTestUtils.getOrCreateDataPartitionWithRetry(
              database,
              slot,
              slot + 1,
              baseStartTime - 1,
              baseStartTime,
              testTimePartitionInterval);
      Assert.assertNotNull(dataPartitionTableResp);

      TSeriesPartitionSlot seriesPartitionSlot = new TSeriesPartitionSlot(slot);
      TTimePartitionSlot timePartitionSlot =
          new TTimePartitionSlot((baseStartTime - 1) * testTimePartitionInterval);
      Assert.assertEquals(
          dataAllotTable1.get(seriesPartitionSlot),
          dataPartitionTableResp
              .getDataPartitionTable()
              .get(database)
              .get(seriesPartitionSlot)
              .get(timePartitionSlot)
              .get(0));
    }

    // Test4: future DataPartitions will inherit predecessor
    allocatedSlots.clear();
    for (int i = 0; i < 10; i++) {
      int slot = random.nextInt(testSeriesSlotNum);
      while (allocatedSlots.contains(slot)) {
        slot = random.nextInt(testSeriesSlotNum);
      }
      allocatedSlots.add(slot);
      dataPartitionTableResp =
          ConfigNodeTestUtils.getOrCreateDataPartitionWithRetry(
              database,
              slot,
              slot + 1,
              baseStartTime + 999,
              baseStartTime + 1000,
              testTimePartitionInterval);
      Assert.assertNotNull(dataPartitionTableResp);

      TSeriesPartitionSlot seriesPartitionSlot = new TSeriesPartitionSlot(slot);
      TTimePartitionSlot timePartitionSlot =
          new TTimePartitionSlot((baseStartTime + 999) * testTimePartitionInterval);
      Assert.assertEquals(
          dataAllotTable2.get(seriesPartitionSlot),
          dataPartitionTableResp
              .getDataPartitionTable()
              .get(database)
              .get(seriesPartitionSlot)
              .get(timePartitionSlot)
              .get(0));
    }
  }
}
