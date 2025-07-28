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

package org.apache.iotdb.confignode.manager.load.balancer.region;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class GreedyCopySetRegionGroupAllocatorTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(GreedyCopySetRegionGroupAllocatorTest.class);

  private static final GreedyRegionGroupAllocator GREEDY_ALLOCATOR =
      new GreedyRegionGroupAllocator();
  private static final GreedyCopySetRegionGroupAllocator GREEDY_COPY_SET_ALLOCATOR =
      new GreedyCopySetRegionGroupAllocator();

  private static final Random RANDOM = new Random();
  private static final int TEST_DATABASE_NUM = 3;
  private static final int TEST_DATA_NODE_NUM = 21;
  private static final int DATA_REGION_PER_DATA_NODE = 5;
  private static final Map<Integer, TDataNodeConfiguration> AVAILABLE_DATA_NODE_MAP =
      new HashMap<>();
  private static final Map<Integer, Double> FREE_SPACE_MAP = new HashMap<>();

  @BeforeClass
  public static void setUp() {
    // Construct 21 DataNodes
    Random random = new Random();
    for (int i = 1; i <= TEST_DATA_NODE_NUM; i++) {
      AVAILABLE_DATA_NODE_MAP.put(
          i, new TDataNodeConfiguration().setLocation(new TDataNodeLocation().setDataNodeId(i)));
      FREE_SPACE_MAP.put(i, random.nextDouble());
    }
  }

  @Test
  public void test2Factor() {
    testRegionDistributionAndScatterWidth(2);
  }

  @Test
  public void test3Factor() {
    testRegionDistributionAndScatterWidth(3);
  }

  private void testRegionDistributionAndScatterWidth(int replicationFactor) {
    final int dataRegionGroupAllotment =
        DATA_REGION_PER_DATA_NODE * TEST_DATA_NODE_NUM / replicationFactor;
    final int dataRegionGroupPerDatabase = dataRegionGroupAllotment / TEST_DATABASE_NUM;

    /* Allocate DataRegionGroups */
    List<TRegionReplicaSet> greedyResult = new ArrayList<>();
    List<TRegionReplicaSet> greedyCopySetResult = new ArrayList<>();
    Map<Integer, List<TRegionReplicaSet>> greedyCopySetDatabaseResult = new TreeMap<>();
    // Map<DataNodeId, RegionGroup Count> for greedy algorithm
    Map<Integer, Integer> greedyRegionCounter = new TreeMap<>();
    // Map<DataNodeId, RegionGroup Count> for greedy-copy-set algorithm
    Map<Integer, Integer> greedyCopySetRegionCounter = new TreeMap<>();
    // Map<DatabaseId, Map<DataNodeId, RegionGroup Count>>
    Map<Integer, Map<Integer, Integer>> greedyCopySetDatabaseRegionCounter = new TreeMap<>();
    for (int i = 0; i < TEST_DATABASE_NUM; i++) {
      greedyCopySetDatabaseResult.put(i, new ArrayList<>());
    }
    for (int index = 0; index < dataRegionGroupPerDatabase * TEST_DATABASE_NUM; index++) {
      TRegionReplicaSet greedyRegionGroup =
          GREEDY_ALLOCATOR.generateOptimalRegionReplicasDistribution(
              AVAILABLE_DATA_NODE_MAP,
              FREE_SPACE_MAP,
              greedyResult,
              greedyResult,
              replicationFactor,
              new TConsensusGroupId(TConsensusGroupType.DataRegion, index));
      greedyResult.add(greedyRegionGroup);
      greedyRegionGroup
          .getDataNodeLocations()
          .forEach(
              dataNodeLocation ->
                  greedyRegionCounter.merge(dataNodeLocation.getDataNodeId(), 1, Integer::sum));
      int databaseId = RANDOM.nextInt(TEST_DATABASE_NUM);
      TRegionReplicaSet greedyCopySetRegionGroup =
          GREEDY_COPY_SET_ALLOCATOR.generateOptimalRegionReplicasDistribution(
              AVAILABLE_DATA_NODE_MAP,
              FREE_SPACE_MAP,
              greedyCopySetResult,
              greedyCopySetDatabaseResult.get(databaseId),
              replicationFactor,
              new TConsensusGroupId(TConsensusGroupType.DataRegion, index));
      greedyCopySetResult.add(greedyCopySetRegionGroup);
      greedyCopySetDatabaseResult.get(databaseId).add(greedyCopySetRegionGroup);
      greedyCopySetRegionGroup
          .getDataNodeLocations()
          .forEach(
              dataNodeLocation -> {
                greedyCopySetRegionCounter.merge(dataNodeLocation.getDataNodeId(), 1, Integer::sum);
                greedyCopySetDatabaseRegionCounter
                    .computeIfAbsent(databaseId, empty -> new TreeMap<>())
                    .merge(dataNodeLocation.getDataNodeId(), 1, Integer::sum);
              });
      LOGGER.info(
          "After allocate RegionGroup: {}, Database: {}, plan: {}",
          index,
          databaseId,
          greedyCopySetRegionGroup.getDataNodeLocations().stream()
              .map(TDataNodeLocation::getDataNodeId)
              .collect(Collectors.toList()));
      for (int i = 0; i < TEST_DATABASE_NUM; i++) {
        LOGGER.info("Database {}: {}", i, greedyCopySetDatabaseRegionCounter.get(i));
      }
      LOGGER.info("Cluster   : {}", greedyCopySetRegionCounter);
      for (int i = 1; i <= TEST_DATA_NODE_NUM; i++) {
        Assert.assertTrue(
            greedyCopySetRegionCounter.getOrDefault(i, 0) <= DATA_REGION_PER_DATA_NODE);
      }
    }

    /* Statistics result */
    // Map<DataNodeId, ScatterWidth> for greedy algorithm
    // where a true in the bitset denotes the corresponding DataNode can help the DataNode in
    // Map-Key to share the RegionGroup-leader and restore data when restarting.
    // The more true in the bitset, the more safety the cluster DataNode in Map-Key is.
    Map<Integer, BitSet> greedyScatterWidth = new TreeMap<>();
    for (TRegionReplicaSet replicaSet : greedyResult) {
      for (int i = 0; i < replicationFactor; i++) {
        for (int j = i + 1; j < replicationFactor; j++) {
          int dataNodeId1 = replicaSet.getDataNodeLocations().get(i).getDataNodeId();
          int dataNodeId2 = replicaSet.getDataNodeLocations().get(j).getDataNodeId();
          greedyScatterWidth.computeIfAbsent(dataNodeId1, empty -> new BitSet()).set(dataNodeId2);
          greedyScatterWidth.computeIfAbsent(dataNodeId2, empty -> new BitSet()).set(dataNodeId1);
        }
      }
    }
    // Map<DataNodeId, ScatterWidth> for greedy-copy-set algorithm, ditto
    Map<Integer, BitSet> greedyCopySetScatterWidth = new TreeMap<>();
    for (TRegionReplicaSet replicaSet : greedyCopySetResult) {
      for (int i = 0; i < replicationFactor; i++) {
        for (int j = i + 1; j < replicationFactor; j++) {
          int dataNodeId1 = replicaSet.getDataNodeLocations().get(i).getDataNodeId();
          int dataNodeId2 = replicaSet.getDataNodeLocations().get(j).getDataNodeId();
          greedyCopySetScatterWidth
              .computeIfAbsent(dataNodeId1, empty -> new BitSet())
              .set(dataNodeId2);
          greedyCopySetScatterWidth
              .computeIfAbsent(dataNodeId2, empty -> new BitSet())
              .set(dataNodeId1);
        }
      }
    }

    /* Check result */
    int greedyScatterWidthSum = 0;
    int greedyMinScatterWidth = Integer.MAX_VALUE;
    int greedyMaxScatterWidth = Integer.MIN_VALUE;
    int greedyCopySetScatterWidthSum = 0;
    int greedyCopySetMinScatterWidth = Integer.MAX_VALUE;
    int greedyCopySetMaxScatterWidth = Integer.MIN_VALUE;
    int greedyCopySetMaxRegionCount = 0;
    int greedyCopySetMinRegionCount = Integer.MAX_VALUE;
    for (int i = 1; i <= TEST_DATA_NODE_NUM; i++) {
      Assert.assertTrue(greedyRegionCounter.get(i) <= DATA_REGION_PER_DATA_NODE);
      Assert.assertTrue(greedyCopySetRegionCounter.get(i) <= DATA_REGION_PER_DATA_NODE);
      greedyCopySetMinRegionCount =
          Math.min(greedyCopySetMinRegionCount, greedyCopySetRegionCounter.get(i));
      greedyCopySetMaxRegionCount =
          Math.max(greedyCopySetMaxRegionCount, greedyCopySetRegionCounter.get(i));

      int scatterWidth = greedyScatterWidth.get(i).cardinality();
      greedyScatterWidthSum += scatterWidth;
      greedyMinScatterWidth = Math.min(greedyMinScatterWidth, scatterWidth);
      greedyMaxScatterWidth = Math.max(greedyMaxScatterWidth, scatterWidth);

      scatterWidth = greedyCopySetScatterWidth.get(i).cardinality();
      greedyCopySetScatterWidthSum += scatterWidth;
      greedyCopySetMinScatterWidth = Math.min(greedyCopySetMinScatterWidth, scatterWidth);
      greedyCopySetMaxScatterWidth = Math.max(greedyCopySetMaxScatterWidth, scatterWidth);
    }
    // The maximal Region count - minimal Region count should be less than or equal to 1
    Assert.assertTrue(greedyCopySetMaxRegionCount - greedyCopySetMinRegionCount <= 1);
    for (int i = 0; i < TEST_DATABASE_NUM; i++) {
      greedyCopySetMaxRegionCount = 0;
      greedyCopySetMinRegionCount = Integer.MAX_VALUE;
      if (greedyCopySetDatabaseRegionCounter.containsKey(i)) {
        continue;
      }
      for (int j = 1; j <= TEST_DATA_NODE_NUM; j++) {
        if (greedyCopySetDatabaseRegionCounter.get(i).containsKey(j)) {
          greedyCopySetMinRegionCount =
              Math.min(
                  greedyCopySetMinRegionCount, greedyCopySetDatabaseRegionCounter.get(i).get(j));
          greedyCopySetMaxRegionCount =
              Math.max(
                  greedyCopySetMaxRegionCount, greedyCopySetDatabaseRegionCounter.get(i).get(j));
        }
      }
      // The maximal Region count - minimal Region count should be less than or equal to 1 for each
      // database
      Assert.assertTrue(greedyCopySetMaxRegionCount - greedyCopySetMinRegionCount <= 1);
    }

    LOGGER.info(
        "replicationFactor: {}, Scatter width for greedy: avg={}, min={}, max={}",
        replicationFactor,
        (double) greedyScatterWidthSum / TEST_DATA_NODE_NUM,
        greedyMinScatterWidth,
        greedyMaxScatterWidth);
    LOGGER.info(
        "replicationFactor: {}, Scatter width for greedyCopySet: avg={}, min={}, max={}",
        replicationFactor,
        (double) greedyCopySetScatterWidthSum / TEST_DATA_NODE_NUM,
        greedyCopySetMinScatterWidth,
        greedyCopySetMaxScatterWidth);
    Assert.assertTrue(greedyCopySetScatterWidthSum >= greedyScatterWidthSum);
    Assert.assertTrue(greedyCopySetMaxScatterWidth >= greedyMaxScatterWidth);
    Assert.assertTrue(greedyCopySetMinScatterWidth >= greedyMinScatterWidth);
  }
}
