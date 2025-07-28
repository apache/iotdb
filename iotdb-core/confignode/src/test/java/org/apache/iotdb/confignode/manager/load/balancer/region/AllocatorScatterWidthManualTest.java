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

/**
 * Assign an allocator than run this test manually. This test will show the scatter width
 * distribution of the specified allocator
 */
public class AllocatorScatterWidthManualTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(AllocatorScatterWidthManualTest.class);

  private static final IRegionGroupAllocator ALLOCATOR = new GreedyRegionGroupAllocator();

  private static final int TEST_DATA_NODE_NUM = 50;
  private static final int DATA_REGION_PER_DATA_NODE = 5;
  private static final int DATA_REPLICATION_FACTOR = 3;

  private static final Map<Integer, TDataNodeConfiguration> AVAILABLE_DATA_NODE_MAP =
      new HashMap<>();
  private static final Map<Integer, Double> FREE_SPACE_MAP = new HashMap<>();

  @BeforeClass
  public static void setUp() {
    // Construct TEST_DATA_NODE_NUM DataNodes
    Random random = new Random();
    for (int i = 1; i <= TEST_DATA_NODE_NUM; i++) {
      AVAILABLE_DATA_NODE_MAP.put(
          i, new TDataNodeConfiguration().setLocation(new TDataNodeLocation().setDataNodeId(i)));
      FREE_SPACE_MAP.put(i, random.nextDouble());
    }
  }

  @Test
  public void testScatterWidth() {
    final int dataRegionGroupNum =
        DATA_REGION_PER_DATA_NODE * TEST_DATA_NODE_NUM / DATA_REPLICATION_FACTOR;

    /* Allocate DataRegionGroups */
    long startTime = System.currentTimeMillis();
    List<TRegionReplicaSet> allocateResult = new ArrayList<>();
    for (int index = 0; index < dataRegionGroupNum; index++) {
      allocateResult.add(
          ALLOCATOR.generateOptimalRegionReplicasDistribution(
              AVAILABLE_DATA_NODE_MAP,
              FREE_SPACE_MAP,
              allocateResult,
              allocateResult,
              DATA_REPLICATION_FACTOR,
              new TConsensusGroupId(TConsensusGroupType.DataRegion, index)));
    }
    long endTime = System.currentTimeMillis();

    /* Statistics result */
    // Map<DataNodeId, RegionGroup Count>
    Map<Integer, Integer> regionCounter = new HashMap<>();
    allocateResult.forEach(
        regionReplicaSet ->
            regionReplicaSet
                .getDataNodeLocations()
                .forEach(
                    dataNodeLocation ->
                        regionCounter.merge(dataNodeLocation.getDataNodeId(), 1, Integer::sum)));
    // Map<DataNodeId, ScatterWidth>
    // where a true in the bitset denotes the corresponding DataNode can help the DataNode in
    // Map-Key to share the RegionGroup-leader and restore data when restarting.
    // The more true in the bitset, the more safety the cluster DataNode in Map-Key is.
    Map<Integer, BitSet> scatterWidthMap = new HashMap<>();
    for (TRegionReplicaSet replicaSet : allocateResult) {
      for (int i = 0; i < DATA_REPLICATION_FACTOR; i++) {
        for (int j = i + 1; j < DATA_REPLICATION_FACTOR; j++) {
          int dataNodeId1 = replicaSet.getDataNodeLocations().get(i).getDataNodeId();
          int dataNodeId2 = replicaSet.getDataNodeLocations().get(j).getDataNodeId();
          scatterWidthMap.computeIfAbsent(dataNodeId1, empty -> new BitSet()).set(dataNodeId2);
          scatterWidthMap.computeIfAbsent(dataNodeId2, empty -> new BitSet()).set(dataNodeId1);
        }
      }
    }
    int scatterWidthSum = 0;
    int minScatterWidth = Integer.MAX_VALUE;
    int maxScatterWidth = Integer.MIN_VALUE;
    for (int i = 1; i <= TEST_DATA_NODE_NUM; i++) {
      Assert.assertTrue(regionCounter.get(i) <= DATA_REGION_PER_DATA_NODE);
      int scatterWidth = scatterWidthMap.get(i).cardinality();
      scatterWidthSum += scatterWidth;
      minScatterWidth = Math.min(minScatterWidth, scatterWidth);
      maxScatterWidth = Math.max(maxScatterWidth, scatterWidth);
    }

    LOGGER.info("Test {}:", ALLOCATOR.getClass().getSimpleName());
    LOGGER.info(
        "Allocate {} DataRegionGroups for {} DataNodes in {}ms",
        dataRegionGroupNum,
        TEST_DATA_NODE_NUM,
        endTime - startTime);
    LOGGER.info(
        "Scatter width avg: {}, min: {}, max: {}",
        (double) scatterWidthSum / TEST_DATA_NODE_NUM,
        minScatterWidth,
        maxScatterWidth);
  }
}
