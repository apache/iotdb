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
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class GreedyRegionGroupAllocatorTest {

  private static final GreedyRegionGroupAllocator ALLOCATOR = new GreedyRegionGroupAllocator();
  private static final int TEST_REPLICATION_FACTOR = 3;

  @Test
  public void testEvenDistribution() {
    /* Construct input data */
    Map<Integer, TDataNodeConfiguration> availableDataNodeMap = new ConcurrentHashMap<>();
    Map<Integer, Double> freeSpaceMap = new ConcurrentHashMap<>();
    Random random = new Random();
    // Set 6 DataNodes
    for (int i = 0; i < 6; i++) {
      availableDataNodeMap.put(
          i, new TDataNodeConfiguration().setLocation(new TDataNodeLocation().setDataNodeId(i)));
      freeSpaceMap.put(i, random.nextDouble());
    }

    /* Allocate 6 RegionGroups */
    List<TRegionReplicaSet> allocatedRegionGroups = new ArrayList<>();
    for (int index = 0; index < 6; index++) {
      TRegionReplicaSet newRegionGroup =
          ALLOCATOR.generateOptimalRegionReplicasDistribution(
              availableDataNodeMap,
              freeSpaceMap,
              allocatedRegionGroups,
              TEST_REPLICATION_FACTOR,
              new TConsensusGroupId(TConsensusGroupType.DataRegion, index));
      allocatedRegionGroups.add(newRegionGroup);
    }

    /* Check result */
    Map<Integer, AtomicInteger> regionCounter = new ConcurrentHashMap<>();
    allocatedRegionGroups.forEach(
        regionReplicaSet ->
            regionReplicaSet
                .getDataNodeLocations()
                .forEach(
                    dataNodeLocation ->
                        regionCounter
                            .computeIfAbsent(
                                dataNodeLocation.getDataNodeId(), empty -> new AtomicInteger(0))
                            .getAndIncrement()));
    // Each DataNode should have exactly 3 Regions since the all 18 Regions are distributed to 6
    // DataNodes evenly
    Assert.assertEquals(6, regionCounter.size());
    regionCounter.forEach((dataNodeId, regionCount) -> Assert.assertEquals(3, regionCount.get()));
  }

  @Test
  public void testUnevenDistribution() {
    /* Construct input data */
    Map<Integer, TDataNodeConfiguration> availableDataNodeMap = new ConcurrentHashMap<>();
    // Set 4 DataNodes
    for (int i = 0; i < 4; i++) {
      availableDataNodeMap.put(
          i, new TDataNodeConfiguration().setLocation(new TDataNodeLocation().setDataNodeId(i)));
    }
    Map<Integer, Double> freeSpaceMap = new ConcurrentHashMap<>();
    freeSpaceMap.put(0, 20000331d);
    freeSpaceMap.put(1, 20000522d);
    freeSpaceMap.put(2, 666d);
    freeSpaceMap.put(3, 999d);

    /* Allocate the first RegionGroup */
    List<TRegionReplicaSet> allocatedRegionGroups = new ArrayList<>();
    TRegionReplicaSet newRegionGroup =
        ALLOCATOR.generateOptimalRegionReplicasDistribution(
            availableDataNodeMap,
            freeSpaceMap,
            allocatedRegionGroups,
            TEST_REPLICATION_FACTOR,
            new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 0));
    allocatedRegionGroups.add(newRegionGroup);
    Set<Integer> dataNodeIdSet = new HashSet<>();
    newRegionGroup
        .getDataNodeLocations()
        .forEach(dataNodeLocation -> dataNodeIdSet.add(dataNodeLocation.getDataNodeId()));
    // The result should be the 3 DataNodes who have the maximum free disk space
    Assert.assertTrue(dataNodeIdSet.contains(0));
    Assert.assertTrue(dataNodeIdSet.contains(1));
    Assert.assertTrue(dataNodeIdSet.contains(3));
    dataNodeIdSet.clear();

    /* Allocate the second RegionGroup */
    newRegionGroup =
        ALLOCATOR.generateOptimalRegionReplicasDistribution(
            availableDataNodeMap,
            freeSpaceMap,
            allocatedRegionGroups,
            TEST_REPLICATION_FACTOR,
            new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 1));
    newRegionGroup
        .getDataNodeLocations()
        .forEach(dataNodeLocation -> dataNodeIdSet.add(dataNodeLocation.getDataNodeId()));
    // The result should contain the DataNode-2 and
    // other 2 DataNodes who have the maximum free disk space
    Assert.assertTrue(dataNodeIdSet.contains(0));
    Assert.assertTrue(dataNodeIdSet.contains(1));
    Assert.assertTrue(dataNodeIdSet.contains(2));
  }
}
