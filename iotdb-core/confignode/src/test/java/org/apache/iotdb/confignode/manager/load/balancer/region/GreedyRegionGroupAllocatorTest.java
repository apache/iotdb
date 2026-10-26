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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class GreedyRegionGroupAllocatorTest {

  private static final GreedyRegionGroupAllocator ALLOCATOR = new GreedyRegionGroupAllocator();
  private static final int TEST_REPLICATION_FACTOR = 3;

  /**
   * Multi-database regression: each database's replicas should be evenly distributed across all
   * DataNodes. With 2 databases × 30 RGs × rf 3 on 10 DNs, per-(db, DN) replica count must be
   * exactly 9 (60 RGs × 3 / 10 / 2 = 9).
   */
  @Test
  public void testPerDatabaseBalance() {
    int dataNodeNum = 10;
    int databaseNum = 2;
    int regionGroupsPerDatabase = 30;
    int rf = 3;

    Map<Integer, TDataNodeConfiguration> availableDataNodeMap = new ConcurrentHashMap<>();
    Map<Integer, Double> freeSpaceMap = new ConcurrentHashMap<>();
    Random random = new Random(0);
    for (int i = 1; i <= dataNodeNum; i++) {
      availableDataNodeMap.put(
          i, new TDataNodeConfiguration().setLocation(new TDataNodeLocation().setDataNodeId(i)));
      freeSpaceMap.put(i, random.nextDouble());
    }

    List<TRegionReplicaSet> allocatedRegionGroups = new ArrayList<>();
    Map<Integer, List<TRegionReplicaSet>> databaseAllocatedRegionGroups = new TreeMap<>();
    for (int db = 0; db < databaseNum; db++) {
      databaseAllocatedRegionGroups.put(db, new ArrayList<>());
    }

    int regionId = 0;
    for (int round = 0; round < regionGroupsPerDatabase; round++) {
      for (int db = 0; db < databaseNum; db++) {
        TRegionReplicaSet newRegionGroup =
            ALLOCATOR.generateOptimalRegionReplicasDistribution(
                availableDataNodeMap,
                freeSpaceMap,
                allocatedRegionGroups,
                databaseAllocatedRegionGroups.get(db),
                rf,
                new TConsensusGroupId(TConsensusGroupType.DataRegion, regionId++));
        allocatedRegionGroups.add(newRegionGroup);
        databaseAllocatedRegionGroups.get(db).add(newRegionGroup);
      }
    }

    int expectedPerDb = regionGroupsPerDatabase * rf / dataNodeNum;
    for (int db = 0; db < databaseNum; db++) {
      Map<Integer, Integer> perDnReplicaCount = new HashMap<>();
      for (TRegionReplicaSet rg : databaseAllocatedRegionGroups.get(db)) {
        for (TDataNodeLocation loc : rg.getDataNodeLocations()) {
          perDnReplicaCount.merge(loc.getDataNodeId(), 1, Integer::sum);
        }
      }
      for (int dnId = 1; dnId <= dataNodeNum; dnId++) {
        Assert.assertEquals(
            "db " + db + " dn " + dnId + " replica count",
            expectedPerDb,
            perDnReplicaCount.getOrDefault(dnId, 0).intValue());
      }
    }

    Map<Integer, Integer> globalCount = new HashMap<>();
    for (TRegionReplicaSet rg : allocatedRegionGroups) {
      for (TDataNodeLocation loc : rg.getDataNodeLocations()) {
        globalCount.merge(loc.getDataNodeId(), 1, Integer::sum);
      }
    }
    int expectedGlobal = databaseNum * regionGroupsPerDatabase * rf / dataNodeNum;
    for (int dnId = 1; dnId <= dataNodeNum; dnId++) {
      Assert.assertEquals(
          "dn " + dnId + " global replica count",
          expectedGlobal,
          globalCount.getOrDefault(dnId, 0).intValue());
    }
  }

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
