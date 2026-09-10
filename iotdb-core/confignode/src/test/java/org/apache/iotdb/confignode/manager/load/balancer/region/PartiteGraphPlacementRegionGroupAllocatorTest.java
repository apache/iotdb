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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

public class PartiteGraphPlacementRegionGroupAllocatorTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PartiteGraphPlacementRegionGroupAllocatorTest.class);

  private static final PartiteGraphPlacementRegionGroupAllocator ALLOCATOR =
      new PartiteGraphPlacementRegionGroupAllocator();

  private static final int TEST_DATA_NODE_NUM = 20;
  private static final Map<Integer, TDataNodeConfiguration> AVAILABLE_DATA_NODE_MAP =
      new HashMap<>();
  private static final Map<Integer, Double> FREE_SPACE_MAP = new HashMap<>();

  @BeforeClass
  public static void setUp() {
    Random random = new Random(0);
    for (int i = 1; i <= TEST_DATA_NODE_NUM; i++) {
      AVAILABLE_DATA_NODE_MAP.put(
          i, new TDataNodeConfiguration().setLocation(new TDataNodeLocation().setDataNodeId(i)));
      FREE_SPACE_MAP.put(i, random.nextDouble());
    }
  }

  /**
   * Regression for the user's reported scenario: 20 DataNodes, 2 databases, 60 DataRegions per
   * database, replication factor 3.
   *
   * <p>With the legacy PGP (per-db blind) implementation, the per-(database, DataNode) replica
   * count could swing as far as {2, ..., 15}: some DataNodes ended up holding 15 replicas of one
   * database while only 3 of the other. The per-db-aware implementation must distribute each
   * database's replicas evenly across all DataNodes.
   */
  @Test
  public void testTwoDatabasesPerDbBalance() {
    final int replicationFactor = 3;
    final int regionGroupsPerDatabase = 60;
    final int databaseNum = 2;

    List<TRegionReplicaSet> allocatedRegionGroups = new ArrayList<>();
    Map<Integer, List<TRegionReplicaSet>> databaseAllocatedRegionGroups = new TreeMap<>();
    for (int db = 0; db < databaseNum; db++) {
      databaseAllocatedRegionGroups.put(db, new ArrayList<>());
    }

    // Interleave region group creation between databases the same way IoTDB does in practice
    int regionId = 0;
    for (int round = 0; round < regionGroupsPerDatabase; round++) {
      for (int db = 0; db < databaseNum; db++) {
        TRegionReplicaSet newRegionGroup =
            ALLOCATOR.generateOptimalRegionReplicasDistribution(
                AVAILABLE_DATA_NODE_MAP,
                FREE_SPACE_MAP,
                allocatedRegionGroups,
                databaseAllocatedRegionGroups.get(db),
                replicationFactor,
                new TConsensusGroupId(TConsensusGroupType.DataRegion, regionId++));
        allocatedRegionGroups.add(newRegionGroup);
        databaseAllocatedRegionGroups.get(db).add(newRegionGroup);
      }
    }

    // Per-(db, DN) replica count must be tightly balanced
    for (int db = 0; db < databaseNum; db++) {
      Map<Integer, Integer> perDnReplicaCount =
          countReplicasPerDataNode(databaseAllocatedRegionGroups.get(db));
      int minCount = Integer.MAX_VALUE;
      int maxCount = Integer.MIN_VALUE;
      for (int dnId = 1; dnId <= TEST_DATA_NODE_NUM; dnId++) {
        int c = perDnReplicaCount.getOrDefault(dnId, 0);
        minCount = Math.min(minCount, c);
        maxCount = Math.max(maxCount, c);
      }
      LOGGER.info("db {}: per-DN replica min={}, max={}", db, minCount, maxCount);
      // Expected ideal: each DN holds (regionGroupsPerDatabase * replicationFactor) /
      // TEST_DATA_NODE_NUM = 9 replicas of each db. Max - min should not exceed 1.
      Assert.assertTrue(
          "Per-db replica spread too wide: max=" + maxCount + ", min=" + minCount,
          maxCount - minCount <= 1);
    }

    // Global per-DN replica count must also be tightly balanced
    Map<Integer, Integer> globalCount = countReplicasPerDataNode(allocatedRegionGroups);
    int globalMin = Integer.MAX_VALUE;
    int globalMax = Integer.MIN_VALUE;
    for (int dnId = 1; dnId <= TEST_DATA_NODE_NUM; dnId++) {
      int c = globalCount.getOrDefault(dnId, 0);
      globalMin = Math.min(globalMin, c);
      globalMax = Math.max(globalMax, c);
    }
    LOGGER.info("global per-DN replica min={}, max={}", globalMin, globalMax);
    Assert.assertTrue(
        "Global replica spread too wide: max=" + globalMax + ", min=" + globalMin,
        globalMax - globalMin <= 1);
  }

  /** Multi-database (4 dbs) interleaved allocation with replication factor 3. */
  @Test
  public void testMultiDatabasePerDbBalance() {
    // 40 RGs per db × rf 3 = 120 replicas per db → 6 per DN (integer avg)
    runMultiDatabaseTest(4, 3, 40);
  }

  /** Multi-database (3 dbs) interleaved allocation with replication factor 2. */
  @Test
  public void testTwoFactorMultiDatabaseBalance() {
    // 40 RGs per db × rf 2 = 80 replicas per db → 4 per DN (integer avg)
    runMultiDatabaseTest(3, 2, 40);
  }

  /** Multi-database (3 dbs) interleaved allocation with replication factor 5. */
  @Test
  public void testFiveFactorMultiDatabaseBalance() {
    // 20 RGs per db × rf 5 = 100 replicas per db → 5 per DN (integer avg)
    runMultiDatabaseTest(3, 5, 20);
  }

  private void runMultiDatabaseTest(
      int databaseNum, int replicationFactor, int regionGroupsPerDatabase) {
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
                AVAILABLE_DATA_NODE_MAP,
                FREE_SPACE_MAP,
                allocatedRegionGroups,
                databaseAllocatedRegionGroups.get(db),
                replicationFactor,
                new TConsensusGroupId(TConsensusGroupType.DataRegion, regionId++));
        allocatedRegionGroups.add(newRegionGroup);
        databaseAllocatedRegionGroups.get(db).add(newRegionGroup);
      }
    }

    for (int db = 0; db < databaseNum; db++) {
      Map<Integer, Integer> perDnReplicaCount =
          countReplicasPerDataNode(databaseAllocatedRegionGroups.get(db));
      int minCount = Integer.MAX_VALUE;
      int maxCount = Integer.MIN_VALUE;
      for (int dnId = 1; dnId <= TEST_DATA_NODE_NUM; dnId++) {
        int c = perDnReplicaCount.getOrDefault(dnId, 0);
        minCount = Math.min(minCount, c);
        maxCount = Math.max(maxCount, c);
      }
      LOGGER.info(
          "rf={}, dbs={}, db {}: per-DN replica min={}, max={}",
          replicationFactor,
          databaseNum,
          db,
          minCount,
          maxCount);
      Assert.assertTrue(
          "Per-db replica spread too wide for db " + db + ": max=" + maxCount + ", min=" + minCount,
          maxCount - minCount <= 1);
    }

    Map<Integer, Integer> globalCount = countReplicasPerDataNode(allocatedRegionGroups);
    int globalMin = Integer.MAX_VALUE;
    int globalMax = Integer.MIN_VALUE;
    for (int dnId = 1; dnId <= TEST_DATA_NODE_NUM; dnId++) {
      int c = globalCount.getOrDefault(dnId, 0);
      globalMin = Math.min(globalMin, c);
      globalMax = Math.max(globalMax, c);
    }
    LOGGER.info(
        "rf={}, dbs={}, global per-DN replica min={}, max={}",
        replicationFactor,
        databaseNum,
        globalMin,
        globalMax);
    Assert.assertTrue(
        "Global replica spread too wide: max=" + globalMax + ", min=" + globalMin,
        globalMax - globalMin <= 1);
  }

  private Map<Integer, Integer> countReplicasPerDataNode(List<TRegionReplicaSet> regionGroups) {
    Map<Integer, Integer> counter = new HashMap<>();
    for (TRegionReplicaSet rg : regionGroups) {
      for (TDataNodeLocation loc : rg.getDataNodeLocations()) {
        counter.merge(loc.getDataNodeId(), 1, Integer::sum);
      }
    }
    return counter;
  }
}
