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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Parameterized scale-in tests for {@link GreedyCopySetRegionGroupAllocator} (GCR), {@link
 * GreedyRegionGroupAllocator} (Greedy), and {@link RandomRegionGroupAllocator} (Random) {@code
 * removeNodeReplicaSelect}.
 *
 * <p>All scenarios remove exactly one node, matching the current algorithm's design which handles
 * one removed replica per RegionGroup per invocation.
 *
 * <p>Scenario parameters are chosen so that {@code regionGroupCount * replicaCount} is divisible by
 * {@code totalNodeCount}, ensuring perfectly even initial distribution across all nodes. For
 * multi-database scenarios, {@code regionGroupCount} is also divisible by {@code databaseCount}.
 *
 * <p>For GCR, strict assertions apply: after migration, max load must not exceed idealCeil. For
 * Greedy, only basic correctness is checked (mutual exclusion, all regions assigned). Both
 * algorithms are compared on load variance to quantify quality differences.
 */
@RunWith(Parameterized.class)
public class ScaleInAllocatorTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(ScaleInAllocatorTest.class);

  private static final IRegionGroupAllocator CAR_ALLOCATOR = new CostAwareRegionGroupAllocator();
  private static final IRegionGroupAllocator GREEDY_ALLOCATOR = new GreedyRegionGroupAllocator();
  private static final IRegionGroupAllocator RANDOM_ALLOCATOR = new RandomRegionGroupAllocator();
  private static final PartiteGraphPlacementRegionGroupAllocator PGP_ALLOCATOR =
      new PartiteGraphPlacementRegionGroupAllocator();

  private final String allocatorName;
  private final IRegionGroupAllocator allocator;
  private final int totalNodeCount;
  private final int regionGroupCount;
  private final int replicaCount;
  private final int databaseCount;

  @Parameterized.Parameters(name = "algo={0}, nodes={1}->remove1, |RG|={2}, r={3}, |DB|={4}")
  public static Collection<Object[]> data() {
    List<Object[]> params = new ArrayList<>();
    // {totalNodes, regionGroupCount, replicaCount, databaseCount}
    // Constraints:
    //   - regionGroupCount * replicaCount % totalNodeCount == 0  (even initial distribution)
    //   - regionGroupCount % databaseCount == 0  (even distribution across databases)
    Object[][] scenarios =
        new Object[][] {
          // --- Single-database scenarios ---
          {5, 10, 2, 1}, // 5->4: 20 replicas / 5 nodes = 4 each
          {6, 15, 2, 1}, // 6->5: 30 replicas / 6 nodes = 5 each
          {6, 12, 3, 1}, // 6->5 triple replica: 36 replicas / 6 nodes = 6 each
          {8, 20, 2, 1}, // 8->7: 40 replicas / 8 nodes = 5 each
          {10, 30, 2, 1}, // 10->9: 60 replicas / 10 nodes = 6 each
          {100, 1000, 2, 1}, // 100->99: 2000 replicas / 100 nodes = 20 each
          // --- Multi-database scenarios ---
          {6, 15, 2, 3}, // 6->5, 3 databases: 30 replicas / 6 = 5 each, 5 RG/DB
          {8, 24, 2, 3}, // 8->7, 3 databases: 48 replicas / 8 = 6 each, 8 RG/DB

          // === 4->3 single-database, r=2 ===
          // regionGroupCount * 2 must be divisible by 4 => regionGroupCount is even
          {4, 2, 2, 1}, // 4 replicas / 4 = 1 each, migrate 1
          {4, 4, 2, 1}, // 8 replicas / 4 = 2 each, migrate 2
          {4, 6, 2, 1}, // 12 replicas / 4 = 3 each, migrate 3
          {4, 8, 2, 1}, // 16 replicas / 4 = 4 each, migrate 4
          {4, 10, 2, 1}, // 20 replicas / 4 = 5 each, migrate 5
          {4, 12, 2, 1}, // 24 replicas / 4 = 6 each, migrate 6
          {4, 14, 2, 1}, // 28 replicas / 4 = 7 each, migrate 7
          {4, 16, 2, 1}, // 32 replicas / 4 = 8 each, migrate 8
          {4, 18, 2, 1}, // 36 replicas / 4 = 9 each, migrate 9
          {4, 20, 2, 1}, // 40 replicas / 4 = 10 each, migrate 10

          // === 4->3 single-database, r=3 ===
          // regionGroupCount * 3 must be divisible by 4 => regionGroupCount is multiple of 4
          {4, 4, 3, 1}, // 12 replicas / 4 = 3 each, migrate 3
          {4, 8, 3, 1}, // 24 replicas / 4 = 6 each, migrate 6
          {4, 12, 3, 1}, // 36 replicas / 4 = 9 each, migrate 9
          {4, 16, 3, 1}, // 48 replicas / 4 = 12 each, migrate 12
        };

    // Filter out invalid scenarios and build params
    for (Object[] scenario : scenarios) {
      int totalNodes = (int) scenario[0];
      int rgCount = (int) scenario[1];
      int replica = (int) scenario[2];
      int dbCount = (int) scenario[3];
      if (rgCount * replica % totalNodes != 0 || rgCount % dbCount != 0) {
        continue; // skip invalid scenarios
      }
      params.add(new Object[] {"CAR", scenario[0], scenario[1], scenario[2], scenario[3]});
      params.add(new Object[] {"Greedy", scenario[0], scenario[1], scenario[2], scenario[3]});
      params.add(new Object[] {"Random", scenario[0], scenario[1], scenario[2], scenario[3]});
    }
    return params;
  }

  public ScaleInAllocatorTest(
      String allocatorName,
      int totalNodeCount,
      int regionGroupCount,
      int replicaCount,
      int databaseCount) {
    this.allocatorName = allocatorName;
    this.allocator =
        "CAR".equals(allocatorName)
            ? CAR_ALLOCATOR
            : "Random".equals(allocatorName) ? RANDOM_ALLOCATOR : GREEDY_ALLOCATOR;
    this.totalNodeCount = totalNodeCount;
    this.regionGroupCount = regionGroupCount;
    this.replicaCount = replicaCount;
    this.databaseCount = databaseCount;
  }

  @Test
  public void testScaleIn() {
    int remainNodeCount = totalNodeCount - 1;
    int totalReplicas = regionGroupCount * replicaCount;
    int removedNodeId = totalNodeCount; // remove the last node

    LOGGER.info(
        "[{}] Scale-in test: nodes={}->{}, |RG|={}, r={}, |DB|={}",
        allocatorName,
        totalNodeCount,
        remainNodeCount,
        regionGroupCount,
        replicaCount,
        databaseCount);

    // 1. Build all nodes and allocate RegionGroups using GCR allocator
    Map<Integer, TDataNodeConfiguration> allNodeMap = buildNodeMap(1, totalNodeCount);
    Map<Integer, Double> freeSpaceMap = buildUniformFreeSpaceMap(1, totalNodeCount);

    String[] dbNames = new String[databaseCount];
    for (int i = 0; i < databaseCount; i++) {
      dbNames[i] = "db" + i;
    }

    List<TRegionReplicaSet> allAllocated = new ArrayList<>();
    Map<String, List<TRegionReplicaSet>> dbAllocatedMap = new HashMap<>();
    for (String db : dbNames) {
      dbAllocatedMap.put(db, new ArrayList<>());
    }

    for (int i = 0; i < regionGroupCount; i++) {
      String db = dbNames[i % databaseCount];
      List<TRegionReplicaSet> dbList = dbAllocatedMap.get(db);
      TRegionReplicaSet rs =
          PGP_ALLOCATOR.generateOptimalRegionReplicasDistribution(
              allNodeMap,
              freeSpaceMap,
              allAllocated,
              dbList,
              replicaCount,
              new TConsensusGroupId(TConsensusGroupType.DataRegion, i));
      allAllocated.add(rs);
      dbList.add(new TRegionReplicaSet(rs));
    }

    // Verify even initial distribution across ALL nodes
    Map<Integer, Integer> initialRegionCounter =
        computeRegionCounter(allAllocated, allNodeMap.keySet());
    int expectedPerNode = totalReplicas / totalNodeCount;
    for (Map.Entry<Integer, Integer> entry : initialRegionCounter.entrySet()) {
      Assert.assertEquals(
          "[" + allocatorName + "] Initial distribution uneven on node " + entry.getKey(),
          expectedPerNode,
          (int) entry.getValue());
    }

    // 2. Build available nodes (excluding the removed node)
    Map<Integer, TDataNodeConfiguration> availableNodeMap = buildNodeMap(1, remainNodeCount);
    Map<Integer, Double> availableFreeSpaceMap = buildUniformFreeSpaceMap(1, remainNodeCount);

    // 3. Build region -> database mapping
    Map<TConsensusGroupId, String> regionDatabaseMap = new HashMap<>();
    for (Map.Entry<String, List<TRegionReplicaSet>> entry : dbAllocatedMap.entrySet()) {
      for (TRegionReplicaSet rs : entry.getValue()) {
        regionDatabaseMap.put(rs.getRegionId(), entry.getKey());
      }
    }

    // 4. Identify affected regions: remove the single node's replica from each affected RG
    Map<TConsensusGroupId, TRegionReplicaSet> remainReplicasMap = new HashMap<>();
    for (TRegionReplicaSet rs : allAllocated) {
      boolean affected =
          rs.getDataNodeLocations().stream().anyMatch(loc -> loc.getDataNodeId() == removedNodeId);
      if (affected) {
        // Remove the replica on the removed node
        rs.getDataNodeLocations().removeIf(loc -> loc.getDataNodeId() == removedNodeId);
        TRegionReplicaSet remainSet = new TRegionReplicaSet();
        remainSet.setRegionId(rs.getRegionId());
        remainSet.setDataNodeLocations(new ArrayList<>(rs.getDataNodeLocations()));
        remainReplicasMap.put(rs.getRegionId(), remainSet);
      }
    }

    // The number of affected regions should equal expectedPerNode (the removed node's load)
    Assert.assertEquals(
        "[" + allocatorName + "] Affected region count mismatch",
        expectedPerNode,
        remainReplicasMap.size());

    LOGGER.info(
        "[{}] Before migration: regionCounter={}",
        allocatorName,
        computeRegionCounter(allAllocated, availableNodeMap.keySet()));
    LOGGER.info(
        "[{}] Regions to migrate: {} (from node {})",
        allocatorName,
        remainReplicasMap.size(),
        removedNodeId);

    // Build mock regionDiskUsageMap: half regions 100MB, half regions 200MB
    Map<TConsensusGroupId, Long> regionDiskUsageMap = new HashMap<>();
    int half = allAllocated.size() / 2;
    for (int i = 0; i < allAllocated.size(); i++) {
      TRegionReplicaSet rs = allAllocated.get(i);
      regionDiskUsageMap.put(rs.getRegionId(), i < half ? 100_000_000L : 200_000_000L);
    }

    // 5. Execute removeNodeReplicaSelect
    // Greedy and Random internally run multiple rounds and pick the worst-variance result.
    // GCR runs once (deterministic optimal).
    Map<TConsensusGroupId, TDataNodeConfiguration> result =
        allocator.removeNodeReplicaSelect(
            availableNodeMap,
            availableFreeSpaceMap,
            allAllocated,
            regionDatabaseMap,
            dbAllocatedMap,
            remainReplicasMap,
            regionDiskUsageMap);

    // Apply result to allAllocated for metric computation
    for (Map.Entry<TConsensusGroupId, TDataNodeConfiguration> entry : result.entrySet()) {
      TConsensusGroupId regionId = entry.getKey();
      TDataNodeLocation newLoc = entry.getValue().getLocation();
      for (TRegionReplicaSet rs : allAllocated) {
        if (rs.getRegionId().equals(regionId)) {
          rs.getDataNodeLocations().add(newLoc);
          break;
        }
      }
    }

    Map<Integer, Integer> afterRegionCounter =
        computeRegionCounter(allAllocated, availableNodeMap.keySet());
    int afterMaxLoad = afterRegionCounter.values().stream().max(Integer::compareTo).orElse(0);
    int afterTotalReplicas = afterRegionCounter.values().stream().mapToInt(Integer::intValue).sum();
    long afterVariance = computeVariance(afterRegionCounter);

    Map<String, Integer> afterDbMaxLoad =
        computeDbMaxLoad(allAllocated, regionDatabaseMap, availableNodeMap.keySet(), dbNames);
    int afterDbLoadSquaredSum = afterDbMaxLoad.values().stream().mapToInt(v -> v * v).sum();

    LOGGER.info("[{}] After: regionCounter={}", allocatorName, afterRegionCounter);
    LOGGER.info("[{}] After: maxLoad={}, variance={}", allocatorName, afterMaxLoad, afterVariance);
    LOGGER.info(
        "[{}] After: dbMaxLoad={}, dbLoadSquaredSum={}",
        allocatorName,
        afterDbMaxLoad,
        afterDbLoadSquaredSum);

    // 7. Assertions

    // (a) All affected regions must be assigned a target
    Assert.assertEquals(
        "[" + allocatorName + "] Not all regions assigned",
        remainReplicasMap.size(),
        result.size());

    // (b) Total replica count must be preserved (no region lost or duplicated)
    Assert.assertEquals(
        "[" + allocatorName + "] Total replica count mismatch after migration",
        totalReplicas,
        afterTotalReplicas);

    // (c) Each RG must have exactly replicaCount replicas on available nodes
    for (TRegionReplicaSet rs : allAllocated) {
      long availableCount =
          rs.getDataNodeLocations().stream()
              .filter(loc -> availableNodeMap.containsKey(loc.getDataNodeId()))
              .count();
      Assert.assertEquals(
          "[" + allocatorName + "] Region " + rs.getRegionId() + " has wrong replica count",
          replicaCount,
          (int) availableCount);
    }

    // (d) No duplicate nodes within any RG
    for (TRegionReplicaSet rs : allAllocated) {
      Set<Integer> nodeIds = new HashSet<>();
      for (TDataNodeLocation loc : rs.getDataNodeLocations()) {
        Assert.assertTrue(
            "["
                + allocatorName
                + "] Duplicate node "
                + loc.getDataNodeId()
                + " in region "
                + rs.getRegionId(),
            nodeIds.add(loc.getDataNodeId()));
      }
    }

    // (e) All assigned nodes must be in available nodes
    for (Map.Entry<TConsensusGroupId, TDataNodeConfiguration> entry : result.entrySet()) {
      int assignedNodeId = entry.getValue().getLocation().getDataNodeId();
      Assert.assertTrue(
          "[" + allocatorName + "] Assigned node " + assignedNodeId + " not in available nodes",
          availableNodeMap.containsKey(assignedNodeId));
    }

    // (f) GCR-only: max load must not exceed idealCeil
    // Random is a baseline without load-awareness; it is not expected to achieve optimal balance
    if ("GCR".equals(allocatorName)) {
      int idealCeil = (totalReplicas + remainNodeCount - 1) / remainNodeCount;
      Assert.assertTrue(
          "[" + allocatorName + "] maxLoad " + afterMaxLoad + " exceeds idealCeil " + idealCeil,
          afterMaxLoad <= idealCeil);
    }

    // (g) Log comparative summary
    LOGGER.info(
        "[{}] Summary: maxLoad={}, variance={}, dbLoadSquaredSum={}",
        allocatorName,
        afterMaxLoad,
        afterVariance,
        afterDbLoadSquaredSum);
  }

  // ===== Utility Methods =====

  private static Map<Integer, TDataNodeConfiguration> buildNodeMap(int from, int to) {
    Map<Integer, TDataNodeConfiguration> nodeMap = new TreeMap<>();
    for (int i = from; i <= to; i++) {
      nodeMap.put(
          i, new TDataNodeConfiguration().setLocation(new TDataNodeLocation().setDataNodeId(i)));
    }
    return nodeMap;
  }

  private static Map<Integer, Double> buildUniformFreeSpaceMap(int from, int to) {
    Map<Integer, Double> spaceMap = new TreeMap<>();
    for (int i = from; i <= to; i++) {
      spaceMap.put(i, 1.0);
    }
    return spaceMap;
  }

  private static Map<Integer, Integer> computeRegionCounter(
      List<TRegionReplicaSet> regionGroups, Set<Integer> nodeIds) {
    Map<Integer, Integer> counter = new TreeMap<>();
    for (int id : nodeIds) {
      counter.put(id, 0);
    }
    for (TRegionReplicaSet rs : regionGroups) {
      for (TDataNodeLocation loc : rs.getDataNodeLocations()) {
        if (nodeIds.contains(loc.getDataNodeId())) {
          counter.merge(loc.getDataNodeId(), 1, Integer::sum);
        }
      }
    }
    return counter;
  }

  private static long computeVariance(Map<Integer, Integer> counter) {
    if (counter.isEmpty()) {
      return 0L;
    }
    long n = counter.size();
    long sum = 0L;
    long sumSq = 0L;
    for (int v : counter.values()) {
      sum += v;
      sumSq += (long) v * v;
    }
    return n * sumSq - sum * sum;
  }

  private static Map<String, Integer> computeDbMaxLoad(
      List<TRegionReplicaSet> regionGroups,
      Map<TConsensusGroupId, String> regionDbMap,
      Set<Integer> nodeIds,
      String[] dbNames) {
    Map<String, Map<Integer, Integer>> dbNodeCounter = new HashMap<>();
    for (String db : dbNames) {
      Map<Integer, Integer> nodeCounter = new TreeMap<>();
      for (int nodeId : nodeIds) {
        nodeCounter.put(nodeId, 0);
      }
      dbNodeCounter.put(db, nodeCounter);
    }
    for (TRegionReplicaSet rs : regionGroups) {
      String db = regionDbMap.get(rs.getRegionId());
      if (db != null && dbNodeCounter.containsKey(db)) {
        for (TDataNodeLocation loc : rs.getDataNodeLocations()) {
          if (nodeIds.contains(loc.getDataNodeId())) {
            dbNodeCounter.get(db).merge(loc.getDataNodeId(), 1, Integer::sum);
          }
        }
      }
    }
    Map<String, Integer> result = new TreeMap<>();
    for (String db : dbNames) {
      int maxLoad = dbNodeCounter.get(db).values().stream().max(Integer::compareTo).orElse(0);
      result.put(db, maxLoad);
    }
    return result;
  }
}
