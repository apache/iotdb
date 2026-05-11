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

package org.apache.iotdb.confignode.manager.load.balancer.region.migrator;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.confignode.manager.load.balancer.region.PartiteGraphPlacementRegionGroupAllocator;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionGroupStatistics;

import org.junit.Assert;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/** Shared utilities for GCR Migrator tests. */
class CARMigratorTestHelper {

  /**
   * Scale factor for disk variance computation: bytes → MB. Must match {@link
   * CostAwareRegionGroupMigrator#DISK_SCALE_FACTOR}.
   */
  static final long DISK_SCALE_FACTOR = 1_000_000L;

  private static final PartiteGraphPlacementRegionGroupAllocator ALLOCATOR =
      new PartiteGraphPlacementRegionGroupAllocator();

  // ===== Construction Utilities =====

  /** Build a nodeMap from the given nodeIds. */
  static Map<Integer, TDataNodeConfiguration> buildNodeMap(int... nodeIds) {
    Map<Integer, TDataNodeConfiguration> nodeMap = new TreeMap<>();
    for (int id : nodeIds) {
      nodeMap.put(
          id, new TDataNodeConfiguration().setLocation(new TDataNodeLocation().setDataNodeId(id)));
    }
    return nodeMap;
  }

  /** Build a uniform freeDiskSpaceMap (all nodes get 1.0). */
  static Map<Integer, Double> buildUniformSpaceMap(int... nodeIds) {
    Map<Integer, Double> spaceMap = new TreeMap<>();
    for (int id : nodeIds) {
      spaceMap.put(id, 1.0);
    }
    return spaceMap;
  }

  /** Use PGP allocator to generate regionCount RegionGroups on the given nodes. */
  static List<TRegionReplicaSet> allocateWithPGP(
      Map<Integer, TDataNodeConfiguration> nodeMap,
      Map<Integer, Double> spaceMap,
      int regionCount,
      int replicaCount) {
    List<TRegionReplicaSet> result = new ArrayList<>();
    for (int i = 0; i < regionCount; i++) {
      result.add(
          ALLOCATOR.generateOptimalRegionReplicasDistribution(
              nodeMap,
              spaceMap,
              result,
              result,
              replicaCount,
              new TConsensusGroupId(TConsensusGroupType.DataRegion, i)));
    }
    return result;
  }

  /** Build a statisticsMap with per-RegionGroup diskUsage from the given array. */
  static Map<TConsensusGroupId, RegionGroupStatistics> buildStatisticsMap(
      List<TRegionReplicaSet> regionGroups, long[] diskUsages) {
    Assert.assertEquals(
        "diskUsages array length must match regionGroups size",
        regionGroups.size(),
        diskUsages.length);
    Map<TConsensusGroupId, RegionGroupStatistics> statsMap = new TreeMap<>();
    for (int i = 0; i < regionGroups.size(); i++) {
      RegionGroupStatistics stats = RegionGroupStatistics.generateDefaultRegionGroupStatistics();
      stats.setDiskUsage(diskUsages[i]);
      statsMap.put(regionGroups.get(i).getRegionId(), stats);
    }
    return statsMap;
  }

  /** Build a statisticsMap with uniform diskUsage for all RegionGroups. */
  static Map<TConsensusGroupId, RegionGroupStatistics> buildUniformStatisticsMap(
      List<TRegionReplicaSet> regionGroups, long diskUsage) {
    long[] usages = new long[regionGroups.size()];
    for (int i = 0; i < usages.length; i++) {
      usages[i] = diskUsage;
    }
    return buildStatisticsMap(regionGroups, usages);
  }

  /**
   * Build a list of TRegionReplicaSet from explicit node placements.
   *
   * <p>Each element of {@code placements} is an int array of DataNodeIds forming one RegionGroup's
   * replicas. The region IDs are assigned sequentially starting from 0.
   *
   * <p>Example: {@code buildManualReplicaSets(new int[]{1,2}, new int[]{2,3}, new int[]{1,3})}
   * creates 3 RegionGroups with replicas [N1,N2], [N2,N3], [N1,N3].
   */
  static List<TRegionReplicaSet> buildManualReplicaSets(int[]... placements) {
    List<TRegionReplicaSet> result = new ArrayList<>();
    for (int i = 0; i < placements.length; i++) {
      TRegionReplicaSet rrs = new TRegionReplicaSet();
      rrs.setRegionId(new TConsensusGroupId(TConsensusGroupType.DataRegion, i));
      for (int nodeId : placements[i]) {
        rrs.addToDataNodeLocations(new TDataNodeLocation().setDataNodeId(nodeId));
      }
      result.add(rrs);
    }
    return result;
  }

  // ===== Metric Computation =====

  /** Compute per-node Region count from a collection of RegionReplicaSets. */
  static Map<Integer, Integer> computeRegionCounter(
      Collection<TRegionReplicaSet> regionGroups, Set<Integer> allNodeIds) {
    Map<Integer, Integer> counter = new TreeMap<>();
    for (int id : allNodeIds) {
      counter.put(id, 0);
    }
    for (TRegionReplicaSet rrs : regionGroups) {
      for (TDataNodeLocation loc : rrs.getDataNodeLocations()) {
        counter.merge(loc.getDataNodeId(), 1, Integer::sum);
      }
    }
    return counter;
  }

  /** Compute per-node disk usage from a collection of RegionReplicaSets and their statistics. */
  static Map<Integer, Long> computeDiskCounter(
      Collection<TRegionReplicaSet> regionGroups,
      Map<TConsensusGroupId, RegionGroupStatistics> statsMap,
      Set<Integer> allNodeIds) {
    Map<Integer, Long> counter = new TreeMap<>();
    for (int id : allNodeIds) {
      counter.put(id, 0L);
    }
    for (TRegionReplicaSet rrs : regionGroups) {
      long disk = statsMap.get(rrs.getRegionId()).getDiskUsage();
      for (TDataNodeLocation loc : rrs.getDataNodeLocations()) {
        counter.merge(loc.getDataNodeId(), disk, Long::sum);
      }
    }
    return counter;
  }

  /**
   * Compute integer-proportional variance: n * Σx_i² - (Σx_i)².
   *
   * <p>This equals n² * Var(x), using pure integer arithmetic to avoid floating-point precision
   * loss. Consistent with the formula used in GreedyCopySetRegionGroupMigrator.
   *
   * @param scaleFactor values are divided by scaleFactor before computation (use 1L for no
   *     scaling). Disk counters should use DISK_SCALE_FACTOR to match the algorithm and prevent
   *     long overflow at TB-scale.
   */
  static long computeVariance(Map<Integer, ? extends Number> counterMap, long scaleFactor) {
    if (counterMap.isEmpty()) {
      return 0L;
    }
    long n = counterMap.size();
    long sumValues = 0L;
    long sumSquares = 0L;
    for (Number v : counterMap.values()) {
      long val = v.longValue() / scaleFactor;
      sumValues += val;
      sumSquares += val * val;
    }
    return n * sumSquares - sumValues * sumValues;
  }

  /** Convenience overload with no scaling (scaleFactor = 1). Suitable for region counters. */
  static long computeVariance(Map<Integer, ? extends Number> counterMap) {
    return computeVariance(counterMap, 1L);
  }

  /**
   * Count the number of individual replica migrations (a replica moved from one node to another).
   */
  static int countMigrations(
      List<TRegionReplicaSet> before, Map<TConsensusGroupId, TRegionReplicaSet> after) {
    int migrations = 0;
    for (TRegionReplicaSet beforeRrs : before) {
      TConsensusGroupId groupId = beforeRrs.getRegionId();
      TRegionReplicaSet afterRrs = after.get(groupId);
      Set<Integer> beforeNodes =
          beforeRrs.getDataNodeLocations().stream()
              .map(TDataNodeLocation::getDataNodeId)
              .collect(Collectors.toSet());
      Set<Integer> afterNodes =
          afterRrs.getDataNodeLocations().stream()
              .map(TDataNodeLocation::getDataNodeId)
              .collect(Collectors.toSet());
      // Count nodes in afterNodes that are not in beforeNodes (new placements = migrations)
      for (int nodeId : afterNodes) {
        if (!beforeNodes.contains(nodeId)) {
          migrations++;
        }
      }
    }
    return migrations;
  }

  /**
   * Compute total migration cost in bytes: for each region, count the number of new replica
   * placements (added nodes) and multiply by that region's diskUsage. This correctly accounts for
   * cases where multiple replicas of the same region are migrated (e.g., [1,2] -> [4,5] costs
   * 2×diskUsage, while [1,2] -> [1,5] costs 1×diskUsage).
   */
  static long computeMigrationCost(
      List<TRegionReplicaSet> before,
      Map<TConsensusGroupId, TRegionReplicaSet> after,
      Map<TConsensusGroupId, RegionGroupStatistics> statsMap) {
    long totalCost = 0L;
    for (TRegionReplicaSet beforeRrs : before) {
      TConsensusGroupId groupId = beforeRrs.getRegionId();
      TRegionReplicaSet afterRrs = after.get(groupId);
      Set<Integer> beforeNodes =
          beforeRrs.getDataNodeLocations().stream()
              .map(TDataNodeLocation::getDataNodeId)
              .collect(Collectors.toSet());
      Set<Integer> afterNodes =
          afterRrs.getDataNodeLocations().stream()
              .map(TDataNodeLocation::getDataNodeId)
              .collect(Collectors.toSet());
      // Count how many new nodes were added (each requires copying the region data)
      int addedCount = 0;
      for (int nodeId : afterNodes) {
        if (!beforeNodes.contains(nodeId)) {
          addedCount++;
        }
      }
      totalCost += addedCount * statsMap.get(groupId).getDiskUsage();
    }
    return totalCost;
  }

  // ===== Assertions =====

  /**
   * Assert that every RegionGroup in the plan has exactly replicaCount replicas, and all replicas
   * are on distinct nodes (replica exclusion constraint).
   */
  static void assertReplicaConstraint(
      Map<TConsensusGroupId, TRegionReplicaSet> plan, int replicaCount) {
    for (Map.Entry<TConsensusGroupId, TRegionReplicaSet> entry : plan.entrySet()) {
      TConsensusGroupId groupId = entry.getKey();
      TRegionReplicaSet rrs = entry.getValue();
      List<TDataNodeLocation> locations = rrs.getDataNodeLocations();

      Assert.assertEquals(
          "RegionGroup " + groupId + " should have exactly " + replicaCount + " replicas",
          replicaCount,
          locations.size());

      Set<Integer> nodeIds = new HashSet<>();
      for (TDataNodeLocation loc : locations) {
        nodeIds.add(loc.getDataNodeId());
      }
      Assert.assertEquals(
          "RegionGroup " + groupId + " has duplicate node placements",
          replicaCount,
          nodeIds.size());
    }
  }

  /**
   * Assert that a metric has not worsened (after <= before). Variance metrics should not increase
   * after migration.
   */
  static void assertNotWorse(long before, long after, String metricName) {
    Assert.assertTrue(
        metricName + " worsened after migration: before=" + before + ", after=" + after,
        after <= before);
  }

  /** Format a disk counter map (bytes) to MB for readable logging. e.g. {1=500MB, 2=50MB} */
  static String diskCounterToMB(Map<Integer, Long> diskCounter) {
    StringBuilder sb = new StringBuilder("{");
    boolean first = true;
    for (Map.Entry<Integer, Long> entry : diskCounter.entrySet()) {
      if (!first) sb.append(", ");
      sb.append(entry.getKey())
          .append("=")
          .append(entry.getValue() / DISK_SCALE_FACTOR)
          .append("MB");
      first = false;
    }
    sb.append("}");
    return sb.toString();
  }

  private CARMigratorTestHelper() {
    // utility class
  }
}
