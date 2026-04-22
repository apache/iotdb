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
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.confignode.manager.load.balancer.region.PartiteGraphPlacementRegionGroupAllocator;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionGroupStatistics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * PGP Rebalance migrator for experimental comparison (Scheme C).
 *
 * <p>Algorithm: sort all RegionGroups by disk usage in descending order, then re-run the PGP
 * allocation algorithm from scratch on all available nodes. This produces a theoretically optimal
 * placement for both region count and disk balance, but completely ignores the existing placement,
 * resulting in a very high migration cost.
 *
 * <p>Purpose: serves as an ablation baseline to demonstrate the necessity of migration cost
 * optimization in the GCR algorithm. Expected result: excellent balance quality (comparable to
 * GCR), but significantly higher migration cost.
 */
public class PGPRebalanceRegionGroupMigrator implements IRegionGroupMigrator {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PGPRebalanceRegionGroupMigrator.class);

  private final PartiteGraphPlacementRegionGroupAllocator pgpAllocator =
      new PartiteGraphPlacementRegionGroupAllocator();

  @Override
  public Map<TConsensusGroupId, TRegionReplicaSet> autoBalanceRegionReplicasDistribution(
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap,
      Map<TConsensusGroupId, RegionGroupStatistics> regionGroupStatisticsMap,
      List<TRegionReplicaSet> allocatedRegionGroups,
      int replicationFactor,
      List<Integer> targetNodeIds) {

    // 1. Sort all RegionGroups by diskUsage descending (large regions first)
    List<TRegionReplicaSet> sorted = new ArrayList<>(allocatedRegionGroups);
    sorted.sort(
        Comparator.comparingLong(
                (TRegionReplicaSet rrs) ->
                    regionGroupStatisticsMap.get(rrs.getRegionId()).getDiskUsage())
            .reversed());

    // 2. Build uniform freeDiskSpaceMap for all available nodes
    Map<Integer, Double> spaceMap = new TreeMap<>();
    for (int nodeId : availableDataNodeMap.keySet()) {
      spaceMap.put(nodeId, 1.0);
    }

    // 3. Re-allocate all RegionGroups from scratch using PGP
    List<TRegionReplicaSet> newAllocated = new ArrayList<>();
    Map<TConsensusGroupId, TRegionReplicaSet> result = new TreeMap<>();

    logSummary(
        "Initial",
        availableDataNodeMap.keySet(),
        allocatedRegionGroups,
        allocatedRegionGroups,
        regionGroupStatisticsMap);

    for (TRegionReplicaSet rrs : sorted) {
      TConsensusGroupId groupId = rrs.getRegionId();
      TRegionReplicaSet newRrs =
          pgpAllocator.generateOptimalRegionReplicasDistribution(
              availableDataNodeMap,
              spaceMap,
              newAllocated,
              newAllocated,
              replicationFactor,
              groupId);
      newAllocated.add(newRrs);
      result.put(groupId, newRrs);
    }

    // 4. Count migrations and log details (one line per individual replica move)
    int migrations = 0;
    List<Object[]> migrationLog = new ArrayList<>();
    for (TRegionReplicaSet beforeRrs : allocatedRegionGroups) {
      TConsensusGroupId groupId = beforeRrs.getRegionId();
      TRegionReplicaSet afterRrs = result.get(groupId);
      Set<Integer> beforeNodes =
          beforeRrs.getDataNodeLocations().stream()
              .map(TDataNodeLocation::getDataNodeId)
              .collect(Collectors.toSet());
      Set<Integer> afterNodes =
          afterRrs.getDataNodeLocations().stream()
              .map(TDataNodeLocation::getDataNodeId)
              .collect(Collectors.toSet());
      Set<Integer> added = new HashSet<>(afterNodes);
      added.removeAll(beforeNodes);
      if (!added.isEmpty()) {
        Set<Integer> removed = new HashSet<>(beforeNodes);
        removed.removeAll(afterNodes);
        long disk = regionGroupStatisticsMap.get(groupId).getDiskUsage();
        migrations += added.size();
        // Pair removed and added nodes for per-replica logging
        List<Integer> removedList = new ArrayList<>(removed);
        List<Integer> addedList = new ArrayList<>(added);
        for (int i = 0; i < addedList.size(); i++) {
          migrationLog.add(
              new Object[] {groupId, removedList.get(i), addedList.get(i), disk / 1_000_000L});
        }
      }
    }

    LOGGER.info("[PGPRebalance] PGP rebalance completed: {} migrations", migrations);
    for (Object[] log : migrationLog) {
      LOGGER.info(
          "[PGPRebalance] Region {} : Node {} -> Node {} (disk={}MB)",
          log[0],
          log[1],
          log[2],
          log[3]);
    }

    logSummary(
        "Final",
        availableDataNodeMap.keySet(),
        new ArrayList<>(result.values()),
        allocatedRegionGroups,
        regionGroupStatisticsMap);

    return result;
  }

  /**
   * Log a unified summary. Computes regionCounter and diskCounter from afterGroups, and computes
   * migrations and cost by comparing afterGroups against beforeGroups.
   */
  private void logSummary(
      String label,
      Set<Integer> allNodeIds,
      List<TRegionReplicaSet> afterGroups,
      List<TRegionReplicaSet> beforeGroups,
      Map<TConsensusGroupId, RegionGroupStatistics> regionGroupStatisticsMap) {

    if (!LOGGER.isDebugEnabled()) {
      return;
    }

    // Build regionCounter and diskCounter from afterGroups
    Map<Integer, Integer> regionCounter = new HashMap<>();
    Map<Integer, Long> diskCounter = new HashMap<>();
    for (int nodeId : allNodeIds) {
      regionCounter.put(nodeId, 0);
      diskCounter.put(nodeId, 0L);
    }
    for (TRegionReplicaSet rrs : afterGroups) {
      long disk = regionGroupStatisticsMap.get(rrs.getRegionId()).getDiskUsage();
      for (TDataNodeLocation loc : rrs.getDataNodeLocations()) {
        int nodeId = loc.getDataNodeId();
        regionCounter.merge(nodeId, 1, Integer::sum);
        diskCounter.merge(nodeId, disk, Long::sum);
      }
    }

    // Build before node sets for migration/cost calculation
    Map<TConsensusGroupId, Set<Integer>> beforeMap = new HashMap<>();
    for (TRegionReplicaSet rrs : beforeGroups) {
      beforeMap.put(
          rrs.getRegionId(),
          rrs.getDataNodeLocations().stream()
              .map(TDataNodeLocation::getDataNodeId)
              .collect(Collectors.toSet()));
    }

    // Compute migrations and cost
    long migrations = 0;
    long costBytes = 0;
    for (TRegionReplicaSet afterRrs : afterGroups) {
      TConsensusGroupId groupId = afterRrs.getRegionId();
      Set<Integer> beforeNodes = beforeMap.get(groupId);
      if (beforeNodes == null) {
        continue;
      }
      int addedCount = 0;
      for (TDataNodeLocation loc : afterRrs.getDataNodeLocations()) {
        if (!beforeNodes.contains(loc.getDataNodeId())) {
          addedCount++;
        }
      }
      migrations += addedCount;
      costBytes += addedCount * regionGroupStatisticsMap.get(groupId).getDiskUsage();
    }

    MigratorLogHelper.logSummary(
        LOGGER,
        "PGPRebalance",
        label,
        allNodeIds,
        regionCounter,
        diskCounter,
        migrations,
        costBytes);
  }
}
