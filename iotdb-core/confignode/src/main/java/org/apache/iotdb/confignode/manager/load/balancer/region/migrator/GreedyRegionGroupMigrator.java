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
 * Greedy baseline migrator for experimental comparison (Scheme B).
 *
 * <p>Algorithm: repeatedly pick a region from the node with the highest disk usage and migrate it
 * to the node with the lowest disk usage, subject to replica exclusion constraints. Each region is
 * migrated at most once. The loop terminates when the disk difference between max and min nodes is
 * within 1% of the mean, or when no feasible migration can be found, or after 1000 iterations.
 *
 * <p>Two modes:
 *
 * <ul>
 *   <li>Scale-out (targetNodeIds non-empty): fromSet = all nodes, toSet = targetNodeIds
 *   <li>Global balance (targetNodeIds empty): fromSet = toSet = all nodes
 * </ul>
 */
public class GreedyRegionGroupMigrator implements IRegionGroupMigrator {

  private static final Logger LOGGER = LoggerFactory.getLogger(GreedyRegionGroupMigrator.class);

  /** Convergence threshold: stop when max-min disk difference < mean * CONVERGENCE_RATIO. */
  private static final double CONVERGENCE_RATIO = 0.01;

  /** Maximum number of greedy iterations to prevent infinite loops. */
  private static final int MAX_ITERATIONS = 1000;

  @Override
  public Map<TConsensusGroupId, TRegionReplicaSet> autoBalanceRegionReplicasDistribution(
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap,
      Map<TConsensusGroupId, RegionGroupStatistics> regionGroupStatisticsMap,
      List<TRegionReplicaSet> allocatedRegionGroups,
      int replicationFactor,
      List<Integer> targetNodeIds) {

    // Build a mutable working copy: groupId -> list of current node IDs
    Map<TConsensusGroupId, List<Integer>> currentPlacement = new TreeMap<>();
    for (TRegionReplicaSet rrs : allocatedRegionGroups) {
      List<Integer> nodeIds =
          rrs.getDataNodeLocations().stream()
              .map(TDataNodeLocation::getDataNodeId)
              .collect(Collectors.toList());
      currentPlacement.put(rrs.getRegionId(), new ArrayList<>(nodeIds));
    }

    // Determine fromSet and toSet
    Set<Integer> allNodeIds = availableDataNodeMap.keySet();
    Set<Integer> fromSet;
    Set<Integer> toSet;
    if (targetNodeIds != null && !targetNodeIds.isEmpty()) {
      // Scale-out mode: migrate from all nodes to target nodes
      fromSet = new HashSet<>(allNodeIds);
      toSet = new HashSet<>(targetNodeIds);
    } else {
      // Global balance mode: migrate among all nodes
      fromSet = new HashSet<>(allNodeIds);
      toSet = new HashSet<>(allNodeIds);
    }

    // Compute per-node disk usage
    Map<Integer, Long> diskCounter = new HashMap<>();
    for (int nodeId : allNodeIds) {
      diskCounter.put(nodeId, 0L);
    }
    for (Map.Entry<TConsensusGroupId, List<Integer>> entry : currentPlacement.entrySet()) {
      long disk = regionGroupStatisticsMap.get(entry.getKey()).getDiskUsage();
      for (int nodeId : entry.getValue()) {
        diskCounter.merge(nodeId, disk, Long::sum);
      }
    }

    logSummary(
        "Initial",
        allNodeIds,
        currentPlacement,
        diskCounter,
        regionGroupStatisticsMap,
        allocatedRegionGroups);

    // Track which regions have already been migrated (each region migrates at most once)
    Set<TConsensusGroupId> migratedRegions = new HashSet<>();
    // Record migration details for logging
    List<Object[]> migrationLog = new ArrayList<>();

    // Greedy loop
    for (int iter = 0; iter < MAX_ITERATIONS; iter++) {
      // Find maxNode in fromSet and minNode in toSet by disk usage
      int maxNode = -1;
      long maxDisk = Long.MIN_VALUE;
      for (int nodeId : fromSet) {
        long d = diskCounter.getOrDefault(nodeId, 0L);
        if (d > maxDisk) {
          maxDisk = d;
          maxNode = nodeId;
        }
      }
      int minNode = -1;
      long minDisk = Long.MAX_VALUE;
      for (int nodeId : toSet) {
        long d = diskCounter.getOrDefault(nodeId, 0L);
        if (d < minDisk) {
          minDisk = d;
          minNode = nodeId;
        }
      }

      if (maxNode == -1 || minNode == -1 || maxNode == minNode) {
        break;
      }

      // Convergence check: stop if disk difference < mean * CONVERGENCE_RATIO
      long totalDisk = 0L;
      for (long d : diskCounter.values()) {
        totalDisk += d;
      }
      double mean = (double) totalDisk / diskCounter.size();
      if (maxDisk - minDisk < mean * CONVERGENCE_RATIO) {
        LOGGER.info("[Greedy] Converged at iteration {} (disk diff < 1% of mean)", iter);
        break;
      }

      // Collect candidate regions on maxNode, sorted by disk usage descending
      final int srcNode = maxNode;
      final int dstNode = minNode;
      List<TConsensusGroupId> candidates = new ArrayList<>();
      for (Map.Entry<TConsensusGroupId, List<Integer>> entry : currentPlacement.entrySet()) {
        if (entry.getValue().contains(srcNode)) {
          candidates.add(entry.getKey());
        }
      }
      // Sort by disk usage descending (prefer moving large regions first)
      candidates.sort(
          Comparator.comparingLong(
                  (TConsensusGroupId gid) -> regionGroupStatisticsMap.get(gid).getDiskUsage())
              .reversed());

      boolean migrated = false;
      for (TConsensusGroupId candidateId : candidates) {
        // Skip if already migrated
        if (migratedRegions.contains(candidateId)) {
          continue;
        }
        // Replica exclusion: dstNode must not already be in this region's replica set
        List<Integer> replicas = currentPlacement.get(candidateId);
        if (replicas.contains(dstNode)) {
          continue;
        }

        // Execute migration: replace srcNode with dstNode in the replica set
        long regionDisk = regionGroupStatisticsMap.get(candidateId).getDiskUsage();
        replicas.set(replicas.indexOf(srcNode), dstNode);
        diskCounter.merge(srcNode, -regionDisk, Long::sum);
        diskCounter.merge(dstNode, regionDisk, Long::sum);
        migratedRegions.add(candidateId);
        migrationLog.add(new Object[] {candidateId, srcNode, dstNode, regionDisk / 1_000_000L});
        migrated = true;
        break;
      }

      if (!migrated) {
        // No feasible migration found this round — deadlock, stop
        LOGGER.info("[Greedy] Stopped at iteration {} (no feasible migration)", iter);
        break;
      }
    }

    // Build result: Map<TConsensusGroupId, TRegionReplicaSet>
    Map<TConsensusGroupId, TRegionReplicaSet> result = new TreeMap<>();
    for (Map.Entry<TConsensusGroupId, List<Integer>> entry : currentPlacement.entrySet()) {
      TRegionReplicaSet rrs = new TRegionReplicaSet();
      rrs.setRegionId(entry.getKey());
      for (int nodeId : entry.getValue()) {
        rrs.addToDataNodeLocations(new TDataNodeLocation().setDataNodeId(nodeId));
      }
      result.put(entry.getKey(), rrs);
    }

    LOGGER.info("[Greedy] Greedy migrator completed: {} migrations", migratedRegions.size());
    for (Object[] log : migrationLog) {
      LOGGER.info(
          "[Greedy] Region {} : Node {} -> Node {} (disk={}MB)", log[0], log[1], log[2], log[3]);
    }

    logSummary(
        "Final",
        allNodeIds,
        currentPlacement,
        diskCounter,
        regionGroupStatisticsMap,
        allocatedRegionGroups);

    return result;
  }

  /**
   * Log a unified summary. Computes regionCounter from currentPlacement, and computes migrations
   * and cost by comparing currentPlacement against the original allocatedRegionGroups.
   */
  private void logSummary(
      String label,
      Set<Integer> allNodeIds,
      Map<TConsensusGroupId, List<Integer>> currentPlacement,
      Map<Integer, Long> diskCounter,
      Map<TConsensusGroupId, RegionGroupStatistics> regionGroupStatisticsMap,
      List<TRegionReplicaSet> allocatedRegionGroups) {

    // Build regionCounter from currentPlacement
    Map<Integer, Integer> regionCounter = new HashMap<>();
    for (int nodeId : allNodeIds) {
      regionCounter.put(nodeId, 0);
    }
    for (List<Integer> nodeIds : currentPlacement.values()) {
      for (int nodeId : nodeIds) {
        regionCounter.merge(nodeId, 1, Integer::sum);
      }
    }

    // Compute migrations and cost by comparing before vs after
    long migrations = 0;
    long costBytes = 0;
    for (TRegionReplicaSet beforeRrs : allocatedRegionGroups) {
      TConsensusGroupId groupId = beforeRrs.getRegionId();
      Set<Integer> beforeNodes =
          beforeRrs.getDataNodeLocations().stream()
              .map(TDataNodeLocation::getDataNodeId)
              .collect(Collectors.toSet());
      List<Integer> afterNodeList = currentPlacement.get(groupId);
      if (afterNodeList == null) {
        continue;
      }
      int addedCount = 0;
      for (int nodeId : afterNodeList) {
        if (!beforeNodes.contains(nodeId)) {
          addedCount++;
        }
      }
      migrations += addedCount;
      costBytes += addedCount * regionGroupStatisticsMap.get(groupId).getDiskUsage();
    }

    MigratorLogHelper.logSummary(
        LOGGER, "Greedy", label, allNodeIds, regionCounter, diskCounter, migrations, costBytes);
  }
}
