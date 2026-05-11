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
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.confignode.manager.load.balancer.region.migrator.MigratorLogHelper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Cost-Aware Region Group Allocator for scale-in scenarios.
 *
 * <p>Uses Batch DFS search with variance-based multi-objective optimization to select optimal
 * destination nodes for regions that need to be migrated during node removal (scale-in).
 *
 * <p>Optimization metrics (lexicographic comparison):
 *
 * <ol>
 *   <li>Var(region): region count variance across nodes (smaller is better, exact comparison)
 *   <li>Var(disk): disk usage variance across nodes (smaller is better, with ε-tolerance)
 *   <li>Scatter: replica co-location count (larger is better, reverse comparison)
 * </ol>
 *
 * <p>This allocator is only intended for {@code removeNodeReplicaSelect}. The {@code
 * generateOptimalRegionReplicasDistribution} method throws {@link UnsupportedOperationException}.
 */
public class CostAwareRegionGroupAllocator implements IRegionGroupAllocator {

  private static final Logger LOGGER = LoggerFactory.getLogger(CostAwareRegionGroupAllocator.class);

  /** Batch size for DFS search. */
  private static final int BATCH_SIZE = 12;

  /** Maximum number of candidate nodes to consider per region in DFS. */
  private static final int MAX_CANDIDATES = 4;

  /** Relative tolerance for Var(disk) comparison (1%). */
  private static final double DISK_VARIANCE_EPSILON = 0.01;

  // ===== Counters (indexed by nodeId) =====

  /** regionCounter[nodeId] = number of Regions on that node. */
  private int[] regionCounter;

  /** diskCounter[nodeId] = total disk usage (bytes) on that node. */
  private long[] diskCounter;

  /** combinationCounter[i][j] = number of RegionGroups with replicas on both node i and node j. */
  private int[][] combinationCounter;

  // ===== DFS state =====

  /** Regions to be migrated in current DFS batch. */
  private List<TConsensusGroupId> dfsRegionKeys;

  /** For each region, the allowed candidate destination node IDs. */
  private Map<TConsensusGroupId, List<Integer>> allowedCandidatesMap;

  /** Best assignment found so far (nodeId for each region in dfsRegionKeys). */
  private int[] bestAssignment;

  /**
   * Best metrics found so far: [regionVariance, diskVariance, scatter]. Lexicographic comparison
   * with ε-tolerance for diskVariance.
   */
  private long[] bestMetrics;

  /** scatterDelta[i][nodeId] = scatter increment if region i is placed on nodeId. */
  private int[][] scatterDelta;

  /** Set of available node IDs (for variance computation). */
  private Set<Integer> availableNodeIds;

  /** regionDiskUsageMap from the caller (bytes per RegionGroup). */
  private Map<TConsensusGroupId, Long> regionDiskUsageMap;

  // ===== Incremental variance state =====

  /** Running sum of regionCounter[nodeId] + additionalRegionLoad[nodeId] for available nodes. */
  private long incrRegionSum;

  /** Running sum of squares for region counts. */
  private long incrRegionSumSq;

  /** Running sum of (diskCounter[nodeId] + additionalDiskLoad[nodeId]) / DISK_SCALE_FACTOR. */
  private long incrDiskSum;

  /** Running sum of squares for disk usage (in MB). */
  private long incrDiskSumSq;

  public CostAwareRegionGroupAllocator() {
    // Empty constructor
  }

  @Override
  public TRegionReplicaSet generateOptimalRegionReplicasDistribution(
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap,
      Map<Integer, Double> freeDiskSpaceMap,
      List<TRegionReplicaSet> allocatedRegionGroups,
      List<TRegionReplicaSet> databaseAllocatedRegionGroups,
      int replicationFactor,
      TConsensusGroupId consensusGroupId) {
    throw new UnsupportedOperationException(
        "CARRegionGroupAllocator is only intended for removeNodeReplicaSelect (scale-in).");
  }

  @Override
  public Map<TConsensusGroupId, TDataNodeConfiguration> removeNodeReplicaSelect(
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap,
      Map<Integer, Double> freeDiskSpaceMap,
      List<TRegionReplicaSet> allocatedRegionGroups,
      Map<TConsensusGroupId, String> regionDatabaseMap,
      Map<String, List<TRegionReplicaSet>> databaseAllocatedRegionGroupMap,
      Map<TConsensusGroupId, TRegionReplicaSet> remainReplicasMap,
      Map<TConsensusGroupId, Long> regionDiskUsageMap) {
    try {
      // 1. Prepare: compute regionCounter, diskCounter, and combinationCounter
      prepare(availableDataNodeMap, allocatedRegionGroups, regionDiskUsageMap);

      ScaleInAllocatorLogger.logSummary(
          LOGGER,
          "CAR",
          "Initial",
          availableDataNodeMap,
          allocatedRegionGroups,
          null,
          regionDatabaseMap,
          remainReplicasMap,
          regionDiskUsageMap,
          ScaleInAllocatorLogger.inferRemovedNodeMap(
              availableDataNodeMap,
              regionDatabaseMap,
              databaseAllocatedRegionGroupMap,
              remainReplicasMap));

      // 2. Build allowed candidate set for each region that needs to be migrated.
      List<TConsensusGroupId> regionKeys = new ArrayList<>(remainReplicasMap.keySet());
      allowedCandidatesMap = new HashMap<>();
      for (TConsensusGroupId regionId : regionKeys) {
        TRegionReplicaSet remainReplicaSet = remainReplicasMap.get(regionId);
        Set<Integer> notAllowedNodes =
            remainReplicaSet.getDataNodeLocations().stream()
                .map(TDataNodeLocation::getDataNodeId)
                .collect(Collectors.toSet());

        // Allowed candidates are the nodes not in the exclusion set
        List<Integer> candidates =
            availableDataNodeMap.keySet().stream()
                .filter(nodeId -> !notAllowedNodes.contains(nodeId))
                .collect(Collectors.toList());
        Collections.shuffle(candidates);
        // Sort candidates: regionCounter ascending, then diskCounter ascending
        candidates.sort(
            (a, b) -> {
              int cmp = Integer.compare(regionCounter[a], regionCounter[b]);
              return (cmp != 0) ? cmp : Long.compare(diskCounter[a], diskCounter[b]);
            });

        allowedCandidatesMap.put(regionId, candidates);
      }

      // Sort regionKeys by the size of its candidate list (most constrained first)
      regionKeys.sort(Comparator.comparingInt(id -> allowedCandidatesMap.get(id).size()));

      // 3. Batch DFS
      Map<TConsensusGroupId, TDataNodeConfiguration> result = new HashMap<>();

      for (int start = 0; start < regionKeys.size(); start += BATCH_SIZE) {
        dfsRegionKeys = regionKeys.subList(start, Math.min(start + BATCH_SIZE, regionKeys.size()));
        int batchSize = dfsRegionKeys.size();

        // Re-sort and truncate candidates based on current counters
        for (TConsensusGroupId regionId : dfsRegionKeys) {
          List<Integer> candidates = allowedCandidatesMap.get(regionId);
          candidates.sort(
              (a, b) -> {
                int cmp = Integer.compare(regionCounter[a], regionCounter[b]);
                return (cmp != 0) ? cmp : Long.compare(diskCounter[a], diskCounter[b]);
              });
          if (candidates.size() > MAX_CANDIDATES) {
            allowedCandidatesMap.put(regionId, candidates.subList(0, MAX_CANDIDATES));
          }
        }

        // Initialize buffers
        bestAssignment = new int[batchSize];
        bestMetrics = new long[] {Long.MAX_VALUE, Long.MAX_VALUE, Long.MIN_VALUE};
        int[] currentAssignment = new int[batchSize];
        int[] additionalRegionLoad = new int[regionCounter.length];
        long[] additionalDiskLoad = new long[diskCounter.length];

        // Initialize incremental variance state from current counters
        incrRegionSum = 0;
        incrRegionSumSq = 0;
        incrDiskSum = 0;
        incrDiskSumSq = 0;
        for (int nodeId : availableNodeIds) {
          long r = regionCounter[nodeId];
          incrRegionSum += r;
          incrRegionSumSq += r * r;
          long d = diskCounter[nodeId] / MigratorLogHelper.DISK_SCALE_FACTOR;
          incrDiskSum += d;
          incrDiskSumSq += d * d;
        }

        // Pre-compute scatter deltas
        scatterDelta = new int[batchSize][regionCounter.length];
        for (int r = 0; r < batchSize; r++) {
          TConsensusGroupId regionId = dfsRegionKeys.get(r);
          for (int nodeId : allowedCandidatesMap.get(regionId)) {
            int inc = 0;
            for (TDataNodeLocation loc : remainReplicasMap.get(regionId).getDataNodeLocations()) {
              inc += combinationCounter[nodeId][loc.getDataNodeId()];
            }
            scatterDelta[r][nodeId] = inc;
          }
        }

        dfsRemoveNodeReplica(
            0, 0, currentAssignment, additionalRegionLoad, additionalDiskLoad, remainReplicasMap);

        if (bestMetrics[0] == Long.MAX_VALUE) {
          // This should not happen if there is at least one valid assignment
          return Collections.emptyMap();
        }

        // Apply batch results: update counters
        for (int i = 0; i < batchSize; i++) {
          TConsensusGroupId regionId = dfsRegionKeys.get(i);
          int chosenNodeId = bestAssignment[i];
          result.put(regionId, availableDataNodeMap.get(chosenNodeId));

          regionCounter[chosenNodeId]++;
          long regionDisk = this.regionDiskUsageMap.getOrDefault(regionId, 0L);
          diskCounter[chosenNodeId] += regionDisk;
          for (TDataNodeLocation loc : remainReplicasMap.get(regionId).getDataNodeLocations()) {
            combinationCounter[chosenNodeId][loc.getDataNodeId()]++;
            combinationCounter[loc.getDataNodeId()][chosenNodeId]++;
          }
        }
      }

      ScaleInAllocatorLogger.logSummary(
          LOGGER,
          "CAR",
          "Final",
          availableDataNodeMap,
          allocatedRegionGroups,
          result,
          regionDatabaseMap,
          remainReplicasMap,
          regionDiskUsageMap,
          null);

      return result;
    } finally {
      clear();
    }
  }

  /**
   * DFS method that searches for optimal migration target assignments.
   *
   * <p>Enumerates all possible assignments (one candidate for each region in the batch) and selects
   * the one that minimizes the lexicographic cost function [var(region), var(disk), scatter].
   *
   * @param index Current DFS level, corresponding to dfsRegionKeys.get(index)
   * @param currentScatter The accumulated scatter value for the partial assignment
   * @param currentAssignment Current partial assignment; its length equals the batch size
   * @param additionalRegionLoad Extra region load currently assigned to each node
   * @param additionalDiskLoad Extra disk load (bytes) currently assigned to each node
   * @param remainReplicasMap The remaining replicas for each region
   */
  private void dfsRemoveNodeReplica(
      int index,
      int currentScatter,
      int[] currentAssignment,
      int[] additionalRegionLoad,
      long[] additionalDiskLoad,
      Map<TConsensusGroupId, TRegionReplicaSet> remainReplicasMap) {
    if (index == dfsRegionKeys.size()) {
      int n = availableNodeIds.size();
      long regionVariance = n * incrRegionSumSq - incrRegionSum * incrRegionSum;
      long diskVariance = n * incrDiskSumSq - incrDiskSum * incrDiskSum;
      long[] currentMetrics = new long[] {regionVariance, diskVariance, currentScatter};

      int cmp = compareMetrics(currentMetrics, bestMetrics);
      if (cmp < 0) {
        System.arraycopy(currentAssignment, 0, bestAssignment, 0, dfsRegionKeys.size());
        System.arraycopy(currentMetrics, 0, bestMetrics, 0, currentMetrics.length);
      }
      return;
    }

    TConsensusGroupId regionId = dfsRegionKeys.get(index);
    long regionDisk = regionDiskUsageMap.getOrDefault(regionId, 0L);
    long regionDiskMB = regionDisk / MigratorLogHelper.DISK_SCALE_FACTOR;
    List<Integer> candidates = allowedCandidatesMap.get(regionId);

    for (int candidate : candidates) {
      currentAssignment[index] = candidate;

      // Incremental update for region variance
      long oldRegion = regionCounter[candidate] + additionalRegionLoad[candidate];
      additionalRegionLoad[candidate]++;
      incrRegionSum += 1;
      incrRegionSumSq += 2L * oldRegion + 1; // (old+1)² - old² = 2*old + 1

      // Incremental update for disk variance
      long oldDiskMB =
          (diskCounter[candidate] + additionalDiskLoad[candidate])
              / MigratorLogHelper.DISK_SCALE_FACTOR;
      additionalDiskLoad[candidate] += regionDisk;
      long newDiskMB =
          (diskCounter[candidate] + additionalDiskLoad[candidate])
              / MigratorLogHelper.DISK_SCALE_FACTOR;
      long diskMBDelta = newDiskMB - oldDiskMB;
      incrDiskSum += diskMBDelta;
      incrDiskSumSq += newDiskMB * newDiskMB - oldDiskMB * oldDiskMB;

      int newScatter = currentScatter + scatterDelta[index][candidate];

      dfsRemoveNodeReplica(
          index + 1,
          newScatter,
          currentAssignment,
          additionalRegionLoad,
          additionalDiskLoad,
          remainReplicasMap);

      // Backtrack
      additionalDiskLoad[candidate] -= regionDisk;
      additionalRegionLoad[candidate]--;
      incrRegionSum -= 1;
      incrRegionSumSq -= 2L * oldRegion + 1;
      incrDiskSum -= diskMBDelta;
      incrDiskSumSq -= newDiskMB * newDiskMB - oldDiskMB * oldDiskMB;
    }
  }

  /**
   * Lexicographic comparison of two metric arrays: [regionVariance, diskVariance, scatter].
   *
   * <ol>
   *   <li>regionVariance: smaller is better (exact comparison)
   *   <li>diskVariance: smaller is better (with ε-tolerance)
   *   <li>scatter: larger is better (reverse comparison)
   * </ol>
   *
   * @return negative if newMetrics is better, positive if bestMetrics is better, 0 if equal
   */
  private int compareMetrics(long[] newMetrics, long[] bestMetrics) {
    // 1. regionVariance: exact comparison
    int cmp = Long.compare(newMetrics[0], bestMetrics[0]);
    if (cmp != 0) {
      return cmp;
    }

    // 2. diskVariance: with ε-tolerance
    long minDiskVar = Math.min(newMetrics[1], bestMetrics[1]);
    long epsilon = (long) (minDiskVar * DISK_VARIANCE_EPSILON);
    if (Math.abs(newMetrics[1] - bestMetrics[1]) > epsilon) {
      return Long.compare(newMetrics[1], bestMetrics[1]);
    }

    // 3. scatter: larger is better (reverse order)
    return Long.compare(bestMetrics[2], newMetrics[2]);
  }

  /**
   * Initialize counters from the current allocation state.
   *
   * @param availableDataNodeMap currently available DataNodes
   * @param allocatedRegionGroups already allocated RegionGroups in the cluster
   * @param regionDiskUsageMap disk usage (bytes) per RegionGroup
   */
  private void prepare(
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap,
      List<TRegionReplicaSet> allocatedRegionGroups,
      Map<TConsensusGroupId, Long> regionDiskUsageMap) {

    this.regionDiskUsageMap = regionDiskUsageMap;
    this.availableNodeIds = new HashSet<>(availableDataNodeMap.keySet());

    // Find the maximum DataNodeId
    int maxDataNodeId =
        Math.max(
            availableDataNodeMap.keySet().stream().max(Integer::compareTo).orElse(0),
            allocatedRegionGroups.stream()
                .flatMap(regionGroup -> regionGroup.getDataNodeLocations().stream())
                .mapToInt(TDataNodeLocation::getDataNodeId)
                .max()
                .orElse(0));

    // Initialize counters
    regionCounter = new int[maxDataNodeId + 1];
    Arrays.fill(regionCounter, 0);
    diskCounter = new long[maxDataNodeId + 1];
    Arrays.fill(diskCounter, 0);
    combinationCounter = new int[maxDataNodeId + 1][maxDataNodeId + 1];
    for (int i = 0; i <= maxDataNodeId; i++) {
      Arrays.fill(combinationCounter[i], 0);
    }

    // Compute counters from allocated region groups
    for (TRegionReplicaSet regionReplicaSet : allocatedRegionGroups) {
      long diskUsage = regionDiskUsageMap.getOrDefault(regionReplicaSet.getRegionId(), 0L);
      List<TDataNodeLocation> dataNodeLocations = regionReplicaSet.getDataNodeLocations();
      for (int i = 0; i < dataNodeLocations.size(); i++) {
        int nodeI = dataNodeLocations.get(i).getDataNodeId();
        regionCounter[nodeI]++;
        diskCounter[nodeI] += diskUsage;
        for (int j = i + 1; j < dataNodeLocations.size(); j++) {
          int nodeJ = dataNodeLocations.get(j).getDataNodeId();
          combinationCounter[nodeI][nodeJ]++;
          combinationCounter[nodeJ][nodeI]++;
        }
      }
    }
  }

  /** Clear temporary state to avoid impacting subsequent calls. */
  private void clear() {
    regionCounter = null;
    diskCounter = null;
    combinationCounter = null;
    dfsRegionKeys = null;
    allowedCandidatesMap = null;
    bestAssignment = null;
    bestMetrics = null;
    scatterDelta = null;
    availableNodeIds = null;
    regionDiskUsageMap = null;
  }
}
