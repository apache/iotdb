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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class CostAwareRegionGroupMigrator implements IRegionGroupMigrator {
  private static final Logger LOGGER = LoggerFactory.getLogger(CostAwareRegionGroupMigrator.class);

  /** Disk deviation threshold δ for Phase 2 bidirectional migration (default 20%). */
  private static final double DISK_DEVIATION_THRESHOLD = 0.2;

  private int replicationFactor;
  // The number of allocated Regions in each DataNode
  private int[] regionCounter;
  // The number of disk usage in each DataNode
  private long[] diskCounter;
  // The number of allocated Regions in each DataNode within the same Database
  private int[] databaseRegionCounter;
  // The number of 2-Region combinations in current cluster
  private int[][] combinationCounter;

  // DataNodeId sets for quick lookup — defines the search space for migration
  private Set<Integer> availableFromDataNodeSet;
  private Set<Integer> availableToDataNodeSet;
  // For each region, the allowed migration options
  private Map<TConsensusGroupId, List<MigrateOption>> allowedMigrateOptions;
  // Statistics of RegionGroups
  private Map<TConsensusGroupId, RegionGroupStatistics> regionGroupStatisticsMap;
  // dfs batch size
  private static final int BATCH_SIZE = 8;
  // Beam width for DFS search (dynamically set based on batch count)
  private int maxBeam = BATCH_SIZE;
  // A list of regions that need to be migrated.
  private List<TConsensusGroupId> dfsRegionKeys;
  // Buffer holding best assignment arrays.
  private MigrateOption[] bestAssignment;
  // An int array holding the best metrics found so far:
  // [diskVariance, regionVariance, migrateCost, scatter]
  private long[] bestMetrics;
  // Pre-calculation, scatterDelta[i][j] means the scatter difference between region i and the
  // option j
  private int[][] scatterDelta;

  private Map<TConsensusGroupId, List<Integer>> replicaNodesIdMap;

  // All available DataNode IDs (used for variance computation)
  private Set<Integer> allAvailableNodeIds;

  // ===== Incremental variance state for DFS =====
  private long incrRegionSum;
  private long incrRegionSumSq;
  private long incrDiskSum;
  private long incrDiskSumSq;

  private static class MigrateOption {
    protected final boolean isMigration;
    protected final int fromNodeId;
    protected final int toNodeId;

    public MigrateOption(boolean isMigration, int fromNodeId, int toNodeId) {
      this.isMigration = isMigration;
      this.fromNodeId = fromNodeId;
      this.toNodeId = toNodeId;
    }
  }

  /**
   * Initialize counters (regionCounter, diskCounter, databaseRegionCounter, combinationCounter)
   * from the current allocation state.
   */
  private void initializeCounters(
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap,
      List<TRegionReplicaSet> allocatedRegionGroups,
      List<TRegionReplicaSet> databaseAllocatedRegionGroups) {

    // Store the maximum DataNodeId
    int maxDataNodeId =
        Math.max(
            availableDataNodeMap.keySet().stream().max(Integer::compareTo).orElse(0),
            allocatedRegionGroups.stream()
                .flatMap(regionGroup -> regionGroup.getDataNodeLocations().stream())
                .mapToInt(TDataNodeLocation::getDataNodeId)
                .max()
                .orElse(0));

    // Compute regionCounter, databaseRegionCounter and combinationCounter
    regionCounter = new int[maxDataNodeId + 1];
    Arrays.fill(regionCounter, 0);
    diskCounter = new long[maxDataNodeId + 1];
    Arrays.fill(diskCounter, 0);
    databaseRegionCounter = new int[maxDataNodeId + 1];
    Arrays.fill(databaseRegionCounter, 0);
    combinationCounter = new int[maxDataNodeId + 1][maxDataNodeId + 1];
    for (int i = 0; i <= maxDataNodeId; i++) {
      Arrays.fill(combinationCounter[i], 0);
    }
    for (TRegionReplicaSet regionReplicaSet : allocatedRegionGroups) {
      List<TDataNodeLocation> dataNodeLocations = regionReplicaSet.getDataNodeLocations();
      for (int i = 0; i < dataNodeLocations.size(); i++) {
        regionCounter[dataNodeLocations.get(i).getDataNodeId()]++;
        diskCounter[dataNodeLocations.get(i).getDataNodeId()] +=
            regionGroupStatisticsMap.get(regionReplicaSet.getRegionId()).getDiskUsage();
        for (int j = i + 1; j < dataNodeLocations.size(); j++) {
          combinationCounter[dataNodeLocations.get(i).getDataNodeId()][
              dataNodeLocations.get(j).getDataNodeId()]++;
          combinationCounter[dataNodeLocations.get(j).getDataNodeId()][
              dataNodeLocations.get(i).getDataNodeId()]++;
        }
      }
    }
    for (TRegionReplicaSet regionReplicaSet : databaseAllocatedRegionGroups) {
      List<TDataNodeLocation> dataNodeLocations = regionReplicaSet.getDataNodeLocations();
      for (TDataNodeLocation dataNodeLocation : dataNodeLocations) {
        databaseRegionCounter[dataNodeLocation.getDataNodeId()]++;
      }
    }

    allAvailableNodeIds = new HashSet<>(availableDataNodeMap.keySet());
  }

  @Override
  public Map<TConsensusGroupId, TRegionReplicaSet> autoBalanceRegionReplicasDistribution(
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap,
      Map<TConsensusGroupId, RegionGroupStatistics> regionGroupStatisticsMap,
      List<TRegionReplicaSet> allocatedRegionGroups,
      int replicationFactor,
      List<Integer> targetNodeIds) {
    this.regionGroupStatisticsMap = regionGroupStatisticsMap;
    this.replicationFactor = replicationFactor;
    this.replicaNodesIdMap =
        allocatedRegionGroups.stream()
            .collect(
                Collectors.toMap(
                    TRegionReplicaSet::getRegionId,
                    regionReplicaSet ->
                        regionReplicaSet.getDataNodeLocations().stream()
                            .map(TDataNodeLocation::getDataNodeId)
                            .collect(Collectors.toList())));
    // 1. Initialize counters
    initializeCounters(availableDataNodeMap, allocatedRegionGroups, Collections.emptyList());
    logSummary("Initial", null);

    // 2. Build allowed migration set for each region.
    // No migration: 1 option.
    // Migrate once: Select a source replica from ∈ ReplicaSet and a target node to ∈ Nodes \
    // ReplicaSet, for a total of r * (n - r) options.
    // So the total options are r * (n - r) + 1.
    List<TConsensusGroupId> regionKeys =
        allocatedRegionGroups.stream()
            .map(TRegionReplicaSet::getRegionId)
            .collect(Collectors.toList());

    Map<TConsensusGroupId, MigrateOption> result;

    if (targetNodeIds != null && !targetNodeIds.isEmpty()) {
      // ===== Unidirectional mode (scale-out) =====
      result = executeUnidirectionalMigration(availableDataNodeMap, regionKeys, targetNodeIds);
    } else {
      // ===== Bidirectional mode (global balance) =====
      result = executeBidirectionalMigration(availableDataNodeMap, regionKeys);
    }
    logSummary("Final", result);

    // Construct the final result
    return constructResult(result, regionKeys, availableDataNodeMap);
  }

  // ==================== Unidirectional Mode (Scale-Out) ====================

  /**
   * Unidirectional migration: migrate regions from source nodes to specified target nodes. Used for
   * scale-out scenarios.
   */
  private Map<TConsensusGroupId, MigrateOption> executeUnidirectionalMigration(
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap,
      List<TConsensusGroupId> regionKeys,
      List<Integer> targetNodeIds) {

    // Determine to set
    availableToDataNodeSet = new HashSet<>();
    for (Integer targetNodeId : targetNodeIds) {
      if (availableDataNodeMap.containsKey(targetNodeId)) {
        availableToDataNodeSet.add(targetNodeId);
      }
    }
    if (availableToDataNodeSet.isEmpty()) {
      LOGGER.warn("[LoadBalance] No valid target nodes, returning empty migration plan");
      Map<TConsensusGroupId, MigrateOption> result = new HashMap<>();
      for (TConsensusGroupId regionId : regionKeys) {
        result.put(regionId, new MigrateOption(false, -1, -1));
      }
      return result;
    }

    availableFromDataNodeSet = new HashSet<>(allAvailableNodeIds);
    availableFromDataNodeSet.removeAll(availableToDataNodeSet);

    // Build migration options and sort regions
    buildMigrateOptions(regionKeys, availableDataNodeMap);
    sortRegionsByMaxNodeDiskThenRegionDisk(regionKeys);

    // Execute batch DFS
    return executeBatchDFS(regionKeys);
  }

  // ==================== Bidirectional Mode (Global Balance) ====================

  /**
   * Bidirectional migration: Phase 1 (region balance) → Phase 2 (disk balance). Used for global
   * load balance scenarios when no target nodes are specified.
   */
  private Map<TConsensusGroupId, MigrateOption> executeBidirectionalMigration(
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap,
      List<TConsensusGroupId> regionKeys) {

    LOGGER.info("[LoadBalance] Entering bidirectional mode (LOAD BALANCE ALL)");

    Map<TConsensusGroupId, MigrateOption> result = new HashMap<>();
    for (TConsensusGroupId regionId : regionKeys) {
      result.put(regionId, new MigrateOption(false, -1, -1));
    }

    // Phase 1: Region balance
    Set<TConsensusGroupId> migratedRegions =
        executePhase1RegionBalance(availableDataNodeMap, regionKeys, result);
    logSummary("Phase1", result);

    // Phase 2: Disk balance (only for regions not migrated in Phase 1)
    executePhase2DiskBalance(regionKeys, result);

    return result;
  }

  /**
   * Phase 1: Balance region count across nodes. Migrates regions from nodes with regionCount >
   * idealCeil to nodes with regionCount < idealFloor.
   *
   * @return set of region IDs that were migrated in this phase
   */
  private Set<TConsensusGroupId> executePhase1RegionBalance(
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap,
      List<TConsensusGroupId> regionKeys,
      Map<TConsensusGroupId, MigrateOption> result) {

    Set<TConsensusGroupId> migratedRegions = new HashSet<>();

    int totalRegions = 0;
    for (int nodeId : allAvailableNodeIds) {
      totalRegions += regionCounter[nodeId];
    }
    int nodeCount = allAvailableNodeIds.size();
    int idealFloor = totalRegions / nodeCount;
    int idealCeil = idealFloor + (totalRegions % nodeCount == 0 ? 0 : 1);

    LOGGER.info(
        "[LoadBalance] Phase 1: Region balance. totalRegions={}, nodeCount={}, ideal=[{}, {}]",
        totalRegions,
        nodeCount,
        idealFloor,
        idealCeil);
    if (!needsRegionBalance(idealFloor, idealCeil)) {
      LOGGER.info("[LoadBalance] Phase 1: Skipped, regions already balanced");
      return migratedRegions;
    }

    // Filter regions that can participate in Phase 1
    // (their replicas include at least one over-loaded node)
    List<TConsensusGroupId> phase1Regions = new ArrayList<>();
    for (TConsensusGroupId regionId : regionKeys) {
      List<Integer> replicaNodeIds = replicaNodesIdMap.get(regionId);
      boolean hasOverNode = false;
      for (int nodeId : replicaNodeIds) {
        if (regionCounter[nodeId] > idealCeil) {
          hasOverNode = true;
          break;
        }
      }
      if (hasOverNode) {
        phase1Regions.add(regionId);
      }
    }

    // Execute batch DFS in rounds, refreshing from/to sets each batch
    for (int start = 0; start < phase1Regions.size(); start += BATCH_SIZE) {
      // Refresh from/to sets before each batch
      if (!refreshPhase1CandidateSets(idealFloor, idealCeil)) {
        LOGGER.info("[LoadBalance] Phase 1: Region balance achieved, stopping");
        break;
      }

      List<TConsensusGroupId> batchRegions =
          phase1Regions.subList(start, Math.min(start + BATCH_SIZE, phase1Regions.size()));

      // Rebuild migration options for this batch with current from/to sets
      buildMigrateOptions(batchRegions, availableDataNodeMap);

      // Sort batch regions
      sortRegionsByMaxNodeDiskThenRegionDisk(batchRegions);

      // Execute one batch
      Map<TConsensusGroupId, MigrateOption> batchResult = executeSingleBatch(batchRegions);

      // Apply results
      for (Map.Entry<TConsensusGroupId, MigrateOption> entry : batchResult.entrySet()) {
        TConsensusGroupId regionId = entry.getKey();
        MigrateOption option = entry.getValue();
        result.put(regionId, option);
        if (option.isMigration) {
          migratedRegions.add(regionId);
          applyMigration(regionId, option);
        }
      }
    }

    for (TConsensusGroupId regionId : migratedRegions) {
      MigrateOption option = result.get(regionId);
      LOGGER.info(
          "[LoadBalance] Phase 1: Region {} : Node {} -> Node {} (disk={})",
          regionId,
          option.fromNodeId,
          option.toNodeId,
          toMB(regionGroupStatisticsMap.get(regionId).getDiskUsage()));
    }
    LOGGER.info(
        "[LoadBalance] Phase 1 completed. Migrations: {}, Var(region)={}, Var(disk)={}",
        migratedRegions.size(),
        computeRegionVariance(),
        computeDiskVariance());
    LOGGER.info("[LoadBalance] Phase 1 distribution: {}", getNodeDistribution());

    return migratedRegions;
  }

  /** Check if any node has regionCount outside [idealFloor, idealCeil]. */
  private boolean needsRegionBalance(int idealFloor, int idealCeil) {
    for (int nodeId : allAvailableNodeIds) {
      if (regionCounter[nodeId] > idealCeil || regionCounter[nodeId] < idealFloor) {
        return true;
      }
    }
    return false;
  }

  /**
   * Refresh Phase 1 from/to candidate sets based on current region distribution.
   *
   * @return true if there are still over/under-loaded nodes to fix
   */
  private boolean refreshPhase1CandidateSets(int idealFloor, int idealCeil) {
    availableFromDataNodeSet = new HashSet<>();
    availableToDataNodeSet = new HashSet<>();
    for (int nodeId : allAvailableNodeIds) {
      if (regionCounter[nodeId] > idealCeil) {
        availableFromDataNodeSet.add(nodeId);
      }
      if (regionCounter[nodeId] < idealFloor) {
        availableToDataNodeSet.add(nodeId);
      }
    }
    return !availableFromDataNodeSet.isEmpty() && !availableToDataNodeSet.isEmpty();
  }

  /**
   * Phase 2: Balance disk usage across nodes using greedy pairwise swap. A swap atomically
   * exchanges two regions between an over-loaded and an under-loaded node, preserving Var(region)
   * while optimizing Var(disk).
   *
   * <p>Phase 1 migrated regions CAN participate in swap. When a Phase 1-migrated region is swapped,
   * its migration destination is "amended" (e.g., A→B becomes A→C), keeping total migration count
   * at one per region.
   *
   * <p>Uses deviation threshold δ to select participant nodes. Each round enumerates all valid swap
   * pairs, picks the best one (by Var(disk) → SwapCost → Scatter), applies it, and repeats until no
   * improving swap exists or the ε-tolerance is reached.
   */
  private void executePhase2DiskBalance(
      List<TConsensusGroupId> regionKeys, Map<TConsensusGroupId, MigrateOption> result) {

    // Compute mean disk usage
    long totalDisk = 0;
    for (int nodeId : allAvailableNodeIds) {
      totalDisk += diskCounter[nodeId];
    }
    double meanDisk = (double) totalDisk / allAvailableNodeIds.size();
    double upperThreshold = meanDisk * (1 + DISK_DEVIATION_THRESHOLD);
    double lowerThreshold = meanDisk * (1 - DISK_DEVIATION_THRESHOLD);

    // Compute participant set
    Set<Integer> participantSet = new HashSet<>();
    for (int nodeId : allAvailableNodeIds) {
      if (diskCounter[nodeId] > upperThreshold || diskCounter[nodeId] < lowerThreshold) {
        participantSet.add(nodeId);
      }
    }

    if (participantSet.isEmpty()) {
      LOGGER.info(
          "[LoadBalance] Phase 2: Skipped, disk usage within threshold (δ={})",
          DISK_DEVIATION_THRESHOLD);
      return;
    }

    long phase2StartDiskVariance = computeDiskVariance();
    LOGGER.info(
        "[LoadBalance] Phase 2: Disk balance (swap). participants={}, Var(disk)={}",
        participantSet.size(),
        phase2StartDiskVariance);

    // Build currentReplicaMap: the actual replica positions AFTER Phase 1.
    // For Phase 1 migrated regions, apply the migration to the original replicaNodesIdMap.
    // Note: replicaNodesIdMap itself is NOT mutated here — we build a separate snapshot.
    Map<TConsensusGroupId, List<Integer>> currentReplicaMap = new HashMap<>();
    for (TConsensusGroupId regionId : regionKeys) {
      List<Integer> original = replicaNodesIdMap.get(regionId);
      MigrateOption phase1Option = result.get(regionId);
      if (phase1Option != null && phase1Option.isMigration) {
        List<Integer> updated = new ArrayList<>(original);
        int idx = updated.indexOf(phase1Option.fromNodeId);
        if (idx >= 0) {
          updated.set(idx, phase1Option.toNodeId);
        }
        currentReplicaMap.put(regionId, updated);
      } else {
        currentReplicaMap.put(regionId, new ArrayList<>(original));
      }
    }

    // All regions are candidates for swap (participant threshold is only the gate to enter Phase
    // 2).
    // Build nodeToRegions based on currentReplicaMap (post-Phase 1 positions).
    Map<Integer, List<TConsensusGroupId>> nodeToRegions = new HashMap<>();
    Set<TConsensusGroupId> candidateRegionSet = new HashSet<>();
    for (TConsensusGroupId regionId : regionKeys) {
      List<Integer> replicaNodeIds = currentReplicaMap.get(regionId);
      candidateRegionSet.add(regionId);
      for (int nodeId : replicaNodeIds) {
        nodeToRegions.computeIfAbsent(nodeId, k -> new ArrayList<>()).add(regionId);
      }
    }

    // KM matching loop: each round finds a globally optimal set of non-conflicting swaps
    int phase2Swaps = 0;
    List<String> swapLog = new ArrayList<>();
    long prevDiskVariance = phase2StartDiskVariance;
    while (true) {
      // 1. Refresh overNodes / underNodes based on current diskCounter
      List<Integer> overNodes = new ArrayList<>();
      List<Integer> underNodes = new ArrayList<>();
      for (int nodeId : allAvailableNodeIds) {
        if (diskCounter[nodeId] > meanDisk) {
          overNodes.add(nodeId);
        } else if (diskCounter[nodeId] < meanDisk) {
          underNodes.add(nodeId);
        }
      }
      if (overNodes.isEmpty() || underNodes.isEmpty()) {
        break;
      }

      // 2. Collect left candidates (regions on overNodes) and right candidates (on underNodes)
      List<TConsensusGroupId> leftRegions = new ArrayList<>();
      List<Integer> leftNodes = new ArrayList<>();
      List<TConsensusGroupId> rightRegions = new ArrayList<>();
      List<Integer> rightNodes = new ArrayList<>();

      for (int overNode : overNodes) {
        for (TConsensusGroupId regionId :
            nodeToRegions.getOrDefault(overNode, Collections.emptyList())) {
          if (candidateRegionSet.contains(regionId)) {
            List<Integer> replica = currentReplicaMap.get(regionId);
            if (replica != null && replica.contains(overNode)) {
              leftRegions.add(regionId);
              leftNodes.add(overNode);
            }
          }
        }
      }
      for (int underNode : underNodes) {
        for (TConsensusGroupId regionId :
            nodeToRegions.getOrDefault(underNode, Collections.emptyList())) {
          if (candidateRegionSet.contains(regionId)) {
            List<Integer> replica = currentReplicaMap.get(regionId);
            if (replica != null && replica.contains(underNode)) {
              rightRegions.add(regionId);
              rightNodes.add(underNode);
            }
          }
        }
      }

      if (leftRegions.isEmpty() || rightRegions.isEmpty()) {
        break;
      }

      int leftSize = leftRegions.size();
      int rightSize = rightRegions.size();
      int n = Math.max(leftSize, rightSize);

      // 3. Build weight matrix: weight[i][j] = diskA - diskB if swap is feasible, else 0
      long[][] weight = new long[n][n];
      for (int i = 0; i < leftSize; i++) {
        TConsensusGroupId regionA = leftRegions.get(i);
        int nodeI = leftNodes.get(i);
        List<Integer> replicaA = currentReplicaMap.get(regionA);
        long diskA = regionGroupStatisticsMap.get(regionA).getDiskUsage();

        for (int j = 0; j < rightSize; j++) {
          TConsensusGroupId regionB = rightRegions.get(j);
          int nodeJ = rightNodes.get(j);
          if (regionA.equals(regionB)) {
            continue;
          }
          List<Integer> replicaB = currentReplicaMap.get(regionB);

          // Constraint checks
          if (replicaA.contains(nodeJ)) {
            continue; // nodeJ already in regionA's replica set
          }
          if (replicaB.contains(nodeI)) {
            continue; // nodeI already in regionB's replica set
          }
          if (!isSwapAllowed(result, regionA, nodeI, nodeJ)) {
            continue;
          }
          if (!isSwapAllowed(result, regionB, nodeJ, nodeI)) {
            continue;
          }
          long diskB = regionGroupStatisticsMap.get(regionB).getDiskUsage();
          if (diskA <= diskB) {
            continue; // direction: only move larger-disk region from overNode
          }

          weight[i][j] = diskA - diskB; // net disk transfer benefit
        }
      }

      // 4. Solve maximum weight matching using KM algorithm
      int[] matchR = kmMaxWeightMatching(weight, n);

      // 5. Apply matched swaps with conflict checks:
      //    - A region must not appear in two swaps per round (usedThisRound).
      //    - A node must not participate in more than one swap per round (usedNodesThisRound)
      //      to prevent cumulative over/under-loading in a single KM round.
      boolean anySwapped = false;
      Set<TConsensusGroupId> usedThisRound = new HashSet<>();
      Set<Integer> usedNodesThisRound = new HashSet<>();
      for (int j = 0; j < rightSize; j++) {
        int i = matchR[j];
        if (i < 0 || i >= leftSize) {
          continue;
        }
        if (weight[i][j] <= 0) {
          continue; // no feasible edge or no benefit
        }

        TConsensusGroupId regionA = leftRegions.get(i);
        int nodeI = leftNodes.get(i);
        TConsensusGroupId regionB = rightRegions.get(j);
        int nodeJ = rightNodes.get(j);

        // A region (with r replicas) may appear both as left and right candidate.
        // Skip if either region was already used in a swap this round.
        if (usedThisRound.contains(regionA) || usedThisRound.contains(regionB)) {
          continue;
        }

        // Per-node limit: each node participates in at most one swap per KM round.
        // This prevents cumulative overload (e.g., one underNode receiving 5 large regions).
        if (usedNodesThisRound.contains(nodeI) || usedNodesThisRound.contains(nodeJ)) {
          continue;
        }

        applySwapOnCurrentMap(regionA, regionB, nodeI, nodeJ, currentReplicaMap);
        amendResult(result, regionA, nodeI, nodeJ);
        amendResult(result, regionB, nodeJ, nodeI);
        candidateRegionSet.remove(regionA);
        candidateRegionSet.remove(regionB);
        usedThisRound.add(regionA);
        usedThisRound.add(regionB);
        usedNodesThisRound.add(nodeI);
        usedNodesThisRound.add(nodeJ);
        anySwapped = true;
        phase2Swaps++;
        swapLog.add(
            String.format(
                "Region %s (Node %d->%d, disk=%s) <-> Region %s (Node %d->%d, disk=%s)",
                regionA,
                nodeI,
                nodeJ,
                toMB(regionGroupStatisticsMap.get(regionA).getDiskUsage()),
                regionB,
                nodeJ,
                nodeI,
                toMB(regionGroupStatisticsMap.get(regionB).getDiskUsage())));
      }

      if (!anySwapped) {
        break;
      }

      // 6. Rebuild nodeToRegions from currentReplicaMap after swaps
      nodeToRegions.clear();
      for (TConsensusGroupId regionId : regionKeys) {
        List<Integer> replicaNodeIds = currentReplicaMap.get(regionId);
        for (int nodeId : replicaNodeIds) {
          nodeToRegions.computeIfAbsent(nodeId, k -> new ArrayList<>()).add(regionId);
        }
      }

      // 7. ε-tolerance: stop if Var(disk) improvement is negligible
      long newDiskVariance = computeDiskVariance();
      if (prevDiskVariance > 0
          && Math.abs(newDiskVariance - prevDiskVariance)
              <= (long) (prevDiskVariance * DISK_VARIANCE_EPSILON)) {
        break;
      }
      prevDiskVariance = newDiskVariance;
    }

    for (String log : swapLog) {
      LOGGER.info("[LoadBalance] Phase 2: {}", log);
    }
    LOGGER.info(
        "[LoadBalance] Phase 2 completed. Swaps: {}, Var(disk): {} -> {}",
        phase2Swaps,
        phase2StartDiskVariance,
        computeDiskVariance());
  }

  /**
   * Apply a swap on the currentReplicaMap (Phase 2's working state). Updates counters
   * (regionCounter, diskCounter, combinationCounter) and currentReplicaMap.
   *
   * <p>regionA moves from nodeI→nodeJ, regionB moves from nodeJ→nodeI.
   */
  private void applySwapOnCurrentMap(
      TConsensusGroupId regionA,
      TConsensusGroupId regionB,
      int nodeI,
      int nodeJ,
      Map<TConsensusGroupId, List<Integer>> currentReplicaMap) {
    // Step 1: regionA: nodeI → nodeJ (use currentReplicaMap for combinationCounter)
    applyMigrationWithReplicaMap(regionA, nodeI, nodeJ, currentReplicaMap.get(regionA));
    List<Integer> replicaA = currentReplicaMap.get(regionA);
    replicaA.set(replicaA.indexOf(nodeI), nodeJ);

    // Step 2: regionB: nodeJ → nodeI (regionA's map already updated)
    applyMigrationWithReplicaMap(regionB, nodeJ, nodeI, currentReplicaMap.get(regionB));
    List<Integer> replicaB = currentReplicaMap.get(regionB);
    replicaB.set(replicaB.indexOf(nodeJ), nodeI);
  }

  /**
   * Apply a single region migration using an explicit replica list (instead of replicaNodesIdMap).
   * Updates regionCounter, diskCounter, databaseRegionCounter, and combinationCounter.
   */
  private void applyMigrationWithReplicaMap(
      TConsensusGroupId regionId, int fromNodeId, int toNodeId, List<Integer> replicaNodeIds) {
    for (int replicaNodeId : replicaNodeIds) {
      if (replicaNodeId == fromNodeId) {
        continue;
      }
      combinationCounter[replicaNodeId][fromNodeId]--;
      combinationCounter[fromNodeId][replicaNodeId]--;
      combinationCounter[replicaNodeId][toNodeId]++;
      combinationCounter[toNodeId][replicaNodeId]++;
    }
    long regionDisk = regionGroupStatisticsMap.get(regionId).getDiskUsage();
    regionCounter[fromNodeId]--;
    regionCounter[toNodeId]++;
    databaseRegionCounter[fromNodeId]--;
    databaseRegionCounter[toNodeId]++;
    diskCounter[fromNodeId] -= regionDisk;
    diskCounter[toNodeId] += regionDisk;
  }

  /**
   * Amend the migration result for a region after a Phase 2 swap.
   *
   * <p>A region's replica is being swapped from swapFrom → swapTo. Three cases:
   *
   * <ul>
   *   <li><b>Case A</b>: Phase 1 migrated this region (originalFrom → P1To), and swap moves the
   *       Phase 1-migrated replica (swapFrom == P1To). Amend to (originalFrom → swapTo). Special
   *       sub-case: if swapTo == originalFrom, net zero → isMigration=false.
   *   <li><b>Case B</b>: Phase 1 migrated this region, but swap moves a different (original)
   *       replica (swapFrom != P1To, swapTo == originalFrom). Rewrite as (swapFrom → P1To). Special
   *       sub-case: if swapFrom == P1To (impossible by Case A priority), skip.
   *   <li><b>Case C</b>: Region was NOT Phase 1-migrated. New migration (swapFrom → swapTo).
   * </ul>
   */
  private void amendResult(
      Map<TConsensusGroupId, MigrateOption> result,
      TConsensusGroupId regionId,
      int swapFrom,
      int swapTo) {
    MigrateOption existing = result.get(regionId);
    if (existing != null && existing.isMigration) {
      int originalFrom = existing.fromNodeId;
      int p1To = existing.toNodeId;
      if (swapFrom == p1To) {
        // Case A: swap moves the Phase 1-migrated replica
        if (originalFrom == swapTo) {
          // Back to origin → net zero
          result.put(regionId, new MigrateOption(false, -1, -1));
        } else {
          // Amend destination: originalFrom → swapTo
          result.put(regionId, new MigrateOption(true, originalFrom, swapTo));
        }
      } else {
        // Case B: swap moves an original (non-Phase 1) replica, swapTo == originalFrom
        // Rewrite: instead of migrating originalFrom→p1To + swapFrom→originalFrom,
        // just migrate swapFrom→p1To (single migration)
        result.put(regionId, new MigrateOption(true, swapFrom, p1To));
      }
    } else {
      // Case C: not migrated before → new migration
      result.put(regionId, new MigrateOption(true, swapFrom, swapTo));
    }
  }

  /**
   * Check if a swap is allowed for a region under the one-migration-per-region constraint.
   *
   * <p>If the region was Phase 1-migrated (originalFrom → p1To):
   *
   * <ul>
   *   <li>If swap moves the Phase 1-migrated replica (swapFrom == p1To): always allowed (Case A).
   *   <li>If swap moves an original replica (swapFrom != p1To): only allowed if swapTo ==
   *       originalFrom (Case B), so the two migrations can be merged into one.
   * </ul>
   *
   * <p>If the region was NOT Phase 1-migrated: always allowed.
   */
  private boolean isSwapAllowed(
      Map<TConsensusGroupId, MigrateOption> result,
      TConsensusGroupId regionId,
      int swapFrom,
      int swapTo) {
    MigrateOption existing = result.get(regionId);
    if (existing == null || !existing.isMigration) {
      return true; // Not Phase 1-migrated, always allowed
    }
    int p1To = existing.toNodeId;
    int originalFrom = existing.fromNodeId;
    if (swapFrom == p1To) {
      return true; // Case A: moving the Phase 1-migrated replica
    }
    // Case B: moving an original replica — only allowed if target is the original location
    return swapTo == originalFrom;
  }

  /**
   * Kuhn-Munkres (Hungarian) algorithm for maximum weight matching in a bipartite graph.
   *
   * <p>Finds a perfect matching that maximizes the total weight. The input is an n×n weight matrix
   * (padded with zeros for non-square bipartite graphs). Weights of 0 represent absent edges.
   *
   * <p>Time complexity: O(n³).
   *
   * @param w n×n weight matrix; w[i][j] = benefit of matching left vertex i to right vertex j
   * @param n size of the square matrix
   * @return matchR array where matchR[j] = i means right vertex j is matched to left vertex i; -1
   *     means unmatched
   */
  private static int[] kmMaxWeightMatching(long[][] w, int n) {
    // Labels for left and right vertices
    long[] lx = new long[n]; // lx[i] = max(w[i][j]) for all j
    long[] ly = new long[n]; // ly[j] = 0 initially
    int[] matchL = new int[n]; // matchL[i] = j means left i matched to right j
    int[] matchR = new int[n]; // matchR[j] = i means right j matched to left i
    Arrays.fill(matchL, -1);
    Arrays.fill(matchR, -1);

    // Initialize labels
    for (int i = 0; i < n; i++) {
      lx[i] = Long.MIN_VALUE;
      for (int j = 0; j < n; j++) {
        lx[i] = Math.max(lx[i], w[i][j]);
      }
    }

    for (int i = 0; i < n; i++) {
      // For each left vertex i, find an augmenting path in the equality graph
      long[] slack =
          new long[n]; // slack[j] = min over visited left vertices of (lx[i]+ly[j]-w[i][j])
      Arrays.fill(slack, Long.MAX_VALUE);
      int[] slackFrom = new int[n]; // which left vertex achieved the slack
      boolean[] visitedL = new boolean[n];
      boolean[] visitedR = new boolean[n];
      int[] prev = new int[n]; // prev[j] = left vertex that reached right vertex j in BFS
      Arrays.fill(prev, -1);

      // Start BFS from left vertex i
      // Use a queue-like approach: we start with left vertex i exposed
      // We'll use the "slack-based" O(n³) implementation
      visitedL[i] = true;
      // Initialize slack from vertex i
      for (int j = 0; j < n; j++) {
        slack[j] = lx[i] + ly[j] - w[i][j];
        slackFrom[j] = i;
      }

      while (true) {
        // Find the minimum slack among unvisited right vertices
        int minJ = -1;
        long minSlack = Long.MAX_VALUE;
        for (int j = 0; j < n; j++) {
          if (!visitedR[j] && slack[j] < minSlack) {
            minSlack = slack[j];
            minJ = j;
          }
        }

        // Adjust labels by minSlack
        if (minSlack > 0) {
          for (int k = 0; k < n; k++) {
            if (visitedL[k]) {
              lx[k] -= minSlack;
            }
            if (visitedR[k]) {
              ly[k] += minSlack;
            } else {
              slack[k] -= minSlack;
            }
          }
        }

        // Now minJ is a right vertex in the equality graph
        visitedR[minJ] = true;
        prev[minJ] = slackFrom[minJ];

        if (matchR[minJ] < 0) {
          // Found an augmenting path ending at unmatched right vertex minJ
          // Trace back and flip the matching
          int j = minJ;
          while (j >= 0) {
            int leftV = prev[j];
            int prevJ = matchL[leftV];
            matchR[j] = leftV;
            matchL[leftV] = j;
            j = prevJ;
          }
          break; // Done with left vertex i
        }

        // minJ is matched to some left vertex — continue BFS
        int nextLeft = matchR[minJ];
        visitedL[nextLeft] = true;
        // Update slack from nextLeft
        for (int j = 0; j < n; j++) {
          if (!visitedR[j]) {
            long newSlack = lx[nextLeft] + ly[j] - w[nextLeft][j];
            if (newSlack < slack[j]) {
              slack[j] = newSlack;
              slackFrom[j] = nextLeft;
            }
          }
        }
      }
    }

    return matchR;
  }

  // ==================== Common Batch DFS Logic ====================

  /**
   * Build allowed migration options for the given regions based on the current from/to candidate
   * sets.
   */
  private void buildMigrateOptions(
      List<TConsensusGroupId> regionKeys,
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap) {
    allowedMigrateOptions = new HashMap<>();
    for (TConsensusGroupId regionId : regionKeys) {
      List<MigrateOption> migrateOptions = new ArrayList<>();
      // No migration option
      migrateOptions.add(new MigrateOption(false, -1, -1));
      // Migration options
      List<Integer> replicaNodeIds = replicaNodesIdMap.get(regionId);
      for (Integer fromNodeId : replicaNodeIds) {
        for (Integer toNodeId : availableDataNodeMap.keySet()) {
          if (!replicaNodeIds.contains(toNodeId)
              && availableFromDataNodeSet.contains(fromNodeId)
              && availableToDataNodeSet.contains(toNodeId)) {
            migrateOptions.add(new MigrateOption(true, fromNodeId, toNodeId));
          }
        }
      }
      migrateOptions.sort(
          Comparator.comparingLong(
              option -> option.isMigration ? diskCounter[option.toNodeId] : Long.MAX_VALUE));
      allowedMigrateOptions.put(regionId, migrateOptions);
    }
  }

  /**
   * Sort regions by: 1. Maximum disk load among replica nodes (descending) 2. Region's own
   * diskUsage (descending) for tie-breaking
   */
  private void sortRegionsByMaxNodeDiskThenRegionDisk(List<TConsensusGroupId> regionKeys) {
    regionKeys.sort(
        (a, b) -> {
          long maxA =
              replicaNodesIdMap.get(a).stream()
                  .mapToLong(nodeId -> diskCounter[nodeId])
                  .max()
                  .orElse(0L);
          long maxB =
              replicaNodesIdMap.get(b).stream()
                  .mapToLong(nodeId -> diskCounter[nodeId])
                  .max()
                  .orElse(0L);
          if (maxA != maxB) {
            return Long.compare(maxB, maxA);
          }
          long diskA = regionGroupStatisticsMap.get(a).getDiskUsage();
          long diskB = regionGroupStatisticsMap.get(b).getDiskUsage();
          return Long.compare(diskB, diskA);
        });
  }

  /**
   * Execute batch DFS over all given regions, returning the migration plan. Applies migrations to
   * counters but does NOT log individual migrations — callers handle their own logging.
   */
  private Map<TConsensusGroupId, MigrateOption> executeBatchDFS(
      List<TConsensusGroupId> regionKeys) {
    Map<TConsensusGroupId, MigrateOption> result = new HashMap<>();

    // Determine beam width based on total batch count
    int batches = (regionKeys.size() + BATCH_SIZE - 1) / BATCH_SIZE;
    if (batches <= 5) {
      maxBeam = 8;
    } else if (batches <= 30) {
      maxBeam = 6;
    } else {
      maxBeam = 5;
    }

    for (int start = 0; start < regionKeys.size(); start += BATCH_SIZE) {
      List<TConsensusGroupId> batchRegions =
          regionKeys.subList(start, Math.min(start + BATCH_SIZE, regionKeys.size()));

      Map<TConsensusGroupId, MigrateOption> batchResult = executeSingleBatch(batchRegions);

      // Apply results and update global state
      for (Map.Entry<TConsensusGroupId, MigrateOption> entry : batchResult.entrySet()) {
        TConsensusGroupId regionId = entry.getKey();
        MigrateOption option = entry.getValue();
        result.put(regionId, option);
        if (option.isMigration) {
          applyMigration(regionId, option);
        }
      }
    }

    return result;
  }

  /** Execute a single batch of DFS search and return the best assignment. */
  private Map<TConsensusGroupId, MigrateOption> executeSingleBatch(
      List<TConsensusGroupId> batchRegions) {
    dfsRegionKeys = batchRegions;
    int batchSize = batchRegions.size();

    // currentAssignment holds the candidate option chosen for the region at that index
    MigrateOption[] currentAssignment = new MigrateOption[batchSize];
    // Initialize buffer
    bestAssignment = new MigrateOption[batchSize];
    Arrays.fill(bestAssignment, new MigrateOption(false, -1, -1));

    // Pre-calculate scatterDelta
    scatterDelta = new int[batchSize][];
    for (int i = 0; i < batchSize; i++) {
      TConsensusGroupId regionId = dfsRegionKeys.get(i);
      List<Integer> replicaNodeIds = replicaNodesIdMap.get(regionId);
      List<MigrateOption> options = allowedMigrateOptions.get(regionId);
      int optionSize = options.size();
      scatterDelta[i] = new int[optionSize];
      scatterDelta[i][0] = 0;
      for (int j = 0; j < optionSize; j++) {
        MigrateOption option = options.get(j);
        int fromNodeId = option.fromNodeId;
        int toNodeId = option.toNodeId;
        int newScatter = 0;
        int currentScatter = 0;
        if (option.isMigration) {
          for (Integer replicaNodeId : replicaNodeIds) {
            if (replicaNodeId == fromNodeId) {
              continue;
            }
            newScatter += combinationCounter[replicaNodeId][toNodeId];
            currentScatter += combinationCounter[replicaNodeId][fromNodeId];
          }
        }
        scatterDelta[i][j] = newScatter - currentScatter;
      }
    }

    bestMetrics = new long[] {Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, Long.MIN_VALUE};

    // Initialize incremental variance state from current counters
    incrRegionSum = 0;
    incrRegionSumSq = 0;
    incrDiskSum = 0;
    incrDiskSumSq = 0;
    for (int nodeId : allAvailableNodeIds) {
      long r = (nodeId < regionCounter.length) ? regionCounter[nodeId] : 0;
      incrRegionSum += r;
      incrRegionSumSq += r * r;
      long d =
          ((nodeId < diskCounter.length) ? diskCounter[nodeId] : 0L)
              / MigratorLogHelper.DISK_SCALE_FACTOR;
      incrDiskSum += d;
      incrDiskSumSq += d * d;
    }

    dfsOptimalDistribution(0, 0, currentAssignment, 0);

    // Collect results
    Map<TConsensusGroupId, MigrateOption> batchResult = new HashMap<>();
    for (int i = 0; i < batchSize; i++) {
      batchResult.put(dfsRegionKeys.get(i), bestAssignment[i]);
    }
    return batchResult;
  }

  /** Apply a migration decision: update counters and combinationCounter. */
  private void applyMigration(TConsensusGroupId regionId, MigrateOption option) {
    if (!option.isMigration) {
      return;
    }
    applyMigrationWithReplicaMap(
        regionId, option.fromNodeId, option.toNodeId, replicaNodesIdMap.get(regionId));
  }

  /** Construct the final TRegionReplicaSet result from migration decisions. */
  private Map<TConsensusGroupId, TRegionReplicaSet> constructResult(
      Map<TConsensusGroupId, MigrateOption> result,
      List<TConsensusGroupId> regionKeys,
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap) {
    Map<TConsensusGroupId, TRegionReplicaSet> finalResult = new HashMap<>();
    for (TConsensusGroupId regionId : regionKeys) {
      TRegionReplicaSet currentRegionReplicaSet = new TRegionReplicaSet();
      currentRegionReplicaSet.setRegionId(regionId);
      MigrateOption option = result.get(regionId);
      List<Integer> replicaNodeIds = replicaNodesIdMap.get(regionId);
      for (Integer nodeId : replicaNodeIds) {
        if (option.isMigration && nodeId == option.fromNodeId) {
          currentRegionReplicaSet.addToDataNodeLocations(
              availableDataNodeMap.get(option.toNodeId).getLocation());
        } else {
          currentRegionReplicaSet.addToDataNodeLocations(
              availableDataNodeMap.get(nodeId).getLocation());
        }
      }
      finalResult.put(regionId, currentRegionReplicaSet);
    }
    return finalResult;
  }

  // ==================== DFS Search ====================

  private void dfsOptimalDistribution(
      int index, int currentScatter, MigrateOption[] currentAssignment, long migrateCost) {

    if (index == dfsRegionKeys.size()) {
      int n = allAvailableNodeIds.size();
      long diskVariance = n * incrDiskSumSq - incrDiskSum * incrDiskSum;
      long regionVariance = n * incrRegionSumSq - incrRegionSum * incrRegionSum;
      long[] currentMetrics =
          new long[] {diskVariance, regionVariance, migrateCost, currentScatter};

      if (compareMetrics(currentMetrics, bestMetrics) < 0) {
        System.arraycopy(currentAssignment, 0, bestAssignment, 0, currentAssignment.length);
        System.arraycopy(currentMetrics, 0, bestMetrics, 0, currentMetrics.length);
      }
      return;
    }
    // Try each candidate option for the region at the current index.
    TConsensusGroupId regionId = dfsRegionKeys.get(index);
    List<MigrateOption> options = allowedMigrateOptions.get(regionId);
    // Explore options in a promising order: disk load asc, then regionCount asc.
    Integer[] orderedIndices = new Integer[options.size()];
    for (int i = 0; i < options.size(); i++) {
      orderedIndices[i] = i;
    }
    Arrays.sort(
        orderedIndices,
        (i1, i2) -> {
          MigrateOption o1 = options.get(i1);
          MigrateOption o2 = options.get(i2);
          long d1 = o1.isMigration ? diskCounter[o1.toNodeId] : Long.MAX_VALUE;
          long d2 = o2.isMigration ? diskCounter[o2.toNodeId] : Long.MAX_VALUE;
          if (d1 != d2) {
            return Long.compare(d1, d2);
          }
          long r1 = o1.isMigration ? regionCounter[o1.toNodeId] : Integer.MAX_VALUE;
          long r2 = o2.isMigration ? regionCounter[o2.toNodeId] : Integer.MAX_VALUE;
          return Long.compare(r1, r2);
        });
    int beamLimit = Math.min(maxBeam, orderedIndices.length);
    for (int k = 0; k < beamLimit; k++) {
      int optionIndex = orderedIndices[k];
      MigrateOption option = options.get(optionIndex);
      currentAssignment[index] = option;
      long regionDisk =
          option.isMigration ? regionGroupStatisticsMap.get(regionId).getDiskUsage() : 0;

      // Incremental update counters and variance state
      long oldFromRegion = 0, oldToRegion = 0;
      long oldFromDiskMB = 0, oldToDiskMB = 0;
      long newFromDiskMB = 0, newToDiskMB = 0;
      if (option.isMigration) {
        oldFromRegion = regionCounter[option.fromNodeId];
        oldToRegion = regionCounter[option.toNodeId];
        regionCounter[option.fromNodeId]--;
        regionCounter[option.toNodeId]++;
        databaseRegionCounter[option.fromNodeId]--;
        databaseRegionCounter[option.toNodeId]++;
        // Region variance incremental: from node loses 1, to node gains 1
        incrRegionSum += 0; // net change is 0 (one -1 and one +1)
        incrRegionSumSq += -2L * oldFromRegion + 1 + 2L * oldToRegion + 1;
        // = (oldFrom-1)² - oldFrom² + (oldTo+1)² - oldTo²

        oldFromDiskMB = diskCounter[option.fromNodeId] / MigratorLogHelper.DISK_SCALE_FACTOR;
        oldToDiskMB = diskCounter[option.toNodeId] / MigratorLogHelper.DISK_SCALE_FACTOR;
        diskCounter[option.fromNodeId] -= regionDisk;
        diskCounter[option.toNodeId] += regionDisk;
        newFromDiskMB = diskCounter[option.fromNodeId] / MigratorLogHelper.DISK_SCALE_FACTOR;
        newToDiskMB = diskCounter[option.toNodeId] / MigratorLogHelper.DISK_SCALE_FACTOR;
        incrDiskSum += (newFromDiskMB - oldFromDiskMB) + (newToDiskMB - oldToDiskMB);
        incrDiskSumSq +=
            newFromDiskMB * newFromDiskMB
                - oldFromDiskMB * oldFromDiskMB
                + newToDiskMB * newToDiskMB
                - oldToDiskMB * oldToDiskMB;
      }

      // Update scatter
      int newScatter = currentScatter + scatterDelta[index][optionIndex];
      long nextMigrateCost = migrateCost + regionDisk;
      // Recurse
      dfsOptimalDistribution(index + 1, newScatter, currentAssignment, nextMigrateCost);

      // Backtrack: restore counters and variance state
      if (option.isMigration) {
        diskCounter[option.fromNodeId] += regionDisk;
        diskCounter[option.toNodeId] -= regionDisk;
        regionCounter[option.fromNodeId]++;
        regionCounter[option.toNodeId]--;
        databaseRegionCounter[option.fromNodeId]++;
        databaseRegionCounter[option.toNodeId]--;
        incrRegionSumSq -= -2L * oldFromRegion + 1 + 2L * oldToRegion + 1;
        incrDiskSum -= (newFromDiskMB - oldFromDiskMB) + (newToDiskMB - oldToDiskMB);
        incrDiskSumSq -=
            newFromDiskMB * newFromDiskMB
                - oldFromDiskMB * oldFromDiskMB
                + newToDiskMB * newToDiskMB
                - oldToDiskMB * oldToDiskMB;
      }
    }
  }

  // ==================== Evaluation ====================

  /**
   * Relative tolerance for Var(disk) comparison: if two solutions' Var(disk) differ by less than
   * this fraction of the better value, they are considered equal, and MigrateCost breaks the tie.
   * This prevents micro-improvements in Var(disk) from justifying extra migrations.
   */
  private static final double DISK_VARIANCE_EPSILON = 0.01;

  /**
   * Lexicographic comparison of two complete solution metrics. Priority order: 1. Var_region
   * (smaller is better) — absolute priority 2. Var_disk (smaller is better, with ε-tolerance) 3.
   * MigrateCost (smaller is better) 4. Scatter (larger is better)
   *
   * <p>Var(disk) uses a relative ε-tolerance: if the difference is within 1% of the smaller value,
   * the two are treated as equal and MigrateCost decides. This avoids excessive migrations for
   * negligible disk balance improvements.
   *
   * @return negative if newMetrics is better, positive if bestMetrics is better, 0 if equal
   */
  private int compareMetrics(long[] newMetrics, long[] bestMetrics) {
    // metrics: [diskVariance, regionVariance, migrateCost, scatter]
    // 1. Var_region: smaller is better (exact comparison)
    int cmp = Long.compare(newMetrics[1], bestMetrics[1]);
    if (cmp != 0) {
      return cmp;
    }
    // 2. Var_disk: smaller is better (with ε-tolerance)
    long minDiskVar = Math.min(newMetrics[0], bestMetrics[0]);
    long epsilon = (long) (minDiskVar * DISK_VARIANCE_EPSILON);
    if (Math.abs(newMetrics[0] - bestMetrics[0]) > epsilon) {
      return Long.compare(newMetrics[0], bestMetrics[0]);
    }
    // 3. MigrateCost: smaller is better
    cmp = Long.compare(newMetrics[2], bestMetrics[2]);
    if (cmp != 0) {
      return cmp;
    }
    // 4. Scatter: larger is better (reverse order)
    return Long.compare(bestMetrics[3], newMetrics[3]);
  }

  /**
   * Compute integer-proportional disk variance using the formula: n * Σx_i² - (Σx_i)².
   *
   * <p>Disk values are scaled from bytes to MB before computation to prevent long overflow.
   */
  private long computeDiskVariance() {
    if (allAvailableNodeIds.isEmpty()) {
      return 0L;
    }

    long sumValues = 0L;
    long sumSquares = 0L;
    int count = 0;
    for (Integer nodeId : allAvailableNodeIds) {
      long value = (nodeId < diskCounter.length) ? diskCounter[nodeId] : 0L;
      long scaledValue = value / MigratorLogHelper.DISK_SCALE_FACTOR;
      sumValues += scaledValue;
      sumSquares += scaledValue * scaledValue;
      count++;
    }

    return count * sumSquares - sumValues * sumValues;
  }

  /**
   * Compute integer-proportional region variance using the formula: n * Σx_i² - (Σx_i)².
   *
   * <p>Pure integer arithmetic ensures zero precision loss.
   */
  private long computeRegionVariance() {
    if (allAvailableNodeIds.isEmpty()) {
      return 0L;
    }

    long sumValues = 0L;
    long sumSquares = 0L;
    int count = 0;
    for (Integer nodeId : allAvailableNodeIds) {
      long value = (nodeId < regionCounter.length) ? regionCounter[nodeId] : 0;
      sumValues += value;
      sumSquares += value * value;
      count++;
    }

    return count * sumSquares - sumValues * sumValues;
  }

  // ==================== Utility ====================

  /** Format bytes as MB string for logging. */
  private static String toMB(long bytes) {
    return (bytes / MigratorLogHelper.DISK_SCALE_FACTOR) + "MB";
  }

  private String getNodeDistribution() {
    StringBuilder sb = new StringBuilder("{");
    boolean first = true;
    for (int nodeId : allAvailableNodeIds) {
      if (!first) {
        sb.append(", ");
      }
      sb.append("N")
          .append(nodeId)
          .append("=[region=")
          .append(regionCounter[nodeId])
          .append(", disk=")
          .append(toMB(diskCounter[nodeId]))
          .append("]");
      first = false;
    }
    sb.append("}");
    return sb.toString();
  }

  /**
   * Compute the number of migrations and total migration cost by comparing the original replica
   * distribution (replicaNodesIdMap) with the final distribution derived from the result map.
   *
   * <p>A single region may be migrated in Phase 1 and then swapped in Phase 2, but amendResult()
   * merges the path into one net migration record. The cost is computed by counting new replica
   * placements (nodes in after but not in before) and multiplying by the region's disk usage.
   *
   * @param result the migration decision map (null means no migrations yet)
   * @return long[]{migrations, costBytes}
   */
  private long[] computeMigrationsAndCost(Map<TConsensusGroupId, MigrateOption> result) {
    if (result == null) {
      return new long[] {0L, 0L};
    }
    long migrations = 0;
    long costBytes = 0;
    for (Map.Entry<TConsensusGroupId, MigrateOption> entry : result.entrySet()) {
      TConsensusGroupId regionId = entry.getKey();
      MigrateOption option = entry.getValue();
      List<Integer> beforeNodes = replicaNodesIdMap.get(regionId);
      if (beforeNodes == null || !option.isMigration) {
        continue;
      }
      // Compute after nodes: replace fromNodeId with toNodeId
      Set<Integer> beforeSet = new HashSet<>(beforeNodes);
      List<Integer> afterNodes = new ArrayList<>(beforeNodes);
      int idx = afterNodes.indexOf(option.fromNodeId);
      if (idx >= 0) {
        afterNodes.set(idx, option.toNodeId);
      }
      // Count new placements (in after but not in before)
      int addedCount = 0;
      for (int nodeId : afterNodes) {
        if (!beforeSet.contains(nodeId)) {
          addedCount++;
        }
      }
      migrations += addedCount;
      costBytes += addedCount * regionGroupStatisticsMap.get(regionId).getDiskUsage();
    }
    return new long[] {migrations, costBytes};
  }

  /**
   * Log a unified summary at a checkpoint. Converts internal array-based counters to Maps and
   * delegates to {@link MigratorLogHelper#logSummary}.
   */
  private void logSummary(String label, Map<TConsensusGroupId, MigrateOption> result) {

    long[] mc = computeMigrationsAndCost(result);
    long migrations = mc[0];
    long costBytes = mc[1];

    // Convert array-based counters to Maps for the shared helper
    Map<Integer, Integer> regionCounterMap = new HashMap<>();
    Map<Integer, Long> diskCounterMap = new HashMap<>();
    for (int nodeId : allAvailableNodeIds) {
      regionCounterMap.put(nodeId, regionCounter[nodeId]);
      diskCounterMap.put(nodeId, diskCounter[nodeId]);
    }

    MigratorLogHelper.logSummary(
        LOGGER,
        "LoadBalance",
        label,
        allAvailableNodeIds,
        regionCounterMap,
        diskCounterMap,
        migrations,
        costBytes);
  }
}
