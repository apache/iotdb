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

public class GreedyCopySetRegionGroupMigrator implements IRegionGroupMigrator {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(GreedyCopySetRegionGroupMigrator.class);

  private int replicationFactor;
  // Sorted available DataNodeIds
  private int[] dataNodeIds;
  // The number of allocated Regions in each DataNode
  private int[] regionCounter;
  // The number of disk usage in each DataNode
  private long[] diskCounter;
  // The number of allocated Regions in each DataNode within the same Database
  private int[] databaseRegionCounter;
  // The number of 2-Region combinations in current cluster
  private int[][] combinationCounter;

  // DataNodeId sets for quick lookup
  private Set<Integer> availableFromDataNodeSet;
  private Set<Integer> availableToDataNodeSet;
  // For each region, the allowed migration options
  private Map<TConsensusGroupId, List<MigrateOption>> allowedMigrateOptions;
  // Statistics of RegionGroups
  private Map<TConsensusGroupId, RegionGroupStatistics> regionGroupStatisticsMap;
  // dfs batch size
  private int BATCH_SIZE = 8;
  // A list of regions that need to be migrated.
  private List<TConsensusGroupId> dfsRegionKeys;
  // Buffer holding best assignment arrays.
  private MigrateOption[] bestAssignment;
  // An int array holding the best metrics found so far: [maxGlobalLoad, maxDatabaseLoad,
  // scatterValue].
  private long[] bestMetrics;
  // Pre-calculation, scatterDelta[i][j] means the scatter difference between region i and the
  // option j
  private int[][] scatterDelta;

  private Map<TConsensusGroupId, List<Integer>> replicaNodesIdMap;

  // ---------------- Multi-objective weighted-sum scoring (with approximate normalization)
  // ----------------
  // Weights (can be tuned later or exposed via configuration)
  // Design principle: balance improvement should be valued more than migration cost
  // When diskVariance improves by X, it should be worth more than migrating X disk units
  private double weightDiskVariance = 4;
  private double weightRegionVariance = 3;
  private double weightMigrateCost = 2;
  private double weightScatter = 1; // note: scatter is maximized, so will be subtracted in score

  // Normalization scales computed per DFS batch
  private double scaleDiskVariance = 1.0;
  private double scaleRegionVariance = 1.0;
  private double scaleMigrateCost = 1.0;
  private double scaleScatter = 1.0;
  private long scatterMinBound = 0L; // for min-max normalization of scatter
  private long scatterMaxBound = 1L;

  // Min-Max bounds per batch for normalization
  private long diskVarMinBound = 0L;
  private long diskVarMaxBound = 1L;
  private long regionVarMinBound = 0L;
  private long regionVarMaxBound = 1L;
  private long migrateMinBound = 0L;
  private long migrateMaxBound = 1L;

  // Best scalar score found so far in current batch (lower is better)
  private double bestScore;
  // A supportive pruning threshold derived from the best plan's disk variance (max-min)
  private long bestDiskVarianceThreshold;

  private static class MigrateOption {
    protected final Boolean isMigration;
    protected final int fromNodeId;
    protected final int toNodeId;

    public MigrateOption(Boolean isMigration, int fromNodeId, int toNodeId) {
      this.isMigration = isMigration;
      this.fromNodeId = fromNodeId;
      this.toNodeId = toNodeId;
    }
  }

  private void prepare(
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap,
      List<TRegionReplicaSet> allocatedRegionGroups,
      List<TRegionReplicaSet> databaseAllocatedRegionGroups,
      List<Integer> targetNodeIds) {

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

    List<Integer> dataNodeIdList = new ArrayList<>(availableDataNodeMap.keySet());
    // Print disk usage for all nodes before sorting
    LOGGER.info("[LoadBalance] Disk usage for all nodes before sorting:");
    for (Integer nodeId : dataNodeIdList) {
      LOGGER.info(
          "[LoadBalance] Node {}: diskUsage={}, regionCount={}",
          nodeId,
          diskCounter[nodeId],
          regionCounter[nodeId]);
    }
    // Sort by disk usage first, then by region count if disk usage is equal
    dataNodeIdList.sort(
        Comparator.comparingLong((Integer node) -> diskCounter[node])
            .thenComparingInt(node -> regionCounter[node]));
    // Print disk usage for all nodes after sorting
    LOGGER.info("[LoadBalance] Node order after sorting by disk usage (ascending):");
    for (int i = 0; i < dataNodeIdList.size(); i++) {
      Integer nodeId = dataNodeIdList.get(i);
      LOGGER.info(
          "[LoadBalance] Rank {}: Node {} (diskUsage={}, regionCount={})",
          i,
          nodeId,
          diskCounter[nodeId],
          regionCounter[nodeId]);
    }
    availableToDataNodeSet = new HashSet<>();
    if (targetNodeIds != null && !targetNodeIds.isEmpty()) {
      // Use specified target nodes
      for (Integer targetNodeId : targetNodeIds) {
        if (availableDataNodeMap.containsKey(targetNodeId)) {
          availableToDataNodeSet.add(targetNodeId);
          LOGGER.info(
              "[LoadBalance] Using specified target node: Node {} (diskUsage={}, regionCount={})",
              targetNodeId,
              diskCounter[targetNodeId],
              regionCounter[targetNodeId]);
        } else {
          LOGGER.warn(
              "[LoadBalance] Specified target node {} is not available, skipping", targetNodeId);
        }
      }
    }
    // If no target nodes were specified or all specified nodes are invalid, auto-select
    if (availableToDataNodeSet.isEmpty() && !dataNodeIdList.isEmpty()) {
      availableToDataNodeSet.add(dataNodeIdList.get(0));
      LOGGER.info(
          "[LoadBalance] Auto-selected target node: Node {} (diskUsage={}, regionCount={})",
          dataNodeIdList.get(0),
          diskCounter[dataNodeIdList.get(0)],
          regionCounter[dataNodeIdList.get(0)]);
    }
    availableFromDataNodeSet = new HashSet<>();
    for (int i = 0; i < dataNodeIdList.size(); i++) {
      Integer nodeId = dataNodeIdList.get(i);
      if (!availableToDataNodeSet.contains(nodeId)) {
        availableFromDataNodeSet.add(nodeId);
        LOGGER.info(
            "[LoadBalance] Selected as source node: Node {} (diskUsage={}, regionCount={})",
            nodeId,
            diskCounter[nodeId],
            regionCounter[nodeId]);
      }
    }
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
    // 1. prepare: compute regionCounter, databaseRegionCounter, and combinationCounter
    prepare(availableDataNodeMap, allocatedRegionGroups, Collections.emptyList(), targetNodeIds);

    // 2. Build allowed migration set for each region.
    // No migration: 1 option.
    // Migrate once: Select a source replica from ∈ ReplicaSet and a target node to ∈ Nodes \
    // ReplicaSet, for a total of r * (n - r) options.
    // So the total options are r * (n - r) + 1.
    List<TConsensusGroupId> regionKeys =
        allocatedRegionGroups.stream()
            .map(TRegionReplicaSet::getRegionId)
            .collect(Collectors.toList());
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

    // Sort regionKeys by the maximum current disk load among its involved replica nodes (desc)
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
          return Long.compare(maxB, maxA);
        });

    // 3. Batch DFS
    Map<TConsensusGroupId, MigrateOption> result = new HashMap<>();

    for (int start = 0; start < regionKeys.size(); start += BATCH_SIZE) {
      dfsRegionKeys = regionKeys.subList(start, Math.min(start + BATCH_SIZE, regionKeys.size()));
      int batchSize = dfsRegionKeys.size();

      // currentAssignment holds the candidate option chosen for the region at that index
      MigrateOption[] currentAssignment = new MigrateOption[batchSize];
      // Initialize buffer
      bestAssignment = new MigrateOption[batchSize];
      Arrays.fill(bestAssignment, new MigrateOption(false, -1, -1));
      // Initialize batch-level normalization scales and best score
      long initialDiskVariance = computeDiskVariance();
      long initialRegionVariance = computeRegionVariance();
      // Estimate an upper bound of migrate cost as the sum of all regions' disk usage within the
      // batch
      long migrateCostUpperBound = 0L;
      for (int i = 0; i < batchSize; i++) {
        migrateCostUpperBound += regionGroupStatisticsMap.get(dfsRegionKeys.get(i)).getDiskUsage();
      }
      // Min-Max bounds:
      // - Disk variance and region variance use max-min definition; conservative upper bound by
      // adding 2*sum(batchDisk) and 2*batchSize respectively
      diskVarMinBound = 0L;
      diskVarMaxBound = initialDiskVariance + 2L * migrateCostUpperBound;
      regionVarMinBound = 0L;
      regionVarMaxBound = initialRegionVariance + 2L * batchSize;
      // For migrate cost, use a more reasonable upper bound that relates to the potential
      // balance improvement. If we can improve diskVariance by X, migrating X units should
      // be considered acceptable. So we scale migrateCost relative to initialDiskVariance.
      // This ensures that migration cost is normalized in a way that's comparable to balance
      // improvement.
      migrateMinBound = 0L;
      // Use max of: batch cost upper bound, or a fraction of initial disk variance
      // This makes migrateCost normalization more comparable to diskVariance improvement
      migrateMaxBound = Math.max(migrateCostUpperBound, Math.max(1L, initialDiskVariance));
      // Scales (1 / (max - min))
      scaleDiskVariance = 1.0 / Math.max(1L, (diskVarMaxBound - diskVarMinBound));
      scaleRegionVariance = 1.0 / Math.max(1L, (regionVarMaxBound - regionVarMinBound));
      scaleMigrateCost = 1.0 / Math.max(1L, (migrateMaxBound - migrateMinBound));
      // Precompute scatter bounds (min and max possible deltas over the batch)
      long tmpScatterMin = 0L;
      long tmpScatterMax = 0L;
      // We'll fill scatterDelta first, then compute per-index min/max across options below

      // pre-calculate scatterDelta
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
        // compute min/max for this index
        int minAtI = Integer.MAX_VALUE;
        int maxAtI = Integer.MIN_VALUE;
        for (int v : scatterDelta[i]) {
          if (v < minAtI) {
            minAtI = v;
          }
          if (v > maxAtI) {
            maxAtI = v;
          }
        }
        tmpScatterMin += minAtI;
        tmpScatterMax += maxAtI;
      }

      scatterMinBound = tmpScatterMin;
      scatterMaxBound = Math.max(tmpScatterMin + 1L, tmpScatterMax); // ensure range>=1
      scaleScatter = 1.0 / Math.max(1L, (scatterMaxBound - scatterMinBound));

      bestScore = Double.POSITIVE_INFINITY;
      bestDiskVarianceThreshold = Long.MAX_VALUE;
      // Initialize bestMetrics with maximum values to represent "no solution found yet"
      bestMetrics = new long[] {Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE};

      dfsOptimalDistribution(0, 0, currentAssignment, 0);

      for (int i = 0; i < batchSize; i++) {
        TConsensusGroupId regionId = dfsRegionKeys.get(i);
        result.put(regionId, bestAssignment[i]);

        // update combinationCounter
        MigrateOption option = bestAssignment[i];
        if (option.isMigration) {
          List<Integer> replicaNodeIds = replicaNodesIdMap.get(regionId);
          for (Integer replicaNodeId : replicaNodeIds) {
            if (replicaNodeId == option.fromNodeId) {
              continue;
            }
            combinationCounter[replicaNodeId][option.fromNodeId]--;
            combinationCounter[option.fromNodeId][replicaNodeId]--;
            combinationCounter[replicaNodeId][option.toNodeId]++;
            combinationCounter[option.toNodeId][replicaNodeId]++;
          }
          long regionDisk = regionGroupStatisticsMap.get(regionId).getDiskUsage();
          regionCounter[option.fromNodeId]--;
          regionCounter[option.toNodeId]++;
          databaseRegionCounter[option.fromNodeId]--;
          databaseRegionCounter[option.toNodeId]++;
          diskCounter[option.fromNodeId] -= regionDisk;
          diskCounter[option.toNodeId] += regionDisk;
        }
      }
    }
    // 4. Construct the result
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

  private void dfsOptimalDistribution(
      int index, int currentScatter, MigrateOption[] currentAssignment, long migrateCost) {
    long[] currentMetrics = evaluateCurrentAssignment(currentScatter, migrateCost);
    double currentScore = computeScore(currentMetrics);

    if (index == dfsRegionKeys.size()) {
      // Log the complete migration plan and scores
      //      logMigrationPlanAndScores(currentAssignment, currentMetrics, currentScore, bestScore);

      if (currentScore < bestScore) {
        bestScore = currentScore;
        bestDiskVarianceThreshold = currentMetrics[0];
        System.arraycopy(currentAssignment, 0, bestAssignment, 0, currentAssignment.length);
        System.arraycopy(currentMetrics, 0, bestMetrics, 0, currentMetrics.length);
        //        LOGGER.info("✓ Selected this migration plan (new best score: {})", currentScore);
      } else {
        //        LOGGER.info("✗ Did not select this migration plan (current score: {} >= best
        // score: {})", currentScore, bestScore);
      }
      return;
    }
    // Try each candidate option for the region at the current index.
    TConsensusGroupId regionId = dfsRegionKeys.get(index);
    List<MigrateOption> options = allowedMigrateOptions.get(regionId);
    // Explore options in a promising order: estimated next max disk load asc, then scatterDelta
    // asc.
    Integer[] orderedIndices = new Integer[options.size()];
    for (int i = 0; i < options.size(); i++) {
      orderedIndices[i] = i;
    }
    final long regionDiskUsage = regionGroupStatisticsMap.get(regionId).getDiskUsage();
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
    int beamLimit = Math.min(BATCH_SIZE, orderedIndices.length);
    for (int k = 0; k < beamLimit; k++) {
      int optionIndex = orderedIndices[k];
      MigrateOption option = options.get(optionIndex);
      // prune by estimated next max disk load upper bound
      long estimatedNextMax = estimateNextMaxDiskLoad(option, regionDiskUsage);
      if (estimatedNextMax > bestDiskVarianceThreshold) {
        //        continue;
      }
      currentAssignment[index] = option;
      long regionDisk =
          option.isMigration ? regionGroupStatisticsMap.get(regionId).getDiskUsage() : 0;
      // Update counters
      if (option.isMigration) {
        regionCounter[option.fromNodeId]--;
        regionCounter[option.toNodeId]++;
        databaseRegionCounter[option.fromNodeId]--;
        databaseRegionCounter[option.toNodeId]++;
        diskCounter[option.fromNodeId] -= regionDisk;
        diskCounter[option.toNodeId] += regionDisk;
      }
      // Update scatter
      int newScatter = currentScatter + scatterDelta[index][optionIndex];
      long nextMigrateCost = migrateCost + regionDisk;
      // Recurse
      dfsOptimalDistribution(index + 1, newScatter, currentAssignment, nextMigrateCost);
      // Backtrack: restore counters
      if (option.isMigration) {
        regionCounter[option.fromNodeId]++;
        regionCounter[option.toNodeId]--;
        databaseRegionCounter[option.fromNodeId]++;
        databaseRegionCounter[option.toNodeId]--;
        diskCounter[option.fromNodeId] += regionDisk;
        diskCounter[option.toNodeId] -= regionDisk;
      }
    }
  }

  private long[] evaluateCurrentAssignment(int currentScatter, long migrateCost) {
    long diskVariance = computeDiskVariance();
    long regionVariance = computeRegionVariance();
    return new long[] {diskVariance, regionVariance, migrateCost, currentScatter};
  }

  private double computeScore(long[] metrics) {
    // metrics: [diskVariance, regionVariance, migrateCost, currentScatter]
    // Min-Max normalization: (X - X_min) / (X_max - X_min)
    double normDisk =
        (Math.min(diskVarMaxBound, Math.max(diskVarMinBound, metrics[0])) - diskVarMinBound)
            * scaleDiskVariance;
    double normRegion =
        (Math.min(regionVarMaxBound, Math.max(regionVarMinBound, metrics[1])) - regionVarMinBound)
            * scaleRegionVariance;
    double normMigrate =
        (Math.min(migrateMaxBound, Math.max(migrateMinBound, metrics[2])) - migrateMinBound)
            * scaleMigrateCost;
    // scatter is to be maximized, do min-max normalization over estimated bounds
    double normScatter =
        (Math.min(scatterMaxBound, Math.max(scatterMinBound, metrics[3])) - scatterMinBound)
            * scaleScatter;
    // Weighted sum: minimize (disk + region + migrate) - scatter
    return weightDiskVariance * normDisk
        + weightRegionVariance * normRegion
        + weightMigrateCost * normMigrate
        - weightScatter * normScatter;
  }

  private void logMigrationPlanAndScores(
      MigrateOption[] assignment, long[] metrics, double totalScore, double bestScore) {
    // Build migration plan description
    StringBuilder planBuilder = new StringBuilder();
    int migrationCount = 0;
    for (int i = 0; i < assignment.length; i++) {
      MigrateOption option = assignment[i];
      if (option.isMigration) {
        if (migrationCount > 0) {
          planBuilder.append(", ");
        }
        planBuilder
            .append("Region")
            .append(dfsRegionKeys.get(i).getId())
            .append(": ")
            .append(option.fromNodeId)
            .append("->")
            .append(option.toNodeId);
        migrationCount++;
      }
    }

    if (dfsRegionKeys.get(0).getId() >= 8) {
      return;
    }

    String plan = migrationCount == 0 ? "No migration" : planBuilder.toString();

    // Calculate normalized and weighted scores
    double normDisk =
        (Math.min(diskVarMaxBound, Math.max(diskVarMinBound, metrics[0])) - diskVarMinBound)
            * scaleDiskVariance;
    double normRegion =
        (Math.min(regionVarMaxBound, Math.max(regionVarMinBound, metrics[1])) - regionVarMinBound)
            * scaleRegionVariance;
    double normMigrate =
        (Math.min(migrateMaxBound, Math.max(migrateMinBound, metrics[2])) - migrateMinBound)
            * scaleMigrateCost;
    double normScatter =
        (Math.min(scatterMaxBound, Math.max(scatterMinBound, metrics[3])) - scatterMinBound)
            * scaleScatter;

    double weightedDisk = weightDiskVariance * normDisk;
    double weightedRegion = weightRegionVariance * normRegion;
    double weightedMigrate = weightMigrateCost * normMigrate;
    double weightedScatter = -weightScatter * normScatter; // Note: scatter is subtracted

    LOGGER.info(
        "=== Migration Plan Evaluation ===\n"
            + "  Migration plan: {}\n"
            + "  Raw metrics: diskVariance={}, regionVariance={}, migrateCost={}, scatter={}\n"
            + "  Normalized values: normDisk={}, normRegion={}, normMigrate={}, normScatter={}\n"
            + "  Weighted scores: disk={} (weight={}), region={} (weight={}), migrate={} (weight={}), scatter={} (weight={})\n"
            + "  Total score: {} (current best: {})",
        plan,
        metrics[0],
        metrics[1],
        metrics[2],
        metrics[3],
        String.format("%.4f", normDisk),
        String.format("%.4f", normRegion),
        String.format("%.4f", normMigrate),
        String.format("%.4f", normScatter),
        String.format("%.4f", weightedDisk),
        weightDiskVariance,
        String.format("%.4f", weightedRegion),
        weightRegionVariance,
        String.format("%.4f", weightedMigrate),
        weightMigrateCost,
        String.format("%.4f", weightedScatter),
        weightScatter,
        String.format("%.4f", totalScore),
        bestScore == Double.POSITIVE_INFINITY ? "∞" : String.format("%.4f", bestScore));
  }

  private long computeDiskVariance() {
    // Consider all available nodes (both from and to sets), not just nodes with usage > 0
    // This ensures we correctly calculate variance even when some nodes have 0 usage
    Set<Integer> allAvailableNodes = new HashSet<>();
    allAvailableNodes.addAll(availableFromDataNodeSet);
    allAvailableNodes.addAll(availableToDataNodeSet);

    if (allAvailableNodes.isEmpty()) {
      return 0L;
    }

    // Calculate mean
    double sum = 0.0;
    int count = 0;
    for (Integer nodeId : allAvailableNodes) {
      long value = (nodeId < diskCounter.length) ? diskCounter[nodeId] : 0L;
      sum += value;
      count++;
    }
    if (count == 0) {
      return 0L;
    }
    double mean = sum / count;

    // Calculate variance: Σ(xi - mean)² / n
    double sq = 0.0;
    for (Integer nodeId : allAvailableNodes) {
      long value = (nodeId < diskCounter.length) ? diskCounter[nodeId] : 0L;
      double diff = value - mean;
      sq += diff * diff;
    }
    double variance = sq / count;
    return Math.round(variance);
  }

  private long computeRegionVariance() {
    // Consider all available nodes (both from and to sets), not just nodes with regions > 0
    // This ensures we correctly calculate variance even when some nodes have 0 regions
    Set<Integer> allAvailableNodes = new HashSet<>();
    allAvailableNodes.addAll(availableFromDataNodeSet);
    allAvailableNodes.addAll(availableToDataNodeSet);

    if (allAvailableNodes.isEmpty()) {
      return 0L;
    }

    // Calculate mean
    double sum = 0.0;
    int count = 0;
    for (Integer nodeId : allAvailableNodes) {
      int value = (nodeId < regionCounter.length) ? regionCounter[nodeId] : 0;
      sum += value;
      count++;
    }
    if (count == 0) {
      return 0L;
    }
    double mean = sum / count;

    // Calculate variance: Σ(xi - mean)² / n
    double sq = 0.0;
    for (Integer nodeId : allAvailableNodes) {
      int value = (nodeId < regionCounter.length) ? regionCounter[nodeId] : 0;
      double diff = value - mean;
      sq += diff * diff;
    }
    double variance = sq / count;
    return Math.round(variance);
  }

  private long estimateNextMaxDiskLoad(MigrateOption option, long regionDiskUsage) {
    long currentMax = Arrays.stream(diskCounter).max().orElse(0L);
    if (!option.isMigration) {
      return currentMax;
    }
    long toAfter = diskCounter[option.toNodeId] + regionDiskUsage;
    // Conservative upper bound: new maximum cannot be less than max(currentMax, toAfter)
    return Math.max(currentMax, toAfter);
  }
}
