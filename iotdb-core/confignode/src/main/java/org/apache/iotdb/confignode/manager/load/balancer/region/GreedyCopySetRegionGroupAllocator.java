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

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Map.Entry.comparingByValue;

/** Allocate Region through Greedy and CopySet Algorithm. */
public class GreedyCopySetRegionGroupAllocator implements IRegionGroupAllocator {

  private static final SecureRandom RANDOM = new SecureRandom();
  private static final int GCR_MAX_OPTIMAL_PLAN_NUM = 10;

  private int replicationFactor;
  // Sorted available DataNodeIds
  private int[] dataNodeIds;
  // The number of allocated Regions in each DataNode
  private int[] regionCounter;
  // The number of allocated Regions in each DataNode within the same Database
  private int[] databaseRegionCounter;
  // The number of 2-Region combinations in current cluster
  private int[][] combinationCounter;
  // The initial load for each database on each datanode
  private Map<String, int[]> initialDbLoad;

  // First Key: the sum of Regions at the DataNodes in the allocation result is minimal
  private int optimalRegionSum;
  // Second Key: the sum of Regions at the DataNodes within the same Database
  // in the allocation result is minimal
  private int optimalDatabaseRegionSum;
  // Third Key: the sum of overlapped 2-Region combination Regions with
  // other allocated RegionGroups is minimal
  private int optimalCombinationSum;
  private List<int[]> optimalReplicaSets;

  // Pre-calculation, scatterDelta[i][j] means the scatter increment between region i and the old
  // replica set when region i is placed on node j
  private int[][] scatterDelta;
  // For each region, the allowed candidate destination node IDs.
  private Map<TConsensusGroupId, List<Integer>> allowedCandidatesMap;
  // A list of regions that need to be migrated.
  private List<TConsensusGroupId> dfsRegionKeys;
  // A mapping from each region identifier to its corresponding database name.
  private Map<TConsensusGroupId, String> regionDatabaseMap;
  // Buffer holding best assignment arrays.
  private int[] bestAssignment;
  // An int array holding the best metrics found so far: [maxGlobalLoad, maxDatabaseLoad,
  // scatterValue].
  private int[] bestMetrics;
  // dfsRemoveNodeReplica batch size
  private static final int BATCH_SIZE = 12;

  private static class DataNodeEntry {

    // First key: the number of Regions in the DataNode, ascending order
    private final int regionCount;
    // Second key: the number of Regions in the DataNode within the same Database, ascending order
    private final int databaseRegionCount;
    // Third key: the scatter width of the DataNode, ascending order
    private final int scatterWidth;
    // Forth key: a random weight, ascending order
    private final int randomWeight;

    public DataNodeEntry(int databaseRegionCount, int regionCount, int scatterWidth) {
      this.databaseRegionCount = databaseRegionCount;
      this.regionCount = regionCount;
      this.scatterWidth = scatterWidth;
      this.randomWeight = RANDOM.nextInt();
    }

    public int compare(DataNodeEntry e) {
      return regionCount != e.regionCount
          ? Integer.compare(regionCount, e.regionCount)
          : databaseRegionCount != e.databaseRegionCount
              ? Integer.compare(databaseRegionCount, e.databaseRegionCount)
              : scatterWidth != e.scatterWidth
                  ? Integer.compare(scatterWidth, e.scatterWidth)
                  : Integer.compare(randomWeight, e.randomWeight);
    }
  }

  public GreedyCopySetRegionGroupAllocator() {
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
    try {
      this.replicationFactor = replicationFactor;
      prepare(availableDataNodeMap, allocatedRegionGroups, databaseAllocatedRegionGroups);
      dfsAllocateReplica(-1, 0, new int[replicationFactor], 0, 0);

      // Randomly pick one optimal plan as result
      Collections.shuffle(optimalReplicaSets);
      int[] optimalReplicaSet = optimalReplicaSets.get(0);
      TRegionReplicaSet result = new TRegionReplicaSet();
      result.setRegionId(consensusGroupId);
      for (int i = 0; i < replicationFactor; i++) {
        result.addToDataNodeLocations(availableDataNodeMap.get(optimalReplicaSet[i]).getLocation());
      }

      return result;
    } finally {
      clear();
    }
  }

  @Override
  public Map<TConsensusGroupId, TDataNodeConfiguration> removeNodeReplicaSelect(
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap,
      Map<Integer, Double> freeDiskSpaceMap,
      List<TRegionReplicaSet> allocatedRegionGroups,
      Map<TConsensusGroupId, String> regionDatabaseMap,
      Map<String, List<TRegionReplicaSet>> databaseAllocatedRegionGroupMap,
      Map<TConsensusGroupId, TRegionReplicaSet> remainReplicasMap) {
    try {
      // 1. prepare: compute regionCounter, databaseRegionCounter, and combinationCounter

      prepare(availableDataNodeMap, allocatedRegionGroups, Collections.emptyList());
      computeInitialDbLoad(availableDataNodeMap, databaseAllocatedRegionGroupMap);

      // 2. Build allowed candidate set for each region that needs to be migrated.
      // For each region in remainReplicasMap, the candidate destination nodes are all nodes in
      // availableDataNodeMap
      // excluding those already in the remain replica set.
      List<TConsensusGroupId> regionKeys = new ArrayList<>(remainReplicasMap.keySet());
      allowedCandidatesMap = new HashMap<>();
      this.regionDatabaseMap = regionDatabaseMap;
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
                .sorted(
                    (a, b) -> {
                      int cmp = Integer.compare(regionCounter[a], regionCounter[b]);
                      return (cmp != 0)
                          ? cmp
                          : Integer.compare(databaseRegionCounter[a], databaseRegionCounter[b]);
                    })
                .collect(Collectors.toList());
        Collections.shuffle(candidates);

        // Sort candidates in ascending order of current global load (regionCounter)
        allowedCandidatesMap.put(regionId, candidates);
      }

      // Optionally, sort regionKeys by the size of its candidate list (smaller candidate sets
      // first)
      regionKeys.sort(Comparator.comparingInt(id -> allowedCandidatesMap.get(id).size()));

      // 3. Batch DFS
      Map<TConsensusGroupId, TDataNodeConfiguration> result = new HashMap<>();

      for (int start = 0; start < regionKeys.size(); start += BATCH_SIZE) {
        dfsRegionKeys = regionKeys.subList(start, Math.min(start + BATCH_SIZE, regionKeys.size()));
        int batchSize = dfsRegionKeys.size();

        // Initialize buffer
        bestAssignment = new int[batchSize];
        // bestMetrics holds the best found metrics: [maxGlobalLoad, maxDatabaseLoad, scatterValue].
        bestMetrics = new int[] {Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE};
        // currentAssignment holds the candidate nodeId chosen for the region at that index
        int[] currentAssignment = new int[batchSize];
        // additionalLoad holds the number of extra regions assigned to each node in this migration
        // solution.
        int[] additionalLoad = new int[regionCounter.length];

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

        int currentMaxGlobalLoad = 0;
        for (int nodeId = 0; nodeId < regionCounter.length; nodeId++) {
          currentMaxGlobalLoad = Math.max(currentMaxGlobalLoad, regionCounter[nodeId]);
        }

        dfsRemoveNodeReplica(0, currentMaxGlobalLoad, 0, currentAssignment, additionalLoad);

        if (bestMetrics[0] == Integer.MAX_VALUE) {
          // This should not happen if there is at least one valid assignment
          return Collections.emptyMap();
        }

        for (int i = 0; i < batchSize; i++) {
          TConsensusGroupId regionId = dfsRegionKeys.get(i);
          int chosenNodeId = bestAssignment[i];
          result.put(regionId, availableDataNodeMap.get(chosenNodeId));

          regionCounter[chosenNodeId]++;
          String db = regionDatabaseMap.get(regionId);
          if (db != null) {
            int[] dbLoad = initialDbLoad.computeIfAbsent(db, k -> new int[regionCounter.length]);
            dbLoad[chosenNodeId]++;
          }
          for (TDataNodeLocation loc : remainReplicasMap.get(regionId).getDataNodeLocations()) {
            combinationCounter[chosenNodeId][loc.getDataNodeId()]++;
            combinationCounter[loc.getDataNodeId()][chosenNodeId]++;
          }
        }
      }
      return result;
    } finally {
      // Clear any temporary state to avoid impacting subsequent calls
      clear();
    }
  }

  /**
   * DFS method that searches for migration target assignments.
   *
   * <p>It enumerates all possible assignments (one candidate for each region) and collects
   * candidate solutions in the optimalAssignments buffer. The evaluation metrics for each complete
   * assignment (i.e. when index == regionKeys.size()) are:
   *
   * <p>1. Max global load: the maximum over nodes of (regionCounter[node] + additionalLoad[node])
   * 2. Max database load: the maximum over nodes of (databaseRegionCounter[node] +
   * additionalLoad[node]) 3. Scatter value: computed per region, summing the combinationCounter for
   * every pair in the complete replica set. The complete replica set for a region includes nodes in
   * its remain replica set plus the newly assigned node.
   *
   * <p>The candidates are compared lexicographically (first by global load, then by database load,
   * then by scatter). When a better candidate is found, the optimalAssignments buffer is cleared
   * and updated; if the new candidate matches the best found metrics, it is added to the buffer.
   *
   * <p>DFS search is pruned if the optimalAssignments buffer reaches CAPACITY.
   *
   * @param index Current DFS level, corresponding to regionKeys.get(index)
   * @param currentMaxGlobalLoad The maximum global load across all data nodes.
   * @param currentScatter The scatter value for the complete assignment.
   * @param currentAssignment Current partial assignment; its length equals the number of regions.
   * @param additionalLoad Extra load currently assigned to each node.
   */
  private void dfsRemoveNodeReplica(
      int index,
      int currentMaxGlobalLoad,
      int currentScatter,
      int[] currentAssignment,
      int[] additionalLoad) {
    // Compute the maximum global load and maximum database load among all nodes that received
    // additional load.
    int[] currentMetrics = getCurrentMetrics(additionalLoad, currentScatter, currentAssignment);
    // Lexicographically compare currentMetrics with bestMetrics.
    // If currentMetrics is better than bestMetrics, update bestMetrics and clear the candidate
    // buffer.
    boolean isBetter = false;
    boolean isEqual = true;
    for (int i = 0; i < 3; i++) {
      if (currentMetrics[i] < bestMetrics[i]) {
        isBetter = true;
        isEqual = false;
        break;
      } else if (currentMetrics[i] > bestMetrics[i]) {
        isEqual = false;
        break;
      }
    }
    if (!isBetter && !isEqual) {
      return;
    }

    if (index == dfsRegionKeys.size()) {
      if (isBetter) {
        bestMetrics[0] = currentMetrics[0];
        bestMetrics[1] = currentMetrics[1];
        bestMetrics[2] = currentMetrics[2];
        System.arraycopy(currentAssignment, 0, bestAssignment, 0, dfsRegionKeys.size());
      }
      return;
    }

    // Process the region at the current index.
    TConsensusGroupId regionId = dfsRegionKeys.get(index);
    List<Integer> candidates = allowedCandidatesMap.get(regionId);
    for (Integer candidate : candidates) {
      currentAssignment[index] = candidate;
      currentScatter += scatterDelta[index][currentAssignment[index]];
      additionalLoad[candidate]++;
      int nextMaxGlobalLoad =
          Math.max(currentMaxGlobalLoad, regionCounter[candidate] + additionalLoad[candidate]);

      dfsRemoveNodeReplica(
          index + 1, nextMaxGlobalLoad, currentScatter, currentAssignment, additionalLoad);
      // Backtrack
      additionalLoad[candidate]--;
      currentScatter -= scatterDelta[index][currentAssignment[index]];
    }
  }

  /**
   * Computes the squared sum of the maximum load for each database.
   *
   * <p>For each database, this method calculates the maximum load on any data node by summing the
   * initial load (from {@code initialDbLoad}) with the additional load assigned during migration
   * (accumulated in {@code currentAssignment}), and then squares this maximum load. Finally, it
   * returns the sum of these squared maximum loads across all databases.
   *
   * @param currentAssignment an array where each element is the nodeId assigned for the
   *     corresponding region in {@code regionKeys}.
   * @param regionKeys a list of region identifiers (TConsensusGroupId) representing the regions
   *     under migration.
   * @param regionDatabaseMap a mapping from each region identifier to its corresponding database
   *     name.
   * @return the sum of the squares of the maximum loads computed for each database.
   */
  private int computeDatabaseLoadSquaredSum(
      int[] currentAssignment,
      List<TConsensusGroupId> regionKeys,
      Map<TConsensusGroupId, String> regionDatabaseMap) {
    Map<String, int[]> extraLoadPerDb = new HashMap<>();
    // Initialize extra load counters for each database using the number of nodes from
    // regionCounter.
    for (String db : initialDbLoad.keySet()) {
      extraLoadPerDb.put(db, new int[regionCounter.length]);
    }
    // Accumulate extra load per database based on the current assignment.
    for (int i = 0; i < regionKeys.size(); i++) {
      TConsensusGroupId regionId = regionKeys.get(i);
      String db = regionDatabaseMap.get(regionId);
      int nodeId = currentAssignment[i];
      extraLoadPerDb.get(db)[nodeId]++;
    }
    int sumSquared = 0;
    // For each database, compute the maximum load across nodes and add its square to the sum.
    for (String db : initialDbLoad.keySet()) {
      int[] initLoads = initialDbLoad.get(db);
      int[] extras = extraLoadPerDb.get(db);
      int maxLoad = 0;
      for (int nodeId = 0; nodeId < regionCounter.length; nodeId++) {
        int load = initLoads[nodeId] + extras[nodeId];
        if (load > maxLoad) {
          maxLoad = load;
        }
      }
      sumSquared += maxLoad * maxLoad;
    }
    return sumSquared;
  }

  /**
   * Computes the current migration metrics.
   *
   * <p>This method calculates three key metrics:
   *
   * <ol>
   *   <li><strong>Max Global Load:</strong> The maximum load among all nodes, computed as the sum
   *       of the initial region load (from {@code regionCounter}) and the additional load (from
   *       {@code additionalLoad}).
   *   <li><strong>Database Load Squared Sum:</strong> The squared sum of the maximum load per
   *       database, which is computed by {@link #computeDatabaseLoadSquaredSum(int[], List, Map)}.
   *   <li><strong>Scatter Value:</strong> A provided metric that reflects additional balancing
   *       criteria.
   * </ol>
   *
   * The metrics are returned as an array of three integers in the order: [maxGlobalLoad,
   * databaseLoadSquaredSum, scatterValue].
   *
   * @param additionalLoad an array representing the additional load assigned to each node during
   *     migration.
   * @param currentScatter the current scatter value metric.
   * @param currentAssignment an array where each element is the nodeId assigned for the
   *     corresponding region in {@code regionKeys}.
   * @return an integer array of size 3: [maxGlobalLoad, databaseLoadSquaredSum, scatterValue].
   */
  private int[] getCurrentMetrics(
      int[] additionalLoad, int currentScatter, int[] currentAssignment) {
    int currentMaxGlobalLoad = 0;
    // Calculate the maximum global load across all data nodes.
    for (int nodeId = 0; nodeId < additionalLoad.length; nodeId++) {
      int globalLoad = regionCounter[nodeId] + additionalLoad[nodeId];
      currentMaxGlobalLoad = Math.max(currentMaxGlobalLoad, globalLoad);
    }
    // Compute the database load squared sum using the helper method.
    int dbLoadSquaredSum =
        computeDatabaseLoadSquaredSum(currentAssignment, dfsRegionKeys, regionDatabaseMap);
    // Build current metrics in order [maxGlobalLoad, maxDatabaseLoad, scatterValue]
    return new int[] {currentMaxGlobalLoad, dbLoadSquaredSum, currentScatter};
  }

  /**
   * Compute the initial load for each database on each data node.
   *
   * @param availableDataNodeMap currently available DataNodes, ensure size() >= replicationFactor
   * @param databaseAllocatedRegionGroupMap Mapping of each database to its list of replica sets.
   */
  private void computeInitialDbLoad(
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap,
      Map<String, List<TRegionReplicaSet>> databaseAllocatedRegionGroupMap) {
    initialDbLoad = new HashMap<>();

    // Iterate over each database and count the number of regions on each data node across all its
    // replica sets.
    for (String database : databaseAllocatedRegionGroupMap.keySet()) {
      List<TRegionReplicaSet> replicaSets = databaseAllocatedRegionGroupMap.get(database);
      int[] load = new int[regionCounter.length];
      for (TRegionReplicaSet replicaSet : replicaSets) {
        for (TDataNodeLocation location : replicaSet.getDataNodeLocations()) {
          int nodeId = location.getDataNodeId();
          if (availableDataNodeMap.containsKey(nodeId)) {
            load[nodeId]++;
          }
        }
      }
      initialDbLoad.put(database, load);
    }
  }

  /**
   * Prepare some statistics before dfs.
   *
   * @param availableDataNodeMap currently available DataNodes, ensure size() >= replicationFactor
   * @param allocatedRegionGroups already allocated RegionGroups in the cluster
   * @param databaseAllocatedRegionGroups already allocated RegionGroups in the same Database
   */
  private void prepare(
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

    // Compute the DataNodeIds through sorting the DataNodeEntryMap
    Map<Integer, DataNodeEntry> dataNodeEntryMap = new HashMap<>(maxDataNodeId + 1);
    availableDataNodeMap
        .keySet()
        .forEach(
            dataNodeId -> {
              int scatterWidth = 0;
              for (int j = 0; j <= maxDataNodeId; j++) {
                if (combinationCounter[dataNodeId][j] > 0) {
                  // Each exists 2-Region combination extends
                  // the scatter width of current DataNode by 1
                  scatterWidth++;
                }
              }
              dataNodeEntryMap.put(
                  dataNodeId,
                  new DataNodeEntry(
                      databaseRegionCounter[dataNodeId], regionCounter[dataNodeId], scatterWidth));
            });
    dataNodeIds =
        dataNodeEntryMap.entrySet().stream()
            .sorted(comparingByValue(DataNodeEntry::compare))
            .map(Map.Entry::getKey)
            .collect(Collectors.toList())
            .stream()
            .mapToInt(Integer::intValue)
            .toArray();

    // Reset the optimal result
    optimalDatabaseRegionSum = Integer.MAX_VALUE;
    optimalRegionSum = Integer.MAX_VALUE;
    optimalCombinationSum = Integer.MAX_VALUE;
    optimalReplicaSets = new ArrayList<>();
  }

  /**
   * Dfs each possible allocation plan, and keep those with the highest priority: First Key: the sum
   * of Regions at the DataNodes in the allocation result is minimal, Second Key: the sum of
   * intersected Regions with other allocated RegionGroups is minimal.
   *
   * @param lastIndex last decided index in dataNodeIds
   * @param currentReplica current replica index
   * @param currentReplicaSet current allocation plan
   * @param databaseRegionSum the sum of Regions at the DataNodes within the same Database in the
   *     current allocation plan
   * @param regionSum the sum of Regions at the DataNodes in the current allocation plan
   */
  private void dfsAllocateReplica(
      int lastIndex,
      int currentReplica,
      int[] currentReplicaSet,
      int databaseRegionSum,
      int regionSum) {
    if (regionSum > optimalRegionSum) {
      // Pruning: no needs for further searching when the first key
      // is bigger than the historical optimal result
      return;
    }
    if (regionSum == optimalRegionSum && databaseRegionSum > optimalDatabaseRegionSum) {
      // Pruning: no needs for further searching when the second key
      // is bigger than the historical optimal result
      return;
    }

    if (currentReplica == replicationFactor) {
      // A complete allocation plan is found
      int combinationSum = 0;
      for (int i = 0; i < replicationFactor; i++) {
        for (int j = i + 1; j < replicationFactor; j++) {
          combinationSum += combinationCounter[currentReplicaSet[i]][currentReplicaSet[j]];
        }
      }
      if (regionSum == optimalRegionSum
          && databaseRegionSum == optimalDatabaseRegionSum
          && combinationSum > optimalCombinationSum) {
        // Pruning: no needs for further searching when the third key
        // is bigger than the historical optimal result
        return;
      }

      if (regionSum < optimalRegionSum
          || databaseRegionSum < optimalDatabaseRegionSum
          || combinationSum < optimalCombinationSum) {
        // Reset the optimal result when a better one is found
        optimalDatabaseRegionSum = databaseRegionSum;
        optimalRegionSum = regionSum;
        optimalCombinationSum = combinationSum;
        optimalReplicaSets.clear();
      }
      optimalReplicaSets.add(Arrays.copyOf(currentReplicaSet, replicationFactor));
      return;
    }

    for (int i = lastIndex + 1; i < dataNodeIds.length; i++) {
      // Decide the next DataNodeId in the allocation plan
      currentReplicaSet[currentReplica] = dataNodeIds[i];
      dfsAllocateReplica(
          i,
          currentReplica + 1,
          currentReplicaSet,
          databaseRegionSum + databaseRegionCounter[dataNodeIds[i]],
          regionSum + regionCounter[dataNodeIds[i]]);
      if (optimalReplicaSets.size() == GCR_MAX_OPTIMAL_PLAN_NUM) {
        // Pruning: no needs for further searching when
        // the number of optimal plans reaches the limitation
        return;
      }
    }
  }

  void clear() {
    optimalReplicaSets.clear();
  }
}
