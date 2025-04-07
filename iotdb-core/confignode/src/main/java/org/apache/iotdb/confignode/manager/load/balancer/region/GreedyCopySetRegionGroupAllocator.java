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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Map.Entry.comparingByValue;

/** Allocate Region through Greedy and CopySet Algorithm. */
public class GreedyCopySetRegionGroupAllocator implements IRegionGroupAllocator {

  private static final Random RANDOM = new Random();
  private static final int GCR_MAX_OPTIMAL_PLAN_NUM = 100;

  private int replicationFactor;
  // Sorted available DataNodeIds
  private int[] dataNodeIds;
  // The number of allocated Regions in each DataNode
  private int[] regionCounter;
  // The number of allocated Regions in each DataNode within the same Database
  private int[] databaseRegionCounter;
  // The number of 2-Region combinations in current cluster
  private int[][] combinationCounter;
  // target destination datanode
  private int targetDataNode = -1;
  // remain replica set datanode
  private Set<Integer> remainReplicaDataNodeIds;

  // First Key: the sum of Regions at the DataNodes in the allocation result is minimal
  int optimalRegionSum;
  // Second Key: the sum of Regions at the DataNodes within the same Database
  // in the allocation result is minimal
  int optimalDatabaseRegionSum;
  // Third Key: the sum of overlapped 2-Region combination Regions with
  // other allocated RegionGroups is minimal
  int optimalCombinationSum;
  List<int[]> optimalReplicaSets;

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
      prepare(
          replicationFactor,
          availableDataNodeMap,
          allocatedRegionGroups,
          databaseAllocatedRegionGroups);
      dfs(-1, 0, new int[replicationFactor], 0, 0);

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
  public TDataNodeConfiguration selectDestDataNode(
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap,
      Map<Integer, Double> freeDiskSpaceMap,
      List<TRegionReplicaSet> allocatedRegionGroups,
      List<TRegionReplicaSet> databaseAllocatedRegionGroups,
      int replicationFactor,
      TConsensusGroupId consensusGroupId,
      TRegionReplicaSet remainReplicaSet) {
    try {
      prepare(
          replicationFactor,
          availableDataNodeMap,
          allocatedRegionGroups,
          databaseAllocatedRegionGroups);
      remainReplicaDataNodeIds = new HashSet<>();
      int[] currentReplicaSet = new int[replicationFactor];
      int databaseRegionSum = 0;
      int regionSum = 0;
      for (int i = 0; i < remainReplicaSet.getDataNodeLocations().size(); i++) {
        int dataNodeId = remainReplicaSet.getDataNodeLocations().get(i).getDataNodeId();
        currentReplicaSet[i] = dataNodeId;
        databaseRegionSum += databaseRegionCounter[dataNodeId];
        regionSum += regionCounter[dataNodeId];
        remainReplicaDataNodeIds.add(dataNodeId);
      }

      dfs(-1, replicationFactor - 1, currentReplicaSet, databaseRegionSum, regionSum);
      // Randomly pick one optimal plan as result
      Collections.shuffle(optimalReplicaSets);
      int[] optimalReplicaSet = optimalReplicaSets.get(0);
      for (int i = 0; i < replicationFactor; i++) {
        if (remainReplicaDataNodeIds.contains(optimalReplicaSet[i])) {
          continue;
        }
        targetDataNode = optimalReplicaSet[i];
      }

      return availableDataNodeMap.get(targetDataNode);

    } finally {
      clear();
    }
  }

  /**
   * Prepare some statistics before dfs.
   *
   * @param replicationFactor replication factor in the cluster
   * @param availableDataNodeMap currently available DataNodes, ensure size() >= replicationFactor
   * @param allocatedRegionGroups already allocated RegionGroups in the cluster
   * @param databaseAllocatedRegionGroups already allocated RegionGroups in the same Database
   */
  private void prepare(
      int replicationFactor,
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap,
      List<TRegionReplicaSet> allocatedRegionGroups,
      List<TRegionReplicaSet> databaseAllocatedRegionGroups) {

    this.replicationFactor = replicationFactor;
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
    remainReplicaDataNodeIds = new HashSet<>();
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
  private void dfs(
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
      if (remainReplicaDataNodeIds.contains(dataNodeIds[i])) {
        continue;
      }
      currentReplicaSet[currentReplica] = dataNodeIds[i];
      dfs(
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
