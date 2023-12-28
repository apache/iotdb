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
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import static java.util.Map.Entry.comparingByValue;

/** Allocate Region through Greedy and CopySet Algorithm. */
public class GreedyCopySetRegionGroupAllocator implements IRegionGroupAllocator {

  private static final Random RANDOM = new Random();

  private int replicationFactor;
  // Sorted available DataNodeIds
  private int[] dataNodeIds;
  // The number of allocated Regions in each DataNode
  private int[] regionCounter;
  // The number of 2-Region combinations in current cluster
  private int[][] combinationCounter;

  // First Key: the sum of Regions at the DataNodes in the allocation result is minimal
  int optimalRegionSum;
  // Second Key: the sum of overlapped 2-Region combination Regions with other allocated
  // RegionGroups is minimal
  int optimalCombinationSum;
  List<int[]> optimalReplicaSets;
  private static final int MAX_OPTIMAL_PLAN_NUM = 10;

  private static class DataNodeEntry {

    private final int dataNodeId;

    // First key: the number of Regions in the DataNode
    private final int regionCount;
    // Second key: the scatter width of the DataNode
    private final int scatterWidth;
    // Third key: a random weight
    private final int randomWeight;

    public DataNodeEntry(int dataNodeId, int regionCount, int scatterWidth) {
      this.dataNodeId = dataNodeId;
      this.regionCount = regionCount;
      this.scatterWidth = scatterWidth;
      this.randomWeight = RANDOM.nextInt();
    }

    public int getDataNodeId() {
      return dataNodeId;
    }

    public int compare(DataNodeEntry e) {
      return regionCount != e.regionCount
          ? Integer.compare(regionCount, e.regionCount)
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
      int replicationFactor,
      TConsensusGroupId consensusGroupId) {
    try {
      prepare(replicationFactor, availableDataNodeMap, allocatedRegionGroups);
      dfs(-1, 0, new int[replicationFactor], 0);

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

  /**
   * Prepare some statistics before dfs.
   *
   * @param replicationFactor replication factor in the cluster
   * @param availableDataNodeMap currently available DataNodes, ensure size() >= replicationFactor
   * @param allocatedRegionGroups already allocated RegionGroups in the cluster
   */
  private void prepare(
      int replicationFactor,
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap,
      List<TRegionReplicaSet> allocatedRegionGroups) {

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

    // Compute regionCounter and combinationCounter
    regionCounter = new int[maxDataNodeId + 1];
    Arrays.fill(regionCounter, 0);
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
                  new DataNodeEntry(dataNodeId, regionCounter[dataNodeId], scatterWidth));
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
   * @param regionSum the sum of Regions at the DataNodes in the current allocation plan
   */
  private void dfs(int lastIndex, int currentReplica, int[] currentReplicaSet, int regionSum) {
    if (regionSum > optimalRegionSum) {
      // Pruning: no needs for further searching when the first key
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

      if (regionSum < optimalRegionSum || combinationSum < optimalCombinationSum) {
        // Reset the optimal result when a better one is found
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
      dfs(i, currentReplica + 1, currentReplicaSet, regionSum + regionCounter[dataNodeIds[i]]);
      if (optimalReplicaSets.size() == MAX_OPTIMAL_PLAN_NUM) {
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
