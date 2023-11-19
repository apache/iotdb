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
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/** Allocate Region through Greedy and CopySet Algorithm */
public class GreedyCopySetRegionGroupAllocator implements IRegionGroupAllocator {

  int replicationFactor;
  // RegionGroup allocation BitSet
  private List<BitSet> allocatedBitSets;
  // Map<DataNodeId, RegionGroup count>
  private Map<Integer, AtomicInteger> regionCounter;
  // Available DataNodeIds
  private Integer[] dataNodeIds;

  // First Key: the sum of Regions at the DataNodes in the allocation result is minimal
  int optimalRegionSum;
  // Second Key: the sum of intersected Regions with other allocated RegionGroups is minimal
  int optimalIntersectionSum;
  List<Integer[]> optimalReplicaSets;

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

    prepare(replicationFactor, availableDataNodeMap, allocatedRegionGroups);
    dfs(-1, 0, new Integer[replicationFactor], 0, 0);

    // Randomly pick one optimal plan as result
    Collections.shuffle(optimalReplicaSets);
    Integer[] optimalReplicaSet = optimalReplicaSets.get(0);
    TRegionReplicaSet result = new TRegionReplicaSet();
    result.setRegionId(consensusGroupId);
    for (int i = 0; i < replicationFactor; i++) {
      result.addToDataNodeLocations(availableDataNodeMap.get(optimalReplicaSet[i]).getLocation());
    }
    return result;
  }

  /**
   * Prepare some statistics before dfs
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
    int maxDataNodeId = availableDataNodeMap.keySet().stream().max(Integer::compareTo).orElse(0);
    for (TRegionReplicaSet regionReplicaSet : allocatedRegionGroups) {
      for (TDataNodeLocation dataNodeLocation : regionReplicaSet.getDataNodeLocations()) {
        // Store the maximum DataNodeId in this algorithm loop
        maxDataNodeId = Math.max(maxDataNodeId, dataNodeLocation.getDataNodeId());
      }
    }

    // Convert the allocatedRegionGroups into allocatedBitSets,
    // where a true in BitSet corresponding to a DataNodeId in the RegionGroup
    allocatedBitSets = new ArrayList<>();
    for (TRegionReplicaSet regionReplicaSet : allocatedRegionGroups) {
      BitSet bitSet = new BitSet(maxDataNodeId + 1);
      for (TDataNodeLocation dataNodeLocation : regionReplicaSet.getDataNodeLocations()) {
        bitSet.set(dataNodeLocation.getDataNodeId());
      }
      allocatedBitSets.add(bitSet);
    }

    // Count the number of Regions in each DataNode
    regionCounter = new HashMap<>();
    for (int i = 0; i <= maxDataNodeId; i++) {
      regionCounter.put(i, new AtomicInteger(0));
    }
    allocatedRegionGroups.forEach(
        regionGroup ->
            regionGroup
                .getDataNodeLocations()
                .forEach(
                    dataNodeLocation ->
                        regionCounter.get(dataNodeLocation.getDataNodeId()).incrementAndGet()));

    // Reset the optimal result
    dataNodeIds = new Integer[availableDataNodeMap.size()];
    availableDataNodeMap.keySet().toArray(dataNodeIds);
    optimalRegionSum = Integer.MAX_VALUE;
    optimalIntersectionSum = Integer.MAX_VALUE;
    optimalReplicaSets = new ArrayList<>();
  }

  /**
   * Dfs each possible allocation plan, and keep those with the highest priority: First Key: the sum
   * of Regions at the DataNodes in the allocation result is minimal, Second Key: the sum of
   * intersected Regions with other allocated RegionGroups is minimal
   *
   * @param lastIndex last decided index in dataNodeIds
   * @param currentReplica current replica index
   * @param currentReplicaSet current allocation plan
   * @param regionSum the sum of Regions at the DataNodes in the current allocation plan
   * @param intersectionSum the sum of intersected Regions with other allocated RegionGroups in
   *     current allocation plan
   */
  private void dfs(
      int lastIndex,
      int currentReplica,
      Integer[] currentReplicaSet,
      int regionSum,
      int intersectionSum) {
    if (regionSum > optimalRegionSum || intersectionSum > optimalRegionSum) {
      // Pruning: no needs for further searching when either the first key or the second key
      // is bigger than the historical optimal result
      return;
    }

    if (currentReplica == replicationFactor) {
      // A complete allocation plan is found
      if (regionSum < optimalRegionSum || intersectionSum < optimalRegionSum) {
        // Reset the optimal result when a better one is found
        optimalRegionSum = regionSum;
        optimalIntersectionSum = intersectionSum;
        optimalReplicaSets.clear();
      }
      optimalReplicaSets.add(Arrays.copyOf(currentReplicaSet, replicationFactor));
      return;
    }

    for (int i = lastIndex + 1; i < dataNodeIds.length; i++) {
      // Decide the next DataNodeId in the allocation plan
      currentReplicaSet[currentReplica] = dataNodeIds[i];
      int intersectionDelta = 0;
      for (BitSet bitSet : allocatedBitSets) {
        intersectionDelta += bitSet.get(dataNodeIds[i]) ? 1 : 0;
      }
      dfs(
          i,
          currentReplica + 1,
          currentReplicaSet,
          regionSum + regionCounter.get(dataNodeIds[i]).get(),
          intersectionSum + intersectionDelta);
    }
  }
}
