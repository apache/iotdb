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
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class PartiteGraphReplicationRegionGroupAllocator implements IRegionGroupAllocator {

  private static final Random RANDOM = new Random();
  private static final GreedyRegionGroupAllocator GREEDY_ALLOCATOR =
      new GreedyRegionGroupAllocator();

  private int subGraphCount;
  private int replicationFactor;
  private int regionPerDataNode;

  private int dataNodeNum;
  // The number of allocated Regions in each DataNode
  private int[] regionCounter;
  // The number of edges in current cluster
  private int[][] combinationCounter;
  private Map<Integer, Integer> fakeToRealIdMap;

  private int alphaDataNodeNum;
  // First Key: the sum of overlapped 2-Region combination Regions with
  // other allocated RegionGroups is minimal
  private int optimalEdgeSum;
  // Second Key: the sum of DataRegions in selected DataNodes is minimal
  private int optimalRegionSum;
  private int[] optimalAlphaNodes;

  @Override
  public TRegionReplicaSet generateOptimalRegionReplicasDistribution(
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap,
      Map<Integer, Double> freeDiskSpaceMap,
      List<TRegionReplicaSet> allocatedRegionGroups,
      List<TRegionReplicaSet> databaseAllocatedRegionGroups,
      int replicationFactor,
      TConsensusGroupId consensusGroupId) {

    this.regionPerDataNode =
        (int)
            (consensusGroupId.getType().equals(TConsensusGroupType.DataRegion)
                ? ConfigNodeDescriptor.getInstance().getConf().getDataRegionPerDataNode()
                : ConfigNodeDescriptor.getInstance().getConf().getSchemaRegionPerDataNode());
    prepare(replicationFactor, availableDataNodeMap, allocatedRegionGroups);

    // Select a set of optimal alpha nodes
    for (int i = 0; i < subGraphCount; i++) {
      subGraphSearch(i, 0, alphaDataNodeNum, 0, 0, new int[alphaDataNodeNum]);
    }
    if (optimalEdgeSum == Integer.MAX_VALUE) {
      return GREEDY_ALLOCATOR.generateOptimalRegionReplicasDistribution(
          availableDataNodeMap,
          freeDiskSpaceMap,
          allocatedRegionGroups,
          databaseAllocatedRegionGroups,
          replicationFactor,
          consensusGroupId);
    }

    // Select the set of optimal beta nodes
    List<Integer> partiteNodes = partiteGraphSearch(optimalAlphaNodes[0] % subGraphCount);
    if (partiteNodes.size() < replicationFactor - alphaDataNodeNum) {
      return GREEDY_ALLOCATOR.generateOptimalRegionReplicasDistribution(
          availableDataNodeMap,
          freeDiskSpaceMap,
          allocatedRegionGroups,
          databaseAllocatedRegionGroups,
          replicationFactor,
          consensusGroupId);
    }

    TRegionReplicaSet result = new TRegionReplicaSet();
    result.setRegionId(consensusGroupId);
    for (int i = 0; i < alphaDataNodeNum; i++) {
      result.addToDataNodeLocations(
          availableDataNodeMap.get(fakeToRealIdMap.get(optimalAlphaNodes[i])).getLocation());
    }
    for (int i = 0; i < replicationFactor - alphaDataNodeNum; i++) {
      result.addToDataNodeLocations(
          availableDataNodeMap.get(fakeToRealIdMap.get(partiteNodes.get(i))).getLocation());
    }
    return result;
  }

  private void prepare(
      int replicationFactor,
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap,
      List<TRegionReplicaSet> allocatedRegionGroups) {

    this.subGraphCount = replicationFactor / 2 + (replicationFactor % 2 == 0 ? 0 : 1);
    this.replicationFactor = replicationFactor;

    this.fakeToRealIdMap = new TreeMap<>();
    Map<Integer, Integer> realToFakeIdMap = new TreeMap<>();
    this.dataNodeNum = availableDataNodeMap.size();
    List<Integer> dataNodeIdList =
        availableDataNodeMap.values().stream()
            .map(c -> c.getLocation().getDataNodeId())
            .collect(Collectors.toList());
    for (int i = 0; i < dataNodeNum; i++) {
      fakeToRealIdMap.put(i, dataNodeIdList.get(i));
      realToFakeIdMap.put(dataNodeIdList.get(i), i);
    }

    // Compute regionCounter and combinationCounter
    this.regionCounter = new int[dataNodeNum];
    Arrays.fill(regionCounter, 0);
    this.combinationCounter = new int[dataNodeNum][dataNodeNum];
    for (int i = 0; i < dataNodeNum; i++) {
      Arrays.fill(combinationCounter[i], 0);
    }
    for (TRegionReplicaSet regionReplicaSet : allocatedRegionGroups) {
      List<TDataNodeLocation> dataNodeLocations = regionReplicaSet.getDataNodeLocations();
      for (int i = 0; i < dataNodeLocations.size(); i++) {
        int fakeIId = realToFakeIdMap.get(dataNodeLocations.get(i).getDataNodeId());
        regionCounter[fakeIId]++;
        for (int j = i + 1; j < dataNodeLocations.size(); j++) {
          int fakeJId = realToFakeIdMap.get(dataNodeLocations.get(j).getDataNodeId());
          combinationCounter[fakeIId][fakeJId] = 1;
          combinationCounter[fakeJId][fakeIId] = 1;
        }
      }
    }

    // Reset the optimal result
    this.alphaDataNodeNum = replicationFactor / 2 + 1;
    this.optimalEdgeSum = Integer.MAX_VALUE;
    this.optimalRegionSum = Integer.MAX_VALUE;
    this.optimalAlphaNodes = new int[alphaDataNodeNum];
  }

  private void subGraphSearch(
      int firstIndex,
      int currentReplica,
      int replicaNum,
      int combinationSum,
      int regionSum,
      int[] currentReplicaSet) {

    if (currentReplica == replicaNum) {
      if (combinationSum < optimalEdgeSum
          || (combinationSum == optimalEdgeSum && regionSum < optimalRegionSum)) {
        // Reset the optimal result when a better one is found
        optimalEdgeSum = combinationSum;
        optimalRegionSum = regionSum;
        optimalAlphaNodes = Arrays.copyOf(currentReplicaSet, replicationFactor);
      } else if (combinationSum == optimalEdgeSum
          && regionSum == optimalRegionSum
          && RANDOM.nextBoolean()) {
        optimalAlphaNodes = Arrays.copyOf(currentReplicaSet, replicationFactor);
      }
      return;
    }

    for (int i = firstIndex; i < dataNodeNum; i += subGraphCount) {
      if (regionCounter[i] >= regionPerDataNode) {
        // Pruning: skip full DataNodes
        continue;
      }
      int nxtCombinationSum = combinationSum;
      for (int j = 0; j < currentReplica; j++) {
        nxtCombinationSum += combinationCounter[i][currentReplicaSet[j]];
      }
      if (combinationSum > optimalEdgeSum) {
        // Pruning: no needs for further searching when the first key
        // is bigger than the historical optimal result
        return;
      }
      int nxtRegionSum = regionSum + regionCounter[i];
      if (combinationSum == optimalEdgeSum && regionSum > optimalRegionSum) {
        // Pruning: no needs for further searching when the second key
        // is bigger than the historical optimal result
        return;
      }
      currentReplicaSet[currentReplica] = i;
      subGraphSearch(
          i + subGraphCount,
          currentReplica + 1,
          replicaNum,
          nxtCombinationSum,
          nxtRegionSum,
          currentReplicaSet);
    }
  }

  private List<Integer> partiteGraphSearch(int selected) {
    List<Integer> partiteNodes = new ArrayList<>();
    for (int partiteIndex = 0; partiteIndex < subGraphCount; partiteIndex++) {
      if (partiteIndex == selected) {
        continue;
      }
      int selectedDataNode = -1;
      int bestScatterWidth = 0;
      int bestRegionSum = Integer.MAX_VALUE;
      for (int i = partiteIndex; i < dataNodeNum; i += subGraphCount) {
        if (regionCounter[i] >= regionPerDataNode) {
          continue;
        }
        int scatterWidth = alphaDataNodeNum;
        for (int k = 0; k < alphaDataNodeNum; k++) {
          scatterWidth -= combinationCounter[i][optimalAlphaNodes[k]];
        }
        if (scatterWidth < bestScatterWidth) {
          continue;
        }
        if (scatterWidth > bestScatterWidth) {
          bestScatterWidth = scatterWidth;
          bestRegionSum = regionCounter[i];
          selectedDataNode = i;
        } else if (regionCounter[i] < bestRegionSum) {
          bestRegionSum = regionCounter[i];
          selectedDataNode = i;
        } else if (regionCounter[i] == bestRegionSum && RANDOM.nextBoolean()) {
          selectedDataNode = i;
        }
      }
      if (selectedDataNode == -1) {
        return new ArrayList<>();
      }
      partiteNodes.add(selectedDataNode);
    }
    return partiteNodes;
  }
}
