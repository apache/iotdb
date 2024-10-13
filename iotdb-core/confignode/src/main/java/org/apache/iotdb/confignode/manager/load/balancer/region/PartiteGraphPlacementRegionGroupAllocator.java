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
import org.apache.iotdb.confignode.manager.load.balancer.region.GreedyRegionGroupAllocator.DataNodeEntry;

import org.apache.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class PartiteGraphPlacementRegionGroupAllocator implements IRegionGroupAllocator {

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
  // Pair<combinationSum, RegionSum>
  Pair<Integer, Integer> bestValue;
  private int[] bestAlphaNodes;

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

    // Select alpha nodes set
    for (int i = 0; i < subGraphCount; i++) {
      subGraphSearch(i, freeDiskSpaceMap);
    }
    if (bestValue.left == Integer.MAX_VALUE) {
      // Use greedy allocator as alternative if no alpha nodes set is found
      return GREEDY_ALLOCATOR.generateOptimalRegionReplicasDistribution(
          availableDataNodeMap,
          freeDiskSpaceMap,
          allocatedRegionGroups,
          databaseAllocatedRegionGroups,
          replicationFactor,
          consensusGroupId);
    }

    // Select the beta nodes sets
    List<Integer> betaDataNodes = partiteGraphSearch(bestAlphaNodes[0] % subGraphCount);
    if (betaDataNodes.size() < replicationFactor - alphaDataNodeNum) {
      // Use greedy allocator as alternative if no beta nodes set is found
      return GREEDY_ALLOCATOR.generateOptimalRegionReplicasDistribution(
          availableDataNodeMap,
          freeDiskSpaceMap,
          allocatedRegionGroups,
          databaseAllocatedRegionGroups,
          replicationFactor,
          consensusGroupId);
    }

    // The next placement scheme is alpha \cup beta
    TRegionReplicaSet result = new TRegionReplicaSet();
    result.setRegionId(consensusGroupId);
    for (int i = 0; i < alphaDataNodeNum; i++) {
      result.addToDataNodeLocations(
          availableDataNodeMap.get(fakeToRealIdMap.get(bestAlphaNodes[i])).getLocation());
    }
    for (int i = 0; i < replicationFactor - alphaDataNodeNum; i++) {
      result.addToDataNodeLocations(
          availableDataNodeMap.get(fakeToRealIdMap.get(betaDataNodes.get(i))).getLocation());
    }
    return result;
  }

  private void prepare(
      int replicationFactor,
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap,
      List<TRegionReplicaSet> allocatedRegionGroups) {
    this.subGraphCount = replicationFactor / 2 + (replicationFactor % 2 == 0 ? 0 : 1);
    this.replicationFactor = replicationFactor;

    // Initialize the fake index for each DataNode,
    // since the index of DataNode might not be continuous
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
    this.bestValue = new Pair<>(Integer.MAX_VALUE, Integer.MAX_VALUE);
    this.bestAlphaNodes = new int[alphaDataNodeNum];
  }

  private Pair<Integer, Integer> valuation(int[] nodes) {
    int edgeSum = 0;
    int regionSum = 0;
    for (int iota : nodes) {
      for (int kappa : nodes) {
        edgeSum += combinationCounter[iota][kappa];
      }
      regionSum += regionCounter[iota];
    }
    return new Pair<>(edgeSum, regionSum);
  }

  private void subGraphSearch(int firstIndex, Map<Integer, Double> freeDiskSpaceMap) {
    List<DataNodeEntry> entryList = new ArrayList<>();
    for (int index = firstIndex; index < dataNodeNum; index += subGraphCount) {
      // Prune: skip filled DataNodes
      if (regionCounter[index] < regionPerDataNode) {
        entryList.add(
            new DataNodeEntry(
                index, regionCounter[index], freeDiskSpaceMap.get(fakeToRealIdMap.get(index))));
      }
    }
    if (entryList.size() < alphaDataNodeNum) {
      // Skip: not enough DataNodes
      return;
    }
    Collections.sort(entryList);
    int[] alphaNodes = new int[alphaDataNodeNum];
    for (int i = 0; i < alphaDataNodeNum - 1; i++) {
      alphaNodes[i] = entryList.get(i).dataNodeId;
    }
    for (int i = alphaDataNodeNum - 1; i < entryList.size(); i++) {
      alphaNodes[alphaDataNodeNum - 1] = entryList.get(i).dataNodeId;
      Pair<Integer, Integer> currentValue = valuation(alphaNodes);
      if (currentValue.left < bestValue.left
          || (currentValue.left.equals(bestValue.left) && currentValue.right < bestValue.right)) {
        bestValue = currentValue;
        System.arraycopy(alphaNodes, 0, bestAlphaNodes, 0, alphaDataNodeNum);
      }
    }
  }

  private List<Integer> partiteGraphSearch(int alphaIndex) {
    List<Integer> betaNodes = new ArrayList<>();
    int[] tmpNodes = new int[alphaDataNodeNum + 1];
    System.arraycopy(bestAlphaNodes, 0, tmpNodes, 0, alphaDataNodeNum);
    for (int partiteIndex = 0; partiteIndex < subGraphCount; partiteIndex++) {
      if (partiteIndex == alphaIndex) {
        // Skip the alphaIndex subgraph
        continue;
      }
      int selectedDataNode = -1;
      Pair<Integer, Integer> tmpValue = new Pair<>(Integer.MAX_VALUE, Integer.MAX_VALUE);
      for (int i = partiteIndex; i < dataNodeNum; i += subGraphCount) {
        if (regionCounter[i] >= regionPerDataNode) {
          // Pruning: skip filled DataNodes
          continue;
        }
        tmpNodes[alphaDataNodeNum] = i;
        Pair<Integer, Integer> currentValue = valuation(tmpNodes);
        if (currentValue.left < tmpValue.left
            || (currentValue.left.equals(tmpValue.left) && currentValue.right < tmpValue.right)) {
          tmpValue = currentValue;
          selectedDataNode = i;
        }
      }
      if (selectedDataNode == -1) {
        return new ArrayList<>();
      }
      betaNodes.add(selectedDataNode);
    }
    return betaNodes;
  }
}
