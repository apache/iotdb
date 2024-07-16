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

public class PGRA implements IRegionGroupAllocator {

  private static class DataNodeEntry {

    private final int fakeId;
    private final int regionCount;
    private final int scatterWidth;
    private final int randomWeight;

    public DataNodeEntry(int fakeId, int regionCount, int scatterWidth, int randomWeight) {
      this.fakeId = fakeId;
      this.regionCount = regionCount;
      this.scatterWidth = scatterWidth;
      this.randomWeight = randomWeight;
    }

    public int compare(PGRA.DataNodeEntry e) {
      return regionCount != e.regionCount
          ? Integer.compare(regionCount, e.regionCount)
          : scatterWidth != e.scatterWidth
              ? Integer.compare(scatterWidth, e.scatterWidth)
              : Integer.compare(randomWeight, e.randomWeight);
    }
  }

  private static final Random RANDOM = new Random();

  private int subGraphCount;
  private int replicationFactor;
  private int regionPerDataNode;

  private int dataNodeNum;
  // The number of allocated Regions in each DataNode
  private int[] regionCounter;
  // The scatter width of each DataNode
  private int[] scatterWidthCounter;
  // The number of 2-Region combinations in current cluster
  private int[][] combinationCounter;
  private Map<Integer, Integer> fakeToRealIdMap;
  private Map<Integer, Integer> realToFakeIdMap;

  private int subDataNodeNum;
  // First Key: the sum of overlapped 2-Region combination Regions with
  // other allocated RegionGroups is minimal
  private int optimalCombinationSum;
  // Second Key: the sum of DataRegions in selected DataNodes is minimal
  private int optimalRegionSum;
  private int[] optimalSubDataNodes;

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
    for (int i = 0; i < subGraphCount; i++) {
      subGraphSearch(i);
    }
    if (optimalCombinationSum == Integer.MAX_VALUE) {
      return new GreedyRegionGroupAllocator()
          .generateOptimalRegionReplicasDistribution(
              availableDataNodeMap,
              freeDiskSpaceMap,
              allocatedRegionGroups,
              databaseAllocatedRegionGroups,
              replicationFactor,
              consensusGroupId);
    }
    List<Integer> partiteNodes = partiteGraphSearch(optimalSubDataNodes[0] % subGraphCount);
    if (partiteNodes.size() < replicationFactor - subDataNodeNum) {
      return new GreedyRegionGroupAllocator()
          .generateOptimalRegionReplicasDistribution(
              availableDataNodeMap,
              freeDiskSpaceMap,
              allocatedRegionGroups,
              databaseAllocatedRegionGroups,
              replicationFactor,
              consensusGroupId);
    }

    TRegionReplicaSet result = new TRegionReplicaSet();
    result.setRegionId(consensusGroupId);
    for (int i = 0; i < subDataNodeNum; i++) {
      result.addToDataNodeLocations(
          availableDataNodeMap.get(fakeToRealIdMap.get(optimalSubDataNodes[i])).getLocation());
    }
    for (int i = 0; i < replicationFactor - subDataNodeNum; i++) {
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
    this.realToFakeIdMap = new TreeMap<>();
    this.dataNodeNum = availableDataNodeMap.size();
    List<Integer> dataNodeIdList =
        availableDataNodeMap.values().stream()
            .map(c -> c.getLocation().getDataNodeId())
            .collect(Collectors.toList());
    for (int i = 0; i < dataNodeNum; i++) {
      fakeToRealIdMap.put(i, dataNodeIdList.get(i));
      realToFakeIdMap.put(dataNodeIdList.get(i), i);
    }

    // Compute regionCounter, combinationCounter and scatterWidthCounter
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
    this.scatterWidthCounter = new int[dataNodeNum];
    Arrays.fill(scatterWidthCounter, 0);
    for (int i = 0; i < dataNodeNum; i++) {
      for (int j = 0; j < dataNodeNum; j++) {
        scatterWidthCounter[i] += combinationCounter[i][j];
      }
    }

    // Reset the optimal result
    this.subDataNodeNum = replicationFactor / 2 + 1;
    this.optimalCombinationSum = Integer.MAX_VALUE;
    this.optimalRegionSum = Integer.MAX_VALUE;
    this.optimalSubDataNodes = new int[subDataNodeNum];
  }

  private void subGraphSearch(int firstIndex) {
    List<DataNodeEntry> entryList = new ArrayList<>();
    for (int i = firstIndex; i < dataNodeNum; i += subGraphCount) {
      if (regionCounter[i] >= regionPerDataNode) {
        continue;
      }
      entryList.add(
          new DataNodeEntry(i, regionCounter[i], scatterWidthCounter[i], RANDOM.nextInt()));
    }
    if (entryList.size() < subDataNodeNum) {
      return;
    }
    entryList.sort(DataNodeEntry::compare);
    int[] subDataNodes = new int[subDataNodeNum];
    // Pick replicationFactor / 2 DataNodes with the smallest regionCount first
    for (int i = 0; i < subDataNodeNum - 1; i++) {
      subDataNodes[i] = entryList.get(i).fakeId;
    }
    int curCombinationSum = Integer.MAX_VALUE;
    int curRegionSum = Integer.MAX_VALUE;
    // Select the last DataNode
    for (int i = subDataNodeNum - 1; i < entryList.size(); i++) {
      int tmpCombinationSum = 0;
      for (int j = 0; j < subDataNodeNum - 1; j++) {
        tmpCombinationSum += combinationCounter[subDataNodes[j]][entryList.get(i).fakeId];
      }
      if (tmpCombinationSum < curCombinationSum) {
        curCombinationSum = tmpCombinationSum;
        curRegionSum = entryList.get(i).regionCount;
        subDataNodes[subDataNodeNum - 1] = entryList.get(i).fakeId;
      } else if (tmpCombinationSum == curCombinationSum
          && entryList.get(i).regionCount < curRegionSum) {
        curRegionSum = entryList.get(i).regionCount;
        subDataNodes[subDataNodeNum - 1] = entryList.get(i).fakeId;
      }
    }
    for (int i = 0; i < subDataNodeNum - 1; i++) {
      curRegionSum += regionCounter[subDataNodes[i]];
      for (int j = i + 1; j < subDataNodeNum - 1; j++) {
        curCombinationSum += combinationCounter[subDataNodes[i]][subDataNodes[j]];
      }
    }
    if (curCombinationSum < optimalCombinationSum
        || (curCombinationSum == optimalCombinationSum && curRegionSum < optimalRegionSum)) {
      optimalCombinationSum = curCombinationSum;
      optimalRegionSum = curRegionSum;
      optimalSubDataNodes = Arrays.copyOf(subDataNodes, subDataNodeNum);
    } else if (curCombinationSum == optimalCombinationSum
        && curRegionSum == optimalRegionSum
        && RANDOM.nextBoolean()) {
      optimalSubDataNodes = Arrays.copyOf(subDataNodes, subDataNodeNum);
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
        int scatterWidth = subDataNodeNum;
        for (int k = 0; k < subDataNodeNum; k++) {
          scatterWidth -= combinationCounter[i][optimalSubDataNodes[k]];
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
