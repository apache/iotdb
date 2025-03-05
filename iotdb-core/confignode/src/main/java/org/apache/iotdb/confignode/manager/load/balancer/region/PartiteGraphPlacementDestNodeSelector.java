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
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class PartiteGraphPlacementDestNodeSelector implements IDestNodeSelector {
  private int replicationFactor;
  private int regionPerDataNode;

  private int dataNodeNum;
  // The number of allocated Regions in each DataNode
  private int[] regionCounter;
  // The number of edges in current cluster
  private int[][] combinationCounter;
  private Map<Integer, Integer> fakeToRealIdMap;

  private Set<Integer> remainDataNodesFakeId;

  // Pair<combinationSum, RegionSum>
  Pair<Integer, Integer> bestValue;

  private Integer targetDataNode = -1;

  @Override
  public TDataNodeConfiguration selectDestDataNode(
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap,
      Map<Integer, Double> freeDiskSpaceMap,
      List<TRegionReplicaSet> allocatedRegionGroups,
      List<TRegionReplicaSet> databaseAllocatedRegionGroups,
      int replicationFactor,
      TConsensusGroupId consensusGroupId,
      TRegionReplicaSet remainReplicaSet) {
    this.regionPerDataNode =
        consensusGroupId.getType().equals(TConsensusGroupType.DataRegion)
            ? ConfigNodeDescriptor.getInstance().getConf().getDataRegionPerDataNode()
            : ConfigNodeDescriptor.getInstance().getConf().getSchemaRegionPerDataNode();
    prepare(replicationFactor, availableDataNodeMap, allocatedRegionGroups, remainReplicaSet);

    searchTargetDataNode(freeDiskSpaceMap);

    if (targetDataNode == -1) {
      Set<Integer> alternativeDataNodes =
          fakeToRealIdMap.keySet().stream()
              .filter(fakeId -> !remainDataNodesFakeId.contains(fakeId))
              .collect(Collectors.toSet());
      return availableDataNodeMap.get(alternativeDataNodes.stream().findAny().orElse(null));
    }

    return availableDataNodeMap.get(fakeToRealIdMap.get(targetDataNode));
  }

  private void prepare(
      int replicationFactor,
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap,
      List<TRegionReplicaSet> allocatedRegionGroups,
      TRegionReplicaSet remainReplicaSet) {
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

    remainDataNodesFakeId =
        remainReplicaSet.dataNodeLocations.stream()
            .map(TDataNodeLocation::getDataNodeId)
            .map(realToFakeIdMap::get)
            .collect(Collectors.toSet());

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

    this.bestValue = new Pair<>(Integer.MAX_VALUE, Integer.MAX_VALUE);
  }

  private void searchTargetDataNode(Map<Integer, Double> freeDiskSpaceMap) {
    List<DataNodeEntry> entryList = new ArrayList<>();

    for (int i = 0; i < dataNodeNum; i++) {
      if (remainDataNodesFakeId.contains(i)) {
        continue;
      }
      if (regionCounter[i] < regionPerDataNode) {
        entryList.add(
            new DataNodeEntry(i, regionCounter[i], freeDiskSpaceMap.get(fakeToRealIdMap.get(i))));
      }
    }
    if (entryList.isEmpty()) {
      return;
    }
    Collections.sort(entryList);

    int[] alternativeReplica = new int[replicationFactor];

    int pos = 0;
    for (Integer integer : remainDataNodesFakeId) {
      alternativeReplica[pos] = integer;
      pos++;
    }

    double minFreeDiskSpace = entryList.get(0).freeDiskSpace;
    for (DataNodeEntry dataNodeEntry : entryList) {
      if (dataNodeEntry.freeDiskSpace > minFreeDiskSpace) {
        break;
      }
      alternativeReplica[replicationFactor - 1] = dataNodeEntry.dataNodeId;
      Pair<Integer, Integer> currentValue = valuation(alternativeReplica);
      if (currentValue.left < bestValue.left
          || (currentValue.left.equals(bestValue.left) && currentValue.right < bestValue.right)) {
        bestValue = currentValue;
        targetDataNode = dataNodeEntry.dataNodeId;
      }
    }
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
}
