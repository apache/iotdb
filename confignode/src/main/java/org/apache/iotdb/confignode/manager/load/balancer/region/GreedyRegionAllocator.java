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
import org.apache.iotdb.common.rpc.thrift.TDataNodeInfo;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Map.Entry.comparingByValue;

/** Allocate Region Greedily */
public class GreedyRegionAllocator implements IRegionAllocator {
  private List<TDataNodeLocation> weightList;

  public GreedyRegionAllocator() {}

  @Override
  public TRegionReplicaSet allocateRegion(
      List<TDataNodeInfo> onlineDataNodes,
      List<TRegionReplicaSet> allocatedRegions,
      int replicationFactor,
      TConsensusGroupId consensusGroupId) {
    // Build weightList order by number of regions allocated asc
    buildWeightList(onlineDataNodes, allocatedRegions);
    return new TRegionReplicaSet(
        consensusGroupId,
        weightList.stream().limit(replicationFactor).collect(Collectors.toList()));
  }

  private void buildWeightList(
      List<TDataNodeInfo> onlineDataNodes, List<TRegionReplicaSet> allocatedRegions) {
    this.weightList = new ArrayList<>();
    Map<TDataNodeLocation, Integer> countMap = new HashMap<>();
    for (TDataNodeInfo dataNodeInfo : onlineDataNodes) {
      countMap.put(dataNodeInfo.getLocation(), 0);
    }

    for (TRegionReplicaSet regionReplicaSet : allocatedRegions) {
      for (TDataNodeLocation dataNodeLocation : regionReplicaSet.getDataNodeLocations()) {
        countMap.computeIfPresent(dataNodeLocation, (dataNode, count) -> (count + 1));
      }
    }

    Map<TDataNodeLocation, Integer> sortedCountMap =
        countMap.entrySet().stream()
            .sorted(comparingByValue())
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    Map.Entry::getValue,
                    (oldV, newV) -> oldV,
                    LinkedHashMap::new));

    for (Map.Entry<TDataNodeLocation, Integer> countEntry : sortedCountMap.entrySet()) {
      weightList.add(countEntry.getKey().deepCopy());
    }
  }
}
