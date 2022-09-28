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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Map.Entry.comparingByValue;

/** Allocate Region Greedily */
public class GreedyRegionAllocator implements IRegionAllocator {

  public GreedyRegionAllocator() {}

  @Override
  public TRegionReplicaSet allocateRegion(
      List<TDataNodeConfiguration> targetDataNodes,
      List<TRegionReplicaSet> allocatedRegions,
      int replicationFactor,
      TConsensusGroupId consensusGroupId) {
    // Build weightList order by number of regions allocated asc
    List<TDataNodeLocation> weightList = buildWeightList(targetDataNodes, allocatedRegions);
    return new TRegionReplicaSet(
        consensusGroupId,
        weightList.stream().limit(replicationFactor).collect(Collectors.toList()));
  }

  private List<TDataNodeLocation> buildWeightList(
      List<TDataNodeConfiguration> onlineDataNodes, List<TRegionReplicaSet> allocatedRegions) {
    Map<TDataNodeLocation, Integer> countMap = new HashMap<>();
    for (TDataNodeConfiguration dataNodeInfo : onlineDataNodes) {
      countMap.put(dataNodeInfo.getLocation(), 0);
    }

    for (TRegionReplicaSet regionReplicaSet : allocatedRegions) {
      for (TDataNodeLocation dataNodeLocation : regionReplicaSet.getDataNodeLocations()) {
        countMap.computeIfPresent(dataNodeLocation, (dataNode, count) -> (count + 1));
      }
    }
    return countMap.entrySet().stream()
        .sorted(comparingByValue())
        .map(e -> e.getKey().deepCopy())
        .collect(Collectors.toList());
  }
}
