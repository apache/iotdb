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
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.util.Map.Entry.comparingByValue;

/** Allocate Region Greedily */
public class GreedyRegionGroupAllocator implements IRegionGroupAllocator {

  private static final AtomicInteger ZERO = new AtomicInteger(0);

  public GreedyRegionGroupAllocator() {
    // Empty constructor
  }

  @Override
  public TRegionReplicaSet generateOptimalRegionReplicasDistribution(
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap,
      Map<Integer, Double> freeDiskSpaceMap,
      List<TRegionReplicaSet> allocatedRegionGroups,
      int replicationFactor,
      TConsensusGroupId consensusGroupId) {
    // Build weightList order by number of regions allocated asc
    List<TDataNodeLocation> weightList =
        buildWeightList(availableDataNodeMap, freeDiskSpaceMap, allocatedRegionGroups);
    return new TRegionReplicaSet(
        consensusGroupId,
        weightList.stream().limit(replicationFactor).collect(Collectors.toList()));
  }

  private List<TDataNodeLocation> buildWeightList(
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap,
      Map<Integer, Double> freeDiskSpaceMap,
      List<TRegionReplicaSet> allocatedRegionGroups) {
    // Map<DataNodeId, Region count>
    Map<Integer, AtomicInteger> regionCounter = new ConcurrentHashMap<>();
    allocatedRegionGroups.forEach(
        regionReplicaSet ->
            regionReplicaSet
                .getDataNodeLocations()
                .forEach(
                    dataNodeLocation ->
                        regionCounter
                            .computeIfAbsent(
                                dataNodeLocation.getDataNodeId(), empty -> new AtomicInteger(0))
                            .getAndIncrement()));

    /* Construct priority map */
    Map<TDataNodeLocation, Pair<Integer, Double>> priorityMap = new ConcurrentHashMap<>();
    availableDataNodeMap
        .keySet()
        .forEach(
            dataNodeId ->
                priorityMap.put(
                    availableDataNodeMap.get(dataNodeId).getLocation(),
                    new Pair<>(
                        regionCounter.getOrDefault(dataNodeId, ZERO).get(),
                        freeDiskSpaceMap.getOrDefault(dataNodeId, 0d))));

    return priorityMap.entrySet().stream()
        .sorted(
            comparingByValue(
                (o1, o2) ->
                    !Objects.equals(o1.getLeft(), o2.getLeft())
                        // Compare the first key(The number of Regions) by ascending order
                        ? o1.getLeft() - o2.getLeft()
                        // Compare the second key(The free disk space) by descending order
                        : (int) (o2.getRight() - o1.getRight())))
        .map(entry -> entry.getKey().deepCopy())
        .collect(Collectors.toList());
  }
}
