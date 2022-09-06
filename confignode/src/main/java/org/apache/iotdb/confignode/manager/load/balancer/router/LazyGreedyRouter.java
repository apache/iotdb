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
package org.apache.iotdb.confignode.manager.load.balancer.router;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * The LazyGreedyRouter mainly applies to the MultiLeader consensus protocol, it will make the
 * number of leaders in each online DataNode as equal as possible
 */
public class LazyGreedyRouter implements IRouter {

  /** Set<DataNodeId> which stores the DataNodes that unable to provide service */
  private final Set<Integer> disabledDataNodes;

  private final Map<TConsensusGroupId, TRegionReplicaSet> routeMap;

  public LazyGreedyRouter() {
    this.disabledDataNodes = Collections.synchronizedSet(new HashSet<>());
    this.routeMap = new ConcurrentHashMap<>();
  }

  /**
   * Update the disabledDataNodes cache in LazyRandomRouter
   *
   * @param disabledDataNodes DataNodes whose status aren't Running
   */
  public void updateDisabledDataNodes(List<TDataNodeConfiguration> disabledDataNodes) {
    synchronized (this.disabledDataNodes) {
      this.disabledDataNodes.clear();
      this.disabledDataNodes.addAll(
          disabledDataNodes.stream()
              .map(dataNodeConfiguration -> dataNodeConfiguration.getLocation().getDataNodeId())
              .collect(Collectors.toList()));
    }
  }

  @Override
  public Map<TConsensusGroupId, TRegionReplicaSet> genLatestRegionRouteMap(
      List<TRegionReplicaSet> replicaSets) {
    synchronized (disabledDataNodes) {
      // Map<DataNodeId, leaderCount> Count the number of leaders in each DataNodes
      Map<Integer, Integer> leaderCounter = new HashMap<>();
      Map<TConsensusGroupId, TRegionReplicaSet> result = new ConcurrentHashMap<>();
      List<TRegionReplicaSet> updateReplicas = new ArrayList<>();

      for (TRegionReplicaSet replicaSet : replicaSets) {
        if (routeEntryNeedsUpdate(replicaSet)) {
          // The greedy algorithm should be performed lastly
          updateReplicas.add(replicaSet);
        } else {
          // Update counter
          leaderCounter.compute(
              routeMap.get(replicaSet.getRegionId()).getDataNodeLocations().get(0).getDataNodeId(),
              (dataNodeId, counter) -> (counter == null ? 1 : counter + 1));
          // Record the unaltered results
          result.put(replicaSet.getRegionId(), routeMap.get(replicaSet.getRegionId()));
        }
      }

      for (TRegionReplicaSet replicaSet : updateReplicas) {
        updateRouteEntry(replicaSet, leaderCounter);
        result.put(replicaSet.getRegionId(), routeMap.get(replicaSet.getRegionId()));
      }

      return result;
    }
  }

  /** Check whether the specific RegionReplicaSet's routing policy needs update */
  private boolean routeEntryNeedsUpdate(TRegionReplicaSet replicaSet) {
    TConsensusGroupId groupId = replicaSet.getRegionId();
    if (!routeMap.containsKey(groupId)) {
      // The RouteEntry needs update when it is not recorded yet
      return true;
    }

    Set<Integer> cacheReplicaSet =
        routeMap.get(groupId).getDataNodeLocations().stream()
            .map(TDataNodeLocation::getDataNodeId)
            .collect(Collectors.toSet());
    Set<Integer> inputReplicaSet =
        replicaSet.getDataNodeLocations().stream()
            .map(TDataNodeLocation::getDataNodeId)
            .collect(Collectors.toSet());
    if (!cacheReplicaSet.equals(inputReplicaSet)) {
      // The RouteEntry needs update when the cached record is outdated
      return true;
    }

    // The RouteEntry needs update when the status of DataNode corresponding to the first priority
    // is unknown
    return disabledDataNodes.contains(
        routeMap.get(groupId).getDataNodeLocations().get(0).getDataNodeId());
  }

  private void updateRouteEntry(TRegionReplicaSet replicaSet, Map<Integer, Integer> leaderCounter) {
    TRegionReplicaSet newRouteEntry = new TRegionReplicaSet(replicaSet);
    Collections.shuffle(newRouteEntry.getDataNodeLocations(), new Random());

    // Greedily select the leader replica
    int leaderIndex = -1;
    int locateLeaderCount = Integer.MAX_VALUE;
    for (int i = 0; i < newRouteEntry.getDataNodeLocationsSize(); i++) {
      int currentDataNodeId = newRouteEntry.getDataNodeLocations().get(i).getDataNodeId();
      if (!disabledDataNodes.contains(currentDataNodeId)
          && leaderCounter.getOrDefault(currentDataNodeId, 0) < locateLeaderCount) {
        leaderIndex = i;
        locateLeaderCount = leaderCounter.getOrDefault(currentDataNodeId, 0);
      }
    }

    if (leaderIndex == -1) {
      // Prevent corner case that all DataNodes fail down
      leaderIndex = 0;
    }

    // Swap leader replica and update statistic
    Collections.swap(newRouteEntry.getDataNodeLocations(), 0, leaderIndex);
    leaderCounter.compute(
        newRouteEntry.getDataNodeLocations().get(0).getDataNodeId(),
        (dataNodeId, counter) -> (counter == null ? 1 : counter + 1));
    routeMap.put(newRouteEntry.getRegionId(), newRouteEntry);
  }

  public Map<TConsensusGroupId, TRegionReplicaSet> getRouteMap() {
    return routeMap;
  }
}
