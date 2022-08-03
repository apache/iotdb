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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class LazyRandomRouter implements IRouter {

  private static final Logger LOGGER = LoggerFactory.getLogger(LazyRandomRouter.class);

  // Set<DataNodeId>
  private final Set<Integer> unknownDataNodes;
  private final Map<TConsensusGroupId, TRegionReplicaSet> routeMap;

  public LazyRandomRouter() {
    this.unknownDataNodes = Collections.synchronizedSet(new HashSet<>());
    this.routeMap = new ConcurrentHashMap<>();
  }

  /**
   * Update unknownDataNodes cache in LazyRandomRouter
   *
   * @param unknownDataNodes DataNodes that have unknown status
   */
  public void updateUnknownDataNodes(List<TDataNodeConfiguration> unknownDataNodes) {
    synchronized (this.unknownDataNodes) {
      this.unknownDataNodes.clear();
      this.unknownDataNodes.addAll(
          unknownDataNodes.stream()
              .map(dataNodeConfiguration -> dataNodeConfiguration.getLocation().getDataNodeId())
              .collect(Collectors.toList()));
    }
  }

  @Override
  public Map<TConsensusGroupId, TRegionReplicaSet> genLatestRegionRouteMap(
      List<TRegionReplicaSet> replicaSets) {
    synchronized (unknownDataNodes) {
      Map<TConsensusGroupId, TRegionReplicaSet> result = new ConcurrentHashMap<>();

      replicaSets.forEach(
          replicaSet -> {
            if (routeEntryNeedsUpdate(replicaSet.getRegionId(), replicaSet)) {
              updateRouteEntry(replicaSet.getRegionId(), replicaSet);
            }
            result.put(replicaSet.getRegionId(), routeMap.get(replicaSet.getRegionId()));
          });

      return result;
    }
  }

  private boolean routeEntryNeedsUpdate(TConsensusGroupId groupId, TRegionReplicaSet replicaSet) {
    if (!routeMap.containsKey(groupId)) {
      // The RouteEntry needs update when it is not recorded yet
      LOGGER.info("[latestRegionRouteMap] Update {} because it's a new Region", groupId);
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
      LOGGER.info("[latestRegionRouteMap] Update {} because it's been migrated", groupId);
      return true;
    }

    // The RouteEntry needs update when the status of DataNode corresponding to the first priority
    // is unknown
    if (unknownDataNodes.contains(
        routeMap.get(groupId).getDataNodeLocations().get(0).getDataNodeId())) {
      LOGGER.info(
          "[latestRegionRouteMap] Update {} because the first DataNode is unknown", groupId);
      return true;
    }

    return false;
  }

  private void updateRouteEntry(TConsensusGroupId groupId, TRegionReplicaSet regionReplicaSet) {
    TRegionReplicaSet newRouteEntry = new TRegionReplicaSet(regionReplicaSet);
    do {
      Collections.shuffle(newRouteEntry.getDataNodeLocations());
    } while (unknownDataNodes.contains(
        newRouteEntry.getDataNodeLocations().get(0).getDataNodeId()));
    routeMap.put(groupId, newRouteEntry);
  }
}
