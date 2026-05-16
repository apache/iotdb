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

package org.apache.iotdb.db.queryengine.plan;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.plan.planner.exceptions.ReplicaSetUnreachableException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Tracks this DataNode's view of cluster network topology for partition-aware query planning. Only
 * stores the set of DataNodes reachable from this node, pushed by ConfigNode's TopologyService.
 */
public class ClusterTopology {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterTopology.class);
  private final Integer myself;
  private final AtomicReference<Map<Integer, TDataNodeLocation>> dataNodes;

  /** DataNode IDs reachable from this node. Empty means not yet probed by TopologyService. */
  private final AtomicReference<Set<Integer>> myReachableNodes;

  private final AtomicBoolean isPartitioned = new AtomicBoolean();

  public static ClusterTopology getInstance() {
    return ClusterTopologyHolder.INSTANCE;
  }

  /** Filters a replica set to only include DataNodes reachable from this node. */
  public TRegionReplicaSet getValidatedReplicaSet(TRegionReplicaSet origin) {
    if (!isPartitioned.get() || origin == null) {
      return origin;
    }
    final Set<Integer> reachable = myReachableNodes.get();
    if (reachable.isEmpty()) {
      return origin;
    }
    final List<TDataNodeLocation> locations = new ArrayList<>();
    for (final TDataNodeLocation location : origin.getDataNodeLocations()) {
      if (reachable.contains(location.getDataNodeId())) {
        locations.add(location);
      }
    }
    return new TRegionReplicaSet(origin.getRegionId(), locations);
  }

  /** Filters region-to-scan mappings, keeping only replicas reachable from this node. */
  public <T> Set<Map.Entry<TRegionReplicaSet, T>> filterReachableCandidates(
      Set<Map.Entry<TRegionReplicaSet, T>> input) {
    if (!isPartitioned.get()) {
      return input;
    }
    final List<TRegionReplicaSet> allSets =
        input.stream().map(Map.Entry::getKey).collect(Collectors.toList());
    final List<TRegionReplicaSet> candidates =
        getReachableCandidates(
            allSets.stream().filter(TRegionReplicaSet::isSetRegionId).collect(Collectors.toList()));
    final Map<TConsensusGroupId, TRegionReplicaSet> newMap = new HashMap<>();
    candidates.forEach(set -> newMap.put(set.getRegionId(), set));
    final Map<TRegionReplicaSet, T> candidateMap = new HashMap<>();
    for (final Map.Entry<TRegionReplicaSet, T> entry : input) {
      final TConsensusGroupId gid = entry.getKey().getRegionId();
      if (gid == null) {
        candidateMap.put(DataPartition.NOT_ASSIGNED, entry.getValue());
        continue;
      }
      final TRegionReplicaSet replicaSet = newMap.get(gid);
      if (replicaSet != null) {
        candidateMap.put(replicaSet, entry.getValue());
      } else {
        // Topology not yet available for this region — return original to avoid silent data loss
        candidateMap.put(entry.getKey(), entry.getValue());
      }
    }
    return candidateMap.entrySet();
  }

  private List<TRegionReplicaSet> getReachableCandidates(List<TRegionReplicaSet> all) {
    if (!isPartitioned.get() || all == null || all.isEmpty()) {
      return all;
    }
    for (TRegionReplicaSet replicaSet : all) {
      if (replicaSet.getDataNodeLocationsSize() == 0) {
        throw new ReplicaSetUnreachableException(replicaSet);
      }
    }

    final Set<Integer> reachable = this.myReachableNodes.get();
    if (reachable.isEmpty()) {
      return all;
    }
    final Map<Integer, TDataNodeLocation> dataNodesCurrent = this.dataNodes.get();

    final List<TRegionReplicaSet> reachableSetCandidates = new ArrayList<>();
    for (final TRegionReplicaSet replicaSet : all) {
      final List<TDataNodeLocation> validLocations =
          replicaSet.getDataNodeLocations().stream()
              .filter(loc -> reachable.contains(loc.getDataNodeId()))
              .map(loc -> dataNodesCurrent.getOrDefault(loc.getDataNodeId(), loc))
              .collect(Collectors.toList());
      if (!validLocations.isEmpty()) {
        reachableSetCandidates.add(new TRegionReplicaSet(replicaSet.getRegionId(), validLocations));
      } else {
        // No reachable replica — return full set so upper layer can handle or report the error
        reachableSetCandidates.add(replicaSet);
      }
    }
    return reachableSetCandidates;
  }

  /** Called by ConfigNode's TopologyService push. Updates this node's reachable set. */
  public void updateTopology(
      final Map<Integer, TDataNodeLocation> dataNodes, Map<Integer, Set<Integer>> latestTopology) {
    final Set<Integer> newReachable = latestTopology.getOrDefault(myself, Collections.emptySet());
    final Set<Integer> oldReachable = this.myReachableNodes.get();

    if (!newReachable.equals(oldReachable)) {
      LOGGER.info(
          DataNodeQueryMessages.TOPOLOGY_LATEST_VIEW_FROM_CONFIG_NODE, myself, newReachable);
      for (int toId : dataNodes.keySet()) {
        boolean wasReachable = oldReachable.contains(toId);
        boolean nowReachable = newReachable.contains(toId);
        if (wasReachable != nowReachable) {
          LOGGER.info(
              "[Topology] DataNode {} is now {} to myself({})",
              toId,
              nowReachable ? "reachable" : "unreachable",
              myself);
        }
      }
      this.myReachableNodes.set(newReachable);
    }
    this.dataNodes.set(dataNodes);
    if (newReachable.isEmpty()) {
      this.isPartitioned.set(false);
    } else {
      this.isPartitioned.set(newReachable.size() != dataNodes.size());
    }
  }

  private ClusterTopology() {
    this.myself =
        IoTDBDescriptor.getInstance().getConfig().generateLocalDataNodeLocation().getDataNodeId();
    this.isPartitioned.set(false);
    this.myReachableNodes = new AtomicReference<>(Collections.emptySet());
    this.dataNodes = new AtomicReference<>(Collections.emptyMap());
  }

  private static class ClusterTopologyHolder {

    private static final ClusterTopology INSTANCE = new ClusterTopology();

    private ClusterTopologyHolder() {}
  }
}
