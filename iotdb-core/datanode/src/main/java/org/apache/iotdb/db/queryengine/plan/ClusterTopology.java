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

public class ClusterTopology {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterTopology.class);
  private final Integer myself;
  private final AtomicReference<Map<Integer, TDataNodeLocation>> dataNodes;
  private final AtomicReference<Map<Integer, Set<Integer>>> topologyMap;
  private final AtomicBoolean isPartitioned = new AtomicBoolean();

  public static ClusterTopology getInstance() {
    return ClusterTopologyHolder.INSTANCE;
  }

  public TRegionReplicaSet getValidatedReplicaSet(TRegionReplicaSet origin) {
    if (!isPartitioned.get() || origin == null) {
      return origin;
    }
    final Set<Integer> reachableToMyself =
        Collections.unmodifiableSet(topologyMap.get().get(myself));
    final List<TDataNodeLocation> locations = new ArrayList<>();
    for (final TDataNodeLocation location : origin.getDataNodeLocations()) {
      if (reachableToMyself.contains(location.getDataNodeId())) {
        locations.add(location);
      }
    }
    return new TRegionReplicaSet(origin.getRegionId(), locations);
  }

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
      }
    }
    return candidateMap.entrySet();
  }

  private List<TRegionReplicaSet> getReachableCandidates(List<TRegionReplicaSet> all) {
    if (!isPartitioned.get() || all == null || all.isEmpty()) {
      return all;
    }
    for (TRegionReplicaSet replicaSet : all) {
      // some TRegionReplicaSet is unreachable since all DataNodes are down
      if (replicaSet.getDataNodeLocationsSize() == 0) {
        throw new ReplicaSetUnreachableException(replicaSet);
      }
    }
    final Map<Integer, Set<Integer>> topologyMapCurrent =
        Collections.unmodifiableMap(this.topologyMap.get());

    // brute-force search to select DataNode candidates that can communicate to all
    // TRegionReplicaSets
    final List<Integer> dataNodeCandidates = new ArrayList<>();
    for (final Integer datanode : topologyMapCurrent.keySet()) {
      boolean reachableToAllSets = true;
      final Set<Integer> datanodeReachableToThis = topologyMapCurrent.get(datanode);
      for (final TRegionReplicaSet replicaSet : all) {
        final List<Integer> replicaNodeLocations =
            replicaSet.getDataNodeLocations().stream()
                .map(TDataNodeLocation::getDataNodeId)
                .collect(Collectors.toList());
        replicaNodeLocations.retainAll(datanodeReachableToThis);
        reachableToAllSets = !replicaNodeLocations.isEmpty();
      }
      if (reachableToAllSets) {
        dataNodeCandidates.add(datanode);
      }
    }

    // select TRegionReplicaSet candidates whose DataNode Locations contain at least one
    // allReachableDataNodes
    final List<TRegionReplicaSet> reachableSetCandidates = new ArrayList<>();
    for (final TRegionReplicaSet replicaSet : all) {
      final List<Integer> commonLocations =
          replicaSet.getDataNodeLocations().stream()
              .map(TDataNodeLocation::getDataNodeId)
              .collect(Collectors.toList());
      commonLocations.retainAll(dataNodeCandidates);
      if (!commonLocations.isEmpty()) {
        final List<TDataNodeLocation> validLocations =
            commonLocations.stream().map(dataNodes.get()::get).collect(Collectors.toList());
        final TRegionReplicaSet validCandidate =
            new TRegionReplicaSet(replicaSet.getRegionId(), validLocations);
        reachableSetCandidates.add(validCandidate);
      }
    }

    return reachableSetCandidates;
  }

  public void updateTopology(
      final Map<Integer, TDataNodeLocation> dataNodes, Map<Integer, Set<Integer>> latestTopology) {
    if (!latestTopology.equals(topologyMap.get())) {
      LOGGER.info("[Topology] latest view from config-node: {}", latestTopology);
      for (int fromId : dataNodes.keySet()) {
        for (int toId : dataNodes.keySet()) {
          boolean originReachable =
              latestTopology.getOrDefault(fromId, Collections.emptySet()).contains(toId);
          boolean newReachable =
              latestTopology.getOrDefault(fromId, Collections.emptySet()).contains(toId);
          if (originReachable != newReachable) {
            LOGGER.info(
                "[Topology] Topology of DataNode {} is now {} to DataNode {}",
                fromId,
                newReachable ? "reachable" : "unreachable",
                toId);
          }
        }
      }
      this.topologyMap.set(latestTopology);
    }
    this.dataNodes.set(dataNodes);
    if (latestTopology.get(myself) == null || latestTopology.get(myself).isEmpty()) {
      // latest topology doesn't include this node information.
      // This mostly happens when this node just starts and haven't report connection details.
      this.isPartitioned.set(false);
    } else {
      this.isPartitioned.set(latestTopology.get(myself).size() != latestTopology.size());
    }
  }

  private ClusterTopology() {
    this.myself =
        IoTDBDescriptor.getInstance().getConfig().generateLocalDataNodeLocation().getDataNodeId();
    this.isPartitioned.set(false);
    this.topologyMap = new AtomicReference<>(Collections.emptyMap());
    this.dataNodes = new AtomicReference<>(Collections.emptyMap());
  }

  private static class ClusterTopologyHolder {

    private static final ClusterTopology INSTANCE = new ClusterTopology();

    private ClusterTopologyHolder() {}
  }
}
