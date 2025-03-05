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
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ClusterTopology {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterTopology.class);
  private final TDataNodeLocation myself;
  private volatile Map<TDataNodeLocation, Set<TDataNodeLocation>> topologyMap;
  private volatile boolean isPartitioned;

  public static ClusterTopology getInstance() {
    return ClusterTopologyHolder.INSTANCE;
  }

  public TRegionReplicaSet getReachableSet(TRegionReplicaSet origin) {
    if (!isPartitioned || origin == null) {
      return origin;
    }
    final List<TDataNodeLocation> locations = new ArrayList<>(origin.getDataNodeLocations());
    final Set<TDataNodeLocation> reachable = topologyMap.get(myself);
    locations.retainAll(reachable);
    return new TRegionReplicaSet(origin.getRegionId(), locations);
  }

  public <T> Set<Map.Entry<TRegionReplicaSet, T>> filterReachableCandidates(
      Set<Map.Entry<TRegionReplicaSet, T>> input) {
    final List<TRegionReplicaSet> allSets =
        input.stream().map(Map.Entry::getKey).collect(Collectors.toList());
    final List<TRegionReplicaSet> candidates = getReachableCandidates(allSets);
    final Map<TConsensusGroupId, TRegionReplicaSet> newSet = new HashMap<>();
    candidates.forEach(set -> newSet.put(set.getRegionId(), set));
    final Map<TRegionReplicaSet, T> candidateMap = new HashMap<>();
    for (final Map.Entry<TRegionReplicaSet, T> entry : input) {
      final TConsensusGroupId gid = entry.getKey().getRegionId();
      if (newSet.containsKey(gid)) {
        candidateMap.put(newSet.get(gid), entry.getValue());
      }
    }
    return candidateMap.entrySet();
  }

  public List<TRegionReplicaSet> getReachableCandidates(List<TRegionReplicaSet> all) {
    if (!isPartitioned || all == null || all.isEmpty()) {
      return all;
    }
    final Map<TDataNodeLocation, Set<TDataNodeLocation>> topologyMapCurrent =
        Collections.unmodifiableMap(this.topologyMap);

    // brute-force search to select DataNode candidates that can communicate to all
    // TRegionReplicaSets
    final List<TDataNodeLocation> dataNodeCandidates = new ArrayList<>();
    for (final TDataNodeLocation datanode : topologyMapCurrent.keySet()) {
      boolean reachableToAllSets = true;
      final Set<TDataNodeLocation> datanodeReachableToThis = topologyMapCurrent.get(datanode);
      for (final TRegionReplicaSet replicaSet : all) {
        final List<TDataNodeLocation> replicaSetLocations =
            new ArrayList<>(replicaSet.getDataNodeLocations());
        replicaSetLocations.retainAll(datanodeReachableToThis);
        reachableToAllSets = !replicaSetLocations.isEmpty();
      }
      if (reachableToAllSets) {
        dataNodeCandidates.add(datanode);
      }
    }

    // select TRegionReplicaSet candidates whose DataNode Locations contain at least one
    // allReachableDataNodes
    final List<TRegionReplicaSet> reachableSetCandidates = new ArrayList<>();
    for (final TRegionReplicaSet replicaSet : all) {
      final List<TDataNodeLocation> commonLocations =
          new ArrayList<>(replicaSet.getDataNodeLocations());
      commonLocations.retainAll(dataNodeCandidates);
      if (!commonLocations.isEmpty()) {
        final TRegionReplicaSet validCandidate =
            new TRegionReplicaSet(replicaSet.getRegionId(), commonLocations);
        reachableSetCandidates.add(validCandidate);
      }
    }

    return reachableSetCandidates;
  }

  public void updateTopology(Map<TDataNodeLocation, Set<TDataNodeLocation>> latest) {
    this.topologyMap = latest;
    this.isPartitioned = latest.get(myself).size() != latest.keySet().size();
    if (isPartitioned) {
      final Set<TDataNodeLocation> allDataLocations = new HashSet<>(latest.keySet());
      allDataLocations.removeAll(latest.get(myself));
      final String partitioned =
          allDataLocations.stream()
              .map(TDataNodeLocation::getDataNodeId)
              .collect(
                  StringBuilder::new, (sb, id) -> sb.append(",").append(id), StringBuilder::append)
              .toString();
      LOGGER.info("This DataNode {} is partitioned with [{}]", myself.getDataNodeId(), partitioned);
    }
  }

  private ClusterTopology() {
    this.myself = IoTDBDescriptor.getInstance().getConfig().generateLocalDataNodeLocation();
    this.isPartitioned = false;
    this.topologyMap = null;
  }

  private static class ClusterTopologyHolder {

    private static final ClusterTopology INSTANCE = new ClusterTopology();

    private ClusterTopologyHolder() {}
  }
}
