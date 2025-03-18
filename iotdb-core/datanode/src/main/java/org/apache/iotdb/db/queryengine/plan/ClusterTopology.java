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
  private final Integer myself;
  private volatile Map<Integer, TDataNodeLocation> dataNodes;
  private volatile Map<Integer, Set<Integer>> topologyMap;
  private volatile boolean isPartitioned;

  public static ClusterTopology getInstance() {
    return ClusterTopologyHolder.INSTANCE;
  }

  public TRegionReplicaSet getReachableSet(TRegionReplicaSet origin) {
    if (!isPartitioned || origin == null) {
      return origin;
    }
    final Set<Integer> reachableToMyself = Collections.unmodifiableSet(topologyMap.get(myself));
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
    final Map<Integer, Set<Integer>> topologyMapCurrent =
        Collections.unmodifiableMap(this.topologyMap);

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
            commonLocations.stream().map(dataNodes::get).collect(Collectors.toList());
        final TRegionReplicaSet validCandidate =
            new TRegionReplicaSet(replicaSet.getRegionId(), validLocations);
        reachableSetCandidates.add(validCandidate);
      }
    }

    return reachableSetCandidates;
  }

  public void updateTopology(
      final Map<Integer, TDataNodeLocation> dataNodes, Map<Integer, Set<Integer>> latestTopology) {
    if (!latestTopology.equals(topologyMap)) {
      LOGGER.info("[Topology] latest view from config-node: {}", latestTopology);
    }
    this.dataNodes = dataNodes;
    this.topologyMap = latestTopology;
    if (latestTopology.get(myself) == null || latestTopology.get(myself).isEmpty()) {
      // latest topology doesn't include this node information.
      // This mostly happens when this node just starts and haven't report connection details.
      this.isPartitioned = false;
    } else {
      this.isPartitioned = latestTopology.get(myself).size() != latestTopology.keySet().size();
    }
    if (isPartitioned && LOGGER.isDebugEnabled()) {
      final Set<Integer> allDataLocations = new HashSet<>(latestTopology.keySet());
      allDataLocations.removeAll(latestTopology.get(myself));
      final String partitioned =
          allDataLocations.stream()
              .collect(
                  StringBuilder::new, (sb, id) -> sb.append(",").append(id), StringBuilder::append)
              .toString();
      LOGGER.debug("This DataNode {} is partitioned with [{}]", myself, partitioned);
    }
  }

  private ClusterTopology() {
    this.myself =
        IoTDBDescriptor.getInstance().getConfig().generateLocalDataNodeLocation().getDataNodeId();
    this.isPartitioned = false;
    this.topologyMap = null;
  }

  private static class ClusterTopologyHolder {

    private static final ClusterTopology INSTANCE = new ClusterTopology();

    private ClusterTopologyHolder() {}
  }
}
