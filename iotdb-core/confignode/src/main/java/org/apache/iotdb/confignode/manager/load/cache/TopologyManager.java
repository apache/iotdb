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

package org.apache.iotdb.confignode.manager.load.cache;

import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * TopologyManager is responsible for maintaining DataNode Cluster Topology. 1. Each DataNode will
 * report local connectivity through heartbeat exchanges. 2. TopologyManager will put together all
 * the reports and form a cluster-wise topology 3. TopologyManager will push the latest topology to
 * each DataNodes via heartbeat exchanges.
 */
public class TopologyManager {
  // Map<NodeId, List<NodeId> that is reachable to this node>
  private final Map<Integer, Set<Integer>> reachableGraph;
  // if topologyChanged, the latest topology will be broadcast to all DataNodes via next heartbeat
  private final AtomicBoolean topologyChanged;

  public TopologyManager() {
    this.reachableGraph = new ConcurrentHashMap<>();
    this.topologyChanged = new AtomicBoolean(false);
  }

  public void clear() {
    reachableGraph.clear();
  }

  public void init(final List<TDataNodeConfiguration> registeredDataNodes) {
    clear();
    // Init the reachableGraph
    final Set<Integer> allIds =
        registeredDataNodes.stream()
            .map(d -> d.getLocation().getDataNodeId())
            .collect(Collectors.toSet());
    allIds.forEach(
        nodeId -> {
          reachableGraph.put(nodeId, Collections.synchronizedSet(new HashSet<>(allIds)));
        });
    topologyChanged.set(true);
  }

  public void addDataNode(int nodeId) {
    // newly added DataNode is reachable to every other DataNodes by default
    reachableGraph.values().forEach(reachable -> reachable.add(nodeId));
    final Set<Integer> allNodeIds = new HashSet<>(reachableGraph.keySet());
    allNodeIds.add(nodeId);
    reachableGraph.put(nodeId, Collections.synchronizedSet(allNodeIds));
    topologyChanged.set(true);
  }

  public void removeDataNode(int nodeId) {
    reachableGraph.remove(nodeId);
    reachableGraph.values().forEach(reachable -> reachable.remove(nodeId));
    topologyChanged.set(true);
  }

  public Optional<Map<Integer, Set<Integer>>> getReachableGraph() {
    if (topologyChanged.compareAndSet(true, false)) {
      return Optional.of(Collections.unmodifiableMap(reachableGraph));
    }
    return Optional.empty();
  }

  public void updateConnectivityMap(int nodeId, Map<TDataNodeLocation, Boolean> connectivityMap) {
    final Set<Integer> reachable =
        connectivityMap.entrySet().stream()
            .filter(Map.Entry::getValue)
            .map(entry -> entry.getKey().getDataNodeId())
            .collect(Collectors.toSet());
    if (!reachableGraph.get(nodeId).equals(reachable)) {
      reachableGraph.put(nodeId, reachable);
      topologyChanged.set(true);
    }
  }
}
