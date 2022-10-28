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
package org.apache.iotdb.confignode.manager.load.balancer.router.mcf;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.utils.TestOnly;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/** Minimum cost flow */
public class MCFLeaderBalancer {

  private static final int INFINITY = Integer.MAX_VALUE;

  private final Map<TConsensusGroupId, TRegionReplicaSet> regionReplicaSetMap;
  private final Map<TConsensusGroupId, Integer> regionLeaderMap;
  private final Set<Integer> disabledDataNodeSet;

  /** Graph nodes */
  // Super source node
  private static final int sNode = 0;
  // Super terminal node
  private static final int tNode = 1;
  // Maximum index of graph nodes
  private int maxNode = tNode + 1;
  // Map<RegionGroupId, rNode>
  private final Map<TConsensusGroupId, Integer> rNodeMap;
  // Map<DataNodeId, >
  private final Map<Integer, Integer> dNodeMap;
  private final Map<Integer, Integer> dNodeReflect;

  private int maxEdge = 0;
  private final List<MCFEdge> mcfEdges;
  private int[] nodeHeadEdge;
  private int[] nodeCurrentEdge;

  private boolean[] isNodeVisited;
  private int[] nodeMinimumCost;

  private int maximumFlow = 0;
  private int minimumCost = 0;

  public MCFLeaderBalancer(
      Map<TConsensusGroupId, TRegionReplicaSet> regionReplicaSetMap,
      Map<TConsensusGroupId, Integer> regionLeaderMap,
      Set<Integer> disabledDataNodeSet) {
    this.regionReplicaSetMap = regionReplicaSetMap;
    this.regionLeaderMap = regionLeaderMap;
    this.disabledDataNodeSet = disabledDataNodeSet;

    this.rNodeMap = new HashMap<>();
    this.dNodeMap = new HashMap<>();
    this.dNodeReflect = new HashMap<>();

    this.mcfEdges = new ArrayList<>();
  }

  public Map<TConsensusGroupId, Integer> generateOptimalLeaderDistribution() {
    constructMCFGraph();
    dinicAlgorithm();
    return collectLeaderDistribution();
  }

  private void constructMCFGraph() {
    /* Indicate nodes in mcf */
    for (TRegionReplicaSet regionReplicaSet : regionReplicaSetMap.values()) {
      rNodeMap.put(regionReplicaSet.getRegionId(), maxNode++);
      for (TDataNodeLocation dataNodeLocation : regionReplicaSet.getDataNodeLocations()) {
        if (!dNodeMap.containsKey(dataNodeLocation.getDataNodeId())) {
          dNodeMap.put(dataNodeLocation.getDataNodeId(), maxNode);
          dNodeReflect.put(maxNode, dataNodeLocation.getDataNodeId());
          maxNode += 1;
        }
      }
    }

    /* Prepare arrays */
    isNodeVisited = new boolean[maxNode];
    nodeMinimumCost = new int[maxNode];
    nodeCurrentEdge = new int[maxNode];
    nodeHeadEdge = new int[maxNode];
    Arrays.fill(nodeHeadEdge, -1);

    /* Construct edges: sNode -> rNodes */
    for (int rNode : rNodeMap.values()) {
      // Capacity: 1, each RegionGroup should have exactly 1 leader
      // Cost: 0,
      addAdjacentEdges(sNode, rNode, 1, 0);
    }

    /* Construct edges: rNodes -> dNodes */
    for (TRegionReplicaSet regionReplicaSet : regionReplicaSetMap.values()) {
      int rNode = rNodeMap.get(regionReplicaSet.getRegionId());
      for (TDataNodeLocation dataNodeLocation : regionReplicaSet.getDataNodeLocations()) {
        int dNode = dNodeMap.get(dataNodeLocation.getDataNodeId());
        int cost =
            regionLeaderMap.getOrDefault(regionReplicaSet.getRegionId(), -1)
                    == dataNodeLocation.getDataNodeId()
                ? 0
                : 1;
        addAdjacentEdges(rNode, dNode, 1, cost);
      }
    }

    /* Construct edges: dNodes -> tNode */

    // Count the possible maximum number of leader in each DataNode
    Map<Integer, AtomicInteger> maxLeaderCounter = new ConcurrentHashMap<>();
    regionReplicaSetMap
        .values()
        .forEach(
            regionReplicaSet ->
                regionReplicaSet
                    .getDataNodeLocations()
                    .forEach(
                        dataNodeLocation ->
                            maxLeaderCounter
                                .computeIfAbsent(
                                    dataNodeLocation.getDataNodeId(), empty -> new AtomicInteger(0))
                                .getAndIncrement()));

    for (Map.Entry<Integer, Integer> dNodeEntry : dNodeMap.entrySet()) {
      int dataNodeId = dNodeEntry.getKey();
      int dNode = dNodeEntry.getValue();

      if (disabledDataNodeSet.contains(dataNodeId)) {
        // Skip disabled DataNode
        continue;
      }

      int maxLeaderCount = maxLeaderCounter.get(dataNodeId).get();
      for (int extraEdge = 1; extraEdge <= maxLeaderCount; extraEdge++) {
        addAdjacentEdges(dNode, tNode, 1, extraEdge * extraEdge);
      }
    }
  }

  private void addAdjacentEdges(int fromNode, int destNode, int capacity, int cost) {
    addEdge(fromNode, destNode, capacity, cost);
    addEdge(destNode, fromNode, 0, -cost);
  }

  private void addEdge(int fromNode, int destNode, int capacity, int cost) {
    MCFEdge edge = new MCFEdge(destNode, capacity, cost, nodeHeadEdge[fromNode]);
    mcfEdges.add(edge);
    nodeHeadEdge[fromNode] = maxEdge++;
  }

  private boolean bellmanFordCheck() {
    Arrays.fill(isNodeVisited, false);
    Arrays.fill(nodeMinimumCost, INFINITY);

    Queue<Integer> queue = new LinkedList<>();
    nodeMinimumCost[sNode] = 0;
    isNodeVisited[sNode] = true;
    queue.offer(sNode);
    while (!queue.isEmpty()) {
      int currentNode = queue.poll();
      isNodeVisited[currentNode] = false;
      for (int currentEdge = nodeHeadEdge[currentNode];
          currentEdge >= 0;
          currentEdge = mcfEdges.get(currentEdge).nextEdge) {
        MCFEdge edge = mcfEdges.get(currentEdge);
        if (edge.capacity > 0
            && nodeMinimumCost[currentNode] + edge.cost < nodeMinimumCost[edge.destNode]) {
          nodeMinimumCost[edge.destNode] = nodeMinimumCost[currentNode] + edge.cost;
          if (!isNodeVisited[edge.destNode]) {
            isNodeVisited[edge.destNode] = true;
            queue.offer(edge.destNode);
          }
        }
      }
    }

    return nodeMinimumCost[tNode] < INFINITY;
  }

  private int dfsAugmentation(int currentNode, int inputFlow) {
    if (currentNode == tNode || inputFlow == 0) {
      return inputFlow;
    }

    int currentEdge;
    int outputFlow = 0;
    isNodeVisited[currentNode] = true;
    for (currentEdge = nodeCurrentEdge[currentNode];
        currentEdge >= 0;
        currentEdge = mcfEdges.get(currentEdge).nextEdge) {
      MCFEdge edge = mcfEdges.get(currentEdge);
      if (nodeMinimumCost[currentNode] + edge.cost == nodeMinimumCost[edge.destNode]
          && edge.capacity > 0
          && !isNodeVisited[edge.destNode]) {

        int subOutputFlow = dfsAugmentation(edge.destNode, Math.min(inputFlow, edge.capacity));

        minimumCost += subOutputFlow * edge.cost;

        edge.capacity -= subOutputFlow;
        mcfEdges.get(currentEdge ^ 1).capacity += subOutputFlow;

        inputFlow -= subOutputFlow;
        outputFlow += subOutputFlow;

        if (inputFlow == 0) {
          break;
        }
      }
    }
    nodeCurrentEdge[currentNode] = currentEdge;

    if (outputFlow > 0) {
      isNodeVisited[currentNode] = false;
    }
    return outputFlow;
  }

  private void dinicAlgorithm() {
    while (bellmanFordCheck()) {
      int currentFlow;
      System.arraycopy(nodeHeadEdge, 0, nodeCurrentEdge, 0, maxNode);
      while ((currentFlow = dfsAugmentation(sNode, INFINITY)) > 0) {
        maximumFlow += currentFlow;
      }
    }
  }

  private Map<TConsensusGroupId, Integer> collectLeaderDistribution() {
    Map<TConsensusGroupId, Integer> result = new ConcurrentHashMap<>();

    rNodeMap.forEach(
        (regionGroupId, rNode) -> {
          boolean matchLeader = false;
          for (int currentEdge = nodeHeadEdge[rNode];
              currentEdge >= 0;
              currentEdge = mcfEdges.get(currentEdge).nextEdge) {
            MCFEdge edge = mcfEdges.get(currentEdge);
            if (edge.destNode != sNode && edge.capacity == 0) {
              matchLeader = true;
              result.put(regionGroupId, dNodeReflect.get(edge.destNode));
            }
          }
          if (!matchLeader) {
            result.put(regionGroupId, regionLeaderMap.get(regionGroupId));
          }
        });

    return result;
  }

  @TestOnly
  public int getMaximumFlow() {
    return maximumFlow;
  }

  @TestOnly
  public int getMinimumCost() {
    return minimumCost;
  }
}
