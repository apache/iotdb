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
package org.apache.iotdb.confignode.manager.load.balancer.router.leader;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.utils.TestOnly;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/** Leader distribution balancer that uses minimum cost flow algorithm */
public class MinCostFlowLeaderBalancer implements ILeaderBalancer {

  private static final int INFINITY = Integer.MAX_VALUE;

  /** Input parameters */
  private final Map<TConsensusGroupId, TRegionReplicaSet> regionReplicaSetMap;

  private final Map<TConsensusGroupId, Integer> regionLeaderMap;
  private final Set<Integer> disabledDataNodeSet;

  /** Graph nodes */
  // Super source node
  private static final int S_NODE = 0;
  // Super terminal node
  private static final int T_NODE = 1;
  // Maximum index of graph nodes
  private int maxNode = T_NODE + 1;
  // Map<RegionGroupId, rNode>
  private final Map<TConsensusGroupId, Integer> rNodeMap;
  // Map<DataNodeId, dNode>
  private final Map<Integer, Integer> dNodeMap;
  // Map<dNode, DataNodeId>
  private final Map<Integer, Integer> dNodeReflect;

  /** Graph edges */
  // Maximum index of graph edges
  private int maxEdge = 0;

  private final List<MinCostFlowEdge> minCostFlowEdges;
  private int[] nodeHeadEdge;
  private int[] nodeCurrentEdge;

  private boolean[] isNodeVisited;
  private int[] nodeMinimumCost;

  private int maximumFlow = 0;
  private int minimumCost = 0;

  public MinCostFlowLeaderBalancer() {
    this.regionReplicaSetMap = new HashMap<>();
    this.regionLeaderMap = new HashMap<>();
    this.disabledDataNodeSet = new HashSet<>();
    this.rNodeMap = new HashMap<>();
    this.dNodeMap = new HashMap<>();
    this.dNodeReflect = new HashMap<>();
    this.minCostFlowEdges = new ArrayList<>();
  }

  @Override
  public Map<TConsensusGroupId, Integer> generateOptimalLeaderDistribution(
      Map<TConsensusGroupId, TRegionReplicaSet> regionReplicaSetMap,
      Map<TConsensusGroupId, Integer> regionLeaderMap,
      Set<Integer> disabledDataNodeSet) {

    initialize(regionReplicaSetMap, regionLeaderMap, disabledDataNodeSet);

    Map<TConsensusGroupId, Integer> result;
    constructMCFGraph();
    dinicAlgorithm();
    result = collectLeaderDistribution();

    clear();
    return result;
  }

  private void initialize(
      Map<TConsensusGroupId, TRegionReplicaSet> regionReplicaSetMap,
      Map<TConsensusGroupId, Integer> regionLeaderMap,
      Set<Integer> disabledDataNodeSet) {
    this.regionReplicaSetMap.putAll(regionReplicaSetMap);
    this.regionLeaderMap.putAll(regionLeaderMap);
    this.disabledDataNodeSet.addAll(disabledDataNodeSet);
  }

  private void clear() {
    this.regionReplicaSetMap.clear();
    this.regionLeaderMap.clear();
    this.disabledDataNodeSet.clear();
    this.rNodeMap.clear();
    this.dNodeMap.clear();
    this.dNodeReflect.clear();
    this.minCostFlowEdges.clear();

    this.nodeHeadEdge = null;
    this.nodeCurrentEdge = null;
    this.isNodeVisited = null;
    this.nodeMinimumCost = null;

    this.maxNode = T_NODE + 1;
    this.maxEdge = 0;
  }

  private void constructMCFGraph() {
    this.maximumFlow = 0;
    this.minimumCost = 0;

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
      // Cost: 0
      addAdjacentEdges(S_NODE, rNode, 1, 0);
    }

    /* Construct edges: rNodes -> dNodes */
    for (TRegionReplicaSet regionReplicaSet : regionReplicaSetMap.values()) {
      int rNode = rNodeMap.get(regionReplicaSet.getRegionId());
      for (TDataNodeLocation dataNodeLocation : regionReplicaSet.getDataNodeLocations()) {
        int dNode = dNodeMap.get(dataNodeLocation.getDataNodeId());
        // Cost: 1 if the dNode is corresponded to the current leader of the rNode,
        //       0 otherwise.
        // Therefore, the RegionGroup will keep the leader as constant as possible.
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
        // Cost: x^2 for the x-th edge at the current dNode.
        // Thus, the leader distribution will be as balance as possible.
        addAdjacentEdges(dNode, T_NODE, 1, extraEdge * extraEdge);
      }
    }
  }

  private void addAdjacentEdges(int fromNode, int destNode, int capacity, int cost) {
    addEdge(fromNode, destNode, capacity, cost);
    addEdge(destNode, fromNode, 0, -cost);
  }

  private void addEdge(int fromNode, int destNode, int capacity, int cost) {
    MinCostFlowEdge edge = new MinCostFlowEdge(destNode, capacity, cost, nodeHeadEdge[fromNode]);
    minCostFlowEdges.add(edge);
    nodeHeadEdge[fromNode] = maxEdge++;
  }

  /**
   * Check whether there is an augmented path in the MCF graph by Bellman-Ford algorithm.
   *
   * <p>Notice: Never use Dijkstra algorithm to replace this since there might exist negative
   * circles.
   *
   * @return True if there exist augmented paths, false otherwise.
   */
  private boolean bellmanFordCheck() {
    Arrays.fill(isNodeVisited, false);
    Arrays.fill(nodeMinimumCost, INFINITY);

    Queue<Integer> queue = new LinkedList<>();
    nodeMinimumCost[S_NODE] = 0;
    isNodeVisited[S_NODE] = true;
    queue.offer(S_NODE);
    while (!queue.isEmpty()) {
      int currentNode = queue.poll();
      isNodeVisited[currentNode] = false;
      for (int currentEdge = nodeHeadEdge[currentNode];
          currentEdge >= 0;
          currentEdge = minCostFlowEdges.get(currentEdge).nextEdge) {
        MinCostFlowEdge edge = minCostFlowEdges.get(currentEdge);
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

    return nodeMinimumCost[T_NODE] < INFINITY;
  }

  /** Do augmentation by dfs algorithm */
  private int dfsAugmentation(int currentNode, int inputFlow) {
    if (currentNode == T_NODE || inputFlow == 0) {
      return inputFlow;
    }

    int currentEdge;
    int outputFlow = 0;
    isNodeVisited[currentNode] = true;
    for (currentEdge = nodeCurrentEdge[currentNode];
        currentEdge >= 0;
        currentEdge = minCostFlowEdges.get(currentEdge).nextEdge) {
      MinCostFlowEdge edge = minCostFlowEdges.get(currentEdge);
      if (nodeMinimumCost[currentNode] + edge.cost == nodeMinimumCost[edge.destNode]
          && edge.capacity > 0
          && !isNodeVisited[edge.destNode]) {

        int subOutputFlow = dfsAugmentation(edge.destNode, Math.min(inputFlow, edge.capacity));

        minimumCost += subOutputFlow * edge.cost;

        edge.capacity -= subOutputFlow;
        minCostFlowEdges.get(currentEdge ^ 1).capacity += subOutputFlow;

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
      while ((currentFlow = dfsAugmentation(S_NODE, INFINITY)) > 0) {
        maximumFlow += currentFlow;
      }
    }
  }

  /** @return Map<RegionGroupId, DataNodeId where the new leader locate> */
  private Map<TConsensusGroupId, Integer> collectLeaderDistribution() {
    Map<TConsensusGroupId, Integer> result = new ConcurrentHashMap<>();

    rNodeMap.forEach(
        (regionGroupId, rNode) -> {
          boolean matchLeader = false;
          for (int currentEdge = nodeHeadEdge[rNode];
              currentEdge >= 0;
              currentEdge = minCostFlowEdges.get(currentEdge).nextEdge) {
            MinCostFlowEdge edge = minCostFlowEdges.get(currentEdge);
            if (edge.destNode != S_NODE && edge.capacity == 0) {
              matchLeader = true;
              result.put(regionGroupId, dNodeReflect.get(edge.destNode));
            }
          }
          if (!matchLeader) {
            result.put(regionGroupId, regionLeaderMap.getOrDefault(regionGroupId, -1));
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

  private static class MinCostFlowEdge {

    private final int destNode;
    private int capacity;
    private final int cost;
    private final int nextEdge;

    private MinCostFlowEdge(int destNode, int capacity, int cost, int nextEdge) {
      this.destNode = destNode;
      this.capacity = capacity;
      this.cost = cost;
      this.nextEdge = nextEdge;
    }
  }
}
