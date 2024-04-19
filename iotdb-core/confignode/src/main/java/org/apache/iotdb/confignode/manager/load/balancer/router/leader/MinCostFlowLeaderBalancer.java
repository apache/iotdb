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
import org.apache.iotdb.confignode.manager.load.cache.node.NodeStatistics;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionStatistics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/** Leader distribution balancer that uses minimum cost flow algorithm */
public class MinCostFlowLeaderBalancer extends AbstractLeaderBalancer {

  private static final int INFINITY = Integer.MAX_VALUE;

  /** Graph nodes */
  // Super source node
  private static final int S_NODE = 0;
  // Super terminal node
  private static final int T_NODE = 1;
  // Maximum index of graph nodes
  private int maxNode = T_NODE + 1;
  // Map<RegionGroupId, rNode>
  private final Map<TConsensusGroupId, Integer> rNodeMap;
  // Map<Database, Map<DataNodeId, sDNode>>
  private final Map<String, Map<Integer, Integer>> sDNodeMap;
  // Map<Database, Map<sDNode, DataNodeId>>
  private final Map<String, Map<Integer, Integer>> sDNodeReflect;
  // Map<DataNodeId, tDNode>
  private final Map<Integer, Integer> tDNodeMap;

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
    super();
    this.rNodeMap = new TreeMap<>();
    this.sDNodeMap = new TreeMap<>();
    this.sDNodeReflect = new TreeMap<>();
    this.tDNodeMap = new TreeMap<>();
    this.minCostFlowEdges = new ArrayList<>();
  }

  @Override
  public Map<TConsensusGroupId, Integer> generateOptimalLeaderDistribution(
      Map<String, List<TConsensusGroupId>> databaseRegionGroupMap,
      Map<TConsensusGroupId, TRegionReplicaSet> regionReplicaSetMap,
      Map<TConsensusGroupId, Integer> regionLeaderMap,
      Map<Integer, NodeStatistics> dataNodeStatisticsMap,
      Map<TConsensusGroupId, Map<Integer, RegionStatistics>> regionStatisticsMap) {
    initialize(
        databaseRegionGroupMap,
        regionReplicaSetMap,
        regionLeaderMap,
        dataNodeStatisticsMap,
        regionStatisticsMap);
    Map<TConsensusGroupId, Integer> result;
    constructMCFGraph();
    dinicAlgorithm();
    result = collectLeaderDistribution();
    clear();
    return result;
  }

  @Override
  protected void clear() {
    super.clear();
    this.rNodeMap.clear();
    this.sDNodeMap.clear();
    this.sDNodeReflect.clear();
    this.tDNodeMap.clear();
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
    for (Map.Entry<String, List<TConsensusGroupId>> databaseEntry :
        databaseRegionGroupMap.entrySet()) {
      String database = databaseEntry.getKey();
      sDNodeMap.put(database, new TreeMap<>());
      sDNodeReflect.put(database, new TreeMap<>());
      List<TConsensusGroupId> regionGroupIds = databaseEntry.getValue();
      for (TConsensusGroupId regionGroupId : regionGroupIds) {
        rNodeMap.put(regionGroupId, maxNode++);
        for (TDataNodeLocation dataNodeLocation :
            regionReplicaSetMap.get(regionGroupId).getDataNodeLocations()) {
          int dataNodeId = dataNodeLocation.getDataNodeId();
          if (isDataNodeAvailable(dataNodeId)) {
            if (!sDNodeMap.get(database).containsKey(dataNodeId)) {
              sDNodeMap.get(database).put(dataNodeId, maxNode);
              sDNodeReflect.get(database).put(maxNode, dataNodeId);
              maxNode += 1;
            }
            if (!tDNodeMap.containsKey(dataNodeId)) {
              tDNodeMap.put(dataNodeId, maxNode);
              maxNode += 1;
            }
          }
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
      // Capacity: 1, Cost: 0, each RegionGroup should elect exactly 1 leader
      addAdjacentEdges(S_NODE, rNode, 1, 0);
    }

    /* Construct edges: rNodes -> sdNodes */
    for (Map.Entry<String, List<TConsensusGroupId>> databaseEntry :
        databaseRegionGroupMap.entrySet()) {
      String database = databaseEntry.getKey();
      for (TConsensusGroupId regionGroupId : databaseEntry.getValue()) {
        int rNode = rNodeMap.get(regionGroupId);
        for (TDataNodeLocation dataNodeLocation :
            regionReplicaSetMap.get(regionGroupId).getDataNodeLocations()) {
          int dataNodeId = dataNodeLocation.getDataNodeId();
          if (isDataNodeAvailable(dataNodeId) && isRegionAvailable(regionGroupId, dataNodeId)) {
            int sDNode = sDNodeMap.get(database).get(dataNodeId);
            // Capacity: 1, Cost: 1 if sDNode is the current leader of the rNode, 0 otherwise.
            // Therefore, the RegionGroup will keep the leader as constant as possible.
            int cost = regionLeaderMap.getOrDefault(regionGroupId, -1) == dataNodeId ? 0 : 1;
            addAdjacentEdges(rNode, sDNode, 1, cost);
          }
        }
      }
    }

    /* Construct edges: sDNodes -> tDNodes */
    for (Map.Entry<String, List<TConsensusGroupId>> databaseEntry :
        databaseRegionGroupMap.entrySet()) {
      String database = databaseEntry.getKey();
      // Map<DataNodeId, leader number>
      Map<Integer, Integer> leaderCounter = new TreeMap<>();
      for (TConsensusGroupId regionGroupId : databaseEntry.getValue()) {
        for (TDataNodeLocation dataNodeLocation :
            regionReplicaSetMap.get(regionGroupId).getDataNodeLocations()) {
          int dataNodeId = dataNodeLocation.getDataNodeId();
          if (isDataNodeAvailable(dataNodeId)) {
            int sDNode = sDNodeMap.get(database).get(dataNodeId);
            int tDNode = tDNodeMap.get(dataNodeId);
            int leaderCount = leaderCounter.merge(dataNodeId, 1, Integer::sum);
            // Capacity: 1, Cost: x^2 for the x-th edge at the current sDNode.
            // Thus, the leader distribution will be as balance as possible within each Database
            // based on the Jensen's-Inequality.
            addAdjacentEdges(sDNode, tDNode, 1, leaderCount * leaderCount);
          }
        }
      }
    }

    /* Construct edges: tDNodes -> tNode */
    // Map<DataNodeId, possible maximum leader>
    // Count the possible maximum number of leader in each DataNode
    Map<Integer, Integer> maxLeaderCounter = new TreeMap<>();
    for (TRegionReplicaSet regionReplicaSet : regionReplicaSetMap.values()) {
      for (TDataNodeLocation dataNodeLocation : regionReplicaSet.getDataNodeLocations()) {
        int dataNodeId = dataNodeLocation.getDataNodeId();
        if (isDataNodeAvailable(dataNodeId)) {
          int tDNode = tDNodeMap.get(dataNodeId);
          int leaderCount = maxLeaderCounter.merge(dataNodeId, 1, Integer::sum);
          // Cost: x^2 for the x-th edge at the current dNode.
          // Thus, the leader distribution will be as balance as possible within the cluster
          // Based on the Jensen's-Inequality.
          addAdjacentEdges(tDNode, T_NODE, 1, leaderCount * leaderCount);
        }
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

    databaseRegionGroupMap.forEach(
        (database, regionGroupIds) ->
            regionGroupIds.forEach(
                regionGroupId -> {
                  boolean matchLeader = false;
                  for (int currentEdge = nodeHeadEdge[rNodeMap.get(regionGroupId)];
                      currentEdge >= 0;
                      currentEdge = minCostFlowEdges.get(currentEdge).nextEdge) {
                    MinCostFlowEdge edge = minCostFlowEdges.get(currentEdge);
                    if (edge.destNode != S_NODE && edge.capacity == 0) {
                      matchLeader = true;
                      result.put(regionGroupId, sDNodeReflect.get(database).get(edge.destNode));
                    }
                  }
                  if (!matchLeader) {
                    result.put(regionGroupId, regionLeaderMap.getOrDefault(regionGroupId, -1));
                  }
                }));

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
