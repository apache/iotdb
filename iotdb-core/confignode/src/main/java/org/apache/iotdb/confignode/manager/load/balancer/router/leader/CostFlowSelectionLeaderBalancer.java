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
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeStatistics;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionStatistics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/** Leader distribution balancer that uses minimum cost flow algorithm */
public class CostFlowSelectionLeaderBalancer extends AbstractLeaderBalancer {

  private static final int INFINITY = Integer.MAX_VALUE;

  /** Graph vertices */
  // Super source vertex
  private static final int S_VERTEX = 0;

  // Super terminal vertex
  private static final int T_VERTEX = 1;
  // Maximum index of graph vertices
  private int maxVertex = T_VERTEX + 1;
  // Map<RegionGroupId, rVertex>
  private final Map<TConsensusGroupId, Integer> rVertexMap;
  // Map<Database, Map<DataNodeId, sDVertex>>
  private final Map<String, Map<Integer, Integer>> sDVertexMap;
  // Map<Database, Map<sDVertex, DataNodeId>>
  private final Map<String, Map<Integer, Integer>> sDVertexReflect;
  // Map<DataNodeId, tDVertex>
  private final Map<Integer, Integer> tDVertexMap;

  /** Graph edges */
  // Maximum index of graph edges
  private int maxEdge = 0;

  private final List<CostFlowEdge> costFlowEdges;
  private int[] vertexHeadEdge;
  private int[] vertexCurrentEdge;

  private boolean[] isVertexVisited;
  private int[] vertexMinimumCost;

  private int maximumFlow = 0;
  private int minimumCost = 0;

  public CostFlowSelectionLeaderBalancer() {
    super();
    this.rVertexMap = new TreeMap<>();
    this.sDVertexMap = new TreeMap<>();
    this.sDVertexReflect = new TreeMap<>();
    this.tDVertexMap = new TreeMap<>();
    this.costFlowEdges = new ArrayList<>();
  }

  @Override
  public Map<TConsensusGroupId, Integer> generateOptimalLeaderDistribution(
      Map<String, List<TConsensusGroupId>> databaseRegionGroupMap,
      Map<TConsensusGroupId, Set<Integer>> regionLocationMap,
      Map<TConsensusGroupId, Integer> regionLeaderMap,
      Map<Integer, NodeStatistics> dataNodeStatisticsMap,
      Map<TConsensusGroupId, Map<Integer, RegionStatistics>> regionStatisticsMap) {
    initialize(
        databaseRegionGroupMap,
        regionLocationMap,
        regionLeaderMap,
        dataNodeStatisticsMap,
        regionStatisticsMap);
    Map<TConsensusGroupId, Integer> result;
    constructFlowNetwork();
    dinicAlgorithm();
    result = collectLeaderDistribution();
    clear();
    return result;
  }

  @Override
  protected void clear() {
    super.clear();
    this.rVertexMap.clear();
    this.sDVertexMap.clear();
    this.sDVertexReflect.clear();
    this.tDVertexMap.clear();
    this.costFlowEdges.clear();
    this.vertexHeadEdge = null;
    this.vertexCurrentEdge = null;
    this.isVertexVisited = null;
    this.vertexMinimumCost = null;
    this.maxVertex = T_VERTEX + 1;
    this.maxEdge = 0;
  }

  private void constructFlowNetwork() {
    this.maximumFlow = 0;
    this.minimumCost = 0;

    /* Indicate vertices */
    for (Map.Entry<String, List<TConsensusGroupId>> databaseEntry :
        databaseRegionGroupMap.entrySet()) {
      String database = databaseEntry.getKey();
      sDVertexMap.put(database, new TreeMap<>());
      sDVertexReflect.put(database, new TreeMap<>());
      for (TConsensusGroupId regionGroupId : databaseEntry.getValue()) {
        if (regionGroupIntersection.contains(regionGroupId)) {
          // Map region to region vertices
          rVertexMap.put(regionGroupId, maxVertex++);
          regionLocationMap
              .get(regionGroupId)
              .forEach(
                  dataNodeId -> {
                    if (isDataNodeAvailable(dataNodeId)) {
                      // Map DataNode to DataNode vertices
                      if (!sDVertexMap.get(database).containsKey(dataNodeId)) {
                        sDVertexMap.get(database).put(dataNodeId, maxVertex);
                        sDVertexReflect.get(database).put(maxVertex, dataNodeId);
                        maxVertex += 1;
                      }
                      if (!tDVertexMap.containsKey(dataNodeId)) {
                        tDVertexMap.put(dataNodeId, maxVertex);
                        maxVertex += 1;
                      }
                    }
                  });
        }
      }
    }

    /* Prepare arrays */
    isVertexVisited = new boolean[maxVertex];
    vertexMinimumCost = new int[maxVertex];
    vertexCurrentEdge = new int[maxVertex];
    vertexHeadEdge = new int[maxVertex];
    Arrays.fill(vertexHeadEdge, -1);

    /* Construct edges: sVertex -> rVertices */
    for (int rVertex : rVertexMap.values()) {
      // Capacity: 1, Cost: 0, select exactly 1 leader for each RegionGroup
      addAdjacentEdges(S_VERTEX, rVertex, 1, 0);
    }

    /* Construct edges: rVertices -> sDVertices */
    for (Map.Entry<String, List<TConsensusGroupId>> databaseEntry :
        databaseRegionGroupMap.entrySet()) {
      String database = databaseEntry.getKey();
      for (TConsensusGroupId regionGroupId : databaseEntry.getValue()) {
        if (regionGroupIntersection.contains(regionGroupId)) {
          int rVertex = rVertexMap.get(regionGroupId);
          regionLocationMap
              .get(regionGroupId)
              .forEach(
                  dataNodeId -> {
                    if (isDataNodeAvailable(dataNodeId)
                        && isRegionAvailable(regionGroupId, dataNodeId)) {
                      int sDVertex = sDVertexMap.get(database).get(dataNodeId);
                      // Capacity: 1, Cost: 1 if the DataNode is the current leader of the
                      // RegionGroup;
                      // 0 otherwise. Thus, the RegionGroup will keep the leader as constant as
                      // possible.
                      int cost =
                          Objects.equals(
                                  regionLeaderMap.getOrDefault(regionGroupId, -1), dataNodeId)
                              ? 0
                              : 1;
                      addAdjacentEdges(rVertex, sDVertex, 1, cost);
                    }
                  });
        }
      }
    }

    /* Construct edges: sDVertices -> tDVertices */
    for (Map.Entry<String, List<TConsensusGroupId>> databaseEntry :
        databaseRegionGroupMap.entrySet()) {
      String database = databaseEntry.getKey();
      // Map<DataNodeId, leader number>
      Map<Integer, Integer> leaderCounter = new TreeMap<>();
      for (TConsensusGroupId regionGroupId : databaseEntry.getValue()) {
        if (regionGroupIntersection.contains(regionGroupId)) {
          regionLocationMap
              .get(regionGroupId)
              .forEach(
                  dataNodeId -> {
                    if (isDataNodeAvailable(dataNodeId)) {
                      int sDVertex = sDVertexMap.get(database).get(dataNodeId);
                      int tDVertex = tDVertexMap.get(dataNodeId);
                      int leaderCount = leaderCounter.merge(dataNodeId, 1, Integer::sum);
                      // Capacity: 1, Cost: 2*x-1 for the x-th edge at the current sDVertex.
                      // Thus, the leader distribution will be as balance as possible within each
                      // Database according to the Jensen's-Inequality.
                      addAdjacentEdges(sDVertex, tDVertex, 1, 2 * leaderCount - 1);
                    }
                  });
        }
      }
    }

    /* Construct edges: tDVertices -> tVertex */
    // Map<DataNodeId, possible maximum leader>
    // Count the possible maximum number of leader in each DataNode
    Map<Integer, Integer> maxLeaderCounter = new TreeMap<>();
    regionLocationMap.forEach(
        (regionGroupId, dataNodeIds) ->
            dataNodeIds.forEach(
                dataNodeId -> {
                  if (isDataNodeAvailable(dataNodeId) && tDVertexMap.containsKey(dataNodeId)) {
                    int tDVertex = tDVertexMap.get(dataNodeId);
                    int leaderCount = maxLeaderCounter.merge(dataNodeId, 1, Integer::sum);
                    // Capacity: 1, Cost: 2*x-1 for the x-th edge at the current tDVertex.
                    // Thus, the leader distribution will be as balance as possible within the
                    // cluster according to the Jensen's-Inequality.
                    addAdjacentEdges(tDVertex, T_VERTEX, 1, 2 * leaderCount - 1);
                  }
                }));
  }

  private void addAdjacentEdges(int fromVertex, int destVertex, int capacity, int cost) {
    addEdge(fromVertex, destVertex, capacity, cost);
    addEdge(destVertex, fromVertex, 0, -cost);
  }

  private void addEdge(int fromVertex, int destVertex, int capacity, int cost) {
    CostFlowEdge edge = new CostFlowEdge(destVertex, capacity, cost, vertexHeadEdge[fromVertex]);
    costFlowEdges.add(edge);
    vertexHeadEdge[fromVertex] = maxEdge++;
  }

  /**
   * Check whether there is an augmented path in the flow network by SPFA algorithm.
   *
   * <p>Notice: Never use Dijkstra algorithm to replace this since there might exist negative
   * circles.
   *
   * @return True if there exist augmented paths, false otherwise.
   */
  private boolean SPFACheck() {
    Arrays.fill(isVertexVisited, false);
    Arrays.fill(vertexMinimumCost, INFINITY);

    Queue<Integer> queue = new LinkedList<>();
    vertexMinimumCost[S_VERTEX] = 0;
    isVertexVisited[S_VERTEX] = true;
    queue.offer(S_VERTEX);
    while (!queue.isEmpty()) {
      int currentVertex = queue.poll();
      isVertexVisited[currentVertex] = false;
      for (int currentEdge = vertexHeadEdge[currentVertex];
          currentEdge >= 0;
          currentEdge = costFlowEdges.get(currentEdge).nextEdge) {
        CostFlowEdge edge = costFlowEdges.get(currentEdge);
        if (edge.capacity > 0
            && vertexMinimumCost[currentVertex] + edge.cost < vertexMinimumCost[edge.destVertex]) {
          vertexMinimumCost[edge.destVertex] = vertexMinimumCost[currentVertex] + edge.cost;
          if (!isVertexVisited[edge.destVertex]) {
            isVertexVisited[edge.destVertex] = true;
            queue.offer(edge.destVertex);
          }
        }
      }
    }

    return vertexMinimumCost[T_VERTEX] < INFINITY;
  }

  /** Do augmentation by dfs algorithm */
  private int dfsAugmentation(int currentVertex, int inputFlow) {
    if (currentVertex == T_VERTEX || inputFlow == 0) {
      return inputFlow;
    }

    int currentEdge;
    int outputFlow = 0;
    isVertexVisited[currentVertex] = true;
    for (currentEdge = vertexCurrentEdge[currentVertex];
        currentEdge >= 0;
        currentEdge = costFlowEdges.get(currentEdge).nextEdge) {
      CostFlowEdge edge = costFlowEdges.get(currentEdge);
      if (vertexMinimumCost[currentVertex] + edge.cost == vertexMinimumCost[edge.destVertex]
          && edge.capacity > 0
          && !isVertexVisited[edge.destVertex]) {

        int subOutputFlow = dfsAugmentation(edge.destVertex, Math.min(inputFlow, edge.capacity));

        minimumCost += subOutputFlow * edge.cost;

        edge.capacity -= subOutputFlow;
        costFlowEdges.get(currentEdge ^ 1).capacity += subOutputFlow;

        inputFlow -= subOutputFlow;
        outputFlow += subOutputFlow;

        if (inputFlow == 0) {
          break;
        }
      }
    }
    vertexCurrentEdge[currentVertex] = currentEdge;

    if (outputFlow > 0) {
      isVertexVisited[currentVertex] = false;
    }
    return outputFlow;
  }

  private void dinicAlgorithm() {
    while (SPFACheck()) {
      int currentFlow;
      System.arraycopy(vertexHeadEdge, 0, vertexCurrentEdge, 0, maxVertex);
      while ((currentFlow = dfsAugmentation(S_VERTEX, INFINITY)) > 0) {
        maximumFlow += currentFlow;
      }
    }
  }

  /**
   * @return Map<RegionGroupId, DataNodeId where the new leader locate>
   */
  private Map<TConsensusGroupId, Integer> collectLeaderDistribution() {
    Map<TConsensusGroupId, Integer> result = new ConcurrentHashMap<>();

    databaseRegionGroupMap.forEach(
        (database, regionGroupIds) ->
            regionGroupIds.forEach(
                regionGroupId -> {
                  int originalLeader = regionLeaderMap.getOrDefault(regionGroupId, -1);
                  if (!regionGroupIntersection.contains(regionGroupId)) {
                    result.put(regionGroupId, originalLeader);
                    return;
                  }
                  boolean matchLeader = false;
                  for (int currentEdge = vertexHeadEdge[rVertexMap.get(regionGroupId)];
                      currentEdge >= 0;
                      currentEdge = costFlowEdges.get(currentEdge).nextEdge) {
                    CostFlowEdge edge = costFlowEdges.get(currentEdge);
                    if (edge.destVertex != S_VERTEX && edge.capacity == 0) {
                      matchLeader = true;
                      result.put(regionGroupId, sDVertexReflect.get(database).get(edge.destVertex));
                    }
                  }
                  if (!matchLeader) {
                    result.put(regionGroupId, originalLeader);
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

  private static class CostFlowEdge {

    private final int destVertex;
    private int capacity;
    private final int cost;
    private final int nextEdge;

    private CostFlowEdge(int destVertex, int capacity, int cost, int nextEdge) {
      this.destVertex = destVertex;
      this.capacity = capacity;
      this.cost = cost;
      this.nextEdge = nextEdge;
    }
  }
}
