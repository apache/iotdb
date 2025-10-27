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
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.cluster.RegionStatus;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeStatistics;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionStatistics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

public abstract class AbstractLeaderBalancer {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractLeaderBalancer.class);
  public static final String GREEDY_POLICY = "GREEDY";
  public static final String CFD_POLICY = "CFD";
  public static final String HASH_POLICY = "HASH";

  // Set<RegionGroupId>
  protected final Set<TConsensusGroupId> regionGroupIntersection;
  // Map<Database, List<RegionGroup>>
  protected final Map<String, List<TConsensusGroupId>> databaseRegionGroupMap;
  // Map<RegionGroupId, Set<DataNodeId>>
  protected final Map<TConsensusGroupId, Set<Integer>> regionLocationMap;
  // Map<RegionGroupId, leaderId>
  protected final Map<TConsensusGroupId, Integer> regionLeaderMap;
  // Map<DataNodeId, NodeStatistics>
  protected final Map<Integer, NodeStatistics> dataNodeStatisticsMap;
  // Map<RegionGroupId, Map<DataNodeId, RegionStatistics>>
  protected final Map<TConsensusGroupId, Map<Integer, RegionStatistics>> regionStatisticsMap;

  protected AbstractLeaderBalancer() {
    this.regionGroupIntersection = new TreeSet<>();
    this.databaseRegionGroupMap = new TreeMap<>();
    this.regionLocationMap = new TreeMap<>();
    this.regionLeaderMap = new TreeMap<>();
    this.dataNodeStatisticsMap = new TreeMap<>();
    this.regionStatisticsMap = new TreeMap<>();
  }

  protected void initialize(
      Map<String, List<TConsensusGroupId>> databaseRegionGroupMap,
      Map<TConsensusGroupId, Set<Integer>> regionLocationMap,
      Map<TConsensusGroupId, Integer> regionLeaderMap,
      Map<Integer, NodeStatistics> dataNodeStatisticsMap,
      Map<TConsensusGroupId, Map<Integer, RegionStatistics>> regionStatisticsMap) {

    this.databaseRegionGroupMap.putAll(databaseRegionGroupMap);
    this.regionLocationMap.putAll(regionLocationMap);
    this.regionLeaderMap.putAll(regionLeaderMap);
    this.dataNodeStatisticsMap.putAll(dataNodeStatisticsMap);
    this.regionStatisticsMap.putAll(regionStatisticsMap);

    Set<TConsensusGroupId> regionGroupUnionSet =
        databaseRegionGroupMap.values().stream().flatMap(List::stream).collect(Collectors.toSet());
    this.regionGroupIntersection.addAll(regionGroupUnionSet);
    this.regionGroupIntersection.retainAll(regionLocationMap.keySet());
    this.regionGroupIntersection.retainAll(regionLeaderMap.keySet());
    this.regionGroupIntersection.retainAll(regionStatisticsMap.keySet());
    regionGroupUnionSet.addAll(regionLocationMap.keySet());
    regionGroupUnionSet.addAll(regionLeaderMap.keySet());
    regionGroupUnionSet.addAll(regionStatisticsMap.keySet());
    Set<TConsensusGroupId> differenceSet =
        regionGroupUnionSet.stream()
            .filter(e -> !regionGroupIntersection.contains(e))
            .collect(Collectors.toSet());
    if (!differenceSet.isEmpty()) {
      LOGGER.warn(
          "[LeaderBalancer] The following RegionGroups' leader cannot be selected because their corresponding caches are incomplete: {}",
          differenceSet);
      Set<TConsensusGroupId> databaseRegionGroupUnionSet =
          databaseRegionGroupMap.values().stream()
              .flatMap(List::stream)
              .collect(Collectors.toSet());
      differenceSet.forEach(
          regionId -> {
            if (!databaseRegionGroupUnionSet.contains(regionId)) {
              LOGGER.warn("[LeaderBalancer] Region: {} not in databaseRegionGroupMap", regionId);
            }
            if (!regionLocationMap.containsKey(regionId)) {
              LOGGER.warn("[LeaderBalancer] Region: {} not in regionLocationMap", regionId);
            }
            if (!regionLeaderMap.containsKey(regionId)) {

              LOGGER.warn("[LeaderBalancer] Region: {} not in regionLeaderMap", regionId);
            }
            if (!regionStatisticsMap.containsKey(regionId)) {
              LOGGER.warn("[LeaderBalancer] Region: {} not in regionStatisticsMap", regionId);
            }
          });
    }
  }

  protected boolean isDataNodeAvailable(int dataNodeId) {
    // RegionGroup-leader can only be placed on Running DataNode
    return dataNodeStatisticsMap.containsKey(dataNodeId)
        && NodeStatus.Running.equals(dataNodeStatisticsMap.get(dataNodeId).getStatus());
  }

  protected boolean isRegionAvailable(TConsensusGroupId regionGroupId, int dataNodeId) {
    // Only Running Region can be selected as RegionGroup-leader
    return regionStatisticsMap.containsKey(regionGroupId)
        && regionStatisticsMap.get(regionGroupId).containsKey(dataNodeId)
        && RegionStatus.Running.equals(
            regionStatisticsMap.get(regionGroupId).get(dataNodeId).getRegionStatus());
  }

  protected void clear() {
    this.regionGroupIntersection.clear();
    this.databaseRegionGroupMap.clear();
    this.regionLocationMap.clear();
    this.regionLeaderMap.clear();
    this.dataNodeStatisticsMap.clear();
    this.regionStatisticsMap.clear();
  }

  /**
   * Generate an optimal leader distribution.
   *
   * @param databaseRegionGroupMap RegionGroup held by each Database
   * @param regionLocationMap All RegionGroups the cluster currently have
   * @param regionLeaderMap The current leader distribution of each RegionGroup
   * @param dataNodeStatisticsMap The current statistics of each DataNode
   * @param regionStatisticsMap The current statistics of each Region
   * @return Map<TConsensusGroupId, Integer>, The optimal leader distribution
   */
  public abstract Map<TConsensusGroupId, Integer> generateOptimalLeaderDistribution(
      Map<String, List<TConsensusGroupId>> databaseRegionGroupMap,
      Map<TConsensusGroupId, Set<Integer>> regionLocationMap,
      Map<TConsensusGroupId, Integer> regionLeaderMap,
      Map<Integer, NodeStatistics> dataNodeStatisticsMap,
      Map<TConsensusGroupId, Map<Integer, RegionStatistics>> regionStatisticsMap);
}
