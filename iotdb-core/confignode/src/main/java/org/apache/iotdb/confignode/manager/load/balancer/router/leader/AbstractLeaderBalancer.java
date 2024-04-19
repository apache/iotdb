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
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.cluster.RegionStatus;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeStatistics;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionStatistics;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public abstract class AbstractLeaderBalancer {

  public static final String GREEDY_POLICY = "GREEDY";
  public static final String CFD_POLICY = "CFD";

  // Map<Database, List<RegionGroup>>
  protected final Map<String, List<TConsensusGroupId>> databaseRegionGroupMap;
  // Map<RegionGroupId, RegionGroup>
  protected final Map<TConsensusGroupId, TRegionReplicaSet> regionReplicaSetMap;
  // Map<RegionGroupId, leaderId>
  protected final Map<TConsensusGroupId, Integer> regionLeaderMap;
  // Map<DataNodeId, NodeStatistics>
  protected final Map<Integer, NodeStatistics> dataNodeStatisticsMap;
  // Map<RegionGroupId, Map<DataNodeId, RegionStatistics>>
  protected final Map<TConsensusGroupId, Map<Integer, RegionStatistics>> regionStatisticsMap;

  protected AbstractLeaderBalancer() {
    this.databaseRegionGroupMap = new TreeMap<>();
    this.regionReplicaSetMap = new TreeMap<>();
    this.regionLeaderMap = new TreeMap<>();
    this.dataNodeStatisticsMap = new TreeMap<>();
    this.regionStatisticsMap = new TreeMap<>();
  }

  protected void initialize(
      Map<String, List<TConsensusGroupId>> databaseRegionGroupMap,
      Map<TConsensusGroupId, TRegionReplicaSet> regionReplicaSetMap,
      Map<TConsensusGroupId, Integer> regionLeaderMap,
      Map<Integer, NodeStatistics> dataNodeStatisticsMap,
      Map<TConsensusGroupId, Map<Integer, RegionStatistics>> regionStatisticsMap) {
    this.databaseRegionGroupMap.putAll(databaseRegionGroupMap);
    this.regionReplicaSetMap.putAll(regionReplicaSetMap);
    this.regionLeaderMap.putAll(regionLeaderMap);
    this.dataNodeStatisticsMap.putAll(dataNodeStatisticsMap);
    this.regionStatisticsMap.putAll(regionStatisticsMap);
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
    this.databaseRegionGroupMap.clear();
    this.regionReplicaSetMap.clear();
    this.regionLeaderMap.clear();
    this.dataNodeStatisticsMap.clear();
    this.regionStatisticsMap.clear();
  }

  /**
   * Generate an optimal leader distribution.
   *
   * @param databaseRegionGroupMap RegionGroup held by each Database
   * @param regionReplicaSetMap All RegionGroups the cluster currently have
   * @param regionLeaderMap The current leader distribution of each RegionGroup
   * @param dataNodeStatisticsMap The current statistics of each DataNode
   * @param regionStatisticsMap The current statistics of each Region
   * @return Map<TConsensusGroupId, Integer>, The optimal leader distribution
   */
  public abstract Map<TConsensusGroupId, Integer> generateOptimalLeaderDistribution(
      Map<String, List<TConsensusGroupId>> databaseRegionGroupMap,
      Map<TConsensusGroupId, TRegionReplicaSet> regionReplicaSetMap,
      Map<TConsensusGroupId, Integer> regionLeaderMap,
      Map<Integer, NodeStatistics> dataNodeStatisticsMap,
      Map<TConsensusGroupId, Map<Integer, RegionStatistics>> regionStatisticsMap);
}
