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
import org.apache.iotdb.confignode.manager.load.cache.node.NodeStatistics;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionStatistics;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/** Leader distribution balancer that uses greedy algorithm */
public class GreedyLeaderBalancer extends AbstractLeaderBalancer {

  public GreedyLeaderBalancer() {
    super();
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
    Map<TConsensusGroupId, Integer> result = constructGreedyDistribution();
    clear();
    return result;
  }

  private Map<TConsensusGroupId, Integer> constructGreedyDistribution() {
    Map<Integer, Integer> leaderCounter = new TreeMap<>();
    regionReplicaSetMap.forEach(
        (regionGroupId, regionGroup) -> {
          int minCount = Integer.MAX_VALUE,
              leaderId = regionLeaderMap.getOrDefault(regionGroupId, -1);
          for (TDataNodeLocation dataNodeLocation : regionGroup.getDataNodeLocations()) {
            int dataNodeId = dataNodeLocation.getDataNodeId();
            if (isDataNodeAvailable(dataNodeId) && isRegionAvailable(regionGroupId, dataNodeId)) {
              // Select the DataNode with the minimal leader count as the new leader
              int count = leaderCounter.getOrDefault(dataNodeId, 0);
              if (count < minCount) {
                minCount = count;
                leaderId = dataNodeId;
              }
            }
          }
          regionLeaderMap.put(regionGroupId, leaderId);
          leaderCounter.merge(leaderId, 1, Integer::sum);
        });
    return new ConcurrentHashMap<>(regionLeaderMap);
  }
}
