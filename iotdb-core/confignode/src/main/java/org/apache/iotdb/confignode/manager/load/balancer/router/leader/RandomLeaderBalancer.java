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
import org.apache.iotdb.confignode.manager.load.cache.node.NodeStatistics;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionStatistics;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class RandomLeaderBalancer extends AbstractLeaderBalancer {

  public RandomLeaderBalancer() {
    super();
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
    Map<TConsensusGroupId, Integer> result = constructRandomDistribution();
    clear();
    return result;
  }

  private Map<TConsensusGroupId, Integer> constructRandomDistribution() {
    Random random = new Random();
    regionLocationMap.forEach(
        (regionGroupId, regionGroup) -> {
          int replicationFactor = regionGroup.size();
          int leaderId = regionGroup.toArray(new Integer[0])[random.nextInt(replicationFactor)];
          while (!dataNodeStatisticsMap.get(leaderId).getStatus().equals(NodeStatus.Running)) {
            leaderId = regionGroup.toArray(new Integer[0])[random.nextInt(replicationFactor)];
          }
          regionLeaderMap.put(regionGroupId, leaderId);
        });
    return new ConcurrentHashMap<>(regionLeaderMap);
  }
}
