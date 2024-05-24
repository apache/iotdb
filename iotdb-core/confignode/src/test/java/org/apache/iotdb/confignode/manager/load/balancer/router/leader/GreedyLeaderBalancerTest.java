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
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.cluster.RegionStatus;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeStatistics;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionStatistics;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class GreedyLeaderBalancerTest {

  private static final GreedyLeaderBalancer BALANCER = new GreedyLeaderBalancer();

  @Test
  public void optimalLeaderDistributionTest() {
    Map<TConsensusGroupId, Set<Integer>> regionReplicaSetMap = new TreeMap<>();
    Map<TConsensusGroupId, Integer> regionLeaderMap = new TreeMap<>();
    Map<Integer, NodeStatistics> dataNodeStatisticsMap = new TreeMap<>();
    Map<TConsensusGroupId, Map<Integer, RegionStatistics>> regionStatisticsMap = new TreeMap<>();
    Random random = new Random();

    // Assuming all DataNodes are in Running status
    for (int i = 0; i < 6; i++) {
      dataNodeStatisticsMap.put(i, new NodeStatistics(NodeStatus.Running));
    }

    // Build 9 RegionGroups in DataNodes 0~2
    for (int i = 0; i < 9; i++) {
      TConsensusGroupId regionGroupId = new TConsensusGroupId(TConsensusGroupType.DataRegion, i);
      List<TDataNodeLocation> dataNodeLocations = new ArrayList<>();
      Map<Integer, RegionStatistics> regionStatistics = new TreeMap<>();
      for (int j = 0; j < 3; j++) {
        dataNodeLocations.add(new TDataNodeLocation().setDataNodeId(j));
        // Assuming all Regions are in Running status
        regionStatistics.put(j, new RegionStatistics(RegionStatus.Running));
      }
      TRegionReplicaSet regionReplicaSet = new TRegionReplicaSet(regionGroupId, dataNodeLocations);
      regionReplicaSetMap.put(
          regionGroupId,
          regionReplicaSet.getDataNodeLocations().stream()
              .map(TDataNodeLocation::getDataNodeId)
              .collect(Collectors.toSet()));
      regionLeaderMap.put(regionGroupId, random.nextInt(3));
      regionStatisticsMap.put(regionGroupId, regionStatistics);
    }

    // Build 9 RegionGroups in DataNodes 3~5
    for (int i = 9; i < 18; i++) {
      TConsensusGroupId regionGroupId = new TConsensusGroupId(TConsensusGroupType.DataRegion, i);
      List<TDataNodeLocation> dataNodeLocations = new ArrayList<>();
      Map<Integer, RegionStatistics> regionStatistics = new TreeMap<>();
      for (int j = 3; j < 6; j++) {
        dataNodeLocations.add(new TDataNodeLocation().setDataNodeId(j));
        // Assuming all Regions are in Running status
        regionStatistics.put(j, new RegionStatistics(RegionStatus.Running));
      }
      TRegionReplicaSet regionReplicaSet = new TRegionReplicaSet(regionGroupId, dataNodeLocations);
      regionReplicaSetMap.put(
          regionGroupId,
          regionReplicaSet.getDataNodeLocations().stream()
              .map(TDataNodeLocation::getDataNodeId)
              .collect(Collectors.toSet()));
      regionLeaderMap.put(regionGroupId, 3 + random.nextInt(3));
      regionStatisticsMap.put(regionGroupId, regionStatistics);
    }

    Map<TConsensusGroupId, Integer> leaderDistribution =
        BALANCER.generateOptimalLeaderDistribution(
            new TreeMap<>(),
            regionReplicaSetMap,
            regionLeaderMap,
            dataNodeStatisticsMap,
            regionStatisticsMap);
    Map<Integer, AtomicInteger> leaderCounter = new ConcurrentHashMap<>();
    leaderDistribution.forEach(
        (regionGroupId, leaderId) ->
            leaderCounter
                .computeIfAbsent(leaderId, empty -> new AtomicInteger(0))
                .getAndIncrement());

    // Each DataNode has exactly 3 leaders
    for (int i = 0; i < 6; i++) {
      Assert.assertEquals(3, leaderCounter.get(i).get());
    }
  }

  @Test
  public void disableTest() {
    Map<TConsensusGroupId, Set<Integer>> regionReplicaSetMap = new TreeMap<>();
    Map<TConsensusGroupId, Integer> regionLeaderMap = new TreeMap<>();
    Map<Integer, NodeStatistics> dataNodeStatisticsMap = new TreeMap<>();
    Map<TConsensusGroupId, Map<Integer, RegionStatistics>> regionStatisticsMap = new TreeMap<>();

    // Assuming DataNode 1 and 4 are disabled
    dataNodeStatisticsMap.put(0, new NodeStatistics(NodeStatus.Running));
    dataNodeStatisticsMap.put(1, new NodeStatistics(NodeStatus.Unknown));
    dataNodeStatisticsMap.put(2, new NodeStatistics(NodeStatus.Running));
    dataNodeStatisticsMap.put(3, new NodeStatistics(NodeStatus.Running));
    dataNodeStatisticsMap.put(4, new NodeStatistics(NodeStatus.ReadOnly));
    dataNodeStatisticsMap.put(5, new NodeStatistics(NodeStatus.Running));

    // Build 10 RegionGroup whose leaders are all 1(Disabled)
    for (int i = 0; i < 10; i++) {
      TConsensusGroupId regionGroupId = new TConsensusGroupId(TConsensusGroupType.DataRegion, i);
      List<TDataNodeLocation> dataNodeLocations = new ArrayList<>();
      Map<Integer, RegionStatistics> regionStatistics = new TreeMap<>();
      for (int j = 0; j < 3; j++) {
        dataNodeLocations.add(new TDataNodeLocation().setDataNodeId(j));
        // Assuming all Regions in DataNode 1 are Unknown
        regionStatistics.put(
            j, new RegionStatistics(j == 1 ? RegionStatus.Unknown : RegionStatus.Running));
      }
      TRegionReplicaSet regionReplicaSet = new TRegionReplicaSet(regionGroupId, dataNodeLocations);
      regionReplicaSetMap.put(
          regionGroupId,
          regionReplicaSet.getDataNodeLocations().stream()
              .map(TDataNodeLocation::getDataNodeId)
              .collect(Collectors.toSet()));
      regionLeaderMap.put(regionGroupId, 1);
      regionStatisticsMap.put(regionGroupId, regionStatistics);
    }

    // Build 10 RegionGroup whose leaders are all 4(Disabled)
    for (int i = 10; i < 20; i++) {
      TConsensusGroupId regionGroupId = new TConsensusGroupId(TConsensusGroupType.DataRegion, i);
      List<TDataNodeLocation> dataNodeLocations = new ArrayList<>();
      Map<Integer, RegionStatistics> regionStatistics = new TreeMap<>();
      for (int j = 3; j < 6; j++) {
        dataNodeLocations.add(new TDataNodeLocation().setDataNodeId(j));
        // Assuming all Regions in DataNode 4 are ReadOnly
        regionStatistics.put(
            j, new RegionStatistics(j == 4 ? RegionStatus.ReadOnly : RegionStatus.Running));
      }
      TRegionReplicaSet regionReplicaSet = new TRegionReplicaSet(regionGroupId, dataNodeLocations);
      regionReplicaSetMap.put(
          regionGroupId,
          regionReplicaSet.getDataNodeLocations().stream()
              .map(TDataNodeLocation::getDataNodeId)
              .collect(Collectors.toSet()));
      regionLeaderMap.put(regionGroupId, 4);
      regionStatisticsMap.put(regionGroupId, regionStatistics);
    }

    Map<TConsensusGroupId, Integer> leaderDistribution =
        BALANCER.generateOptimalLeaderDistribution(
            new TreeMap<>(),
            regionReplicaSetMap,
            regionLeaderMap,
            dataNodeStatisticsMap,
            regionStatisticsMap);
    Map<Integer, AtomicInteger> leaderCounter = new ConcurrentHashMap<>();
    leaderDistribution.forEach(
        (regionGroupId, leaderId) ->
            leaderCounter
                .computeIfAbsent(leaderId, empty -> new AtomicInteger(0))
                .getAndIncrement());

    for (int i = 0; i < 6; i++) {
      if (i != 1 && i != 4) {
        Assert.assertEquals(5, leaderCounter.get(i).get());
      }
    }
  }
}
