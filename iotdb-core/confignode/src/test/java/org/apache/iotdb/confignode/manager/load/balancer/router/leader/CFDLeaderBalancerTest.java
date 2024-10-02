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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class CFDLeaderBalancerTest {

  private static final CostFlowSelectionLeaderBalancer BALANCER =
      new CostFlowSelectionLeaderBalancer();

  private static final String DATABASE = "root.database";

  /** This test shows a simple case that greedy algorithm might fail */
  @Test
  public void optimalLeaderDistributionTest() {
    // Prepare Data
    List<TConsensusGroupId> regionGroupIds = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      regionGroupIds.add(new TConsensusGroupId(TConsensusGroupType.DataRegion, i));
    }
    List<TDataNodeLocation> dataNodeLocations = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      dataNodeLocations.add(new TDataNodeLocation().setDataNodeId(i));
    }
    // DataNode-0: [0, 1, 2], DataNode-1: [0, 1]
    // DataNode-2: [0, 2]   , DataNode-3: [1, 2]
    // The result will be unbalanced if select DataNode-2 as leader for RegionGroup-0
    // and select DataNode-3 as leader for RegionGroup-1
    List<TRegionReplicaSet> regionReplicaSets = new ArrayList<>();
    Map<TConsensusGroupId, Map<Integer, RegionStatistics>> regionStatisticsMap = new TreeMap<>();
    regionReplicaSets.add(
        new TRegionReplicaSet(
            regionGroupIds.get(0),
            Arrays.asList(
                dataNodeLocations.get(0), dataNodeLocations.get(1), dataNodeLocations.get(2))));
    Map<Integer, RegionStatistics> region0 = new TreeMap<>();
    region0.put(0, new RegionStatistics(RegionStatus.Unknown));
    region0.put(1, new RegionStatistics(RegionStatus.Running));
    region0.put(2, new RegionStatistics(RegionStatus.Running));
    regionStatisticsMap.put(regionGroupIds.get(0), region0);
    regionReplicaSets.add(
        new TRegionReplicaSet(
            regionGroupIds.get(1),
            Arrays.asList(
                dataNodeLocations.get(0), dataNodeLocations.get(1), dataNodeLocations.get(3))));
    Map<Integer, RegionStatistics> region1 = new TreeMap<>();
    region1.put(0, new RegionStatistics(RegionStatus.Unknown));
    region1.put(1, new RegionStatistics(RegionStatus.Running));
    region1.put(3, new RegionStatistics(RegionStatus.Running));
    regionStatisticsMap.put(regionGroupIds.get(1), region1);
    regionReplicaSets.add(
        new TRegionReplicaSet(
            regionGroupIds.get(2),
            Arrays.asList(
                dataNodeLocations.get(0), dataNodeLocations.get(2), dataNodeLocations.get(3))));
    Map<Integer, RegionStatistics> region2 = new TreeMap<>();
    region2.put(0, new RegionStatistics(RegionStatus.Unknown));
    region2.put(2, new RegionStatistics(RegionStatus.Running));
    region2.put(3, new RegionStatistics(RegionStatus.Running));
    regionStatisticsMap.put(regionGroupIds.get(2), region2);

    // Prepare input parameters
    Map<String, List<TConsensusGroupId>> databaseRegionGroupMap = new TreeMap<>();
    databaseRegionGroupMap.put(DATABASE, regionGroupIds);
    Map<TConsensusGroupId, Set<Integer>> regionReplicaSetMap = new TreeMap<>();
    regionReplicaSets.forEach(
        regionReplicaSet ->
            regionReplicaSetMap.put(
                regionReplicaSet.getRegionId(),
                regionReplicaSet.getDataNodeLocations().stream()
                    .map(TDataNodeLocation::getDataNodeId)
                    .collect(Collectors.toSet())));
    Map<TConsensusGroupId, Integer> regionLeaderMap = new TreeMap<>();
    regionReplicaSets.forEach(
        regionReplicaSet -> regionLeaderMap.put(regionReplicaSet.getRegionId(), 0));
    Map<Integer, NodeStatistics> dataNodeStatisticsMap = new TreeMap<>();
    dataNodeStatisticsMap.put(0, new NodeStatistics(NodeStatus.Unknown));
    dataNodeStatisticsMap.put(1, new NodeStatistics(NodeStatus.Running));
    dataNodeStatisticsMap.put(2, new NodeStatistics(NodeStatus.Running));
    dataNodeStatisticsMap.put(3, new NodeStatistics(NodeStatus.Running));

    // Do balancing
    Map<TConsensusGroupId, Integer> leaderDistribution =
        BALANCER.generateOptimalLeaderDistribution(
            databaseRegionGroupMap,
            regionReplicaSetMap,
            regionLeaderMap,
            dataNodeStatisticsMap,
            regionStatisticsMap);
    // All RegionGroup got a leader
    Assert.assertEquals(3, leaderDistribution.size());
    // Each DataNode has exactly one leader
    Assert.assertEquals(3, new HashSet<>(leaderDistribution.values()).size());
    // MaxFlow is 3
    Assert.assertEquals(3, BALANCER.getMaximumFlow());
    // MinimumCost is 3(switch leader cost) + 3(load cost, rNode -> sDNode)
    // + 3(load cost, sDNode -> tDNode)
    Assert.assertEquals(3 + 3 + 3, BALANCER.getMinimumCost());
  }

  /** The leader will remain the same if all DataNodes are disabled */
  @Test
  public void disableTest() {
    TRegionReplicaSet regionReplicaSet =
        new TRegionReplicaSet(
            new TConsensusGroupId(TConsensusGroupType.DataRegion, 0),
            Arrays.asList(
                new TDataNodeLocation().setDataNodeId(0),
                new TDataNodeLocation().setDataNodeId(1),
                new TDataNodeLocation().setDataNodeId(2)));

    // Prepare input parameters
    Map<String, List<TConsensusGroupId>> databaseRegionGroupMap = new TreeMap<>();
    databaseRegionGroupMap.put(DATABASE, Collections.singletonList(regionReplicaSet.getRegionId()));
    Map<TConsensusGroupId, Set<Integer>> regionReplicaSetMap = new TreeMap<>();
    regionReplicaSetMap.put(
        regionReplicaSet.getRegionId(),
        regionReplicaSet.getDataNodeLocations().stream()
            .map(TDataNodeLocation::getDataNodeId)
            .collect(Collectors.toSet()));
    Map<TConsensusGroupId, Integer> regionLeaderMap = new TreeMap<>();
    regionLeaderMap.put(regionReplicaSet.getRegionId(), 1);
    Map<Integer, NodeStatistics> nodeStatisticsMap = new TreeMap<>();
    nodeStatisticsMap.put(0, new NodeStatistics(NodeStatus.Unknown));
    nodeStatisticsMap.put(1, new NodeStatistics(NodeStatus.ReadOnly));
    nodeStatisticsMap.put(2, new NodeStatistics(NodeStatus.Removing));
    Map<TConsensusGroupId, Map<Integer, RegionStatistics>> regionStatisticsMap = new TreeMap<>();
    Map<Integer, RegionStatistics> regionStatistics = new TreeMap<>();
    regionStatistics.put(0, new RegionStatistics(RegionStatus.Running));
    regionStatistics.put(1, new RegionStatistics(RegionStatus.Running));
    regionStatistics.put(2, new RegionStatistics(RegionStatus.Running));
    regionStatisticsMap.put(regionReplicaSet.getRegionId(), regionStatistics);

    // Do balancing
    Map<TConsensusGroupId, Integer> leaderDistribution =
        BALANCER.generateOptimalLeaderDistribution(
            databaseRegionGroupMap,
            regionReplicaSetMap,
            regionLeaderMap,
            nodeStatisticsMap,
            regionStatisticsMap);
    Assert.assertEquals(1, leaderDistribution.size());
    Assert.assertEquals(1, new HashSet<>(leaderDistribution.values()).size());
    // Leader remains the same
    Assert.assertEquals(
        regionLeaderMap.get(regionReplicaSet.getRegionId()),
        leaderDistribution.get(regionReplicaSet.getRegionId()));
    // MaxFlow is 0
    Assert.assertEquals(0, BALANCER.getMaximumFlow());
    // MinimumCost is 0
    Assert.assertEquals(0, BALANCER.getMinimumCost());
  }

  @Test
  public void migrateTest() {
    // All DataNodes are in Running status
    Map<Integer, NodeStatistics> nodeStatisticsMap = new TreeMap<>();
    nodeStatisticsMap.put(0, new NodeStatistics(NodeStatus.Running));
    nodeStatisticsMap.put(1, new NodeStatistics(NodeStatus.Running));
    // Prepare RegionGroups
    Map<String, List<TConsensusGroupId>> databaseRegionGroupMap = new TreeMap<>();
    Map<TConsensusGroupId, Set<Integer>> regionReplicaSetMap = new TreeMap<>();
    Map<TConsensusGroupId, Integer> regionLeaderMap = new TreeMap<>();
    Map<TConsensusGroupId, Map<Integer, RegionStatistics>> regionStatisticsMap = new TreeMap<>();
    for (int i = 0; i < 5; i++) {
      TConsensusGroupId regionGroupId = new TConsensusGroupId(TConsensusGroupType.DataRegion, i);
      databaseRegionGroupMap
          .computeIfAbsent(DATABASE, empty -> new ArrayList<>())
          .add(regionGroupId);
      TRegionReplicaSet regionReplicaSet =
          new TRegionReplicaSet(
              regionGroupId,
              Arrays.asList(
                  new TDataNodeLocation().setDataNodeId(0),
                  new TDataNodeLocation().setDataNodeId(1)));
      regionReplicaSetMap.put(
          regionGroupId,
          regionReplicaSet.getDataNodeLocations().stream()
              .map(TDataNodeLocation::getDataNodeId)
              .collect(Collectors.toSet()));
      regionLeaderMap.put(regionGroupId, 0);
      // Assuming all Regions are migrating from DataNode-1 to DataNode-2
      Map<Integer, RegionStatistics> regionStatistics = new TreeMap<>();
      regionStatistics.put(0, new RegionStatistics(RegionStatus.Removing));
      regionStatistics.put(1, new RegionStatistics(RegionStatus.Running));
      regionStatisticsMap.put(regionGroupId, regionStatistics);
    }

    // Do balancing
    Map<TConsensusGroupId, Integer> leaderDistribution =
        BALANCER.generateOptimalLeaderDistribution(
            databaseRegionGroupMap,
            regionReplicaSetMap,
            regionLeaderMap,
            nodeStatisticsMap,
            regionStatisticsMap);
    for (int i = 0; i < 5; i++) {
      // All RegionGroups' leader should be DataNode-1
      Assert.assertEquals(
          1,
          leaderDistribution
              .get(new TConsensusGroupId(TConsensusGroupType.DataRegion, i))
              .intValue());
    }
  }

  /**
   * In this case shows the balance ability for big cluster.
   *
   * <p>i.e. Simulate 1500 RegionGroups and 300 DataNodes
   */
  @Test
  public void bigClusterTest() {
    final int regionGroupNum = 1500;
    final int dataNodeNum = 300;
    final int replicationFactor = 3;
    Map<Integer, NodeStatistics> dataNodeStatisticsMap = new TreeMap<>();
    for (int i = 0; i < dataNodeNum; i++) {
      dataNodeStatisticsMap.put(i, new NodeStatistics(NodeStatus.Running));
    }

    // The loadCost for each DataNode are the same
    int x = regionGroupNum / dataNodeNum;
    int loadCost = 2 * x * x;

    int dataNodeId = 0;
    Random random = new Random();
    Map<String, List<TConsensusGroupId>> databaseRegionGroupMap = new TreeMap<>();
    databaseRegionGroupMap.put(DATABASE, new ArrayList<>());
    Map<TConsensusGroupId, Set<Integer>> regionReplicaSetMap = new TreeMap<>();
    Map<TConsensusGroupId, Integer> regionLeaderMap = new TreeMap<>();
    Map<TConsensusGroupId, Map<Integer, RegionStatistics>> regionStatisticsMap = new TreeMap<>();
    for (int i = 0; i < regionGroupNum; i++) {
      TConsensusGroupId regionGroupId = new TConsensusGroupId(TConsensusGroupType.DataRegion, i);
      int leaderId = (dataNodeId + random.nextInt(replicationFactor)) % dataNodeNum;

      TRegionReplicaSet regionReplicaSet = new TRegionReplicaSet();
      regionReplicaSet.setRegionId(regionGroupId);
      Map<Integer, RegionStatistics> regionStatistics = new TreeMap<>();
      for (int j = 0; j < 3; j++) {
        regionReplicaSet.addToDataNodeLocations(new TDataNodeLocation().setDataNodeId(dataNodeId));
        regionStatistics.put(dataNodeId, new RegionStatistics(RegionStatus.Running));
        dataNodeId = (dataNodeId + 1) % dataNodeNum;
      }
      regionStatisticsMap.put(regionGroupId, regionStatistics);

      databaseRegionGroupMap.get(DATABASE).add(regionGroupId);
      regionReplicaSetMap.put(
          regionGroupId,
          regionReplicaSet.getDataNodeLocations().stream()
              .map(TDataNodeLocation::getDataNodeId)
              .collect(Collectors.toSet()));
      regionLeaderMap.put(regionGroupId, leaderId);
    }

    // Do balancing
    Map<TConsensusGroupId, Integer> leaderDistribution =
        BALANCER.generateOptimalLeaderDistribution(
            databaseRegionGroupMap,
            regionReplicaSetMap,
            regionLeaderMap,
            dataNodeStatisticsMap,
            regionStatisticsMap);
    // All RegionGroup got a leader
    Assert.assertEquals(regionGroupNum, leaderDistribution.size());

    Map<Integer, Integer> leaderCounter = new ConcurrentHashMap<>();
    leaderDistribution.values().forEach(leaderId -> leaderCounter.merge(leaderId, 1, Integer::sum));
    // Every DataNode has leader
    Assert.assertEquals(dataNodeNum, leaderCounter.size());
    // Every DataNode has exactly regionGroupNum / dataNodeNum leaders
    for (int i = 0; i < dataNodeNum; i++) {
      Assert.assertEquals(regionGroupNum / dataNodeNum, leaderCounter.get(i).intValue());
    }

    // MaxFlow is regionGroupNum
    Assert.assertEquals(regionGroupNum, BALANCER.getMaximumFlow());

    int minimumCost = BALANCER.getMinimumCost();
    Assert.assertTrue(minimumCost >= loadCost * dataNodeNum);
    // The number of RegionGroups who have switched leader
    int switchCost = minimumCost - loadCost * dataNodeNum;
    AtomicInteger switchCount = new AtomicInteger(0);
    regionLeaderMap.forEach(
        (regionGroupId, originLeader) -> {
          if (!Objects.equals(originLeader, leaderDistribution.get(regionGroupId))) {
            switchCount.getAndIncrement();
          }
        });
    Assert.assertEquals(switchCost, switchCount.get());

    System.out.printf(
        "MCF algorithm switch leader for %s times to construct a balanced leader distribution of 300 DataNodes and 1500 RegionGroups cluster.%n",
        switchCost);
  }
}
