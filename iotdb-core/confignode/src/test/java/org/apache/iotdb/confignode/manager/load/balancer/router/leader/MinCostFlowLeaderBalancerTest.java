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

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class MinCostFlowLeaderBalancerTest {

  private static final MinCostFlowLeaderBalancer BALANCER = new MinCostFlowLeaderBalancer();

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

    List<TRegionReplicaSet> regionReplicaSets = new ArrayList<>();
    regionReplicaSets.add(
        new TRegionReplicaSet(
            regionGroupIds.get(0),
            Arrays.asList(
                dataNodeLocations.get(0), dataNodeLocations.get(1), dataNodeLocations.get(2))));
    regionReplicaSets.add(
        new TRegionReplicaSet(
            regionGroupIds.get(1),
            Arrays.asList(
                dataNodeLocations.get(0), dataNodeLocations.get(1), dataNodeLocations.get(3))));
    regionReplicaSets.add(
        new TRegionReplicaSet(
            regionGroupIds.get(2),
            Arrays.asList(
                dataNodeLocations.get(0), dataNodeLocations.get(2), dataNodeLocations.get(3))));

    // Prepare input parameters
    Map<TConsensusGroupId, TRegionReplicaSet> regionReplicaSetMap = new HashMap<>();
    regionReplicaSets.forEach(
        regionReplicaSet ->
            regionReplicaSetMap.put(regionReplicaSet.getRegionId(), regionReplicaSet));
    Map<TConsensusGroupId, Integer> regionLeaderMap = new HashMap<>();
    regionReplicaSets.forEach(
        regionReplicaSet -> regionLeaderMap.put(regionReplicaSet.getRegionId(), 0));
    Set<Integer> disabledDataNodeSet = new HashSet<>();
    disabledDataNodeSet.add(0);

    // Do balancing
    Map<TConsensusGroupId, Integer> leaderDistribution =
        BALANCER.generateOptimalLeaderDistribution(
            regionReplicaSetMap, regionLeaderMap, disabledDataNodeSet);
    // All RegionGroup got a leader
    Assert.assertEquals(3, leaderDistribution.size());
    // Each DataNode occurs exactly once
    Assert.assertEquals(3, new HashSet<>(leaderDistribution.values()).size());
    // MaxFlow is 3
    Assert.assertEquals(3, BALANCER.getMaximumFlow());
    // MinimumCost is 3(switch leader cost) + 3(load cost, 1 for each DataNode)
    Assert.assertEquals(3 + 3, BALANCER.getMinimumCost());
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
    Map<TConsensusGroupId, TRegionReplicaSet> regionReplicaSetMap = new HashMap<>();
    regionReplicaSetMap.put(regionReplicaSet.getRegionId(), regionReplicaSet);
    Map<TConsensusGroupId, Integer> regionLeaderMap = new HashMap<>();
    regionLeaderMap.put(regionReplicaSet.getRegionId(), 1);
    Set<Integer> disabledDataNodeSet = new HashSet<>();
    disabledDataNodeSet.add(0);
    disabledDataNodeSet.add(1);
    disabledDataNodeSet.add(2);

    // Do balancing
    Map<TConsensusGroupId, Integer> leaderDistribution =
        BALANCER.generateOptimalLeaderDistribution(
            regionReplicaSetMap, regionLeaderMap, disabledDataNodeSet);
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

    // The loadCost for each DataNode are the same
    int x = regionGroupNum / dataNodeNum;
    // i.e. formula of 1^2 + 2^2 + 3^2 + ...
    int loadCost = x * (x + 1) * (2 * x + 1) / 6;

    int dataNodeId = 0;
    Random random = new Random();
    Map<TConsensusGroupId, TRegionReplicaSet> regionReplicaSetMap = new HashMap<>();
    Map<TConsensusGroupId, Integer> regionLeaderMap = new HashMap<>();
    for (int i = 0; i < regionGroupNum; i++) {
      TConsensusGroupId regionGroupId = new TConsensusGroupId(TConsensusGroupType.DataRegion, i);
      int leaderId = (dataNodeId + random.nextInt(replicationFactor)) % dataNodeNum;

      TRegionReplicaSet regionReplicaSet = new TRegionReplicaSet();
      regionReplicaSet.setRegionId(regionGroupId);
      for (int j = 0; j < 3; j++) {
        regionReplicaSet.addToDataNodeLocations(new TDataNodeLocation().setDataNodeId(dataNodeId));
        dataNodeId = (dataNodeId + 1) % dataNodeNum;
      }

      regionReplicaSetMap.put(regionGroupId, regionReplicaSet);
      regionLeaderMap.put(regionGroupId, leaderId);
    }

    // Do balancing
    Map<TConsensusGroupId, Integer> leaderDistribution =
        BALANCER.generateOptimalLeaderDistribution(
            regionReplicaSetMap, regionLeaderMap, new HashSet<>());
    // All RegionGroup got a leader
    Assert.assertEquals(regionGroupNum, leaderDistribution.size());

    Map<Integer, AtomicInteger> leaderCounter = new ConcurrentHashMap<>();
    leaderDistribution
        .values()
        .forEach(
            leaderId ->
                leaderCounter
                    .computeIfAbsent(leaderId, empty -> new AtomicInteger(0))
                    .getAndIncrement());
    // Every DataNode has leader
    Assert.assertEquals(dataNodeNum, leaderCounter.size());
    // Every DataNode has exactly regionGroupNum / dataNodeNum leaders
    leaderCounter
        .values()
        .forEach(leaderNum -> Assert.assertEquals(regionGroupNum / dataNodeNum, leaderNum.get()));

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
