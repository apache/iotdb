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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class GreedyLeaderBalancerTest {

  private static final GreedyLeaderBalancer BALANCER = new GreedyLeaderBalancer();

  @Test
  public void optimalLeaderDistributionTest() {
    Map<TConsensusGroupId, TRegionReplicaSet> regionReplicaSetMap = new HashMap<>();
    Map<TConsensusGroupId, Integer> regionLeaderMap = new HashMap<>();
    Set<Integer> disabledDataNodeSet = new HashSet<>();
    Random random = new Random();

    // Build 9 RegionGroups in DataNodes 0~2
    for (int i = 0; i < 9; i++) {
      TConsensusGroupId regionGroupId = new TConsensusGroupId(TConsensusGroupType.DataRegion, i);
      TRegionReplicaSet regionReplicaSet =
          new TRegionReplicaSet(
              regionGroupId,
              Arrays.asList(
                  new TDataNodeLocation().setDataNodeId(0),
                  new TDataNodeLocation().setDataNodeId(1),
                  new TDataNodeLocation().setDataNodeId(2)));
      regionReplicaSetMap.put(regionGroupId, regionReplicaSet);
      regionLeaderMap.put(regionGroupId, random.nextInt(3));
    }

    // Build 9 RegionGroups in DataNodes 3~5
    for (int i = 9; i < 18; i++) {
      TConsensusGroupId regionGroupId = new TConsensusGroupId(TConsensusGroupType.DataRegion, i);
      TRegionReplicaSet regionReplicaSet =
          new TRegionReplicaSet(
              regionGroupId,
              Arrays.asList(
                  new TDataNodeLocation().setDataNodeId(3),
                  new TDataNodeLocation().setDataNodeId(4),
                  new TDataNodeLocation().setDataNodeId(5)));
      regionReplicaSetMap.put(regionGroupId, regionReplicaSet);
      regionLeaderMap.put(regionGroupId, 3 + random.nextInt(3));
    }

    Map<TConsensusGroupId, Integer> leaderDistribution =
        BALANCER.generateOptimalLeaderDistribution(
            regionReplicaSetMap, regionLeaderMap, disabledDataNodeSet);
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
    Map<TConsensusGroupId, TRegionReplicaSet> regionReplicaSetMap = new HashMap<>();
    Map<TConsensusGroupId, Integer> regionLeaderMap = new HashMap<>();
    Set<Integer> disabledDataNodeSet = new HashSet<>();

    disabledDataNodeSet.add(1);
    disabledDataNodeSet.add(4);

    // Build 10 RegionGroup whose leaders are all 1(Disabled)
    for (int i = 0; i < 10; i++) {
      TConsensusGroupId regionGroupId = new TConsensusGroupId(TConsensusGroupType.DataRegion, i);
      TRegionReplicaSet regionReplicaSet =
          new TRegionReplicaSet(
              regionGroupId,
              Arrays.asList(
                  new TDataNodeLocation().setDataNodeId(0),
                  new TDataNodeLocation().setDataNodeId(1),
                  new TDataNodeLocation().setDataNodeId(2)));
      regionReplicaSetMap.put(regionGroupId, regionReplicaSet);
      regionLeaderMap.put(regionGroupId, 1);
    }

    // Build 10 RegionGroup whose leaders are all 4(Disabled)
    for (int i = 10; i < 20; i++) {
      TConsensusGroupId regionGroupId = new TConsensusGroupId(TConsensusGroupType.DataRegion, i);
      TRegionReplicaSet regionReplicaSet =
          new TRegionReplicaSet(
              regionGroupId,
              Arrays.asList(
                  new TDataNodeLocation().setDataNodeId(3),
                  new TDataNodeLocation().setDataNodeId(4),
                  new TDataNodeLocation().setDataNodeId(5)));
      regionReplicaSetMap.put(regionGroupId, regionReplicaSet);
      regionLeaderMap.put(regionGroupId, 4);
    }

    Map<TConsensusGroupId, Integer> leaderDistribution =
        BALANCER.generateOptimalLeaderDistribution(
            regionReplicaSetMap, regionLeaderMap, disabledDataNodeSet);
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
