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
package org.apache.iotdb.confignode.manager.load.balancer.router.priority;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.manager.load.cache.node.BaseNodeCache;
import org.apache.iotdb.confignode.manager.load.cache.node.DataNodeHeartbeatCache;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeHeartbeatSample;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LeaderPriorityBalancerTest {

  @Test
  public void testGenRealTimeRoutingPolicy() {
    // Build TDataNodeLocations
    List<TDataNodeLocation> dataNodeLocations = new ArrayList<>();
    for (int i = 0; i < 6; i++) {
      dataNodeLocations.add(
          new TDataNodeLocation(
              i,
              new TEndPoint("0.0.0.0", 6667 + i),
              new TEndPoint("0.0.0.0", 10730 + i),
              new TEndPoint("0.0.0.0", 10740 + i),
              new TEndPoint("0.0.0.0", 10760 + i),
              new TEndPoint("0.0.0.0", 10750 + i)));
    }

    // Build nodeCacheMap
    long currentTimeNs = System.nanoTime();
    Map<Integer, BaseNodeCache> nodeCacheMap = new HashMap<>();
    for (int i = 0; i < 6; i++) {
      nodeCacheMap.put(i, new DataNodeHeartbeatCache(i));
      if (i != 2 && i != 5) {
        nodeCacheMap
            .get(i)
            .cacheHeartbeatSample(new NodeHeartbeatSample(currentTimeNs, NodeStatus.Running));
      }
    }
    nodeCacheMap.values().forEach(baseNodeCache -> baseNodeCache.updateCurrentStatistics(false));

    // Get the loadScoreMap
    Map<Integer, Long> loadScoreMap = new ConcurrentHashMap<>();
    nodeCacheMap.forEach(
        (dataNodeId, heartbeatCache) ->
            loadScoreMap.put(dataNodeId, heartbeatCache.getLoadScore()));

    // Build TRegionReplicaSet
    TConsensusGroupId groupId1 = new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 1);
    TRegionReplicaSet regionReplicaSet1 =
        new TRegionReplicaSet(
            groupId1,
            Arrays.asList(
                dataNodeLocations.get(2), dataNodeLocations.get(1), dataNodeLocations.get(0)));
    TConsensusGroupId groupId2 = new TConsensusGroupId(TConsensusGroupType.DataRegion, 2);
    TRegionReplicaSet regionReplicaSet2 =
        new TRegionReplicaSet(
            groupId2,
            Arrays.asList(
                dataNodeLocations.get(5), dataNodeLocations.get(4), dataNodeLocations.get(3)));
    List<TRegionReplicaSet> regionReplicaSets = Arrays.asList(regionReplicaSet1, regionReplicaSet2);

    // Build leaderMap
    Map<TConsensusGroupId, Integer> leaderMap = new HashMap<>();
    leaderMap.put(groupId1, 1);
    leaderMap.put(groupId2, 4);

    // Check result
    Map<TConsensusGroupId, TRegionReplicaSet> result =
        new LeaderPriorityBalancer()
            .generateOptimalRoutePriority(regionReplicaSets, leaderMap, loadScoreMap);
    TRegionReplicaSet result1 = result.get(groupId1);
    // Leader first
    Assert.assertEquals(dataNodeLocations.get(1), result1.getDataNodeLocations().get(0));
    // The others will be sorted by loadScore
    Assert.assertEquals(dataNodeLocations.get(0), result1.getDataNodeLocations().get(1));
    Assert.assertEquals(dataNodeLocations.get(2), result1.getDataNodeLocations().get(2));
    TRegionReplicaSet result2 = result.get(groupId2);
    // Leader first
    Assert.assertEquals(dataNodeLocations.get(4), result2.getDataNodeLocations().get(0));
    // The others will be sorted by loadScore
    Assert.assertEquals(dataNodeLocations.get(3), result2.getDataNodeLocations().get(1));
    Assert.assertEquals(dataNodeLocations.get(5), result2.getDataNodeLocations().get(2));
  }

  @Test
  public void testLeaderUnavailable() {
    // Build TDataNodeLocations
    List<TDataNodeLocation> dataNodeLocations = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      dataNodeLocations.add(
          new TDataNodeLocation(
              i,
              new TEndPoint("0.0.0.0", 6667 + i),
              new TEndPoint("0.0.0.0", 10730 + i),
              new TEndPoint("0.0.0.0", 10740 + i),
              new TEndPoint("0.0.0.0", 10760 + i),
              new TEndPoint("0.0.0.0", 10750 + i)));
    }

    // Build TRegionReplicaSet
    TConsensusGroupId groupId1 = new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 1);
    TRegionReplicaSet regionReplicaSet1 =
        new TRegionReplicaSet(
            groupId1,
            Arrays.asList(
                dataNodeLocations.get(2), dataNodeLocations.get(1), dataNodeLocations.get(0)));

    // Build leaderMap
    Map<TConsensusGroupId, Integer> leaderMap = new HashMap<>();
    leaderMap.put(groupId1, 1);

    // Build loadScoreMap
    Map<Integer, Long> loadScoreMap = new ConcurrentHashMap<>();
    loadScoreMap.put(0, 10L);
    loadScoreMap.put(2, 20L);
    // The leader is DataNode-1, but it's unavailable
    loadScoreMap.put(1, Long.MAX_VALUE);

    // Check result
    Map<TConsensusGroupId, TRegionReplicaSet> result =
        new LeaderPriorityBalancer()
            .generateOptimalRoutePriority(
                Collections.singletonList(regionReplicaSet1), leaderMap, loadScoreMap);
    // Only sorted by loadScore since the leader is unavailable
    Assert.assertEquals(
        dataNodeLocations.get(0), result.get(groupId1).getDataNodeLocations().get(0));
    Assert.assertEquals(
        dataNodeLocations.get(2), result.get(groupId1).getDataNodeLocations().get(1));
    Assert.assertEquals(
        dataNodeLocations.get(1), result.get(groupId1).getDataNodeLocations().get(2));
  }
}
