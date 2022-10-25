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
package org.apache.iotdb.confignode.manager.load.balancer.router;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.cluster.RegionStatus;
import org.apache.iotdb.confignode.manager.node.BaseNodeCache;
import org.apache.iotdb.confignode.manager.node.DataNodeHeartbeatCache;
import org.apache.iotdb.confignode.manager.node.NodeHeartbeatSample;
import org.apache.iotdb.confignode.manager.partition.RegionGroupCache;
import org.apache.iotdb.confignode.manager.partition.RegionHeartbeatSample;
import org.apache.iotdb.confignode.persistence.partition.statistics.RegionGroupStatistics;
import org.apache.iotdb.mpp.rpc.thrift.THeartbeatResp;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LeaderRouterTest {

  @Test
  public void testGenRealTimeRoutingPolicy() {
    // Build TDataNodeLocations
    List<TDataNodeLocation> dataNodeLocations = new ArrayList<>();
    for (int i = 0; i < 6; i++) {
      dataNodeLocations.add(
          new TDataNodeLocation(
              i,
              new TEndPoint("0.0.0.0", 6667 + i),
              new TEndPoint("0.0.0.0", 9003 + i),
              new TEndPoint("0.0.0.0", 8777 + i),
              new TEndPoint("0.0.0.0", 40010 + i),
              new TEndPoint("0.0.0.0", 50010 + i)));
    }

    // Build nodeCacheMap
    long currentTimeMillis = System.currentTimeMillis();
    Map<Integer, BaseNodeCache> nodeCacheMap = new HashMap<>();
    for (int i = 0; i < 6; i++) {
      nodeCacheMap.put(i, new DataNodeHeartbeatCache());
      // Simulate that the DataNode-i returned a heartbeat at (currentTime - i * 1000) ms
      nodeCacheMap
          .get(i)
          .cacheHeartbeatSample(
              new NodeHeartbeatSample(
                  new THeartbeatResp(currentTimeMillis - i * 1000, NodeStatus.Running.getStatus()),
                  currentTimeMillis - i * 1000));
    }
    nodeCacheMap.values().forEach(BaseNodeCache::updateNodeStatistics);

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

    // Build regionGroupCacheMap
    Map<TConsensusGroupId, RegionGroupCache> regionGroupCacheMap = new HashMap<>();
    regionGroupCacheMap.put(groupId1, new RegionGroupCache(groupId1));
    regionGroupCacheMap.put(groupId2, new RegionGroupCache(groupId2));

    /* Simulate ratis consensus protocol(only one leader) */
    regionGroupCacheMap
        .get(groupId1)
        .cacheHeartbeatSample(0, new RegionHeartbeatSample(10, 10, false, RegionStatus.Running));
    regionGroupCacheMap
        .get(groupId1)
        .cacheHeartbeatSample(1, new RegionHeartbeatSample(11, 11, true, RegionStatus.Running));
    regionGroupCacheMap
        .get(groupId1)
        .cacheHeartbeatSample(2, new RegionHeartbeatSample(12, 12, false, RegionStatus.Running));
    regionGroupCacheMap
        .get(groupId2)
        .cacheHeartbeatSample(3, new RegionHeartbeatSample(13, 13, false, RegionStatus.Running));
    regionGroupCacheMap
        .get(groupId2)
        .cacheHeartbeatSample(4, new RegionHeartbeatSample(14, 14, true, RegionStatus.Running));
    regionGroupCacheMap
        .get(groupId2)
        .cacheHeartbeatSample(5, new RegionHeartbeatSample(15, 15, false, RegionStatus.Running));

    // Update RegionGroupStatistics and get leaderMap
    Map<TConsensusGroupId, Integer> leaderMap = new HashMap<>();
    regionGroupCacheMap.forEach(
        (groupId, regionGroupCache) -> {
          RegionGroupStatistics regionGroupStatistics =
              regionGroupCache.updateRegionGroupStatistics();
          Assert.assertNotNull(regionGroupStatistics);
          leaderMap.put(groupId, regionGroupStatistics.getLeaderDataNodeId());
        });

    // Check result
    Map<TConsensusGroupId, TRegionReplicaSet> result =
        new LeaderRouter(leaderMap, loadScoreMap).getLatestRegionRouteMap(regionReplicaSets);
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

    /* Simulate multiLeader consensus protocol(Each Region believes it is the leader) */
    regionGroupCacheMap
        .get(groupId1)
        .cacheHeartbeatSample(
            0, new RegionHeartbeatSample(100000, 100000, true, RegionStatus.Running));
    regionGroupCacheMap
        .get(groupId1)
        .cacheHeartbeatSample(
            1, new RegionHeartbeatSample(100010, 100010, true, RegionStatus.Running));
    regionGroupCacheMap
        .get(groupId1)
        .cacheHeartbeatSample(
            2, new RegionHeartbeatSample(100020, 100020, true, RegionStatus.Running));
    regionGroupCacheMap
        .get(groupId2)
        .cacheHeartbeatSample(
            3, new RegionHeartbeatSample(100030, 100030, true, RegionStatus.Running));
    regionGroupCacheMap
        .get(groupId2)
        .cacheHeartbeatSample(
            4, new RegionHeartbeatSample(100040, 100040, true, RegionStatus.Running));
    regionGroupCacheMap
        .get(groupId2)
        .cacheHeartbeatSample(
            5, new RegionHeartbeatSample(100050, 100050, true, RegionStatus.Running));

    // Get leaderMap
    leaderMap.clear();
    regionGroupCacheMap.forEach(
        (groupId, regionGroupCache) -> {
          RegionGroupStatistics regionGroupStatistics =
              regionGroupCache.updateRegionGroupStatistics();
          Assert.assertNotNull(regionGroupStatistics);
          leaderMap.put(groupId, regionGroupStatistics.getLeaderDataNodeId());
        });

    // Check result
    result = new LeaderRouter(leaderMap, loadScoreMap).getLatestRegionRouteMap(regionReplicaSets);
    result1 = result.get(groupId1);
    // The others will only be sorted by loadScore
    Assert.assertEquals(dataNodeLocations.get(2), result1.getDataNodeLocations().get(0));
    Assert.assertEquals(dataNodeLocations.get(0), result1.getDataNodeLocations().get(1));
    Assert.assertEquals(dataNodeLocations.get(1), result1.getDataNodeLocations().get(2));
    result2 = result.get(groupId2);
    // The others will only be sorted by loadScore
    Assert.assertEquals(dataNodeLocations.get(5), result2.getDataNodeLocations().get(0));
    Assert.assertEquals(dataNodeLocations.get(3), result2.getDataNodeLocations().get(1));
    Assert.assertEquals(dataNodeLocations.get(4), result2.getDataNodeLocations().get(2));

    /* Simulate multiLeader consensus protocol with a DataNode fails down */
    regionGroupCacheMap
        .get(groupId1)
        .cacheHeartbeatSample(
            0, new RegionHeartbeatSample(200030, 200030, true, RegionStatus.Running));
    regionGroupCacheMap
        .get(groupId1)
        .cacheHeartbeatSample(
            1, new RegionHeartbeatSample(200031, 200031, true, RegionStatus.Running));
    regionGroupCacheMap
        .get(groupId2)
        .cacheHeartbeatSample(
            3, new RegionHeartbeatSample(200033, 200033, true, RegionStatus.Running));
    regionGroupCacheMap
        .get(groupId2)
        .cacheHeartbeatSample(
            4, new RegionHeartbeatSample(200034, 200034, true, RegionStatus.Running));

    // Get leaderMap
    leaderMap.clear();
    regionGroupCacheMap.forEach(
        (groupId, regionGroupCache) -> {
          RegionGroupStatistics regionGroupStatistics =
              regionGroupCache.updateRegionGroupStatistics();
          Assert.assertNotNull(regionGroupStatistics);
          leaderMap.put(groupId, regionGroupStatistics.getLeaderDataNodeId());
        });

    // Check result
    result = new LeaderRouter(leaderMap, loadScoreMap).getLatestRegionRouteMap(regionReplicaSets);
    result1 = result.get(groupId1);
    // The others will only be sorted by loadScore
    Assert.assertEquals(dataNodeLocations.get(1), result1.getDataNodeLocations().get(0));
    Assert.assertEquals(dataNodeLocations.get(0), result1.getDataNodeLocations().get(1));
    Assert.assertEquals(dataNodeLocations.get(2), result1.getDataNodeLocations().get(2));
    result2 = result.get(groupId2);
    // The others will only be sorted by loadScore
    Assert.assertEquals(dataNodeLocations.get(4), result2.getDataNodeLocations().get(0));
    Assert.assertEquals(dataNodeLocations.get(3), result2.getDataNodeLocations().get(1));
    Assert.assertEquals(dataNodeLocations.get(5), result2.getDataNodeLocations().get(2));
  }
}
