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
import org.apache.iotdb.confignode.manager.load.heartbeat.BaseNodeCache;
import org.apache.iotdb.confignode.manager.load.heartbeat.DataNodeHeartbeatCache;
import org.apache.iotdb.confignode.manager.load.heartbeat.NodeHeartbeatSample;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LoadScoreGreedyRouterTest {

  @Test
  public void testGenLoadScoreGreedyRoutingPolicy() {
    /* Build TDataNodeLocations */
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

    /* Build nodeCacheMap */
    long currentTimeMillis = System.currentTimeMillis();
    Map<Integer, BaseNodeCache> nodeCacheMap = new HashMap<>();
    for (int i = 0; i < 6; i++) {
      nodeCacheMap.put(i, new DataNodeHeartbeatCache());
      // Simulate that the DataNode-i returned a heartbeat at (currentTime - i * 1000) ms
      nodeCacheMap
          .get(i)
          .cacheHeartbeatSample(
              new NodeHeartbeatSample(currentTimeMillis - i * 1000, currentTimeMillis - i * 1000));
    }
    nodeCacheMap.values().forEach(BaseNodeCache::updateLoadStatistic);

    /* Get the loadScoreMap */
    Map<Integer, Long> loadScoreMap = new ConcurrentHashMap<>();
    nodeCacheMap.forEach(
        (dataNodeId, heartbeatCache) ->
            loadScoreMap.put(dataNodeId, heartbeatCache.getLoadScore()));

    /* Build TRegionReplicaSet */
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

    /* Check result */
    Map<TConsensusGroupId, TRegionReplicaSet> result =
        new LoadScoreGreedyRouter(loadScoreMap)
            .genLatestRegionRouteMap(Arrays.asList(regionReplicaSet1, regionReplicaSet2));
    Assert.assertEquals(2, result.size());

    TRegionReplicaSet result1 = result.get(groupId1);
    for (int i = 0; i < 3; i++) {
      Assert.assertEquals(dataNodeLocations.get(i), result1.getDataNodeLocations().get(i));
    }

    TRegionReplicaSet result2 = result.get(groupId2);
    for (int i = 3; i < 6; i++) {
      Assert.assertEquals(dataNodeLocations.get(i), result2.getDataNodeLocations().get(i - 3));
    }
  }
}
