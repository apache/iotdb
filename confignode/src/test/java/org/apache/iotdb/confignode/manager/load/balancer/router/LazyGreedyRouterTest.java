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
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class LazyGreedyRouterTest {

  @Test
  public void testGenLatestRegionRouteMap() {
    LazyGreedyRouter lazyGreedyRouter = new LazyGreedyRouter();

    /* Prepare TRegionReplicaSets */
    List<TRegionReplicaSet> regionReplicaSetList = new ArrayList<>();
    for (int i = 0; i < 12; i++) {
      TRegionReplicaSet regionReplicaSet = new TRegionReplicaSet();
      regionReplicaSet.setRegionId(new TConsensusGroupId(TConsensusGroupType.DataRegion, i));
      for (int j = 1; j <= 3; j++) {
        regionReplicaSet.addToDataNodeLocations(new TDataNodeLocation().setDataNodeId(j));
      }
      regionReplicaSetList.add(regionReplicaSet);
    }

    /* Test1: The number of leaders in each DataNode should be approximately 4 */
    Map<TConsensusGroupId, TRegionReplicaSet> routeMap =
        lazyGreedyRouter.getLatestRegionRouteMap(regionReplicaSetList);
    Map<Integer, AtomicInteger> leaderCounter = new HashMap<>();
    routeMap
        .values()
        .forEach(
            regionReplicaSet ->
                leaderCounter
                    .computeIfAbsent(
                        regionReplicaSet.getDataNodeLocations().get(0).getDataNodeId(),
                        empty -> new AtomicInteger(0))
                    .getAndIncrement());
    Assert.assertEquals(3, leaderCounter.size());
    for (int i = 1; i <= 3; i++) {
      Assert.assertTrue(3 <= leaderCounter.get(i).get());
      Assert.assertTrue(leaderCounter.get(i).get() <= 5);
    }

    /* Unknown DataNodes */
    List<TDataNodeConfiguration> dataNodeConfigurations = new ArrayList<>();
    dataNodeConfigurations.add(
        new TDataNodeConfiguration().setLocation(new TDataNodeLocation().setDataNodeId(2)));

    /* Test2: The number of leaders in DataNode-1 and DataNode-3 should be approximately 6 */
    lazyGreedyRouter.updateDisabledDataNodes(dataNodeConfigurations);
    leaderCounter.clear();
    routeMap = lazyGreedyRouter.getLatestRegionRouteMap(regionReplicaSetList);
    routeMap
        .values()
        .forEach(
            regionReplicaSet ->
                leaderCounter
                    .computeIfAbsent(
                        regionReplicaSet.getDataNodeLocations().get(0).getDataNodeId(),
                        empty -> new AtomicInteger(0))
                    .getAndIncrement());
    Assert.assertEquals(2, leaderCounter.size());
    Assert.assertTrue(4 <= leaderCounter.get(1).get());
    Assert.assertTrue(leaderCounter.get(1).get() <= 8);
    Assert.assertTrue(4 <= leaderCounter.get(3).get());
    Assert.assertTrue(leaderCounter.get(3).get() <= 8);
  }

  @Test
  public void testGenLatestRegionRouteMapWithDifferentReplicaSize() {
    LazyGreedyRouter lazyGreedyRouter = new LazyGreedyRouter();

    /* Prepare TRegionReplicaSets */
    List<TRegionReplicaSet> regionReplicaSetList = new ArrayList<>();
    for (int i = 0; i < 12; i++) {
      TRegionReplicaSet regionReplicaSet = new TRegionReplicaSet();
      regionReplicaSet.setRegionId(new TConsensusGroupId(TConsensusGroupType.DataRegion, i));
      for (int j = 1; j <= 3; j++) {
        regionReplicaSet.addToDataNodeLocations(new TDataNodeLocation().setDataNodeId(j));
      }
      regionReplicaSetList.add(regionReplicaSet);
    }
    int dataNodeId = 0;
    for (int i = 12; i < 18; i++) {
      TRegionReplicaSet regionReplicaSet = new TRegionReplicaSet();
      regionReplicaSet.setRegionId(new TConsensusGroupId(TConsensusGroupType.DataRegion, i));
      for (int j = 0; j < 2; j++) {
        regionReplicaSet.addToDataNodeLocations(
            new TDataNodeLocation().setDataNodeId(dataNodeId + 1));
        dataNodeId = (dataNodeId + 1) % 3;
      }
      regionReplicaSetList.add(regionReplicaSet);
    }

    /* Test1: The number of leaders in each DataNode should be approximately 6 */
    Map<TConsensusGroupId, TRegionReplicaSet> routeMap =
        lazyGreedyRouter.getLatestRegionRouteMap(regionReplicaSetList);
    Map<Integer, AtomicInteger> leaderCounter = new HashMap<>();
    routeMap
        .values()
        .forEach(
            regionReplicaSet ->
                leaderCounter
                    .computeIfAbsent(
                        regionReplicaSet.getDataNodeLocations().get(0).getDataNodeId(),
                        empty -> new AtomicInteger(0))
                    .getAndIncrement());
    Assert.assertEquals(3, leaderCounter.size());
    for (int i = 1; i <= 3; i++) {
      Assert.assertTrue(4 <= leaderCounter.get(i).get());
      Assert.assertTrue(leaderCounter.get(i).get() <= 8);
    }

    /* Unknown DataNodes */
    List<TDataNodeConfiguration> dataNodeConfigurations = new ArrayList<>();
    dataNodeConfigurations.add(
        new TDataNodeConfiguration().setLocation(new TDataNodeLocation().setDataNodeId(2)));

    /* Test2: The number of leaders in DataNode-1 and DataNode-3 should be exactly 9 */
    lazyGreedyRouter.updateDisabledDataNodes(dataNodeConfigurations);
    leaderCounter.clear();
    routeMap = lazyGreedyRouter.getLatestRegionRouteMap(regionReplicaSetList);
    routeMap
        .values()
        .forEach(
            regionReplicaSet ->
                leaderCounter
                    .computeIfAbsent(
                        regionReplicaSet.getDataNodeLocations().get(0).getDataNodeId(),
                        empty -> new AtomicInteger(0))
                    .getAndIncrement());
    Assert.assertEquals(2, leaderCounter.size());
    Assert.assertTrue(7 <= leaderCounter.get(1).get());
    Assert.assertTrue(leaderCounter.get(1).get() <= 11);
    Assert.assertTrue(7 <= leaderCounter.get(3).get());
    Assert.assertTrue(leaderCounter.get(3).get() <= 11);
  }
}
