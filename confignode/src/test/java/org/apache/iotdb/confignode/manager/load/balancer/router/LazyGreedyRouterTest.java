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

  private final LazyGreedyRouter lazyGreedyRouter = new LazyGreedyRouter();

  @Test
  public void genLatestRegionRouteMap() {
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

    /* Test1: The number of leaders in each DataNode should be exactly 4 */
    Map<TConsensusGroupId, TRegionReplicaSet> routeMap =
        lazyGreedyRouter.genLatestRegionRouteMap(regionReplicaSetList);
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
      Assert.assertEquals(4, leaderCounter.get(i).get());
    }

    /* Unknown DataNodes */
    List<TDataNodeConfiguration> dataNodeConfigurations = new ArrayList<>();
    dataNodeConfigurations.add(
        new TDataNodeConfiguration().setLocation(new TDataNodeLocation().setDataNodeId(2)));

    /* Test2: The number of leaders in DataNode-1 and DataNode-3 should be exactly 6 */
    lazyGreedyRouter.updateUnknownDataNodes(dataNodeConfigurations);
    leaderCounter.clear();
    routeMap = lazyGreedyRouter.genLatestRegionRouteMap(regionReplicaSetList);
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
    Assert.assertEquals(6, leaderCounter.get(1).get());
    Assert.assertEquals(6, leaderCounter.get(3).get());
  }
}
