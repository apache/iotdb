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
import java.util.List;
import java.util.Map;

public class LazyRandomRouterTest {

  private final LazyRandomRouter lazyRandomRouter = new LazyRandomRouter();

  @Test
  public void genLatestRegionRouteMap() {
    /* Prepare TRegionReplicaSets */
    TRegionReplicaSet regionReplicaSet0 = new TRegionReplicaSet();
    regionReplicaSet0.setRegionId(new TConsensusGroupId(TConsensusGroupType.DataRegion, 0));
    regionReplicaSet0.addToDataNodeLocations(new TDataNodeLocation().setDataNodeId(1));
    regionReplicaSet0.addToDataNodeLocations(new TDataNodeLocation().setDataNodeId(2));
    regionReplicaSet0.addToDataNodeLocations(new TDataNodeLocation().setDataNodeId(3));

    TRegionReplicaSet regionReplicaSet1 = new TRegionReplicaSet();
    regionReplicaSet1.setRegionId(new TConsensusGroupId(TConsensusGroupType.DataRegion, 1));
    regionReplicaSet1.addToDataNodeLocations(new TDataNodeLocation().setDataNodeId(4));
    regionReplicaSet1.addToDataNodeLocations(new TDataNodeLocation().setDataNodeId(5));
    regionReplicaSet1.addToDataNodeLocations(new TDataNodeLocation().setDataNodeId(6));

    TRegionReplicaSet regionReplicaSet2 = new TRegionReplicaSet();
    regionReplicaSet2.setRegionId(new TConsensusGroupId(TConsensusGroupType.DataRegion, 2));
    regionReplicaSet2.addToDataNodeLocations(new TDataNodeLocation().setDataNodeId(1));
    regionReplicaSet2.addToDataNodeLocations(new TDataNodeLocation().setDataNodeId(3));
    regionReplicaSet2.addToDataNodeLocations(new TDataNodeLocation().setDataNodeId(5));

    TRegionReplicaSet regionReplicaSet3 = new TRegionReplicaSet();
    regionReplicaSet3.setRegionId(new TConsensusGroupId(TConsensusGroupType.DataRegion, 3));
    regionReplicaSet3.addToDataNodeLocations(new TDataNodeLocation().setDataNodeId(2));
    regionReplicaSet3.addToDataNodeLocations(new TDataNodeLocation().setDataNodeId(4));
    regionReplicaSet3.addToDataNodeLocations(new TDataNodeLocation().setDataNodeId(6));

    List<TRegionReplicaSet> regionReplicaSetList = new ArrayList<>();
    regionReplicaSetList.add(regionReplicaSet0);
    regionReplicaSetList.add(regionReplicaSet1);
    regionReplicaSetList.add(regionReplicaSet2);
    regionReplicaSetList.add(regionReplicaSet3);

    /* Unknown DataNodes */
    List<TDataNodeConfiguration> dataNodeConfigurations = new ArrayList<>();
    dataNodeConfigurations.add(
        new TDataNodeConfiguration().setLocation(new TDataNodeLocation().setDataNodeId(2)));
    dataNodeConfigurations.add(
        new TDataNodeConfiguration().setLocation(new TDataNodeLocation().setDataNodeId(5)));

    lazyRandomRouter.updateUnknownDataNodes(dataNodeConfigurations);
    Map<TConsensusGroupId, TRegionReplicaSet> routeMap =
        lazyRandomRouter.genLatestRegionRouteMap(regionReplicaSetList);
    routeMap
        .values()
        .forEach(
            regionReplicaSet -> {
              /* Filter unknown */
              Assert.assertNotEquals(
                  2, regionReplicaSet.getDataNodeLocations().get(0).getDataNodeId());
              Assert.assertNotEquals(
                  5, regionReplicaSet.getDataNodeLocations().get(0).getDataNodeId());
            });

    /* Test again */
    dataNodeConfigurations.clear();
    dataNodeConfigurations.add(
        new TDataNodeConfiguration().setLocation(new TDataNodeLocation().setDataNodeId(1)));
    dataNodeConfigurations.add(
        new TDataNodeConfiguration().setLocation(new TDataNodeLocation().setDataNodeId(6)));

    lazyRandomRouter.updateUnknownDataNodes(dataNodeConfigurations);
    routeMap = lazyRandomRouter.genLatestRegionRouteMap(regionReplicaSetList);
    routeMap
        .values()
        .forEach(
            regionReplicaSet -> {
              Assert.assertNotEquals(
                  1, regionReplicaSet.getDataNodeLocations().get(0).getDataNodeId());
              Assert.assertNotEquals(
                  6, regionReplicaSet.getDataNodeLocations().get(0).getDataNodeId());
            });
  }
}
