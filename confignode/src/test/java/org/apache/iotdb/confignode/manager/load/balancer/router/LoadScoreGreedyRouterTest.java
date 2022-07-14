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

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LoadScoreGreedyRouterTest {

  private static final TDataNodeLocation dataNodeLocation1 =
      new TDataNodeLocation(
          1,
          new TEndPoint("0.0.0.0", 6667),
          new TEndPoint("0.0.0.0", 9003),
          new TEndPoint("0.0.0.0", 8777),
          new TEndPoint("0.0.0.0", 40010),
          new TEndPoint("0.0.0.0", 50010));

  private static final TDataNodeLocation dataNodeLocation2 =
      new TDataNodeLocation(
          2,
          new TEndPoint("0.0.0.0", 6668),
          new TEndPoint("0.0.0.0", 9004),
          new TEndPoint("0.0.0.0", 8778),
          new TEndPoint("0.0.0.0", 40011),
          new TEndPoint("0.0.0.0", 50011));

  private static final TDataNodeLocation dataNodeLocation3 =
      new TDataNodeLocation(
          3,
          new TEndPoint("0.0.0.0", 6669),
          new TEndPoint("0.0.0.0", 9005),
          new TEndPoint("0.0.0.0", 8779),
          new TEndPoint("0.0.0.0", 40012),
          new TEndPoint("0.0.0.0", 50012));

  @Test
  public void testGenRealTimeRoutingPolicy() {
    /* Build loadScoreMap */
    Map<Integer, Float> loadScoreMap = new HashMap<>();
    loadScoreMap.put(1, (float) -10.0);
    loadScoreMap.put(2, (float) -20.0);
    loadScoreMap.put(3, (float) -30.0);

    /* Build TRegionReplicaSet */
    TRegionReplicaSet regionReplicaSet = new TRegionReplicaSet();
    TConsensusGroupId groupId = new TConsensusGroupId(TConsensusGroupType.DataRegion, 0);
    List<TDataNodeLocation> dataNodeLocations = new ArrayList<>();
    dataNodeLocations.add(dataNodeLocation1);
    dataNodeLocations.add(dataNodeLocation2);
    dataNodeLocations.add(dataNodeLocation3);
    regionReplicaSet.setRegionId(groupId);
    regionReplicaSet.setDataNodeLocations(dataNodeLocations);

    TRegionReplicaSet result =
        new LoadScoreGreedyRouter(loadScoreMap)
            .genRealTimeRoutingPolicy(Collections.singletonList(regionReplicaSet))
            .get(groupId);
    /* Sort the Replicas by their loadScore */
    Assert.assertEquals(dataNodeLocation3, result.getDataNodeLocations().get(0));
    Assert.assertEquals(dataNodeLocation2, result.getDataNodeLocations().get(1));
    Assert.assertEquals(dataNodeLocation1, result.getDataNodeLocations().get(2));
  }
}
