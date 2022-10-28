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
package org.apache.iotdb.confignode.manager.load.balancer.router.mcf;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

public class MCFLeaderBalancerTest {

  private static final List<TConsensusGroupId> regionGroupIds = new ArrayList<>();
  private static final List<TDataNodeLocation> dataNodeLocations = new ArrayList<>();
  private static final List<TRegionReplicaSet> regionReplicaSets = new ArrayList<>();

  @BeforeClass
  public static void prepareData() {
    for (int i = 0; i < 3; i++) {
      regionGroupIds.add(new TConsensusGroupId(TConsensusGroupType.DataRegion, i));
    }

    for (int i = 0; i < 4; i++) {
      dataNodeLocations.add(new TDataNodeLocation().setDataNodeId(i));
    }

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
  }

  @Test
  public void optimalLeaderDistributionTest() {
    Map<TConsensusGroupId, TRegionReplicaSet> regionReplicaSetMap = new HashMap<>();
    regionReplicaSets.forEach(
        regionReplicaSet ->
            regionReplicaSetMap.put(regionReplicaSet.getRegionId(), regionReplicaSet));
    Map<TConsensusGroupId, Integer> regionLeaderMap = new HashMap<>();
    regionReplicaSets.forEach(
        regionReplicaSet -> regionLeaderMap.put(regionReplicaSet.getRegionId(), 0));
    Set<Integer> disabledDataNodeSet = new HashSet<>();
    disabledDataNodeSet.add(0);

    MCFLeaderBalancer mcfLeaderBalancer =
        new MCFLeaderBalancer(regionReplicaSetMap, regionLeaderMap, disabledDataNodeSet);
    Map<TConsensusGroupId, Integer> leaderDistribution =
        mcfLeaderBalancer.generateOptimalLeaderDistribution();
    Assert.assertEquals(3, leaderDistribution.size());
    Assert.assertEquals(3, new HashSet<>(leaderDistribution.values()).size());
    Assert.assertEquals(3, mcfLeaderBalancer.getMaximumFlow());
    Assert.assertEquals(3 + 3, mcfLeaderBalancer.getMinimumCost());
  }
}
