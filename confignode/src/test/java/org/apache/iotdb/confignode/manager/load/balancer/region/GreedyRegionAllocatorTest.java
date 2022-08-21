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
package org.apache.iotdb.confignode.manager.load.balancer.region;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TNodeResource;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GreedyRegionAllocatorTest {

  @Test
  public void testAllocateRegion() {
    GreedyRegionAllocator greedyRegionAllocator = new GreedyRegionAllocator();
    List<TDataNodeConfiguration> registeredDataNodes =
        Lists.newArrayList(
            new TDataNodeConfiguration(
                new TDataNodeLocation(1, null, null, null, null, null), new TNodeResource()),
            new TDataNodeConfiguration(
                new TDataNodeLocation(2, null, null, null, null, null), new TNodeResource()),
            new TDataNodeConfiguration(
                new TDataNodeLocation(3, null, null, null, null, null), new TNodeResource()));
    List<TRegionReplicaSet> allocatedRegions = new ArrayList<>();
    List<TConsensusGroupId> tConsensusGroupIds =
        Lists.newArrayList(
            new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 0),
            new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 1),
            new TConsensusGroupId(TConsensusGroupType.DataRegion, 2),
            new TConsensusGroupId(TConsensusGroupType.DataRegion, 3),
            new TConsensusGroupId(TConsensusGroupType.DataRegion, 4),
            new TConsensusGroupId(TConsensusGroupType.DataRegion, 5));
    for (TConsensusGroupId tConsensusGroupId : tConsensusGroupIds) {
      TRegionReplicaSet newRegion =
          greedyRegionAllocator.allocateRegion(
              registeredDataNodes, allocatedRegions, 1, tConsensusGroupId);
      allocatedRegions.add(newRegion);
    }

    Map<TDataNodeLocation, Integer> countMap = new HashMap<>();
    for (TDataNodeConfiguration dataNodeInfo : registeredDataNodes) {
      countMap.put(dataNodeInfo.getLocation(), 0);
    }

    for (TRegionReplicaSet regionReplicaSet : allocatedRegions) {
      for (TDataNodeLocation dataNodeLocation : regionReplicaSet.getDataNodeLocations()) {
        countMap.computeIfPresent(dataNodeLocation, (dataNode, count) -> (count + 1));
      }
    }

    Assert.assertTrue(countMap.values().stream().mapToInt(e -> e).max().getAsInt() <= 2);
    Assert.assertTrue(
        Collections.disjoint(
            allocatedRegions.get(0).getDataNodeLocations(),
            allocatedRegions.get(1).getDataNodeLocations()));
    Assert.assertTrue(
        Collections.disjoint(
            allocatedRegions.get(2).getDataNodeLocations(),
            allocatedRegions.get(3).getDataNodeLocations()));
    Assert.assertTrue(
        Collections.disjoint(
            allocatedRegions.get(4).getDataNodeLocations(),
            allocatedRegions.get(5).getDataNodeLocations()));
  }
}
