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
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeStatistics;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class HashLeaderBalancerTest {

  private static final HashLeaderBalancer BALANCER = new HashLeaderBalancer();
  private static final int MIN_VALUE_HASH_REGION_ID = 268255235;

  @Test
  public void minValueHashCodeTest() {
    TConsensusGroupId regionGroupId =
        new TConsensusGroupId(TConsensusGroupType.DataRegion, MIN_VALUE_HASH_REGION_ID);
    Assert.assertEquals(Integer.MIN_VALUE, regionGroupId.hashCode());

    Map<TConsensusGroupId, Set<Integer>> regionReplicaSetMap = new TreeMap<>();
    regionReplicaSetMap.put(regionGroupId, new HashSet<>(Arrays.asList(1, 2, 3)));

    Map<Integer, NodeStatistics> dataNodeStatisticsMap = new TreeMap<>();
    dataNodeStatisticsMap.put(1, new NodeStatistics(NodeStatus.Running));
    dataNodeStatisticsMap.put(2, new NodeStatistics(NodeStatus.Running));
    dataNodeStatisticsMap.put(3, new NodeStatistics(NodeStatus.Running));

    Map<TConsensusGroupId, Integer> leaderDistribution =
        BALANCER.generateOptimalLeaderDistribution(
            new TreeMap<>(),
            regionReplicaSetMap,
            new TreeMap<>(),
            dataNodeStatisticsMap,
            new TreeMap<>());

    Assert.assertEquals(Integer.valueOf(3), leaderDistribution.get(regionGroupId));
  }
}
