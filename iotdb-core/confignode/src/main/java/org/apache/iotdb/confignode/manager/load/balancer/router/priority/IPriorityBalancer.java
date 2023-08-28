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
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;

import java.util.List;
import java.util.Map;

public interface IPriorityBalancer {

  String LEADER_POLICY = "LEADER";
  String GREEDY_POLICY = "GREEDY";

  /**
   * Generate an optimal route priority.
   *
   * @param replicaSets All RegionGroups
   * @param regionLeaderMap The current leader of each RegionGroup
   * @param dataNodeLoadScoreMap The current load score of each DataNode
   * @return Map<TConsensusGroupId, TRegionReplicaSet>, The optimal route priority for each
   *     RegionGroup. The replica with higher sorting result have higher priority.
   */
  Map<TConsensusGroupId, TRegionReplicaSet> generateOptimalRoutePriority(
      List<TRegionReplicaSet> replicaSets,
      Map<TConsensusGroupId, Integer> regionLeaderMap,
      Map<Integer, Long> dataNodeLoadScoreMap);
}
