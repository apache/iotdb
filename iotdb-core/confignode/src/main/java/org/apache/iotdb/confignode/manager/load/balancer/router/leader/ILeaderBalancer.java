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
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;

import java.util.Map;
import java.util.Set;

public interface ILeaderBalancer {

  String GREEDY_POLICY = "GREEDY";
  String MIN_COST_FLOW_POLICY = "MIN_COST_FLOW";

  /**
   * Generate an optimal leader distribution.
   *
   * @param regionReplicaSetMap All RegionGroups the cluster currently have
   * @param regionLeaderMap The current leader of each RegionGroup
   * @param disabledDataNodeSet The DataNodes that currently unable to work(can't place
   *     RegionGroup-leader)
   * @return Map<TConsensusGroupId, Integer>, The optimal leader distribution
   */
  Map<TConsensusGroupId, Integer> generateOptimalLeaderDistribution(
      Map<TConsensusGroupId, TRegionReplicaSet> regionReplicaSetMap,
      Map<TConsensusGroupId, Integer> regionLeaderMap,
      Set<Integer> disabledDataNodeSet);
}
