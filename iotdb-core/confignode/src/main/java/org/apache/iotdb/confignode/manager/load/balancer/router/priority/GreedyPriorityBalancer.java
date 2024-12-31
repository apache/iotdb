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
import java.util.TreeMap;

/** The GreedyPriorityBalancer always pick the Replica with the lowest loadScore */
public class GreedyPriorityBalancer implements IPriorityBalancer {

  public GreedyPriorityBalancer() {
    // Empty constructor
  }

  @Override
  public Map<TConsensusGroupId, TRegionReplicaSet> generateOptimalRoutePriority(
      List<TRegionReplicaSet> replicaSets, Map<TConsensusGroupId, Integer> regionLeaderMap) {

    Map<TConsensusGroupId, TRegionReplicaSet> regionPriorityMap = new TreeMap<>();

    replicaSets.forEach(
        replicaSet -> {
          regionPriorityMap.put(replicaSet.getRegionId(), replicaSet);
        });

    return regionPriorityMap;
  }
}
