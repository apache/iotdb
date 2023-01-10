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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** The LeaderPriorityBalancer always pick the leader Replica */
public class LeaderPriorityBalancer extends GreedyPriorityBalancer implements IPriorityBalancer {

  public LeaderPriorityBalancer() {
    // Empty constructor
  }

  @Override
  public Map<TConsensusGroupId, TRegionReplicaSet> generateOptimalRoutePriority(
      List<TRegionReplicaSet> replicaSets,
      Map<TConsensusGroupId, Integer> regionLeaderMap,
      Map<Integer, Long> dataNodeLoadScoreMap) {

    Map<TConsensusGroupId, TRegionReplicaSet> regionPriorityMap = new ConcurrentHashMap<>();

    replicaSets.forEach(
        replicaSet -> {
          /* 1. Sort replicaSet by loadScore */
          TRegionReplicaSet sortedReplicaSet =
              sortReplicasByLoadScore(replicaSet, dataNodeLoadScoreMap);

          /* 2. Pick leader if leader exists and available */
          int leaderId = regionLeaderMap.getOrDefault(replicaSet.getRegionId(), -1);
          if (leaderId != -1
              && dataNodeLoadScoreMap.getOrDefault(leaderId, Long.MAX_VALUE) < Long.MAX_VALUE) {
            for (int i = 0; i < sortedReplicaSet.getDataNodeLocationsSize(); i++) {
              if (sortedReplicaSet.getDataNodeLocations().get(i).getDataNodeId() == leaderId) {
                Collections.swap(sortedReplicaSet.getDataNodeLocations(), 0, i);
                break;
              }
            }
          }

          regionPriorityMap.put(sortedReplicaSet.getRegionId(), sortedReplicaSet);
        });

    return regionPriorityMap;
  }
}
