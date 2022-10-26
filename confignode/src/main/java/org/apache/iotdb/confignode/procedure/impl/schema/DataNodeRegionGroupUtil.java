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

package org.apache.iotdb.confignode.procedure.impl.schema;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class DataNodeRegionGroupUtil {

  private DataNodeRegionGroupUtil() {}

  /**
   * Try to get and execute request on consensus group leader as possible. If fail to get leader,
   * select some other replica for execution.
   */
  static Map<TDataNodeLocation, List<TConsensusGroupId>> getLeaderDataNodeRegionGroupMap(
      Map<TConsensusGroupId, Integer> leaderMap,
      Map<TConsensusGroupId, TRegionReplicaSet> regionReplicaSetMap) {
    Map<TDataNodeLocation, List<TConsensusGroupId>> dataNodeConsensusGroupIdMap = new HashMap<>();
    regionReplicaSetMap.forEach(
        (consensusGroupId, regionReplicaSet) -> {
          Integer leaderId = leaderMap.get(consensusGroupId);
          TDataNodeLocation leaderDataNodeLocation = null;
          if (leaderId == null || leaderId == -1) {
            leaderDataNodeLocation = regionReplicaSet.getDataNodeLocations().get(0);
          } else {
            for (TDataNodeLocation dataNodeLocation : regionReplicaSet.getDataNodeLocations()) {
              if (dataNodeLocation.getDataNodeId() == leaderId) {
                leaderDataNodeLocation = dataNodeLocation;
                break;
              }
            }
          }
          dataNodeConsensusGroupIdMap
              .computeIfAbsent(leaderDataNodeLocation, k -> new ArrayList<>())
              .add(regionReplicaSet.getRegionId());
        });
    return dataNodeConsensusGroupIdMap;
  }

  /**
   * Try to execute request on all replica of one consensus group. If some replica failed, execute
   * according request on some other replica and let consensus layer to sync it.
   */
  static Map<TDataNodeLocation, List<TConsensusGroupId>> getAllReplicaDataNodeRegionGroupMap(
      Map<TConsensusGroupId, TRegionReplicaSet> regionReplicaSetMap) {
    Map<TDataNodeLocation, List<TConsensusGroupId>> dataNodeConsensusGroupIdMap = new HashMap<>();
    regionReplicaSetMap.forEach(
        (consensusGroupId, regionReplicaSet) -> {
          for (TDataNodeLocation dataNodeLocation : regionReplicaSet.getDataNodeLocations()) {
            dataNodeConsensusGroupIdMap
                .computeIfAbsent(dataNodeLocation, k -> new ArrayList<>())
                .add(regionReplicaSet.getRegionId());
          }
        });
    return dataNodeConsensusGroupIdMap;
  }
}
