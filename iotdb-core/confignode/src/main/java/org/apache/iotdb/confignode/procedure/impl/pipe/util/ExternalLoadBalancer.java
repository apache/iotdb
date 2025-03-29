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

package org.apache.iotdb.confignode.procedure.impl.pipe.util;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.manager.ConfigManager;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ExternalLoadBalancer {
  private final ConfigManager configManager;

  public ExternalLoadBalancer(final ConfigManager configManager) {
    this.configManager = configManager;
  }

  /**
   * Distribute the number of source parallel tasks evenly over the sorted region group ids.
   *
   * @param parallelCount the number of parallel tasks from external source
   * @return a mapping from task index to leader node id
   */
  public Map<Integer, Integer> balance(
      int parallelCount, final Map<TConsensusGroupId, Integer> regionLeaderMap) {
    // Get sorted regionGroupIds
    Map<Integer, Integer> parallelAssignment = new HashMap<>();
    List<Integer> sortedRegionGroupIds =
        regionLeaderMap.entrySet().stream()
            .filter(
                t -> t.getKey().getType() == TConsensusGroupType.DataRegion && t.getValue() != -1)
            .map(t -> t.getKey().getId())
            .sorted()
            .collect(Collectors.toList());

    if (sortedRegionGroupIds.isEmpty()) {
      List<Integer> runningDataNodes =
          configManager.getLoadManager().filterDataNodeThroughStatus(NodeStatus.Running).stream()
              .sorted()
              .collect(Collectors.toList());
      if (runningDataNodes.isEmpty()) {
        throw new IllegalArgumentException("No available datanode to assign tasks");
      }
      int numNodes = runningDataNodes.size();
      for (int i = -1; i >= -parallelCount; i--) {
        int nodeIndex = (-i) % numNodes;
        int datanodeId = runningDataNodes.get(nodeIndex);
        parallelAssignment.put(i, datanodeId);
      }
    } else {
      int numGroups = sortedRegionGroupIds.size();
      for (int i = -1; i >= -parallelCount; i--) {
        int groupIndex = (-i) % numGroups;
        int regionGroupId = sortedRegionGroupIds.get(groupIndex);
        int leaderNodeId =
            regionLeaderMap.get(
                new TConsensusGroupId(TConsensusGroupType.DataRegion, regionGroupId));
        parallelAssignment.put(i, leaderNodeId);
      }
    }
    return parallelAssignment;
  }
}
