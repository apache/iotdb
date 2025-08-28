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
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.pipe.api.exception.PipeException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The PipeExternalSourceLoadBalancer is responsible for assigning parallel extraction tasks from an
 * external source to available DataNodes in the cluster.
 */
public class PipeExternalSourceLoadBalancer {

  /**
   * The BalanceStrategy interface defines the contract for different load balancing strategies.
   * Implementations of this interface should provide a way to balance tasks across DataNodes.
   */
  private interface BalanceStrategy {
    Map<Integer, Integer> balance(
        final int parallelCount,
        final PipeStaticMeta pipeStaticMeta,
        final ConfigManager configManager);
  }

  private final BalanceStrategy strategy;

  public PipeExternalSourceLoadBalancer(final String balanceStrategy) {
    switch (balanceStrategy) {
      case PipeSourceConstant.EXTERNAL_EXTRACTOR_BALANCE_PROPORTION_STRATEGY:
        this.strategy = new ProportionalBalanceStrategy();
        break;
      default:
        throw new IllegalArgumentException("Unknown load balance strategy: " + balanceStrategy);
    }
  }

  /**
   * Balances the given number of parallel tasks across available nodes.
   *
   * @param parallelCount number of external source tasks to distribute
   * @param pipeStaticMeta metadata about the pipe extractor
   * @param configManager reference to ConfigManager for cluster information
   * @return a mapping from task index to leader node id
   */
  public Map<Integer, Integer> balance(
      final int parallelCount,
      final PipeStaticMeta pipeStaticMeta,
      final ConfigManager configManager) {
    return strategy.balance(parallelCount, pipeStaticMeta, configManager);
  }

  public static class ProportionalBalanceStrategy implements BalanceStrategy {

    @Override
    public Map<Integer, Integer> balance(
        final int parallelCount,
        final PipeStaticMeta pipeStaticMeta,
        final ConfigManager configManager) {
      final Map<TConsensusGroupId, Integer> regionLeaderMap =
          configManager.getLoadManager().getRegionLeaderMap();
      final Map<Integer, Integer> taskId2DataNodeId = new HashMap<>();

      // Check for Single Instance Mode:
      //
      // 1. If the pipeStaticMeta indicates that only one instance per node is allowed, tasks are
      // evenly distributed across running DataNodes.
      // 2. If no DataNodes are available, a PipeException is thrown.
      if (pipeStaticMeta
          .getSourceParameters()
          .getBooleanOrDefault(
              Arrays.asList(
                  PipeSourceConstant.EXTERNAL_EXTRACTOR_SINGLE_INSTANCE_PER_NODE_KEY,
                  PipeSourceConstant.EXTERNAL_SOURCE_SINGLE_INSTANCE_PER_NODE_KEY),
              PipeSourceConstant.EXTERNAL_EXTRACTOR_SINGLE_INSTANCE_PER_NODE_DEFAULT_VALUE)) {
        final List<Integer> runningDataNodes =
            configManager.getLoadManager().filterDataNodeThroughStatus(NodeStatus.Running).stream()
                .sorted()
                .collect(Collectors.toList());
        if (runningDataNodes.isEmpty()) {
          throw new PipeException("No available datanode to assign tasks");
        }
        final int numNodes = runningDataNodes.size();
        for (int i = 1; i <= Math.min(numNodes, parallelCount); i++) {
          final int datanodeId = runningDataNodes.get(i - 1);
          taskId2DataNodeId.put(-i, datanodeId);
        }
        return taskId2DataNodeId;
      }

      // Count DataRegions Led by Each DataNode:
      //
      // The method iterates through the regionLeaderMap to count the number of DataRegions led by
      // each DataNode.
      final Map<Integer, Integer> leaderRegionId2DataRegionCountMap = new HashMap<>();
      regionLeaderMap.entrySet().stream()
          .filter(e -> e.getKey().getType() == TConsensusGroupType.DataRegion && e.getValue() != -1)
          .forEach(
              e -> {
                final int leaderRegionDataNodeId = e.getValue();
                leaderRegionId2DataRegionCountMap.put(
                    leaderRegionDataNodeId,
                    leaderRegionId2DataRegionCountMap.getOrDefault(leaderRegionDataNodeId, 0) + 1);
              });

      // Handle No DataRegions:
      //
      // If no DataRegions exist, tasks are evenly distributed across running DataNodes.
      if (leaderRegionId2DataRegionCountMap.isEmpty()) {
        List<Integer> runningDataNodes =
            configManager.getLoadManager().filterDataNodeThroughStatus(NodeStatus.Running).stream()
                .sorted()
                .collect(Collectors.toList());
        if (runningDataNodes.isEmpty()) {
          throw new PipeException("No available datanode to assign tasks");
        }
        final int numNodes = runningDataNodes.size();
        final int quotient = parallelCount / numNodes;
        final int remainder = parallelCount % numNodes;
        int taskIndex = 1;
        for (int i = 0; i < numNodes; i++) {
          int tasksForNode = quotient + (i < remainder ? 1 : 0);
          int datanodeId = runningDataNodes.get(i);
          for (int j = 0; j < tasksForNode; j++) {
            taskId2DataNodeId.put(-taskIndex, datanodeId);
            taskIndex++;
          }
        }
        return taskId2DataNodeId;
      }

      // Proportional Task Distribution:
      //
      // Based on the number of DataRegions led by each DataNode, the method calculates the
      // proportion of tasks each node should handle.
      // Integer parts of the task count are assigned first, and remaining tasks are distributed
      // based on the largest fractional parts.
      final int totalRegions =
          leaderRegionId2DataRegionCountMap.values().stream().mapToInt(Integer::intValue).sum();

      // Calculate exact and floor share of each leader
      final Map<Integer, Double> leaderRegionId2ExactShareMap = new HashMap<>();
      final Map<Integer, Integer> leaderRegionId2AssignedCountMap = new HashMap<>();
      for (Map.Entry<Integer, Integer> entry : leaderRegionId2DataRegionCountMap.entrySet()) {
        final double share = (parallelCount * entry.getValue()) / (double) totalRegions;
        leaderRegionId2ExactShareMap.put(entry.getKey(), share);
        leaderRegionId2AssignedCountMap.put(entry.getKey(), (int) Math.floor(share));
      }

      // Distribute remainder tasks based on largest fractional parts
      final int remainder =
          parallelCount
              - leaderRegionId2AssignedCountMap.values().stream().mapToInt(Integer::intValue).sum();

      final List<Integer> sortedLeaders =
          leaderRegionId2ExactShareMap.keySet().stream()
              .sorted(
                  (l1, l2) -> {
                    final double diff =
                        (leaderRegionId2ExactShareMap.get(l2)
                                - Math.floor(leaderRegionId2ExactShareMap.get(l2)))
                            - (leaderRegionId2ExactShareMap.get(l1)
                                - Math.floor(leaderRegionId2ExactShareMap.get(l1)));
                    return diff > 0 ? 1 : (diff < 0 ? -1 : Integer.compare(l1, l2));
                  })
              .collect(Collectors.toList());
      for (int i = 0; i < remainder; i++) {
        final int leaderId = sortedLeaders.get(i % sortedLeaders.size());
        leaderRegionId2AssignedCountMap.put(
            leaderId, leaderRegionId2AssignedCountMap.get(leaderId) + 1);
      }

      // Generate Task Assignment Result:
      //
      // Finally, the method returns a mapping of task indices to DataNode IDs.
      final List<Integer> stableLeaders = new ArrayList<>(leaderRegionId2AssignedCountMap.keySet());
      Collections.sort(stableLeaders);
      int taskIndex = 1;
      for (final Integer leader : stableLeaders) {
        final int count = leaderRegionId2AssignedCountMap.get(leader);
        for (int i = 0; i < count; i++) {
          taskId2DataNodeId.put(-taskIndex, leader);
          taskIndex++;
        }
      }
      return taskId2DataNodeId;
    }
  }
}
