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
import org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant;
import org.apache.iotdb.confignode.manager.ConfigManager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The ExternalLoadBalancer is responsible for assigning parallel extraction tasks from an external
 * source to available DataNodes in the cluster.
 */
public class ExternalLoadBalancer {
  private final BalanceStrategy strategy;

  public ExternalLoadBalancer(final String balanceStrategy) {
    switch (balanceStrategy) {
      case PipeExtractorConstant.EXTERNAL_EXTRACTOR_BALANCE_PROPORTION_STRATEGY:
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

  public interface BalanceStrategy {
    Map<Integer, Integer> balance(
        final int parallelCount,
        final PipeStaticMeta pipeStaticMeta,
        final ConfigManager configManager);
  }

  public static class ProportionalBalanceStrategy implements BalanceStrategy {
    @Override
    public Map<Integer, Integer> balance(
        final int parallelCount,
        final PipeStaticMeta pipeStaticMeta,
        final ConfigManager configManager) {
      final Map<TConsensusGroupId, Integer> regionLeaderMap =
          configManager.getLoadManager().getRegionLeaderMap();
      final Map<Integer, Integer> parallelAssignment = new HashMap<>();

      // Check if the external extractor is single instance per node
      if (pipeStaticMeta
          .getExtractorParameters()
          .getBooleanOrDefault(
              Arrays.asList(
                  PipeExtractorConstant.EXTERNAL_EXTRACTOR_SINGLE_INSTANCE_PER_NODE_KEY,
                  PipeExtractorConstant.EXTERNAL_SOURCE_SINGLE_INSTANCE_PER_NODE_KEY),
              PipeExtractorConstant.EXTERNAL_EXTRACTOR_SINGLE_INSTANCE_PER_NODE_DEFAULT_VALUE)) {
        final List<Integer> runningDataNodes =
            configManager.getLoadManager().filterDataNodeThroughStatus(NodeStatus.Running).stream()
                .sorted()
                .collect(Collectors.toList());
        if (runningDataNodes.isEmpty()) {
          throw new RuntimeException("No available datanode to assign tasks");
        }
        final int numNodes = runningDataNodes.size();
        for (int i = 1; i <= Math.min(numNodes, parallelCount); i++) {
          final int datanodeId = runningDataNodes.get(i - 1);
          parallelAssignment.put(-i, datanodeId);
        }
        return parallelAssignment;
      }

      // Count how many DataRegions each DataNode leads
      final Map<Integer, Integer> leaderRegionId2DataRegionCountMap = new HashMap<>();
      regionLeaderMap.entrySet().stream()
          .filter(e -> e.getKey().getType() == TConsensusGroupType.DataRegion && e.getValue() != -1)
          .forEach(
              e -> {
                final int leaderId = e.getValue();
                leaderRegionId2DataRegionCountMap.put(
                    leaderId, leaderRegionId2DataRegionCountMap.getOrDefault(leaderId, 0) + 1);
              });

      // distribute evenly if no dataRegion exists
      if (leaderRegionId2DataRegionCountMap.isEmpty()) {
        List<Integer> runningDataNodes =
            configManager.getLoadManager().filterDataNodeThroughStatus(NodeStatus.Running).stream()
                .sorted()
                .collect(Collectors.toList());
        if (runningDataNodes.isEmpty()) {
          throw new RuntimeException("No available datanode to assign tasks");
        }
        final int numNodes = runningDataNodes.size();
        for (int i = 1; i <= parallelCount; i++) {
          final int nodeIndex = (i - 1) % numNodes;
          final int datanodeId = runningDataNodes.get(nodeIndex);
          parallelAssignment.put(-i, datanodeId);
        }
        return parallelAssignment;
      }

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

      final List<Integer> stableLeaders = new ArrayList<>(leaderRegionId2AssignedCountMap.keySet());
      Collections.sort(stableLeaders);
      int taskIndex = 1;
      for (final Integer leader : stableLeaders) {
        final int count = leaderRegionId2AssignedCountMap.get(leader);
        for (int i = 0; i < count; i++) {
          parallelAssignment.put(-taskIndex, leader);
          taskIndex++;
        }
      }
      return parallelAssignment;
    }
  }
}
