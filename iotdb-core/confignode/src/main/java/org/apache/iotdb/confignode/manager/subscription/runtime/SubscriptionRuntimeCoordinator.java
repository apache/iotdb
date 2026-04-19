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

package org.apache.iotdb.confignode.manager.subscription.runtime;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.subscription.meta.topic.TopicMeta;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeStatistics;
import org.apache.iotdb.confignode.manager.load.subscriber.ConsensusGroupStatisticsChangeEvent;
import org.apache.iotdb.confignode.manager.load.subscriber.NodeStatisticsChangeEvent;

import org.apache.tsfile.utils.Pair;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class SubscriptionRuntimeCoordinator {

  private final ConfigManager configManager;
  private final Map<TConsensusGroupId, Pair<Integer, Integer>> regionGroupToRuntimeLeaderPairMap =
      new HashMap<>();
  private final AtomicLong runtimeVersionGenerator = new AtomicLong(System.currentTimeMillis());

  public SubscriptionRuntimeCoordinator(final ConfigManager configManager) {
    this.configManager = configManager;
  }

  public synchronized void handleLeaderChangeEvent(
      final ConsensusGroupStatisticsChangeEvent event) {
    if (!hasAnyConsensusBasedTopic()) {
      return;
    }

    final Map<TConsensusGroupId, Pair<Integer, Integer>> refreshMap = new HashMap<>();
    event
        .getDifferentConsensusGroupStatisticsMap()
        .forEach(
            (regionGroupId, pair) -> {
              if (regionGroupId.getType() != TConsensusGroupType.DataRegion) {
                return;
              }
              final int oldLeaderNodeId = pair.left == null ? -1 : pair.left.getLeaderId();
              final int newLeaderNodeId = pair.right == null ? -1 : pair.right.getLeaderId();
              if (oldLeaderNodeId == newLeaderNodeId) {
                return;
              }
              updateRuntimeLeaderPair(regionGroupId, oldLeaderNodeId, newLeaderNodeId, refreshMap);
            });

    submitRuntimeRefresh(refreshMap);
  }

  public synchronized void handleNodeStatisticsChange(final NodeStatisticsChangeEvent event) {
    if (!hasAnyConsensusBasedTopic()) {
      return;
    }

    final boolean shouldRefreshRuntime =
        event.getDifferentNodeStatisticsMap().values().stream()
            .anyMatch(
                pair -> {
                  final NodeStatus oldStatus = getNodeStatus(pair.getLeft());
                  final NodeStatus newStatus = getNodeStatus(pair.getRight());
                  return oldStatus != newStatus
                      && (isRuntimeSensitiveStatus(oldStatus)
                          || isRuntimeSensitiveStatus(newStatus));
                });
    if (!shouldRefreshRuntime) {
      return;
    }

    seedRuntimeLeaderPairsFromCurrentLeaders();
    submitRuntimeRefresh(new HashMap<>(regionGroupToRuntimeLeaderPairMap));
  }

  public boolean hasAnyConsensusBasedTopic() {
    for (final TopicMeta topicMeta :
        configManager
            .getSubscriptionManager()
            .getSubscriptionCoordinator()
            .getSubscriptionInfo()
            .getAllTopicMeta()) {
      if (topicMeta.getConfig().isConsensusMode()) {
        return true;
      }
    }
    return false;
  }

  private void updateRuntimeLeaderPair(
      final TConsensusGroupId regionGroupId,
      final int oldLeaderNodeId,
      final int newLeaderNodeId,
      final Map<TConsensusGroupId, Pair<Integer, Integer>> refreshMap) {
    if (newLeaderNodeId < 0) {
      regionGroupToRuntimeLeaderPairMap.remove(regionGroupId);
      return;
    }
    final Pair<Integer, Integer> runtimeLeaderPair = new Pair<>(oldLeaderNodeId, newLeaderNodeId);
    regionGroupToRuntimeLeaderPairMap.put(regionGroupId, runtimeLeaderPair);
    refreshMap.put(regionGroupId, runtimeLeaderPair);
  }

  private void seedRuntimeLeaderPairsFromCurrentLeaders() {
    configManager
        .getLoadManager()
        .getRegionLeaderMap()
        .forEach(
            (regionGroupId, leaderId) -> {
              if (regionGroupId.getType() == TConsensusGroupType.DataRegion && leaderId >= 0) {
                regionGroupToRuntimeLeaderPairMap.putIfAbsent(
                    regionGroupId, new Pair<>(-1, leaderId));
              }
            });
  }

  private void submitRuntimeRefresh(
      final Map<TConsensusGroupId, Pair<Integer, Integer>> regionGroupToOldAndNewLeaderPairMap) {
    if (regionGroupToOldAndNewLeaderPairMap.isEmpty()) {
      return;
    }
    configManager
        .getProcedureManager()
        .subscriptionHandleLeaderChange(
            regionGroupToOldAndNewLeaderPairMap,
            runtimeVersionGenerator.updateAndGet(
                currentRuntimeVersion ->
                    Math.max(currentRuntimeVersion + 1, System.currentTimeMillis())));
  }

  private static NodeStatus getNodeStatus(final NodeStatistics statistics) {
    return statistics == null ? NodeStatus.Unknown : statistics.getStatus();
  }

  private static boolean isRuntimeSensitiveStatus(final NodeStatus status) {
    return status == NodeStatus.Unknown || status == NodeStatus.Removing;
  }
}
