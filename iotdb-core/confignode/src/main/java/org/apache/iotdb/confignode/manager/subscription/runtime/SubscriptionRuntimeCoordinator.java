package org.apache.iotdb.confignode.manager.subscription.runtime;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.subscription.meta.topic.TopicMeta;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeStatistics;
import org.apache.iotdb.confignode.manager.load.subscriber.ConsensusGroupStatisticsChangeEvent;
import org.apache.iotdb.confignode.manager.load.subscriber.NodeStatisticsChangeEvent;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;

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
      final String topicMode =
          topicMeta
              .getConfig()
              .getStringOrDefault(TopicConstant.MODE_KEY, TopicConstant.MODE_DEFAULT_VALUE);
      final String topicFormat =
          topicMeta
              .getConfig()
              .getStringOrDefault(TopicConstant.FORMAT_KEY, TopicConstant.FORMAT_DEFAULT_VALUE);
      if (TopicConstant.MODE_LIVE_VALUE.equalsIgnoreCase(topicMode)
          && !TopicConstant.FORMAT_TS_FILE_HANDLER_VALUE.equalsIgnoreCase(topicFormat)) {
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
