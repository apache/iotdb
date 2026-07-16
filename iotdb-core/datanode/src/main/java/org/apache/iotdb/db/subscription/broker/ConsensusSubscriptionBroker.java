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

package org.apache.iotdb.db.subscription.broker;

import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.subscription.config.SubscriptionConfig;
import org.apache.iotdb.consensus.iot.IoTConsensusServerImpl;
import org.apache.iotdb.consensus.iot.SubscriptionWalRetentionPolicy;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.subscription.broker.consensus.ConsensusLogToTabletConverter;
import org.apache.iotdb.db.subscription.broker.consensus.ConsensusPrefetchingQueue;
import org.apache.iotdb.db.subscription.broker.consensus.ConsensusRegionRuntimeState;
import org.apache.iotdb.db.subscription.broker.consensus.ConsensusSubscriptionCommitManager;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.rpc.subscription.payload.poll.RegionProgress;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.poll.TopicProgress;
import org.apache.iotdb.rpc.subscription.payload.request.SubscriptionSeekReq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Consensus-based subscription broker that reads data directly from IoTConsensus WAL. Each instance
 * manages consensus prefetching queues for a single consumer group.
 */
public class ConsensusSubscriptionBroker implements ISubscriptionBroker {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConsensusSubscriptionBroker.class);

  private final String brokerId; // consumer group id

  /** Maps topic name to a list of ConsensusPrefetchingQueues, one per data region. */
  private final Map<String, List<ConsensusPrefetchingQueue>> topicNameToConsensusPrefetchingQueues;

  /** Round-robin counter for fair polling among region queues already assigned to this consumer. */
  private final AtomicInteger pollRoundRobinIndex = new AtomicInteger(0);

  private final Map<String, ConcurrentHashMap<String, Long>> topicConsumerLastPollMs =
      new ConcurrentHashMap<>();

  private final Map<String, TopicOwnershipSnapshot> topicOwnershipSnapshots =
      new ConcurrentHashMap<>();

  private final Object queueLifecycleLock = new Object();

  public ConsensusSubscriptionBroker(final String brokerId) {
    this.brokerId = brokerId;
    this.topicNameToConsensusPrefetchingQueues = new ConcurrentHashMap<>();
  }

  @Override
  public boolean isEmpty() {
    return topicNameToConsensusPrefetchingQueues.isEmpty();
  }

  @Override
  public boolean hasQueue(final String topicName) {
    final List<ConsensusPrefetchingQueue> queues =
        topicNameToConsensusPrefetchingQueues.get(topicName);
    return Objects.nonNull(queues)
        && !queues.isEmpty()
        && queues.stream().anyMatch(q -> !q.isClosed());
  }

  @Override
  public boolean acceptsTopic(final String topicName) {
    return hasQueue(topicName);
  }

  //////////////////////////// poll ////////////////////////////

  @Override
  public List<SubscriptionEvent> poll(
      final String consumerId, final Set<String> topicNames, final long maxBytes) {
    return poll(consumerId, topicNames, maxBytes, Collections.emptyMap());
  }

  public List<SubscriptionEvent> poll(
      final String consumerId,
      final Set<String> topicNames,
      final long maxBytes,
      final Map<String, TopicProgress> progressByTopic) {
    LOGGER.debug(
        DataNodePipeMessages
            .PIPE_LOG_CONSENSUSSUBSCRIPTIONBROKER_POLL_CALLED_CONSUMERID_TOPICNAMES_5F1F5175,
        brokerId,
        consumerId,
        topicNames,
        topicNameToConsensusPrefetchingQueues.size(),
        maxBytes);

    final List<SubscriptionEvent> eventsToPoll = new ArrayList<>();
    final List<SubscriptionEvent> eventsToNack = new ArrayList<>();
    long totalSize = 0;

    for (final String topicName : topicNames) {
      final List<ConsensusPrefetchingQueue> queues =
          topicNameToConsensusPrefetchingQueues.get(topicName);
      if (Objects.isNull(queues) || queues.isEmpty()) {
        continue;
      }

      final TopicOwnershipSnapshot ownershipSnapshot =
          refreshAndGetTopicOwnership(topicName, queues, consumerId);
      final List<ConsensusPrefetchingQueue> assignedQueues =
          getAssignedQueues(queues, consumerId, ownershipSnapshot);
      if (assignedQueues.isEmpty()) {
        continue;
      }

      final List<ConsensusPrefetchingQueue> pollQueues =
          buildPollOrderForAssignedQueues(assignedQueues, topicName);
      final int eventsBeforeTopicPoll = eventsToPoll.size();

      for (final ConsensusPrefetchingQueue consensusQueue : pollQueues) {
        if (consensusQueue.isClosed()) {
          continue;
        }

        final String regionIdStr = consensusQueue.getConsensusGroupId().toString();
        final TopicProgress topicProgress = progressByTopic.get(topicName);
        final RegionProgress regionProgress =
            Objects.nonNull(topicProgress)
                ? topicProgress.getRegionProgress().get(regionIdStr)
                : null;

        final SubscriptionEvent event = consensusQueue.poll(consumerId, regionProgress);
        if (Objects.isNull(event)) {
          continue;
        }

        final long currentSize;
        try {
          currentSize = event.getCurrentResponseSize();
        } catch (final IOException e) {
          eventsToNack.add(event);
          continue;
        }

        eventsToPoll.add(event);
        totalSize += currentSize;

        if (totalSize >= maxBytes) {
          break;
        }
      }
      if (totalSize >= maxBytes) {
        break;
      }
    }

    // Nack any events that had errors
    if (!eventsToNack.isEmpty()) {
      commit(
          consumerId,
          eventsToNack.stream()
              .map(SubscriptionEvent::getCommitContext)
              .collect(Collectors.toList()),
          true);
    }

    LOGGER.debug(
        DataNodePipeMessages
            .PIPE_LOG_CONSENSUSSUBSCRIPTIONBROKER_POLL_RESULT_CONSUMERID_EVENTSPOLLED_06412726,
        brokerId,
        consumerId,
        eventsToPoll.size(),
        eventsToNack.size());

    return eventsToPoll;
  }

  @Override
  public List<SubscriptionEvent> pollTablets(
      final String consumerId, final SubscriptionCommitContext commitContext, final int offset) {
    final String topicName = commitContext.getTopicName();
    final List<ConsensusPrefetchingQueue> queues =
        topicNameToConsensusPrefetchingQueues.get(topicName);
    if (Objects.isNull(queues) || queues.isEmpty()) {
      return Collections.emptyList();
    }

    final ConsensusPrefetchingQueue assignedQueue = getQueueForCommitContext(queues, commitContext);
    if (Objects.isNull(assignedQueue)) {
      return Collections.emptyList();
    }

    final SubscriptionEvent event = assignedQueue.pollTablets(consumerId, commitContext, offset);
    if (Objects.nonNull(event)) {
      return Collections.singletonList(event);
    }
    return Collections.emptyList();
  }

  //////////////////////////// commit ////////////////////////////

  @Override
  public List<SubscriptionCommitContext> commit(
      final String consumerId,
      final List<SubscriptionCommitContext> commitContexts,
      final boolean nack) {
    final List<SubscriptionCommitContext> successfulCommitContexts = new ArrayList<>();
    for (final SubscriptionCommitContext commitContext : commitContexts) {
      final String topicName = commitContext.getTopicName();
      final List<ConsensusPrefetchingQueue> queues =
          topicNameToConsensusPrefetchingQueues.get(topicName);
      if (Objects.isNull(queues) || queues.isEmpty()) {
        LOGGER.warn(
            DataNodePipeMessages
                .PIPE_LOG_CONSENSUSSUBSCRIPTIONBROKER_NO_QUEUES_FOR_TOPIC_TO_COMMIT_7D8CC39D,
            brokerId,
            topicName);
        continue;
      }

      final ConsensusPrefetchingQueue assignedQueue =
          getQueueForCommitContext(queues, commitContext);
      boolean handled = false;
      if (Objects.nonNull(assignedQueue)) {
        final boolean success;
        if (!nack) {
          success = assignedQueue.ackSilent(consumerId, commitContext);
        } else {
          success = assignedQueue.nackSilent(consumerId, commitContext);
        }
        if (success) {
          successfulCommitContexts.add(commitContext);
          handled = true;
        }
      }
      if (!handled) {
        LOGGER.warn(
            DataNodePipeMessages
                .PIPE_LOG_CONSENSUSSUBSCRIPTIONBROKER_COMMIT_CONTEXT_NOT_FOUND_IN_46DF62A6,
            brokerId,
            commitContext,
            queues.size(),
            topicName);
      }
    }
    return successfulCommitContexts;
  }

  @Override
  public int refreshInFlightEventLeases(
      final String consumerId, final List<SubscriptionCommitContext> commitContexts) {
    int refreshedCount = 0;
    for (final SubscriptionCommitContext commitContext : commitContexts) {
      final String topicName = commitContext.getTopicName();
      final List<ConsensusPrefetchingQueue> queues =
          topicNameToConsensusPrefetchingQueues.get(topicName);
      if (Objects.isNull(queues) || queues.isEmpty()) {
        continue;
      }

      final ConsensusPrefetchingQueue assignedQueue =
          getQueueForCommitContext(queues, commitContext);
      if (Objects.nonNull(assignedQueue)
          && assignedQueue.refreshInFlightEventLease(consumerId, commitContext)) {
        refreshedCount++;
      }
    }
    return refreshedCount;
  }

  @Override
  public boolean isCommitContextOutdated(final SubscriptionCommitContext commitContext) {
    final String topicName = commitContext.getTopicName();
    final List<ConsensusPrefetchingQueue> queues =
        topicNameToConsensusPrefetchingQueues.get(topicName);
    if (Objects.isNull(queues) || queues.isEmpty()) {
      return true;
    }
    // Route directly to the correct region queue using regionId
    final String regionId = commitContext.getRegionId();
    for (final ConsensusPrefetchingQueue q : queues) {
      if (!regionId.isEmpty() && !regionId.equals(q.getConsensusGroupId().toString())) {
        continue;
      }
      return q.isCommitContextOutdated(commitContext);
    }
    return true;
  }

  //////////////////////////// seek ////////////////////////////

  public void seek(final String topicName, final short seekType) {
    final List<ConsensusPrefetchingQueue> queues =
        topicNameToConsensusPrefetchingQueues.get(topicName);
    if (Objects.isNull(queues) || queues.isEmpty()) {
      LOGGER.warn(
          DataNodePipeMessages
              .PIPE_LOG_CONSENSUSSUBSCRIPTIONBROKER_NO_QUEUES_FOR_TOPIC_TO_SEEK_6307A90D,
          brokerId,
          topicName);
      return;
    }

    for (final ConsensusPrefetchingQueue queue : queues) {
      if (queue.isClosed()) {
        continue;
      }
      switch (seekType) {
        case SubscriptionSeekReq.SEEK_TO_BEGINNING:
          queue.seekToBeginning();
          break;
        case SubscriptionSeekReq.SEEK_TO_END:
          queue.seekToEnd();
          break;
        default:
          LOGGER.warn(
              DataNodePipeMessages
                  .PIPE_LOG_CONSENSUSSUBSCRIPTIONBROKER_UNSUPPORTED_SEEKTYPE_FOR_TOPIC_EDCA2CF2,
              brokerId,
              seekType,
              topicName);
          break;
      }
    }
  }

  public void seek(final String topicName, final TopicProgress topicProgress) {
    final TopicProgress safeProgress =
        topicProgress != null ? topicProgress : new TopicProgress(Collections.emptyMap());
    final List<ConsensusPrefetchingQueue> queues =
        topicNameToConsensusPrefetchingQueues.get(topicName);
    if (Objects.isNull(queues) || queues.isEmpty()) {
      LOGGER.warn(
          DataNodePipeMessages
              .PIPE_LOG_CONSENSUSSUBSCRIPTIONBROKER_NO_QUEUES_FOR_TOPIC_TO_SEEK_9AC3890C,
          brokerId,
          topicName);
      return;
    }
    for (final ConsensusPrefetchingQueue queue : queues) {
      if (queue.isClosed()) {
        continue;
      }
      final RegionProgress regionProgress =
          safeProgress.getRegionProgress().get(queue.getConsensusGroupId().toString());
      seekQueueToRegionProgress(queue, regionProgress, false);
    }
  }

  public void seekAfter(final String topicName, final TopicProgress topicProgress) {
    final TopicProgress safeProgress =
        topicProgress != null ? topicProgress : new TopicProgress(Collections.emptyMap());
    final List<ConsensusPrefetchingQueue> queues =
        topicNameToConsensusPrefetchingQueues.get(topicName);
    if (Objects.isNull(queues) || queues.isEmpty()) {
      LOGGER.warn(
          DataNodePipeMessages
              .PIPE_LOG_CONSENSUSSUBSCRIPTIONBROKER_NO_QUEUES_FOR_TOPIC_TO_SEEKAFTER_C6D87BFD,
          brokerId,
          topicName);
      return;
    }
    for (final ConsensusPrefetchingQueue queue : queues) {
      if (queue.isClosed()) {
        continue;
      }
      final RegionProgress regionProgress =
          safeProgress.getRegionProgress().get(queue.getConsensusGroupId().toString());
      seekQueueToRegionProgress(queue, regionProgress, true);
    }
  }

  private void seekQueueToRegionProgress(
      final ConsensusPrefetchingQueue queue,
      final RegionProgress regionProgress,
      final boolean seekAfter) {
    if (Objects.isNull(regionProgress) || regionProgress.getWriterPositions().isEmpty()) {
      return;
    }
    if (seekAfter) {
      queue.seekAfterRegionProgress(regionProgress);
    } else {
      queue.seekToRegionProgress(regionProgress);
    }
  }

  //////////////////////////// prefetching ////////////////////////////

  @Override
  public boolean executePrefetch(final String topicName) {
    // Consensus prefetch is fully driven by queue-local wakeup sources and the dedicated delayed
    // scheduler. This interface remains only to satisfy the shared broker contract used by
    // pipe-based subscription.
    return false;
  }

  @Override
  public int getEventCount(final String topicName) {
    final List<ConsensusPrefetchingQueue> queues =
        topicNameToConsensusPrefetchingQueues.get(topicName);
    if (Objects.isNull(queues)) {
      return 0;
    }
    return queues.stream()
        .filter(queue -> !queue.isClosed())
        .mapToInt(ConsensusPrefetchingQueue::getPrefetchedEventCount)
        .sum();
  }

  @Override
  public int getQueueCount() {
    return topicNameToConsensusPrefetchingQueues.size();
  }

  /**
   * Returns per-region lag information for all topics managed by this broker. The result maps
   * "topicName/regionId" to the lag (number of WAL entries behind).
   */
  public Map<String, Long> getLagSummary() {
    final Map<String, Long> lagMap = new ConcurrentHashMap<>();
    for (final Map.Entry<String, List<ConsensusPrefetchingQueue>> entry :
        topicNameToConsensusPrefetchingQueues.entrySet()) {
      for (final ConsensusPrefetchingQueue queue : entry.getValue()) {
        if (!queue.isClosed()) {
          lagMap.put(entry.getKey() + "/" + queue.getConsensusGroupId().toString(), queue.getLag());
        }
      }
    }
    return lagMap;
  }

  private TopicOwnershipSnapshot refreshAndGetTopicOwnership(
      final String topicName,
      final List<ConsensusPrefetchingQueue> queues,
      final String consumerId) {
    final ConcurrentHashMap<String, Long> consumerTimestamps =
        topicConsumerLastPollMs.computeIfAbsent(topicName, ignored -> new ConcurrentHashMap<>());
    consumerTimestamps.put(consumerId, System.currentTimeMillis());
    evictInactiveConsumers(consumerTimestamps);
    final List<String> sortedConsumers = new ArrayList<>(consumerTimestamps.keySet());
    Collections.sort(sortedConsumers);

    final List<String> activeRegionIds =
        queues.stream()
            .filter(q -> !q.isClosed())
            .map(q -> q.getConsensusGroupId().toString())
            .sorted()
            .collect(Collectors.toList());

    final TopicOwnershipSnapshot existingSnapshot = topicOwnershipSnapshots.get(topicName);
    if (Objects.nonNull(existingSnapshot)
        && existingSnapshot.hasSameConsumers(sortedConsumers)
        && existingSnapshot.hasSameRegions(activeRegionIds)) {
      return existingSnapshot;
    }

    final TopicOwnershipSnapshot refreshedSnapshot =
        TopicOwnershipSnapshot.create(sortedConsumers, activeRegionIds);
    topicOwnershipSnapshots.put(topicName, refreshedSnapshot);
    LOGGER.debug(
        DataNodePipeMessages
            .PIPE_LOG_CONSENSUSSUBSCRIPTIONBROKER_REFRESHED_OWNERSHIP_FOR_TOPIC_EB11CF64,
        brokerId,
        topicName,
        sortedConsumers,
        activeRegionIds,
        refreshedSnapshot.getGeneration());
    return refreshedSnapshot;
  }

  private List<ConsensusPrefetchingQueue> getAssignedQueues(
      final List<ConsensusPrefetchingQueue> queues,
      final String consumerId,
      final TopicOwnershipSnapshot ownershipSnapshot) {
    if (Objects.isNull(ownershipSnapshot) || ownershipSnapshot.isEmpty()) {
      return Collections.emptyList();
    }
    final List<ConsensusPrefetchingQueue> assignedQueues = new ArrayList<>();
    for (final ConsensusPrefetchingQueue queue : queues) {
      if (queue.isClosed()) {
        continue;
      }
      if (consumerId.equals(
          ownershipSnapshot.getOwnerConsumerId(queue.getConsensusGroupId().toString()))) {
        assignedQueues.add(queue);
      }
    }
    return assignedQueues;
  }

  private List<ConsensusPrefetchingQueue> buildPollOrderForAssignedQueues(
      final List<ConsensusPrefetchingQueue> assignedQueues, final String topicName) {
    if (assignedQueues.size() <= 1) {
      return assignedQueues;
    }
    final List<ConsensusPrefetchingQueue> pollQueues = new ArrayList<>(assignedQueues);
    if (SubscriptionConfig.getInstance().isSubscriptionConsensusLagBasedPriority()) {
      pollQueues.sort(
          Comparator.comparingLong(ConsensusPrefetchingQueue::getLag)
              .reversed()
              .thenComparing(q -> q.getConsensusGroupId().toString()));
      return pollQueues;
    }

    final int startOffset = Math.floorMod(pollRoundRobinIndex.getAndIncrement(), pollQueues.size());
    final List<ConsensusPrefetchingQueue> orderedQueues = new ArrayList<>(pollQueues.size());
    for (int i = 0; i < pollQueues.size(); i++) {
      orderedQueues.add(pollQueues.get((startOffset + i) % pollQueues.size()));
    }
    LOGGER.debug(
        DataNodePipeMessages
            .PIPE_LOG_CONSENSUSSUBSCRIPTIONBROKER_STABLE_OWNERSHIP_POLL_ORDER_D40BB7D4,
        brokerId,
        topicName,
        orderedQueues.size());
    return orderedQueues;
  }

  private ConsensusPrefetchingQueue getQueueForCommitContext(
      final List<ConsensusPrefetchingQueue> queues, final SubscriptionCommitContext commitContext) {
    final String regionId = commitContext.getRegionId();
    for (final ConsensusPrefetchingQueue queue : queues) {
      if (queue.isClosed()) {
        continue;
      }
      if (Objects.nonNull(regionId)
          && !regionId.isEmpty()
          && !regionId.equals(queue.getConsensusGroupId().toString())) {
        continue;
      }
      return queue;
    }
    return null;
  }

  /** Evicts consumers that have not polled within the configured eviction timeout. */
  private void evictInactiveConsumers(final ConcurrentHashMap<String, Long> consumerTimestamps) {
    final long now = System.currentTimeMillis();
    final long timeout =
        SubscriptionConfig.getInstance().getSubscriptionConsensusConsumerEvictionTimeoutMs();
    consumerTimestamps.entrySet().removeIf(entry -> (now - entry.getValue()) > timeout);
  }

  //////////////////////////// queue management ////////////////////////////

  public ConsensusPrefetchingQueue bindConsensusPrefetchingQueue(
      final String topicName,
      final String orderMode,
      final ConsensusGroupId consensusGroupId,
      final IoTConsensusServerImpl serverImpl,
      final SubscriptionWalRetentionPolicy retentionPolicy,
      final ConsensusLogToTabletConverter converter,
      final ConsensusSubscriptionCommitManager commitManager,
      final RegionProgress fallbackCommittedRegionProgress,
      final long tailStartSearchIndex,
      final long initialRuntimeVersion,
      final boolean initialActive) {
    synchronized (queueLifecycleLock) {
      // Get or create the list of queues for this topic
      final List<ConsensusPrefetchingQueue> queues =
          topicNameToConsensusPrefetchingQueues.computeIfAbsent(
              topicName, k -> new CopyOnWriteArrayList<>());

      // Check for duplicate region binding
      for (final ConsensusPrefetchingQueue existing : queues) {
        if (consensusGroupId.equals(existing.getConsensusGroupId()) && !existing.isClosed()) {
          LOGGER.info(
              DataNodePipeMessages
                  .PIPE_LOG_SUBSCRIPTION_CONSENSUS_PREFETCHING_QUEUE_FOR_TOPIC_REGION_B40792D9,
              topicName,
              consensusGroupId,
              brokerId);
          return existing;
        }
      }

      // Create the per-region consensus queue for this topic.
      final ConsensusPrefetchingQueue consensusQueue =
          new ConsensusPrefetchingQueue(
              brokerId,
              topicName,
              orderMode,
              consensusGroupId,
              serverImpl,
              retentionPolicy,
              converter,
              commitManager,
              fallbackCommittedRegionProgress,
              tailStartSearchIndex,
              initialRuntimeVersion,
              initialActive);
      queues.add(consensusQueue);
      LOGGER.info(
          DataNodePipeMessages
              .PIPE_LOG_SUBSCRIPTION_CREATE_CONSENSUS_PREFETCHING_QUEUE_BOUND_TO_0DBFC05E,
          topicName,
          brokerId,
          consensusGroupId,
          fallbackCommittedRegionProgress,
          tailStartSearchIndex,
          initialRuntimeVersion,
          initialActive,
          queues.size());
      return consensusQueue;
    }
  }

  public void refreshConsensusQueueOrderMode(final String topicName, final String orderMode) {
    final List<ConsensusPrefetchingQueue> queues =
        topicNameToConsensusPrefetchingQueues.get(topicName);
    if (Objects.isNull(queues) || queues.isEmpty()) {
      return;
    }

    for (final ConsensusPrefetchingQueue queue : queues) {
      queue.setOrderMode(orderMode);
    }
  }

  public void unbindConsensusPrefetchingQueue(final String topicName) {
    closeAndRemoveConsensusPrefetchingQueues(topicName, true);
  }

  @Override
  public void unbind(final String topicName) {
    unbindConsensusPrefetchingQueue(topicName);
  }

  public int unbindByRegion(final ConsensusGroupId regionId) {
    final List<ConsensusPrefetchingQueue> queuesToClose = new ArrayList<>();
    final List<String> topicNamesToLog = new ArrayList<>();
    synchronized (queueLifecycleLock) {
      for (final Map.Entry<String, List<ConsensusPrefetchingQueue>> entry :
          topicNameToConsensusPrefetchingQueues.entrySet()) {
        final List<ConsensusPrefetchingQueue> queues = entry.getValue();
        for (final ConsensusPrefetchingQueue queue : new ArrayList<>(queues)) {
          if (regionId.equals(queue.getConsensusGroupId())) {
            queues.remove(queue);
            queuesToClose.add(queue);
            topicNamesToLog.add(entry.getKey());
          }
        }
        if (queues.isEmpty()) {
          topicNameToConsensusPrefetchingQueues.remove(entry.getKey(), queues);
          topicConsumerLastPollMs.remove(entry.getKey());
          topicOwnershipSnapshots.remove(entry.getKey());
        } else {
          topicOwnershipSnapshots.remove(entry.getKey());
        }
      }
    }

    for (int i = 0; i < queuesToClose.size(); i++) {
      queuesToClose.get(i).close();
      LOGGER.info(
          DataNodePipeMessages
              .PIPE_LOG_SUBSCRIPTION_CLOSED_CONSENSUS_PREFETCHING_QUEUE_FOR_TOPIC_3A9DDEC5,
          topicNamesToLog.get(i),
          regionId,
          brokerId);
    }
    return queuesToClose.size();
  }

  /**
   * Activates or deactivates all queues bound to {@code regionId}. Called on leader migration:
   * {@code false} on old leader, {@code true} on new leader. Inactive queues skip prefetching and
   * return null on poll, ensuring only the preferred writer serves subscription data.
   */
  public void setActiveForRegion(final ConsensusGroupId regionId, final boolean active) {
    for (final List<ConsensusPrefetchingQueue> queues :
        topicNameToConsensusPrefetchingQueues.values()) {
      for (final ConsensusPrefetchingQueue q : queues) {
        if (regionId.equals(q.getConsensusGroupId())) {
          q.setActive(active);
        }
      }
    }
  }

  public void setActiveWritersForRegion(
      final ConsensusGroupId regionId, final Set<Integer> activeWriterNodeIds) {
    final Set<Integer> normalizedActiveWriterNodeIds =
        Collections.unmodifiableSet(new LinkedHashSet<>(activeWriterNodeIds));
    for (final List<ConsensusPrefetchingQueue> queues :
        topicNameToConsensusPrefetchingQueues.values()) {
      for (final ConsensusPrefetchingQueue q : queues) {
        if (regionId.equals(q.getConsensusGroupId())) {
          q.setActiveWriterNodeIds(normalizedActiveWriterNodeIds);
        }
      }
    }
  }

  public void applyRuntimeStateForRegion(
      final ConsensusGroupId regionId, final ConsensusRegionRuntimeState runtimeState) {
    for (final List<ConsensusPrefetchingQueue> queues :
        topicNameToConsensusPrefetchingQueues.values()) {
      for (final ConsensusPrefetchingQueue q : queues) {
        if (regionId.equals(q.getConsensusGroupId())) {
          q.applyRuntimeState(runtimeState);
        }
      }
    }
  }

  public void abortPendingSeeksForRuntimeStop() {
    for (final List<ConsensusPrefetchingQueue> queues :
        topicNameToConsensusPrefetchingQueues.values()) {
      for (final ConsensusPrefetchingQueue q : queues) {
        if (!q.isClosed()) {
          q.abortPendingSeekForRuntimeStop();
        }
      }
    }
  }

  @Override
  public void removeQueue(final String topicName) {
    final List<ConsensusPrefetchingQueue> queues =
        topicNameToConsensusPrefetchingQueues.get(topicName);
    if (Objects.nonNull(queues) && !queues.isEmpty()) {
      LOGGER.info(
          DataNodePipeMessages
              .PIPE_LOG_SUBSCRIPTION_CONSENSUS_PREFETCHING_QUEUE_S_BOUND_TO_TOPIC_AB10ED07,
          topicName,
          brokerId);
    }
    closeAndRemoveConsensusPrefetchingQueues(topicName, false);
  }

  private void closeAndRemoveConsensusPrefetchingQueues(
      final String topicName, final boolean warnIfMissing) {
    final List<ConsensusPrefetchingQueue> queuesToClose;
    synchronized (queueLifecycleLock) {
      final List<ConsensusPrefetchingQueue> queues =
          topicNameToConsensusPrefetchingQueues.remove(topicName);
      if (Objects.isNull(queues) || queues.isEmpty()) {
        if (warnIfMissing) {
          LOGGER.warn(
              DataNodePipeMessages
                  .PIPE_LOG_SUBSCRIPTION_CONSENSUS_PREFETCHING_QUEUES_BOUND_TO_TOPIC_63B37089,
              topicName,
              brokerId);
        }
        return;
      }
      queuesToClose = new ArrayList<>(queues);
      topicConsumerLastPollMs.remove(topicName);
      topicOwnershipSnapshots.remove(topicName);
    }

    for (final ConsensusPrefetchingQueue q : queuesToClose) {
      q.close();
    }
    LOGGER.info(
        DataNodePipeMessages
            .PIPE_LOG_SUBSCRIPTION_DROP_ALL_CONSENSUS_PREFETCHING_QUEUE_S_BOUND_FCC1B2C4,
        queuesToClose.size(),
        topicName,
        brokerId);
  }

  private static final class TopicOwnershipSnapshot {

    private final List<String> activeConsumers;
    private final List<String> activeRegionIds;
    private final Map<String, String> ownerByRegionId;
    private final int generation;

    private TopicOwnershipSnapshot(
        final List<String> activeConsumers,
        final List<String> activeRegionIds,
        final Map<String, String> ownerByRegionId,
        final int generation) {
      this.activeConsumers = activeConsumers;
      this.activeRegionIds = activeRegionIds;
      this.ownerByRegionId = ownerByRegionId;
      this.generation = generation;
    }

    private static TopicOwnershipSnapshot create(
        final List<String> activeConsumers, final List<String> activeRegionIds) {
      if (activeConsumers.isEmpty() || activeRegionIds.isEmpty()) {
        return new TopicOwnershipSnapshot(
            Collections.emptyList(), Collections.emptyList(), Collections.emptyMap(), 0);
      }

      final Map<String, String> ownerByRegionId = new ConcurrentHashMap<>();
      final int consumerCount = activeConsumers.size();
      for (final String regionId : activeRegionIds) {
        final int ownerIdx = Math.floorMod(regionId.hashCode(), consumerCount);
        ownerByRegionId.put(regionId, activeConsumers.get(ownerIdx));
      }
      return new TopicOwnershipSnapshot(
          Collections.unmodifiableList(new ArrayList<>(activeConsumers)),
          Collections.unmodifiableList(new ArrayList<>(activeRegionIds)),
          Collections.unmodifiableMap(ownerByRegionId),
          ownerByRegionId.hashCode());
    }

    private boolean isEmpty() {
      return activeConsumers.isEmpty() || activeRegionIds.isEmpty();
    }

    private boolean hasSameConsumers(final List<String> consumers) {
      return activeConsumers.equals(consumers);
    }

    private boolean hasSameRegions(final List<String> regionIds) {
      return activeRegionIds.equals(regionIds);
    }

    private String getOwnerConsumerId(final String regionId) {
      return ownerByRegionId.get(regionId);
    }

    private int getGeneration() {
      return generation;
    }
  }
}
