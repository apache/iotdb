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

package org.apache.iotdb.db.subscription.agent;

import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.consensus.iot.IoTConsensusServerImpl;
import org.apache.iotdb.db.subscription.broker.ConsensusSubscriptionBroker;
import org.apache.iotdb.db.subscription.broker.SubscriptionBroker;
import org.apache.iotdb.db.subscription.broker.consensus.ConsensusLogToTabletConverter;
import org.apache.iotdb.db.subscription.broker.consensus.ConsensusRegionRuntimeState;
import org.apache.iotdb.db.subscription.broker.consensus.ConsensusSubscriptionCommitManager;
import org.apache.iotdb.db.subscription.broker.consensus.ConsensusSubscriptionSetupHandler;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.db.subscription.resource.SubscriptionDataNodeResourceManager;
import org.apache.iotdb.db.subscription.task.subtask.SubscriptionSinkSubtask;
import org.apache.iotdb.rpc.subscription.config.ConsumerConfig;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;
import org.apache.iotdb.rpc.subscription.payload.poll.RegionProgress;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.poll.TopicProgress;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribeSeekReq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

public class SubscriptionBrokerAgent {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionBrokerAgent.class);

  /** Pipe-based subscription brokers, one per consumer group. */
  private final Map<String, SubscriptionBroker> consumerGroupIdToPipeBroker =
      new ConcurrentHashMap<>();

  /** Consensus-based subscription brokers, one per consumer group. */
  private final Map<String, ConsensusSubscriptionBroker> consumerGroupIdToConsensusBroker =
      new ConcurrentHashMap<>();

  private final Cache<Integer> prefetchingQueueCount =
      new Cache<>(this::getPrefetchingQueueCountInternal);

  //////////////////////////// provided for subscription agent ////////////////////////////

  public List<SubscriptionEvent> poll(
      final ConsumerConfig consumerConfig, final Set<String> topicNames, final long maxBytes) {
    return poll(consumerConfig, topicNames, maxBytes, Collections.emptyMap());
  }

  public List<SubscriptionEvent> poll(
      final ConsumerConfig consumerConfig,
      final Set<String> topicNames,
      final long maxBytes,
      final Map<String, TopicProgress> progressByTopic) {
    final String consumerGroupId = consumerConfig.getConsumerGroupId();
    final String consumerId = consumerConfig.getConsumerId();
    final List<SubscriptionEvent> allEvents = new ArrayList<>();
    long remainingBytes = maxBytes;

    // Poll from pipe-based broker
    final SubscriptionBroker pipeBroker = consumerGroupIdToPipeBroker.get(consumerGroupId);
    if (Objects.nonNull(pipeBroker)) {
      final List<SubscriptionEvent> pipeEvents =
          pipeBroker.poll(consumerId, topicNames, remainingBytes);
      allEvents.addAll(pipeEvents);
      for (final SubscriptionEvent event : pipeEvents) {
        try {
          remainingBytes -= event.getCurrentResponseSize();
        } catch (final IOException ignored) {
          // best effort
        }
      }
    }

    // Poll from consensus-based broker
    if (remainingBytes > 0) {
      final ConsensusSubscriptionBroker consensusBroker =
          consumerGroupIdToConsensusBroker.get(consumerGroupId);
      if (Objects.nonNull(consensusBroker)) {
        LOGGER.debug(
            "SubscriptionBrokerAgent: polling consensus broker for consumer group [{}], "
                + "topicNames={}, remainingBytes={}",
            consumerGroupId,
            topicNames,
            remainingBytes);
        allEvents.addAll(
            consensusBroker.poll(consumerId, topicNames, remainingBytes, progressByTopic));
      } else {
        LOGGER.debug(
            "SubscriptionBrokerAgent: no consensus broker for consumer group [{}]",
            consumerGroupId);
      }
    }

    if (allEvents.isEmpty()
        && Objects.isNull(pipeBroker)
        && Objects.isNull(consumerGroupIdToConsensusBroker.get(consumerGroupId))) {
      final String errorMessage =
          String.format("Subscription: no broker bound to consumer group [%s]", consumerGroupId);
      LOGGER.warn(errorMessage);
      throw new SubscriptionException(errorMessage);
    }

    return allEvents;
  }

  public List<SubscriptionEvent> pollTsFile(
      final ConsumerConfig consumerConfig,
      final SubscriptionCommitContext commitContext,
      final long writingOffset) {
    final String consumerGroupId = consumerConfig.getConsumerGroupId();
    // TsFile polling can only be called by pipe-based subscriptions
    final SubscriptionBroker pipeBroker = consumerGroupIdToPipeBroker.get(consumerGroupId);
    if (Objects.isNull(pipeBroker)) {
      final String errorMessage =
          String.format(
              "Subscription: pipe broker bound to consumer group [%s] does not exist",
              consumerGroupId);
      LOGGER.warn(errorMessage);
      throw new SubscriptionException(errorMessage);
    }
    final String consumerId = consumerConfig.getConsumerId();
    return pipeBroker.pollTsFile(consumerId, commitContext, writingOffset);
  }

  public List<SubscriptionEvent> pollTablets(
      final ConsumerConfig consumerConfig,
      final SubscriptionCommitContext commitContext,
      final int offset) {
    final String consumerGroupId = consumerConfig.getConsumerGroupId();
    final String consumerId = consumerConfig.getConsumerId();
    final String topicName = commitContext.getTopicName();

    // Try consensus-based broker first
    final ConsensusSubscriptionBroker consensusBroker =
        consumerGroupIdToConsensusBroker.get(consumerGroupId);
    if (Objects.nonNull(consensusBroker) && consensusBroker.hasQueue(topicName)) {
      return consensusBroker.pollTablets(consumerId, commitContext, offset);
    }

    // Fall back to pipe-based broker
    final SubscriptionBroker pipeBroker = consumerGroupIdToPipeBroker.get(consumerGroupId);
    if (Objects.isNull(pipeBroker)) {
      final String errorMessage =
          String.format(
              "Subscription: broker bound to consumer group [%s] does not exist", consumerGroupId);
      LOGGER.warn(errorMessage);
      throw new SubscriptionException(errorMessage);
    }
    return pipeBroker.pollTablets(consumerId, commitContext, offset);
  }

  /**
   * @return list of successful commit contexts
   */
  public List<SubscriptionCommitContext> commit(
      final ConsumerConfig consumerConfig,
      final List<SubscriptionCommitContext> commitContexts,
      final boolean nack) {
    final String consumerGroupId = consumerConfig.getConsumerGroupId();
    final String consumerId = consumerConfig.getConsumerId();
    final List<SubscriptionCommitContext> allSuccessful = new ArrayList<>();

    final SubscriptionBroker pipeBroker = consumerGroupIdToPipeBroker.get(consumerGroupId);
    final ConsensusSubscriptionBroker consensusBroker =
        consumerGroupIdToConsensusBroker.get(consumerGroupId);

    if (Objects.isNull(pipeBroker) && Objects.isNull(consensusBroker)) {
      final String errorMessage =
          String.format("Subscription: no broker bound to consumer group [%s]", consumerGroupId);
      LOGGER.warn(errorMessage);
      throw new SubscriptionException(errorMessage);
    }

    // Partition commit contexts by which broker owns the topic.
    final List<SubscriptionCommitContext> pipeContexts = new ArrayList<>();
    final List<SubscriptionCommitContext> consensusContexts = new ArrayList<>();
    for (final SubscriptionCommitContext ctx : commitContexts) {
      final String topicName = ctx.getTopicName();
      if (Objects.nonNull(consensusBroker)
          && ConsensusSubscriptionSetupHandler.isConsensusBasedTopic(topicName)) {
        consensusContexts.add(ctx);
      } else {
        pipeContexts.add(ctx);
      }
    }

    if (Objects.nonNull(pipeBroker) && !pipeContexts.isEmpty()) {
      allSuccessful.addAll(pipeBroker.commit(consumerId, pipeContexts, nack));
    }
    if (Objects.nonNull(consensusBroker) && !consensusContexts.isEmpty()) {
      allSuccessful.addAll(consensusBroker.commit(consumerId, consensusContexts, nack));
    }

    return allSuccessful;
  }

  public void seek(
      final ConsumerConfig consumerConfig, final String topicName, final short seekType) {
    final String consumerGroupId = consumerConfig.getConsumerGroupId();

    final ConsensusSubscriptionBroker consensusBroker =
        consumerGroupIdToConsensusBroker.get(consumerGroupId);
    if (Objects.nonNull(consensusBroker) && consensusBroker.hasQueue(topicName)) {
      if (seekType != PipeSubscribeSeekReq.SEEK_TO_BEGINNING
          && seekType != PipeSubscribeSeekReq.SEEK_TO_END) {
        final String errorMessage =
            String.format(
                "Subscription: consensus seek only supports beginning/end or topic progress, "
                    + "consumerGroup=%s, topic=%s, seekType=%s",
                consumerGroupId, topicName, seekType);
        LOGGER.warn(errorMessage);
        throw new SubscriptionException(errorMessage);
      }
      consensusBroker.seek(topicName, seekType);
      return;
    }

    final String errorMessage =
        String.format(
            "Subscription: seek is only supported for consensus-based subscriptions, "
                + "consumerGroup=%s, topic=%s",
            consumerGroupId, topicName);
    LOGGER.warn(errorMessage);
    throw new SubscriptionException(errorMessage);
  }

  public void seekToTopicProgress(
      final ConsumerConfig consumerConfig,
      final String topicName,
      final TopicProgress topicProgress) {
    final String consumerGroupId = consumerConfig.getConsumerGroupId();

    final ConsensusSubscriptionBroker consensusBroker =
        consumerGroupIdToConsensusBroker.get(consumerGroupId);
    if (Objects.nonNull(consensusBroker) && consensusBroker.hasQueue(topicName)) {
      consensusBroker.seek(topicName, topicProgress);
      return;
    }

    final String errorMessage =
        String.format(
            "Subscription: seek(topicProgress) is only supported for consensus-based subscriptions, "
                + "consumerGroup=%s, topic=%s",
            consumerGroupId, topicName);
    LOGGER.warn(errorMessage);
    throw new SubscriptionException(errorMessage);
  }

  public void seekAfterTopicProgress(
      final ConsumerConfig consumerConfig,
      final String topicName,
      final TopicProgress topicProgress) {
    final String consumerGroupId = consumerConfig.getConsumerGroupId();

    final ConsensusSubscriptionBroker consensusBroker =
        consumerGroupIdToConsensusBroker.get(consumerGroupId);
    if (Objects.nonNull(consensusBroker) && consensusBroker.hasQueue(topicName)) {
      consensusBroker.seekAfter(topicName, topicProgress);
      return;
    }

    final String errorMessage =
        String.format(
            "Subscription: seekAfter(topicProgress) is only supported for consensus-based subscriptions, "
                + "consumerGroup=%s, topic=%s",
            consumerGroupId, topicName);
    LOGGER.warn(errorMessage);
    throw new SubscriptionException(errorMessage);
  }

  public boolean isCommitContextOutdated(final SubscriptionCommitContext commitContext) {
    final String consumerGroupId = commitContext.getConsumerGroupId();
    final String topicName = commitContext.getTopicName();

    // Try consensus broker first
    final ConsensusSubscriptionBroker consensusBroker =
        consumerGroupIdToConsensusBroker.get(consumerGroupId);
    if (Objects.nonNull(consensusBroker) && consensusBroker.hasQueue(topicName)) {
      return consensusBroker.isCommitContextOutdated(commitContext);
    }

    // Fall back to pipe broker
    final SubscriptionBroker pipeBroker = consumerGroupIdToPipeBroker.get(consumerGroupId);
    if (Objects.isNull(pipeBroker)) {
      return true;
    }
    return pipeBroker.isCommitContextOutdated(commitContext);
  }

  public List<String> fetchTopicNamesToUnsubscribe(
      final ConsumerConfig consumerConfig, final Set<String> topicNames) {
    final String consumerGroupId = consumerConfig.getConsumerGroupId();

    // Consensus-based subscription topics are unbounded streams, so they do not trigger
    // auto-unsubscribe.
    final ConsensusSubscriptionBroker consensusBroker =
        consumerGroupIdToConsensusBroker.get(consumerGroupId);
    final Set<String> pipeOnlyTopicNames;
    if (Objects.nonNull(consensusBroker)) {
      pipeOnlyTopicNames = new java.util.HashSet<>(topicNames);
      pipeOnlyTopicNames.removeIf(consensusBroker::hasQueue);
    } else {
      pipeOnlyTopicNames = topicNames;
    }

    if (pipeOnlyTopicNames.isEmpty()) {
      return Collections.emptyList();
    }

    final SubscriptionBroker pipeBroker = consumerGroupIdToPipeBroker.get(consumerGroupId);
    if (Objects.isNull(pipeBroker)) {
      return Collections.emptyList();
    }
    return pipeBroker.fetchTopicNamesToUnsubscribe(pipeOnlyTopicNames);
  }

  /////////////////////////////// broker ///////////////////////////////

  public boolean isBrokerExist(final String consumerGroupId) {
    return consumerGroupIdToPipeBroker.containsKey(consumerGroupId)
        || consumerGroupIdToConsensusBroker.containsKey(consumerGroupId);
  }

  public void createBrokerIfNotExist(final String consumerGroupId) {
    consumerGroupIdToPipeBroker.computeIfAbsent(consumerGroupId, SubscriptionBroker::new);
    LOGGER.info("Subscription: create pipe broker bound to consumer group [{}]", consumerGroupId);
  }

  /**
   * @return {@code true} if drop broker success, {@code false} otherwise
   */
  public boolean dropBroker(final String consumerGroupId) {
    final AtomicBoolean dropped = new AtomicBoolean(false);

    // Drop pipe broker
    consumerGroupIdToPipeBroker.compute(
        consumerGroupId,
        (id, broker) -> {
          if (Objects.isNull(broker)) {
            dropped.set(true);
            return null;
          }
          if (!broker.isEmpty()) {
            LOGGER.warn(
                "Subscription: pipe broker bound to consumer group [{}] is not empty when dropping",
                consumerGroupId);
            return broker;
          }
          dropped.set(true);
          LOGGER.info(
              "Subscription: drop pipe broker bound to consumer group [{}]", consumerGroupId);
          return null;
        });

    // Drop consensus broker
    consumerGroupIdToConsensusBroker.compute(
        consumerGroupId,
        (id, broker) -> {
          if (Objects.isNull(broker)) {
            return null;
          }
          if (!broker.isEmpty()) {
            LOGGER.warn(
                "Subscription: consensus broker bound to consumer group [{}] is not empty when dropping",
                consumerGroupId);
            return broker;
          }
          dropped.set(true);
          LOGGER.info(
              "Subscription: drop consensus broker bound to consumer group [{}]", consumerGroupId);
          return null;
        });

    return dropped.get();
  }

  /////////////////////////////// prefetching queue ///////////////////////////////

  public void bindPrefetchingQueue(final SubscriptionSinkSubtask subtask) {
    final String consumerGroupId = subtask.getConsumerGroupId();
    consumerGroupIdToPipeBroker
        .compute(
            consumerGroupId,
            (id, broker) -> {
              if (Objects.isNull(broker)) {
                LOGGER.info(
                    "Subscription: pipe broker bound to consumer group [{}] does not exist, create new for binding prefetching queue",
                    consumerGroupId);
                return new SubscriptionBroker(consumerGroupId);
              }
              return broker;
            })
        .bindPrefetchingQueue(subtask.getTopicName(), subtask.getInputPendingQueue());
    prefetchingQueueCount.invalidate();
  }

  public void bindConsensusPrefetchingQueue(
      final String consumerGroupId,
      final String topicName,
      final String orderMode,
      final ConsensusGroupId consensusGroupId,
      final IoTConsensusServerImpl serverImpl,
      final ConsensusLogToTabletConverter converter,
      final ConsensusSubscriptionCommitManager commitManager,
      final RegionProgress fallbackCommittedRegionProgress,
      final long tailStartSearchIndex,
      final long initialRuntimeVersion,
      final boolean initialActive) {
    consumerGroupIdToConsensusBroker
        .compute(
            consumerGroupId,
            (id, broker) -> {
              if (Objects.isNull(broker)) {
                LOGGER.info(
                    "Subscription: consensus broker bound to consumer group [{}] does not exist, create new for binding consensus prefetching queue",
                    consumerGroupId);
                return new ConsensusSubscriptionBroker(consumerGroupId);
              }
              return broker;
            })
        .bindConsensusPrefetchingQueue(
            topicName,
            orderMode,
            consensusGroupId,
            serverImpl,
            converter,
            commitManager,
            fallbackCommittedRegionProgress,
            tailStartSearchIndex,
            initialRuntimeVersion,
            initialActive);
    prefetchingQueueCount.invalidate();
  }

  public void refreshConsensusQueueOrderMode(final String topicName, final String orderMode) {
    LOGGER.info(
        "SubscriptionBrokerAgent: refreshing consensus queue order-mode for topic [{}] to [{}]",
        topicName,
        orderMode);
    for (final ConsensusSubscriptionBroker broker : consumerGroupIdToConsensusBroker.values()) {
      broker.refreshConsensusQueueOrderMode(topicName, orderMode);
    }
  }

  public void unbindConsensusPrefetchingQueue(
      final String consumerGroupId, final String topicName) {
    final ConsensusSubscriptionBroker broker =
        consumerGroupIdToConsensusBroker.get(consumerGroupId);
    if (Objects.isNull(broker)) {
      LOGGER.warn(
          "Subscription: consensus broker bound to consumer group [{}] does not exist",
          consumerGroupId);
      return;
    }
    broker.unbindConsensusPrefetchingQueue(topicName);
    prefetchingQueueCount.invalidate();
  }

  public void unbindByRegion(final ConsensusGroupId regionId) {
    int totalClosed = 0;
    for (final ConsensusSubscriptionBroker broker : consumerGroupIdToConsensusBroker.values()) {
      totalClosed += broker.unbindByRegion(regionId);
    }
    if (totalClosed > 0) {
      prefetchingQueueCount.invalidate();
      LOGGER.info(
          "Subscription: unbound {} consensus prefetching queue(s) for removed region [{}]",
          totalClosed,
          regionId);
    }
  }

  /**
   * Activates or deactivates all consensus prefetching queues bound to {@code regionId} across all
   * consumer groups. Called on leader migration to ensure only the preferred writer serves
   * subscription data.
   */
  public void setActiveForRegion(final ConsensusGroupId regionId, final boolean active) {
    LOGGER.info(
        "SubscriptionBrokerAgent: setActiveForRegion regionId={}, active={}", regionId, active);
    for (final ConsensusSubscriptionBroker broker : consumerGroupIdToConsensusBroker.values()) {
      broker.setActiveForRegion(regionId, active);
    }
  }

  public void setActiveWritersForRegion(
      final ConsensusGroupId regionId, final Set<Integer> activeWriterNodeIds) {
    LOGGER.info(
        "SubscriptionBrokerAgent: setActiveWritersForRegion regionId={}, activeWriterNodeIds={}",
        regionId,
        activeWriterNodeIds);
    for (final ConsensusSubscriptionBroker broker : consumerGroupIdToConsensusBroker.values()) {
      broker.setActiveWritersForRegion(regionId, activeWriterNodeIds);
    }
  }

  public void applyRuntimeStateForRegion(
      final ConsensusGroupId regionId, final ConsensusRegionRuntimeState runtimeState) {
    LOGGER.info(
        "SubscriptionBrokerAgent: applyRuntimeStateForRegion regionId={}, runtimeState={}",
        regionId,
        runtimeState);
    for (final ConsensusSubscriptionBroker broker : consumerGroupIdToConsensusBroker.values()) {
      broker.applyRuntimeStateForRegion(regionId, runtimeState);
    }
  }

  public void updateCompletedTopicNames(final String consumerGroupId, final String topicName) {
    final SubscriptionBroker pipeBroker = consumerGroupIdToPipeBroker.get(consumerGroupId);
    if (Objects.isNull(pipeBroker)) {
      LOGGER.warn(
          "Subscription: pipe broker bound to consumer group [{}] does not exist", consumerGroupId);
      return;
    }
    pipeBroker.updateCompletedTopicNames(topicName);
  }

  public void unbindPrefetchingQueue(final String consumerGroupId, final String topicName) {
    // Try consensus broker first
    final ConsensusSubscriptionBroker consensusBroker =
        consumerGroupIdToConsensusBroker.get(consumerGroupId);
    if (Objects.nonNull(consensusBroker) && consensusBroker.hasQueue(topicName)) {
      consensusBroker.removeQueue(topicName);
      prefetchingQueueCount.invalidate();
      return;
    }
    // Fall back to pipe broker
    final SubscriptionBroker pipeBroker = consumerGroupIdToPipeBroker.get(consumerGroupId);
    if (Objects.isNull(pipeBroker)) {
      LOGGER.warn(
          "Subscription: broker bound to consumer group [{}] does not exist", consumerGroupId);
      return;
    }
    pipeBroker.unbindPrefetchingQueue(topicName);
    prefetchingQueueCount.invalidate();
  }

  public void removePrefetchingQueue(final String consumerGroupId, final String topicName) {
    // Try consensus broker
    final ConsensusSubscriptionBroker consensusBroker =
        consumerGroupIdToConsensusBroker.get(consumerGroupId);
    if (Objects.nonNull(consensusBroker) && consensusBroker.hasQueue(topicName)) {
      consensusBroker.removeQueue(topicName);
      prefetchingQueueCount.invalidate();
      return;
    }
    // Fall back to pipe broker
    final SubscriptionBroker pipeBroker = consumerGroupIdToPipeBroker.get(consumerGroupId);
    if (Objects.isNull(pipeBroker)) {
      LOGGER.warn(
          "Subscription: broker bound to consumer group [{}] does not exist", consumerGroupId);
      return;
    }
    pipeBroker.removePrefetchingQueue(topicName);
    prefetchingQueueCount.invalidate();
  }

  public boolean executePrefetch(final String consumerGroupId, final String topicName) {
    // Try consensus broker first
    final ConsensusSubscriptionBroker consensusBroker =
        consumerGroupIdToConsensusBroker.get(consumerGroupId);
    if (Objects.nonNull(consensusBroker) && consensusBroker.hasQueue(topicName)) {
      return consensusBroker.executePrefetch(topicName);
    }
    // Fall back to pipe broker
    final SubscriptionBroker pipeBroker = consumerGroupIdToPipeBroker.get(consumerGroupId);
    if (Objects.isNull(pipeBroker)) {
      SubscriptionDataNodeResourceManager.log()
          .schedule(SubscriptionBrokerAgent.class, consumerGroupId, topicName)
          .ifPresent(
              l ->
                  l.warn(
                      "Subscription: broker bound to consumer group [{}] does not exist",
                      consumerGroupId));
      return false;
    }
    return pipeBroker.executePrefetch(topicName);
  }

  public int getPipeEventCount(final String consumerGroupId, final String topicName) {
    // Try consensus broker first
    final ConsensusSubscriptionBroker consensusBroker =
        consumerGroupIdToConsensusBroker.get(consumerGroupId);
    if (Objects.nonNull(consensusBroker) && consensusBroker.hasQueue(topicName)) {
      return consensusBroker.getEventCount(topicName);
    }
    // Fall back to pipe broker
    final SubscriptionBroker pipeBroker = consumerGroupIdToPipeBroker.get(consumerGroupId);
    if (Objects.isNull(pipeBroker)) {
      LOGGER.warn(
          "Subscription: broker bound to consumer group [{}] does not exist", consumerGroupId);
      return 0;
    }
    return pipeBroker.getPipeEventCount(topicName);
  }

  public int getPrefetchingQueueCount() {
    return prefetchingQueueCount.get();
  }

  public Map<String, Long> getConsensusLagSummary() {
    final Map<String, Long> result = new ConcurrentHashMap<>();
    for (final Map.Entry<String, ConsensusSubscriptionBroker> entry :
        consumerGroupIdToConsensusBroker.entrySet()) {
      final String groupId = entry.getKey();
      for (final Map.Entry<String, Long> lag : entry.getValue().getLagSummary().entrySet()) {
        result.put(groupId + "/" + lag.getKey(), lag.getValue());
      }
    }
    return result;
  }

  private int getPrefetchingQueueCountInternal() {
    int count =
        consumerGroupIdToPipeBroker.values().stream()
            .map(SubscriptionBroker::getPrefetchingQueueCount)
            .reduce(0, Integer::sum);
    count +=
        consumerGroupIdToConsensusBroker.values().stream()
            .map(ConsensusSubscriptionBroker::getQueueCount)
            .reduce(0, Integer::sum);
    return count;
  }

  /////////////////////////////// Commit Progress ///////////////////////////////

  public Map<String, ByteBuffer> collectAllRegionCommitProgress(final int dataNodeId) {
    return ConsensusSubscriptionCommitManager.getInstance().collectAllRegionProgress(dataNodeId);
  }

  /**
   * Receives a committed progress broadcast from another DataNode (Leader → Follower). Delegates to
   * CommitManager to update local progress state.
   */
  public void receiveSubscriptionProgress(
      final String consumerGroupId,
      final String topicName,
      final String regionId,
      final long physicalTime,
      final long localSeq,
      final int writerNodeId,
      final long writerEpoch) {
    ConsensusSubscriptionCommitManager.getInstance()
        .receiveProgressBroadcast(
            consumerGroupId,
            topicName,
            regionId,
            physicalTime,
            localSeq,
            writerNodeId,
            writerEpoch);
  }

  /////////////////////////////// Cache ///////////////////////////////

  /**
   * A simple generic cache that computes and stores a value on demand.
   *
   * <p>Both {@code value} and {@code valid} are volatile to ensure visibility across threads. The
   * {@code get()} method uses a local snapshot of {@code valid} to avoid double-read reordering.
   * Concurrent recomputation by multiple threads is benign (idempotent supplier).
   *
   * @param <T> the type of the cached value
   */
  private static class Cache<T> {

    private volatile T value;
    private volatile boolean valid = false;
    private final Supplier<T> supplier;

    /**
     * Construct a cache with a supplier that knows how to compute the value.
     *
     * @param supplier a Supplier that computes the value when needed
     */
    private Cache(final Supplier<T> supplier) {
      this.supplier = supplier;
    }

    /** Invalidate the cache. The next call to get() will recompute the value. */
    private void invalidate() {
      valid = false;
    }

    /**
     * Return the cached value, recomputing it if the cache is invalid.
     *
     * @return the current value, recomputed if necessary
     */
    private T get() {
      if (!valid) {
        final T computed = supplier.get();
        value = computed;
        valid = true;
        return computed;
      }
      return value;
    }
  }
}
