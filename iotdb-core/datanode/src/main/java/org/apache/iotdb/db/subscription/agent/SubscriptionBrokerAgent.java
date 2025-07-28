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

import org.apache.iotdb.db.subscription.broker.SubscriptionBroker;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.db.subscription.resource.SubscriptionDataNodeResourceManager;
import org.apache.iotdb.db.subscription.task.subtask.SubscriptionSinkSubtask;
import org.apache.iotdb.rpc.subscription.config.ConsumerConfig;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private final Map<String, SubscriptionBroker> consumerGroupIdToSubscriptionBroker =
      new ConcurrentHashMap<>();

  private final Cache<Integer> prefetchingQueueCount =
      new Cache<>(this::getPrefetchingQueueCountInternal);

  //////////////////////////// provided for subscription agent ////////////////////////////

  public List<SubscriptionEvent> poll(
      final ConsumerConfig consumerConfig, final Set<String> topicNames, final long maxBytes) {
    final String consumerGroupId = consumerConfig.getConsumerGroupId();
    final SubscriptionBroker broker = consumerGroupIdToSubscriptionBroker.get(consumerGroupId);
    if (Objects.isNull(broker)) {
      final String errorMessage =
          String.format(
              "Subscription: broker bound to consumer group [%s] does not exist", consumerGroupId);
      LOGGER.warn(errorMessage);
      throw new SubscriptionException(errorMessage);
    }
    // TODO: currently we fetch messages from all topics
    final String consumerId = consumerConfig.getConsumerId();
    return broker.poll(consumerId, topicNames, maxBytes);
  }

  public List<SubscriptionEvent> pollTsFile(
      final ConsumerConfig consumerConfig,
      final SubscriptionCommitContext commitContext,
      final long writingOffset) {
    final String consumerGroupId = consumerConfig.getConsumerGroupId();
    final SubscriptionBroker broker = consumerGroupIdToSubscriptionBroker.get(consumerGroupId);
    if (Objects.isNull(broker)) {
      final String errorMessage =
          String.format(
              "Subscription: broker bound to consumer group [%s] does not exist", consumerGroupId);
      LOGGER.warn(errorMessage);
      throw new SubscriptionException(errorMessage);
    }
    final String consumerId = consumerConfig.getConsumerId();
    return broker.pollTsFile(consumerId, commitContext, writingOffset);
  }

  public List<SubscriptionEvent> pollTablets(
      final ConsumerConfig consumerConfig,
      final SubscriptionCommitContext commitContext,
      final int offset) {
    final String consumerGroupId = consumerConfig.getConsumerGroupId();
    final SubscriptionBroker broker = consumerGroupIdToSubscriptionBroker.get(consumerGroupId);
    if (Objects.isNull(broker)) {
      final String errorMessage =
          String.format(
              "Subscription: broker bound to consumer group [%s] does not exist", consumerGroupId);
      LOGGER.warn(errorMessage);
      throw new SubscriptionException(errorMessage);
    }
    final String consumerId = consumerConfig.getConsumerId();
    return broker.pollTablets(consumerId, commitContext, offset);
  }

  /**
   * @return list of successful commit contexts
   */
  public List<SubscriptionCommitContext> commit(
      final ConsumerConfig consumerConfig,
      final List<SubscriptionCommitContext> commitContexts,
      final boolean nack) {
    final String consumerGroupId = consumerConfig.getConsumerGroupId();
    final SubscriptionBroker broker = consumerGroupIdToSubscriptionBroker.get(consumerGroupId);
    if (Objects.isNull(broker)) {
      final String errorMessage =
          String.format(
              "Subscription: broker bound to consumer group [%s] does not exist", consumerGroupId);
      LOGGER.warn(errorMessage);
      throw new SubscriptionException(errorMessage);
    }
    final String consumerId = consumerConfig.getConsumerId();
    return broker.commit(consumerId, commitContexts, nack);
  }

  public boolean isCommitContextOutdated(final SubscriptionCommitContext commitContext) {
    final String consumerGroupId = commitContext.getConsumerGroupId();
    final SubscriptionBroker broker = consumerGroupIdToSubscriptionBroker.get(consumerGroupId);
    if (Objects.isNull(broker)) {
      return true;
    }
    return broker.isCommitContextOutdated(commitContext);
  }

  public List<String> fetchTopicNamesToUnsubscribe(
      final ConsumerConfig consumerConfig, final Set<String> topicNames) {
    final String consumerGroupId = consumerConfig.getConsumerGroupId();
    final SubscriptionBroker broker = consumerGroupIdToSubscriptionBroker.get(consumerGroupId);
    if (Objects.isNull(broker)) {
      return Collections.emptyList();
    }
    return broker.fetchTopicNamesToUnsubscribe(topicNames);
  }

  /////////////////////////////// broker ///////////////////////////////

  public boolean isBrokerExist(final String consumerGroupId) {
    return consumerGroupIdToSubscriptionBroker.containsKey(consumerGroupId);
  }

  public void createBrokerIfNotExist(final String consumerGroupId) {
    consumerGroupIdToSubscriptionBroker.computeIfAbsent(consumerGroupId, SubscriptionBroker::new);
    LOGGER.info("Subscription: create broker bound to consumer group [{}]", consumerGroupId);
  }

  /**
   * @return {@code true} if drop broker success, {@code false} otherwise
   */
  public boolean dropBroker(final String consumerGroupId) {
    final AtomicBoolean dropped = new AtomicBoolean(false);
    consumerGroupIdToSubscriptionBroker.compute(
        consumerGroupId,
        (id, broker) -> {
          if (Objects.isNull(broker)) {
            LOGGER.warn(
                "Subscription: broker bound to consumer group [{}] does not exist",
                consumerGroupId);
            dropped.set(true);
            return null;
          }
          if (!broker.isEmpty()) {
            LOGGER.warn(
                "Subscription: broker bound to consumer group [{}] is not empty when dropping",
                consumerGroupId);
            return broker;
          }
          dropped.set(true);
          LOGGER.info("Subscription: drop broker bound to consumer group [{}]", consumerGroupId);
          return null; // remove this entry
        });
    return dropped.get();
  }

  /////////////////////////////// prefetching queue ///////////////////////////////

  public void bindPrefetchingQueue(final SubscriptionSinkSubtask subtask) {
    final String consumerGroupId = subtask.getConsumerGroupId();
    consumerGroupIdToSubscriptionBroker
        .compute(
            consumerGroupId,
            (id, broker) -> {
              if (Objects.isNull(broker)) {
                LOGGER.info(
                    "Subscription: broker bound to consumer group [{}] does not exist, create new for binding prefetching queue",
                    consumerGroupId);
                // TODO: consider more robust metadata semantics
                return new SubscriptionBroker(consumerGroupId);
              }
              return broker;
            })
        .bindPrefetchingQueue(subtask.getTopicName(), subtask.getInputPendingQueue());
    prefetchingQueueCount.invalidate();
  }

  public void updateCompletedTopicNames(final String consumerGroupId, final String topicName) {
    final SubscriptionBroker broker = consumerGroupIdToSubscriptionBroker.get(consumerGroupId);
    if (Objects.isNull(broker)) {
      LOGGER.warn(
          "Subscription: broker bound to consumer group [{}] does not exist", consumerGroupId);
      return;
    }
    broker.updateCompletedTopicNames(topicName);
  }

  public void unbindPrefetchingQueue(final String consumerGroupId, final String topicName) {
    final SubscriptionBroker broker = consumerGroupIdToSubscriptionBroker.get(consumerGroupId);
    if (Objects.isNull(broker)) {
      LOGGER.warn(
          "Subscription: broker bound to consumer group [{}] does not exist", consumerGroupId);
      return;
    }
    broker.unbindPrefetchingQueue(topicName);
    prefetchingQueueCount.invalidate();
  }

  public void removePrefetchingQueue(final String consumerGroupId, final String topicName) {
    final SubscriptionBroker broker = consumerGroupIdToSubscriptionBroker.get(consumerGroupId);
    if (Objects.isNull(broker)) {
      LOGGER.warn(
          "Subscription: broker bound to consumer group [{}] does not exist", consumerGroupId);
      return;
    }
    broker.removePrefetchingQueue(topicName);
    prefetchingQueueCount.invalidate();
  }

  public boolean executePrefetch(final String consumerGroupId, final String topicName) {
    final SubscriptionBroker broker = consumerGroupIdToSubscriptionBroker.get(consumerGroupId);
    if (Objects.isNull(broker)) {
      SubscriptionDataNodeResourceManager.log()
          .schedule(SubscriptionBrokerAgent.class, consumerGroupId, topicName)
          .ifPresent(
              l ->
                  l.warn(
                      "Subscription: broker bound to consumer group [{}] does not exist",
                      consumerGroupId));
      return false;
    }
    return broker.executePrefetch(topicName);
  }

  public int getPipeEventCount(final String consumerGroupId, final String topicName) {
    final SubscriptionBroker broker = consumerGroupIdToSubscriptionBroker.get(consumerGroupId);
    if (Objects.isNull(broker)) {
      LOGGER.warn(
          "Subscription: broker bound to consumer group [{}] does not exist", consumerGroupId);
      return 0;
    }
    return broker.getPipeEventCount(topicName);
  }

  public int getPrefetchingQueueCount() {
    return prefetchingQueueCount.get();
  }

  private int getPrefetchingQueueCountInternal() {
    return consumerGroupIdToSubscriptionBroker.values().stream()
        .map(SubscriptionBroker::getPrefetchingQueueCount)
        .reduce(0, Integer::sum);
  }

  /////////////////////////////// Cache ///////////////////////////////

  /**
   * A simple generic cache that computes and stores a value on demand.
   *
   * <p>Note that since the get() and invalidate() methods are not modified with synchronized, the
   * value obtained may not be entirely accurate.
   *
   * @param <T> the type of the cached value
   */
  private static class Cache<T> {

    private T value;
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
        value = supplier.get();
        valid = true;
      }
      return value;
    }
  }
}
