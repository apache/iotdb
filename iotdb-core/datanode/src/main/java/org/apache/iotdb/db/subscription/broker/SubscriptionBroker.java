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

import org.apache.iotdb.commons.pipe.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.subscription.agent.SubscriptionAgent;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.db.subscription.event.pipe.SubscriptionPipeEmptyEvent;
import org.apache.iotdb.db.subscription.metric.SubscriptionPrefetchingQueueMetrics;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponse;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponseType;
import org.apache.iotdb.rpc.subscription.payload.poll.TerminationPayload;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext.INVALID_COMMIT_ID;

public class SubscriptionBroker {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionBroker.class);

  private final String brokerId; // consumer group id

  private final Map<String, SubscriptionPrefetchingQueue> topicNameToPrefetchingQueue;
  private final Map<String, String> completedTopicNames;

  // The subscription pipe that was restarted should reuse the previous commit ID.
  private final Map<String, AtomicLong> topicNameToCommitIdGenerator;

  private final LoadingCache<String, SubscriptionStates> consumerIdToSubscriptionStates;

  public SubscriptionBroker(final String brokerId) {
    this.brokerId = brokerId;
    this.topicNameToPrefetchingQueue = new ConcurrentHashMap<>();
    this.completedTopicNames = new ConcurrentHashMap<>();
    this.topicNameToCommitIdGenerator = new ConcurrentHashMap<>();

    this.consumerIdToSubscriptionStates =
        Caffeine.newBuilder()
            // TODO: config
            .expireAfterAccess(60L, TimeUnit.SECONDS)
            .build(consumerId -> new SubscriptionStates());
  }

  public boolean isEmpty() {
    return topicNameToPrefetchingQueue.isEmpty()
        && completedTopicNames.isEmpty()
        && topicNameToCommitIdGenerator.isEmpty();
  }

  //////////////////////////// provided for SubscriptionBrokerAgent ////////////////////////////

  public List<SubscriptionEvent> poll(
      final String consumerId, final Set<String> topicNames, final long maxBytes) {
    final List<SubscriptionEvent> events = new ArrayList<>();
    for (final String topicName : topicNames) {
      final SubscriptionPrefetchingQueue prefetchingQueue =
          topicNameToPrefetchingQueue.get(topicName);
      if (Objects.isNull(prefetchingQueue)) {
        // check if completed
        if (completedTopicNames.containsKey(topicName)) {
          LOGGER.info(
              "Subscription: prefetching queue bound to topic [{}] for consumer group [{}] is completed, return termination response to client",
              topicName,
              brokerId);
          events.add(
              new SubscriptionEvent(
                  new SubscriptionPipeEmptyEvent(),
                  new SubscriptionPollResponse(
                      SubscriptionPollResponseType.TERMINATION.getType(),
                      new TerminationPayload(),
                      new SubscriptionCommitContext(
                          IoTDBDescriptor.getInstance().getConfig().getDataNodeId(),
                          PipeDataNodeAgent.runtime().getRebootTimes(),
                          topicName,
                          brokerId,
                          INVALID_COMMIT_ID))));
          continue;
        }
        // There are two reasons for not printing logs here:
        // 1. There will be a delay in the creation of the prefetching queue after subscription.
        // 2. There is no corresponding prefetching queue on this DN (currently the consumer is
        // fully connected to all DNs).
        continue;
      }
      if (prefetchingQueue.isClosed()) {
        LOGGER.warn(
            "Subscription: prefetching queue bound to topic [{}] for consumer group [{}] is closed",
            topicName,
            brokerId);
        continue;
      }
      final SubscriptionEvent event = prefetchingQueue.poll(consumerId);
      if (Objects.nonNull(event)) {
        events.add(event);
      }
    }

    final Pair<List<SubscriptionEvent>, List<SubscriptionEvent>> eventsToPollWithEventsToNack =
        Objects.requireNonNull(consumerIdToSubscriptionStates.get(consumerId))
            .filter(events, maxBytes);
    commit(
        consumerId,
        eventsToPollWithEventsToNack.right.stream()
            .map(SubscriptionEvent::getCommitContext)
            .collect(Collectors.toList()),
        true);
    return eventsToPollWithEventsToNack.left;
  }

  public List<SubscriptionEvent> pollTsFile(
      final String consumerId,
      final SubscriptionCommitContext commitContext,
      final long writingOffset) {
    final String topicName = commitContext.getTopicName();
    final SubscriptionPrefetchingQueue prefetchingQueue =
        topicNameToPrefetchingQueue.get(topicName);
    if (Objects.isNull(prefetchingQueue)) {
      final String errorMessage =
          String.format(
              "Subscription: prefetching queue bound to topic [%s] for consumer group [%s] does not exist",
              topicName, brokerId);
      LOGGER.warn(errorMessage);
      throw new SubscriptionException(errorMessage);
    }
    if (!(prefetchingQueue instanceof SubscriptionPrefetchingTsFileQueue)) {
      final String errorMessage =
          String.format(
              "Subscription: prefetching queue bound to topic [%s] for consumer group [%s] is invalid",
              topicName, brokerId);
      LOGGER.warn(errorMessage);
      throw new SubscriptionException(errorMessage);
    }
    if (prefetchingQueue.isClosed()) {
      LOGGER.warn(
          "Subscription: prefetching queue bound to topic [{}] for consumer group [{}] is closed",
          topicName,
          brokerId);
      return Collections.emptyList();
    }
    final SubscriptionEvent event =
        ((SubscriptionPrefetchingTsFileQueue) prefetchingQueue)
            .pollTsFile(consumerId, commitContext, writingOffset);
    if (Objects.nonNull(event)) {
      // only one SubscriptionEvent polled currently
      return Collections.singletonList(event);
    }
    return Collections.emptyList();
  }

  public List<SubscriptionEvent> pollTablets(
      final String consumerId, final SubscriptionCommitContext commitContext, final int offset) {
    final String topicName = commitContext.getTopicName();
    final SubscriptionPrefetchingQueue prefetchingQueue =
        topicNameToPrefetchingQueue.get(topicName);
    if (Objects.isNull(prefetchingQueue)) {
      final String errorMessage =
          String.format(
              "Subscription: prefetching queue bound to topic [%s] for consumer group [%s] does not exist",
              topicName, brokerId);
      LOGGER.warn(errorMessage);
      throw new SubscriptionException(errorMessage);
    }
    if (!(prefetchingQueue instanceof SubscriptionPrefetchingTabletQueue)) {
      final String errorMessage =
          String.format(
              "Subscription: prefetching queue bound to topic [%s] for consumer group [%s] is invalid",
              topicName, brokerId);
      LOGGER.warn(errorMessage);
      throw new SubscriptionException(errorMessage);
    }
    if (prefetchingQueue.isClosed()) {
      LOGGER.warn(
          "Subscription: prefetching queue bound to topic [{}] for consumer group [{}] is closed",
          topicName,
          brokerId);
      return Collections.emptyList();
    }
    final SubscriptionEvent event =
        ((SubscriptionPrefetchingTabletQueue) prefetchingQueue)
            .pollTablets(consumerId, commitContext, offset);
    if (Objects.nonNull(event)) {
      // only one SubscriptionEvent polled currently
      return Collections.singletonList(event);
    }
    return Collections.emptyList();
  }

  /**
   * @return list of successful commit contexts
   */
  public List<SubscriptionCommitContext> commit(
      final String consumerId,
      final List<SubscriptionCommitContext> commitContexts,
      final boolean nack) {
    final List<SubscriptionCommitContext> successfulCommitContexts = new ArrayList<>();
    for (final SubscriptionCommitContext commitContext : commitContexts) {
      final String topicName = commitContext.getTopicName();
      final SubscriptionPrefetchingQueue prefetchingQueue =
          topicNameToPrefetchingQueue.get(topicName);
      if (Objects.isNull(prefetchingQueue)) {
        LOGGER.warn(
            "Subscription: prefetching queue bound to topic [{}] for consumer group [{}] does not exist",
            topicName,
            brokerId);
        continue;
      }
      if (prefetchingQueue.isClosed()) {
        LOGGER.warn(
            "Subscription: prefetching queue bound to topic [{}] for consumer group [{}] is closed",
            topicName,
            brokerId);
        continue;
      }
      if (!nack) {
        if (prefetchingQueue.ack(consumerId, commitContext)) {
          successfulCommitContexts.add(commitContext);
        }
      } else {
        if (prefetchingQueue.nack(consumerId, commitContext)) {
          successfulCommitContexts.add(commitContext);
        }
      }
    }
    return successfulCommitContexts;
  }

  /////////////////////////////// prefetching queue ///////////////////////////////

  public void bindPrefetchingQueue(
      final String topicName, final UnboundedBlockingPendingQueue<Event> inputPendingQueue) {
    if (Objects.nonNull(topicNameToPrefetchingQueue.get(topicName))) {
      LOGGER.warn(
          "Subscription: prefetching queue bound to topic [{}] for consumer group [{}] has already existed",
          topicName,
          brokerId);
      return;
    }
    final String topicFormat = SubscriptionAgent.topic().getTopicFormat(topicName);
    final SubscriptionPrefetchingQueue prefetchingQueue;
    if (TopicConstant.FORMAT_TS_FILE_HANDLER_VALUE.equals(topicFormat)) {
      prefetchingQueue =
          new SubscriptionPrefetchingTsFileQueue(
              brokerId,
              topicName,
              new TsFileDeduplicationBlockingPendingQueue(inputPendingQueue),
              topicNameToCommitIdGenerator.computeIfAbsent(topicName, (key) -> new AtomicLong()));
    } else {
      prefetchingQueue =
          new SubscriptionPrefetchingTabletQueue(
              brokerId,
              topicName,
              new TsFileDeduplicationBlockingPendingQueue(inputPendingQueue),
              topicNameToCommitIdGenerator.computeIfAbsent(topicName, (key) -> new AtomicLong()));
    }
    SubscriptionPrefetchingQueueMetrics.getInstance().register(prefetchingQueue);
    topicNameToPrefetchingQueue.put(topicName, prefetchingQueue);
    LOGGER.info(
        "Subscription: create prefetching queue bound to topic [{}] for consumer group [{}]",
        topicName,
        brokerId);
  }

  public void unbindPrefetchingQueue(final String topicName) {
    final SubscriptionPrefetchingQueue prefetchingQueue =
        topicNameToPrefetchingQueue.get(topicName);
    if (Objects.isNull(prefetchingQueue)) {
      LOGGER.warn(
          "Subscription: prefetching queue bound to topic [{}] for consumer group [{}] does not exist",
          topicName,
          brokerId);
      return;
    }

    // mark prefetching queue closed first
    prefetchingQueue.markClosed();

    // mark topic name completed only for topic of snapshot mode
    if (SubscriptionAgent.topic().getTopicMode(topicName).equals(TopicConstant.MODE_SNAPSHOT_VALUE)
        && prefetchingQueue.isCompleted()) {
      completedTopicNames.put(topicName, topicName);
    }

    // clean up events in prefetching queue
    prefetchingQueue.cleanUp();

    // deregister metrics
    SubscriptionPrefetchingQueueMetrics.getInstance()
        .deregister(prefetchingQueue.getPrefetchingQueueId());

    // remove prefetching queue
    topicNameToPrefetchingQueue.remove(topicName);
    LOGGER.info(
        "Subscription: drop prefetching queue bound to topic [{}] for consumer group [{}]",
        topicName,
        brokerId);
  }

  public void removePrefetchingQueue(final String topicName) {
    final SubscriptionPrefetchingQueue prefetchingQueue =
        topicNameToPrefetchingQueue.get(topicName);
    if (Objects.nonNull(prefetchingQueue)) {
      LOGGER.warn(
          "Subscription: prefetching queue bound to topic [{}] for consumer group [{}] still exists",
          topicName,
          brokerId);
      return;
    }

    completedTopicNames.remove(topicName);
    topicNameToCommitIdGenerator.remove(topicName);
  }

  public void executePrefetch(final String topicName) {
    final SubscriptionPrefetchingQueue prefetchingQueue =
        topicNameToPrefetchingQueue.get(topicName);
    if (Objects.isNull(prefetchingQueue)) {
      LOGGER.warn(
          "Subscription: prefetching queue bound to topic [{}] for consumer group [{}] does not exist",
          topicName,
          brokerId);
      return;
    }
    if (prefetchingQueue.isClosed()) {
      LOGGER.warn(
          "Subscription: prefetching queue bound to topic [{}] for consumer group [{}] is closed",
          topicName,
          brokerId);
      return;
    }
    prefetchingQueue.executePrefetch();
  }
}
