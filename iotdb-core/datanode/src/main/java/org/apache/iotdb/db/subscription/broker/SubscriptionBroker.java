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

import org.apache.iotdb.commons.pipe.agent.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.subscription.agent.SubscriptionAgent;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.db.subscription.metric.SubscriptionPrefetchingQueueMetrics;
import org.apache.iotdb.db.subscription.resource.SubscriptionDataNodeResourceManager;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponseType;
import org.apache.iotdb.rpc.subscription.payload.poll.TerminationPayload;
import org.apache.iotdb.session.subscription.util.PollTimer;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
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
    final List<SubscriptionEvent> eventsToPoll = new ArrayList<>();
    final Set<String> candidateTopicNames = prepareCandidateTopicNames(topicNames, eventsToPoll);

    // Sort topic names based on the current subscription states (number of events received per
    // topic)
    final List<String> sortedTopicNames = new ArrayList<>(candidateTopicNames);
    sortedTopicNames.sort(
        Comparator.comparingLong(
            topicName ->
                Objects.requireNonNull(consumerIdToSubscriptionStates.get(consumerId))
                    .getStates(topicName)));

    final List<SubscriptionEvent> eventsToNack = new ArrayList<>();
    long totalSize = 0;
    final Map<String, Long> topicNameToIncrements = new HashMap<>();

    // Iterate over each sorted topic name and poll the corresponding events
    int remainingTopicSize = sortedTopicNames.size();
    for (final String topicName : sortedTopicNames) {
      final SubscriptionPrefetchingQueue prefetchingQueue =
          topicNameToPrefetchingQueue.get(topicName);
      remainingTopicSize -= 1;

      // Recheck
      if (Objects.isNull(prefetchingQueue) || prefetchingQueue.isClosed()) {
        continue;
      }

      // Poll the event from the prefetching queue
      final SubscriptionEvent event;
      if (prefetchingQueue instanceof SubscriptionPrefetchingTsFileQueue) {
        // TODO: current poll timeout is uniform for all candidate topics
        final PollTimer timer =
            new PollTimer(
                System.currentTimeMillis(),
                SubscriptionAgent.receiver().remainingMs() / Math.max(1, remainingTopicSize));
        event = prefetchingQueue.pollV2(consumerId, timer);
      } else {
        // TODO: migrate poll to pollV2
        event = prefetchingQueue.poll(consumerId);
      }
      if (Objects.isNull(event)) {
        continue;
      }

      // Try to get the current size of the event
      final long currentSize;
      try {
        currentSize = event.getCurrentResponseSize();
      } catch (final IOException e) {
        // If there is an error getting the event's size, nack the event
        eventsToNack.add(event);
        continue;
      }

      // Add the event to the poll list
      eventsToPoll.add(event);

      // Increment the event count for the topic
      topicNameToIncrements.merge(event.getCommitContext().getTopicName(), 1L, Long::sum);

      // Update the total size
      totalSize += currentSize;

      // If adding this event exceeds the maxBytes (pessimistic estimation), break the loop
      if (totalSize + currentSize > maxBytes) {
        break;
      }
    }

    // Update the subscription states with the increments for the topics processed
    Objects.requireNonNull(consumerIdToSubscriptionStates.get(consumerId))
        .updateStates(topicNameToIncrements);

    // Commit the nack events for the consumer
    commit(
        consumerId,
        eventsToNack.stream().map(SubscriptionEvent::getCommitContext).collect(Collectors.toList()),
        true);

    // Return the list of events that are to be polled
    return eventsToPoll;
  }

  private Set<String> prepareCandidateTopicNames(
      final Set<String> topicNames,
      final List<SubscriptionEvent> eventsToPoll /* output parameter */) {
    final Set<String> candidateTopicNames = new HashSet<>();
    for (final String topicName : topicNames) {
      final SubscriptionPrefetchingQueue prefetchingQueue =
          topicNameToPrefetchingQueue.get(topicName);
      // If there is no prefetching queue for the topic, check if it's completed
      if (Objects.isNull(prefetchingQueue)) {
        if (completedTopicNames.containsKey(topicName)) {
          LOGGER.info(
              "Subscription: prefetching queue bound to topic [{}] for consumer group [{}] is completed, return termination response to client",
              topicName,
              brokerId);
          // Add a termination event for the completed topic
          eventsToPoll.add(
              new SubscriptionEvent(
                  SubscriptionPollResponseType.TERMINATION.getType(),
                  new TerminationPayload(),
                  new SubscriptionCommitContext(
                      IoTDBDescriptor.getInstance().getConfig().getDataNodeId(),
                      PipeDataNodeAgent.runtime().getRebootTimes(),
                      topicName,
                      brokerId,
                      INVALID_COMMIT_ID)));
          continue;
        }
        // There are two reasons for not printing logs here:
        // 1. There will be a delay in the creation of the prefetching queue after subscription.
        // 2. There is no corresponding prefetching queue on this DN:
        //   2.1. the consumer is fully connected to all DNs currently...
        //   2.2. potential disorder of unbind and close prefetching queue...
        continue;
      }

      // Check if the prefetching queue is closed
      if (prefetchingQueue.isClosed()) {
        SubscriptionDataNodeResourceManager.log()
            .schedule(SubscriptionBroker.class, brokerId, topicName)
            .ifPresent(
                l ->
                    l.warn(
                        "Subscription: prefetching queue bound to topic [{}] for consumer group [{}] is closed",
                        topicName,
                        brokerId));
        continue;
      }

      candidateTopicNames.add(topicName);
    }

    return candidateTopicNames;
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

  public boolean isCommitContextOutdated(final SubscriptionCommitContext commitContext) {
    final String topicName = commitContext.getTopicName();
    final SubscriptionPrefetchingQueue prefetchingQueue =
        topicNameToPrefetchingQueue.get(topicName);
    if (Objects.isNull(prefetchingQueue)) {
      return true;
    }
    return prefetchingQueue.isCommitContextOutdated(commitContext);
  }

  public List<String> fetchTopicNamesToUnsubscribe(final Set<String> topicNames) {
    final List<String> topicNamesToUnsubscribe = new ArrayList<>();

    for (final String topicName : topicNames) {
      final SubscriptionPrefetchingQueue prefetchingQueue =
          topicNameToPrefetchingQueue.get(topicName);
      // If there is no prefetching queue for the topic, check if it's completed
      if (Objects.isNull(prefetchingQueue) && completedTopicNames.containsKey(topicName)) {
        LOGGER.info(
            "Subscription: prefetching queue bound to topic [{}] for consumer group [{}] is completed, reply to client heartbeat request",
            topicName,
            brokerId);
        topicNamesToUnsubscribe.add(topicName);
      }
    }

    return topicNamesToUnsubscribe;
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

  public void updateCompletedTopicNames(final String topicName) {
    // mark topic name completed only for topic of snapshot mode
    if (SubscriptionAgent.topic()
        .getTopicMode(topicName)
        .equals(TopicConstant.MODE_SNAPSHOT_VALUE)) {
      completedTopicNames.put(topicName, topicName);
    }
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
      LOGGER.info(
          "Subscription: prefetching queue bound to topic [{}] for consumer group [{}] still exists, unbind it before closing",
          topicName,
          brokerId);
      // TODO: consider more robust metadata semantics
      unbindPrefetchingQueue(topicName);
    }

    completedTopicNames.remove(topicName);
    topicNameToCommitIdGenerator.remove(topicName);
  }

  public boolean executePrefetch(final String topicName) {
    final SubscriptionPrefetchingQueue prefetchingQueue =
        topicNameToPrefetchingQueue.get(topicName);
    if (Objects.isNull(prefetchingQueue)) {
      SubscriptionDataNodeResourceManager.log()
          .schedule(SubscriptionBroker.class, brokerId, topicName)
          .ifPresent(
              l ->
                  l.warn(
                      "Subscription: prefetching queue bound to topic [{}] for consumer group [{}] does not exist",
                      topicName,
                      brokerId));
      return false;
    }
    if (prefetchingQueue.isClosed()) {
      SubscriptionDataNodeResourceManager.log()
          .schedule(SubscriptionBroker.class, brokerId, topicName)
          .ifPresent(
              l ->
                  l.warn(
                      "Subscription: prefetching queue bound to topic [{}] for consumer group [{}] is closed",
                      topicName,
                      brokerId));
      return false;
    }

    // TODO: migrate executePrefetch to executePrefetchV2
    return prefetchingQueue instanceof SubscriptionPrefetchingTabletQueue
        ? prefetchingQueue.executePrefetch()
        : prefetchingQueue.executePrefetchV2();
  }

  public int getPipeEventCount(final String topicName) {
    final SubscriptionPrefetchingQueue prefetchingQueue =
        topicNameToPrefetchingQueue.get(topicName);
    if (Objects.isNull(prefetchingQueue)) {
      LOGGER.warn(
          "Subscription: prefetching queue bound to topic [{}] for consumer group [{}] does not exist",
          topicName,
          brokerId);
      return 0;
    }
    if (prefetchingQueue.isClosed()) {
      LOGGER.warn(
          "Subscription: prefetching queue bound to topic [{}] for consumer group [{}] is closed",
          topicName,
          brokerId);
      return 0;
    }
    return prefetchingQueue.getPipeEventCount();
  }

  public int getPrefetchingQueueCount() {
    return topicNameToPrefetchingQueue.size();
  }
}
