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

import org.apache.iotdb.consensus.iot.IoTConsensusServerImpl;
import org.apache.iotdb.db.subscription.broker.consensus.ConsensusLogToTabletConverter;
import org.apache.iotdb.db.subscription.broker.consensus.ConsensusPrefetchingQueue;
import org.apache.iotdb.db.subscription.broker.consensus.ConsensusSubscriptionCommitManager;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
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

  /** Shared commit ID generators per topic. */
  private final Map<String, AtomicLong> topicNameToCommitIdGenerator;

  public ConsensusSubscriptionBroker(final String brokerId) {
    this.brokerId = brokerId;
    this.topicNameToConsensusPrefetchingQueues = new ConcurrentHashMap<>();
    this.topicNameToCommitIdGenerator = new ConcurrentHashMap<>();
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

  //////////////////////////// poll ////////////////////////////

  @Override
  public List<SubscriptionEvent> poll(
      final String consumerId, final Set<String> topicNames, final long maxBytes) {
    LOGGER.debug(
        "ConsensusSubscriptionBroker [{}]: poll called, consumerId={}, topicNames={}, "
            + "queueCount={}, maxBytes={}",
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

      // Poll from all region queues for this topic
      for (final ConsensusPrefetchingQueue consensusQueue : queues) {
        if (consensusQueue.isClosed()) {
          continue;
        }

        final SubscriptionEvent event = consensusQueue.poll(consumerId);
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
        "ConsensusSubscriptionBroker [{}]: poll result, consumerId={}, eventsPolled={}, eventsNacked={}",
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

    // Try each region queue until one returns a match
    for (final ConsensusPrefetchingQueue consensusQueue : queues) {
      if (consensusQueue.isClosed()) {
        continue;
      }
      final SubscriptionEvent event = consensusQueue.pollTablets(consumerId, commitContext, offset);
      if (Objects.nonNull(event)) {
        return Collections.singletonList(event);
      }
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
            "ConsensusSubscriptionBroker [{}]: no queues for topic [{}] to commit",
            brokerId,
            topicName);
        continue;
      }

      // Try each region queue for this topic (the event belongs to exactly one region).
      // Don't warn per-queue miss — only warn if NO queue handled the commit.
      boolean handled = false;
      for (final ConsensusPrefetchingQueue consensusQueue : queues) {
        if (consensusQueue.isClosed()) {
          continue;
        }
        final boolean success;
        if (!nack) {
          success = consensusQueue.ackSilent(consumerId, commitContext);
        } else {
          success = consensusQueue.nackSilent(consumerId, commitContext);
        }
        if (success) {
          successfulCommitContexts.add(commitContext);
          handled = true;
          break; // committed in the right queue, no need to try others
        }
      }
      if (!handled) {
        LOGGER.warn(
            "ConsensusSubscriptionBroker [{}]: commit context {} not found in any of {} region queue(s) for topic [{}]",
            brokerId,
            commitContext,
            queues.size(),
            topicName);
      }
    }
    return successfulCommitContexts;
  }

  @Override
  public boolean isCommitContextOutdated(final SubscriptionCommitContext commitContext) {
    final String topicName = commitContext.getTopicName();
    final List<ConsensusPrefetchingQueue> queues =
        topicNameToConsensusPrefetchingQueues.get(topicName);
    if (Objects.isNull(queues) || queues.isEmpty()) {
      return true;
    }
    // Any queue that considers it NOT outdated means it's not outdated
    for (final ConsensusPrefetchingQueue q : queues) {
      if (!q.isCommitContextOutdated(commitContext)) {
        return false;
      }
    }
    return true;
  }

  //////////////////////////// prefetching ////////////////////////////

  @Override
  public boolean executePrefetch(final String topicName) {
    final List<ConsensusPrefetchingQueue> queues =
        topicNameToConsensusPrefetchingQueues.get(topicName);
    if (Objects.isNull(queues) || queues.isEmpty()) {
      return false;
    }
    boolean anyPrefetched = false;
    for (final ConsensusPrefetchingQueue q : queues) {
      if (!q.isClosed() && q.executePrefetch()) {
        anyPrefetched = true;
      }
    }
    return anyPrefetched;
  }

  @Override
  public int getEventCount(final String topicName) {
    final List<ConsensusPrefetchingQueue> queues =
        topicNameToConsensusPrefetchingQueues.get(topicName);
    if (Objects.isNull(queues)) {
      return 0;
    }
    return queues.stream().mapToInt(ConsensusPrefetchingQueue::getPrefetchedEventCount).sum();
  }

  @Override
  public int getQueueCount() {
    return topicNameToConsensusPrefetchingQueues.size();
  }

  //////////////////////////// queue management ////////////////////////////

  public void bindConsensusPrefetchingQueue(
      final String topicName,
      final String consensusGroupId,
      final IoTConsensusServerImpl serverImpl,
      final ConsensusLogToTabletConverter converter,
      final ConsensusSubscriptionCommitManager commitManager,
      final long startSearchIndex) {
    // Get or create the list of queues for this topic
    final List<ConsensusPrefetchingQueue> queues =
        topicNameToConsensusPrefetchingQueues.computeIfAbsent(
            topicName, k -> new CopyOnWriteArrayList<>());

    // Check for duplicate region binding
    for (final ConsensusPrefetchingQueue existing : queues) {
      if (consensusGroupId.equals(existing.getConsensusGroupId()) && !existing.isClosed()) {
        LOGGER.info(
            "Subscription: consensus prefetching queue for topic [{}], region [{}] "
                + "in consumer group [{}] already exists, skipping",
            topicName,
            consensusGroupId,
            brokerId);
        return;
      }
    }

    // Get or create the shared commit ID generator for this topic
    final AtomicLong sharedCommitIdGenerator =
        topicNameToCommitIdGenerator.computeIfAbsent(topicName, k -> new AtomicLong(0));

    final ConsensusPrefetchingQueue consensusQueue =
        new ConsensusPrefetchingQueue(
            brokerId,
            topicName,
            consensusGroupId,
            serverImpl,
            converter,
            commitManager,
            startSearchIndex,
            sharedCommitIdGenerator);
    queues.add(consensusQueue);
    LOGGER.info(
        "Subscription: create consensus prefetching queue bound to topic [{}] for consumer group [{}], "
            + "consensusGroupId={}, startSearchIndex={}, totalRegionQueues={}",
        topicName,
        brokerId,
        consensusGroupId,
        startSearchIndex,
        queues.size());
  }

  public void unbindConsensusPrefetchingQueue(final String topicName) {
    final List<ConsensusPrefetchingQueue> queues =
        topicNameToConsensusPrefetchingQueues.get(topicName);
    if (Objects.isNull(queues) || queues.isEmpty()) {
      LOGGER.warn(
          "Subscription: consensus prefetching queues bound to topic [{}] for consumer group [{}] do not exist",
          topicName,
          brokerId);
      return;
    }

    for (final ConsensusPrefetchingQueue q : queues) {
      q.close();
    }
    topicNameToConsensusPrefetchingQueues.remove(topicName);
    topicNameToCommitIdGenerator.remove(topicName);
    LOGGER.info(
        "Subscription: drop all {} consensus prefetching queue(s) bound to topic [{}] for consumer group [{}]",
        queues.size(),
        topicName,
        brokerId);
  }

  public int unbindByRegion(final String regionId) {
    int closedCount = 0;
    for (final Map.Entry<String, List<ConsensusPrefetchingQueue>> entry :
        topicNameToConsensusPrefetchingQueues.entrySet()) {
      final List<ConsensusPrefetchingQueue> queues = entry.getValue();
      final Iterator<ConsensusPrefetchingQueue> iterator = queues.iterator();
      while (iterator.hasNext()) {
        final ConsensusPrefetchingQueue q = iterator.next();
        if (regionId.equals(q.getConsensusGroupId())) {
          q.close();
          iterator.remove();
          closedCount++;
          LOGGER.info(
              "Subscription: closed consensus prefetching queue for topic [{}] region [{}] "
                  + "in consumer group [{}] due to region removal",
              entry.getKey(),
              regionId,
              brokerId);
        }
      }
    }
    return closedCount;
  }

  @Override
  public void removeQueue(final String topicName) {
    final List<ConsensusPrefetchingQueue> queues =
        topicNameToConsensusPrefetchingQueues.get(topicName);
    if (Objects.nonNull(queues) && !queues.isEmpty()) {
      LOGGER.info(
          "Subscription: consensus prefetching queue(s) bound to topic [{}] for consumer group [{}] still exist, unbind before closing",
          topicName,
          brokerId);
      unbindConsensusPrefetchingQueue(topicName);
    }
  }
}
