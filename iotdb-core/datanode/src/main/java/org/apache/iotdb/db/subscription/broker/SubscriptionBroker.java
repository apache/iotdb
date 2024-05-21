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
import org.apache.iotdb.db.subscription.agent.SubscriptionAgent;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.db.subscription.metric.SubscriptionPrefetchingQueueMetrics;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class SubscriptionBroker {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionBroker.class);

  private final String brokerId; // consumer group id

  private final Map<String, SubscriptionPrefetchingQueue> topicNameToPrefetchingQueue;

  public SubscriptionBroker(final String brokerId) {
    this.brokerId = brokerId;
    this.topicNameToPrefetchingQueue = new ConcurrentHashMap<>();
  }

  public boolean isEmpty() {
    return topicNameToPrefetchingQueue.isEmpty();
  }

  //////////////////////////// provided for SubscriptionBrokerAgent ////////////////////////////

  public List<SubscriptionEvent> poll(final String consumerId, final Set<String> topicNames) {
    final List<SubscriptionEvent> events = new ArrayList<>();
    for (final Map.Entry<String, SubscriptionPrefetchingQueue> entry :
        topicNameToPrefetchingQueue.entrySet()) {
      final String topicName = entry.getKey();
      final SubscriptionPrefetchingQueue prefetchingQueue = entry.getValue();
      if (topicNames.contains(topicName)) {
        final SubscriptionEvent event = prefetchingQueue.poll(consumerId);
        if (Objects.nonNull(event)) {
          events.add(event);
        }
      }
    }
    return events;
  }

  public List<SubscriptionEvent> pollTsFile(
      final String consumerId,
      final String topicName,
      final String fileName,
      final long writingOffset) {
    final SubscriptionPrefetchingQueue prefetchingQueue =
        topicNameToPrefetchingQueue.get(topicName);
    if (Objects.isNull(prefetchingQueue)) {
      final String errorMessage =
          String.format(
              "Subscription: prefetching queue bound to topic [%s] does not exist", topicName);
      LOGGER.warn(errorMessage);
      throw new SubscriptionException(errorMessage);
    }
    if (!(prefetchingQueue instanceof SubscriptionPrefetchingTsFileQueue)) {
      final String errorMessage =
          String.format(
              "Subscription: prefetching queue bound to topic [%s] is invalid", topicName);
      LOGGER.warn(errorMessage);
      throw new SubscriptionException(errorMessage);
    }
    final SubscriptionEvent event =
        ((SubscriptionPrefetchingTsFileQueue) prefetchingQueue)
            .pollTsFile(consumerId, fileName, writingOffset);
    // Only one SubscriptionEvent polled currently...
    return Collections.singletonList(event);
  }

  /**
   * @return list of successful commit contexts
   */
  public List<SubscriptionCommitContext> commit(
      final List<SubscriptionCommitContext> commitContexts, final boolean nack) {
    final List<SubscriptionCommitContext> successfulCommitContexts = new ArrayList<>();
    for (final SubscriptionCommitContext commitContext : commitContexts) {
      final String topicName = commitContext.getTopicName();
      final SubscriptionPrefetchingQueue prefetchingQueue =
          topicNameToPrefetchingQueue.get(topicName);
      if (Objects.isNull(prefetchingQueue)) {
        LOGGER.warn(
            "Subscription: prefetching queue bound to topic [{}] does not exist", topicName);
        continue;
      }
      if (!nack) {
        if (prefetchingQueue.ack(commitContext)) {
          successfulCommitContexts.add(commitContext);
        }
      } else {
        if (prefetchingQueue.nack(commitContext)) {
          successfulCommitContexts.add(commitContext);
        }
      }
    }
    return successfulCommitContexts;
  }

  /////////////////////////////// prefetching queue ///////////////////////////////

  public void bindPrefetchingQueue(
      final String topicName, final UnboundedBlockingPendingQueue<Event> inputPendingQueue) {
    final SubscriptionPrefetchingQueue prefetchingQueue =
        topicNameToPrefetchingQueue.get(topicName);
    if (Objects.nonNull(prefetchingQueue)) {
      LOGGER.warn(
          "Subscription: prefetching queue bound to topic [{}] has already existed", topicName);
      return;
    }
    final String topicFormat = SubscriptionAgent.topic().getTopicFormat(topicName);
    if (TopicConstant.FORMAT_TS_FILE_HANDLER_VALUE.equals(topicFormat)) {
      final SubscriptionPrefetchingQueue queue =
          new SubscriptionPrefetchingTsFileQueue(brokerId, topicName, inputPendingQueue);
      SubscriptionPrefetchingQueueMetrics.getInstance().register(queue);
      topicNameToPrefetchingQueue.put(topicName, queue);
    } else {
      final SubscriptionPrefetchingQueue queue =
          new SubscriptionPrefetchingTabletsQueue(brokerId, topicName, inputPendingQueue);
      SubscriptionPrefetchingQueueMetrics.getInstance().register(queue);
      topicNameToPrefetchingQueue.put(topicName, queue);
    }
  }

  public void unbindPrefetchingQueue(final String topicName) {
    final SubscriptionPrefetchingQueue prefetchingQueue =
        topicNameToPrefetchingQueue.get(topicName);
    if (Objects.isNull(prefetchingQueue)) {
      LOGGER.warn("Subscription: prefetching queue bound to topic [{}] does not exist", topicName);
      return;
    }
    // clean up uncommitted events
    prefetchingQueue.cleanup();
    topicNameToPrefetchingQueue.remove(topicName);
    SubscriptionPrefetchingQueueMetrics.getInstance()
        .deregister(prefetchingQueue.getPrefetchingQueueId());
  }

  public void executePrefetch(final String topicName) {
    final SubscriptionPrefetchingQueue prefetchingQueue =
        topicNameToPrefetchingQueue.get(topicName);
    if (Objects.isNull(prefetchingQueue)) {
      LOGGER.warn("Subscription: prefetching queue bound to topic [{}] does not exist", topicName);
      return;
    }
    prefetchingQueue.executePrefetch();
  }
}
