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

import org.apache.iotdb.commons.pipe.task.connection.BoundedBlockingPendingQueue;
import org.apache.iotdb.db.subscription.agent.SubscriptionAgent;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.db.subscription.timer.SubscriptionPollTimer;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.rpc.subscription.payload.common.SubscriptionCommitContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
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

  public List<SubscriptionEvent> poll(
      final Set<String> topicNames, final SubscriptionPollTimer timer) {
    final List<SubscriptionEvent> events = new ArrayList<>();
    for (final Map.Entry<String, SubscriptionPrefetchingQueue> entry :
        topicNameToPrefetchingQueue.entrySet()) {
      final String topicName = entry.getKey();
      final SubscriptionPrefetchingQueue prefetchingQueue = entry.getValue();
      if (topicNames.contains(topicName)) {
        final SubscriptionEvent event = prefetchingQueue.poll(timer);
        if (Objects.nonNull(event)) {
          events.add(event);
        }
        timer.update();
        if (timer.isExpired()) {
          break;
        }
      }
    }
    return events;
  }

  public List<SubscriptionEvent> pollTsFile(
      String topicName, String fileName, long endWritingOffset) {
    SubscriptionPrefetchingQueue prefetchingQueue = topicNameToPrefetchingQueue.get(topicName);
    if (Objects.isNull(prefetchingQueue)) {
      return null;
    }
    if (!(prefetchingQueue instanceof SubscriptionPrefetchingTsFileQueue)) {
      return null;
    }
    final List<SubscriptionEvent> events = new ArrayList<>();
    final SubscriptionEvent event =
        ((SubscriptionPrefetchingTsFileQueue) prefetchingQueue)
            .pollTsFile(fileName, endWritingOffset);
    if (Objects.nonNull(event)) {
      events.add(event);
    }
    return events;
  }

  public void commit(final List<SubscriptionCommitContext> commitContexts) {
    for (final SubscriptionCommitContext commitContext : commitContexts) {
      final String topicName = commitContext.getTopicName();
      final SubscriptionPrefetchingQueue prefetchingQueue =
          topicNameToPrefetchingQueue.get(topicName);
      if (Objects.isNull(prefetchingQueue)) {
        LOGGER.warn(
            "Subscription: prefetching queue bound to topic [{}] does not exist", topicName);
        continue;
      }
      prefetchingQueue.commit(commitContext);
    }
  }

  /////////////////////////////// prefetching queue ///////////////////////////////

  public void bindPrefetchingQueue(
      final String topicName, final BoundedBlockingPendingQueue<Event> inputPendingQueue) {
    final SubscriptionPrefetchingQueue prefetchingQueue =
        topicNameToPrefetchingQueue.get(topicName);
    if (Objects.nonNull(prefetchingQueue)) {
      LOGGER.warn(
          "Subscription: prefetching queue bound to topic [{}] has already existed", topicName);
      return;
    }
    final String topicFormat = SubscriptionAgent.topic().getTopicFormat(topicName);
    if (TopicConstant.FORMAT_TS_FILE_READER_VALUE.equals(topicFormat)) {
      topicNameToPrefetchingQueue.put(
          topicName,
          new SubscriptionPrefetchingTsFileQueue(brokerId, topicName, inputPendingQueue));
    } else {
      topicNameToPrefetchingQueue.put(
          topicName,
          new SubscriptionPrefetchingTabletsQueue(brokerId, topicName, inputPendingQueue));
    }
  }

  public void unbindPrefetchingQueue(final String topicName) {
    final SubscriptionPrefetchingQueue prefetchingQueue =
        topicNameToPrefetchingQueue.get(topicName);
    if (Objects.isNull(prefetchingQueue)) {
      LOGGER.warn("Subscription: prefetching queue bound to topic [{}] does not exist", topicName);
      return;
    }
    // TODO: do something for events on-the-fly
    topicNameToPrefetchingQueue.remove(topicName);
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
