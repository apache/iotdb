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
import org.apache.iotdb.pipe.api.event.Event;

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

  public SubscriptionBroker(String brokerId) {
    this.brokerId = brokerId;
    this.topicNameToPrefetchingQueue = new ConcurrentHashMap<>();
  }

  public boolean isEmpty() {
    return topicNameToPrefetchingQueue.isEmpty();
  }

  //////////////////////////// provided for SubscriptionBrokerAgent ////////////////////////////

  public List<SerializedEnrichedEvent> poll(Set<String> topicNames) {
    List<SerializedEnrichedEvent> events = new ArrayList<>();
    topicNameToPrefetchingQueue.forEach(
        (topicName, prefetchingQueue) -> {
          if (topicNames.contains(topicName)) {
            SerializedEnrichedEvent event = prefetchingQueue.poll();
            if (Objects.nonNull(event)) {
              events.add(event);
            }
          }
        });
    return events;
  }

  public void commit(Map<String, List<String>> topicNameToSubscriptionCommitIds) {
    for (Map.Entry<String, List<String>> entry : topicNameToSubscriptionCommitIds.entrySet()) {
      String topicName = entry.getKey();
      SubscriptionPrefetchingQueue prefetchingQueue = topicNameToPrefetchingQueue.get(topicName);
      if (Objects.isNull(prefetchingQueue)) {
        LOGGER.warn(
            "Subscription: prefetching queue bound to topic [{}] does not exist", topicName);
        continue;
      }
      prefetchingQueue.commit(entry.getValue());
    }
  }

  /////////////////////////////// prefetching queue ///////////////////////////////

  public void bindPrefetchingQueue(
      String topicName, BoundedBlockingPendingQueue<Event> inputPendingQueue) {
    SubscriptionPrefetchingQueue prefetchingQueue = topicNameToPrefetchingQueue.get(topicName);
    if (Objects.nonNull(prefetchingQueue)) {
      LOGGER.warn(
          "Subscription: prefetching queue bound to topic [{}] has already existed", topicName);
      return;
    }
    topicNameToPrefetchingQueue.put(
        topicName, new SubscriptionPrefetchingQueue(brokerId, topicName, inputPendingQueue));
  }

  public void unbindPrefetchingQueue(String topicName) {
    SubscriptionPrefetchingQueue prefetchingQueue = topicNameToPrefetchingQueue.get(topicName);
    if (Objects.isNull(prefetchingQueue)) {
      LOGGER.warn("Subscription: prefetching queue bound to topic [{}] does not exist", topicName);
      return;
    }
    // TODO: do something for events on-the-fly
    topicNameToPrefetchingQueue.remove(topicName);
  }

  public void executePrefetch(String topicName) {
    SubscriptionPrefetchingQueue prefetchingQueue = topicNameToPrefetchingQueue.get(topicName);
    if (Objects.isNull(prefetchingQueue)) {
      LOGGER.warn("Subscription: prefetching queue bound to topic [{}] does not exist", topicName);
      return;
    }
    prefetchingQueue.executePrefetch();
  }

  public void clearCommittedEvents() {
    topicNameToPrefetchingQueue
        .values()
        .forEach(SubscriptionPrefetchingQueue::clearCommittedEvents);
  }
}
