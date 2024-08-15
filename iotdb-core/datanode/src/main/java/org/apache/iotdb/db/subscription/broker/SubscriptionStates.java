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

import org.apache.iotdb.db.subscription.event.SubscriptionEvent;

import org.apache.tsfile.utils.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages the subscription states for a single consumer, tracking the number of events subscribed
 * to for each topic.
 */
public class SubscriptionStates {

  /** A map to store the number of events this consumer has received per topic. */
  private final Map<String, Long> topicNameToEventCount = new ConcurrentHashMap<>();

  /**
   * Filters the given list of SubscriptionEvents into two categories: events to poll and events to
   * nack. Events to poll are selected based on the available size (maxBytes) and the number of
   * events already received per topic, with preference given to topics with fewer events.
   *
   * @param events the list of SubscriptionEvents to filter
   * @param maxBytes the maximum size (in bytes) of events that can be polled
   * @return a Pair containing two lists: the first list contains the events to poll, and the second
   *     list contains the events to nack
   */
  public Pair<List<SubscriptionEvent>, List<SubscriptionEvent>> filter(
      final List<SubscriptionEvent> events, final long maxBytes) {
    final List<SubscriptionEvent> eventsToPoll = new ArrayList<>();
    final List<SubscriptionEvent> eventsToNack = new ArrayList<>();

    long totalSize = 0;
    final Map<String, Long> topicNameToIncrements = new HashMap<>();

    // Sort the events based on the current subscription state (event counts per topic)
    sort(events);
    for (final SubscriptionEvent event : events) {
      final long currentSize;
      try {
        currentSize = event.getCurrentResponseSize();
      } catch (final IOException e) {
        // If there's an issue with accessing the event's size, nack the event
        eventsToNack.add(event);
        continue;
      }
      // If adding this event's size would not exceed maxBytes, add it to the poll list
      if (totalSize + currentSize < maxBytes) {
        eventsToPoll.add(event);
        totalSize += currentSize;
        topicNameToIncrements.merge(event.getCommitContext().getTopicName(), 1L, Long::sum);
      } else {
        // Otherwise, nack the event
        eventsToNack.add(event);
      }
    }

    // Update the subscription state with the increments calculated during filtering
    update(topicNameToIncrements);
    return new Pair<>(eventsToPoll, eventsToNack);
  }

  /**
   * Updates the subscription state by incrementing the event count for multiple topics.
   *
   * @param topicNameToIncrements a map where the key is the topic name and the value is the number
   *     of events to add to the count
   */
  private void update(final Map<String, Long> topicNameToIncrements) {
    for (final Entry<String, Long> entry : topicNameToIncrements.entrySet()) {
      topicNameToEventCount.merge(entry.getKey(), entry.getValue(), Long::sum);
    }
  }

  /**
   * Sorts a list of SubscriptionEvents according to the event count in the subscription states.
   * Events with fewer counts are prioritized.
   *
   * @param events the list of events to sort
   */
  private void sort(final List<SubscriptionEvent> events) {
    events.sort(
        Comparator.comparingLong(event -> getCount(event.getCommitContext().getTopicName())));
  }

  /**
   * Returns the number of events received for a specific topic.
   *
   * @param topicName the name of the topic
   * @return the number of events received for the topic
   */
  private long getCount(final String topicName) {
    return topicNameToEventCount.getOrDefault(topicName, 0L);
  }
}
