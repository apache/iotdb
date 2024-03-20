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

import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.iotdb.commons.pipe.datastructure.queue.ConcurrentIterableLinkedQueue;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.task.connection.BoundedBlockingPendingQueue;
import org.apache.iotdb.commons.subscription.config.SubscriptionConfig;
import org.apache.iotdb.db.pipe.event.UserDefinedEnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.rpc.subscription.payload.response.EnrichedTablets;
import org.apache.iotdb.tsfile.write.record.Tablet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class SubscriptionPrefetchingQueue {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionBroker.class);

  private final String brokerId; // consumer group id

  private final String topicName;

  private final BoundedBlockingPendingQueue<Event> inputPendingQueue;

  private final SortedMap<String, SerializedEnrichedEvent> uncommittedEvents;
  private final ReentrantReadWriteLock uncommittedEventsLock;
  private final ConcurrentIterableLinkedQueue<SerializedEnrichedEvent> prefetchingQueue;

  // TODO: Consider the stale subscription commit ID; after restarting, the subscription commit ID
  // from before the restart should not be accepted.
  private final AtomicLong subscriptionCommitIdGenerator = new AtomicLong(0);

  public SubscriptionPrefetchingQueue(
      String brokerId, String topicName, BoundedBlockingPendingQueue<Event> inputPendingQueue) {
    this.brokerId = brokerId;
    this.topicName = topicName;
    this.inputPendingQueue = inputPendingQueue;
    this.uncommittedEvents = new TreeMap<>();
    this.uncommittedEventsLock = new ReentrantReadWriteLock(true);
    this.prefetchingQueue = new ConcurrentIterableLinkedQueue<>();
  }

  /////////////////////////////// provided for SubscriptionBroker ///////////////////////////////

  public SerializedEnrichedEvent poll() {
    if (prefetchingQueue.isEmpty()) {
      prefetchOnce(SubscriptionConfig.getInstance().getSubscriptionMaxTabletsPerPrefetching());
      // without serializeOnce here
    }

    SerializedEnrichedEvent event;
    try (ConcurrentIterableLinkedQueue<SerializedEnrichedEvent>.DynamicIterator iter =
        prefetchingQueue.iterateFromEarliest()) {
      if (Objects.nonNull(
          event =
              iter.next(SubscriptionConfig.getInstance().getSubscriptionPollMaxBlockingTimeMs()))) {
        if (!event.serialize()) {
          return null;
        }
        prefetchingQueue.tryRemoveBefore(iter.getNextIndex());
        return event;
      }
    }

    return null;
  }

  public void commit(List<String> subscriptionCommitIds) {
    uncommittedEventsLock.writeLock().lock();
    try {
      commitInternal(subscriptionCommitIds);
    } finally {
      uncommittedEventsLock.writeLock().unlock();
    }
  }

  private void commitInternal(List<String> subscriptionCommitIds) {
    for (String subscriptionCommitId : subscriptionCommitIds) {
      SerializedEnrichedEvent event = uncommittedEvents.get(subscriptionCommitId);
      if (Objects.isNull(event)) {
        LOGGER.warn(
            "Subscription: subscription commit id [{}] does not exist, it may have been committed or something unexpected happened",
            subscriptionCommitId);
        continue;
      }
      event.decreaseReferenceCount();
      uncommittedEvents.remove(subscriptionCommitId);
    }
  }

  /////////////////////////////// prefetch ///////////////////////////////

  public void executePrefetch() {
    prefetchOnce(SubscriptionConfig.getInstance().getSubscriptionMaxTabletsPerPrefetching());
    serializeOnce();
  }

  // TODO: use org.apache.iotdb.db.pipe.resource.memory.PipeMemoryManager.calculateTabletSizeInBytes
  // for limit control
  private void prefetchOnce(long limit) {
    List<Tablet> tablets = new ArrayList<>();
    List<EnrichedEvent> enrichedEvents = new ArrayList<>();

    Event event;
    while (Objects.nonNull(
        event = UserDefinedEnrichedEvent.maybeOf(inputPendingQueue.waitedPoll()))) {
      if (!(event instanceof EnrichedEvent)) {
        LOGGER.warn("Subscription: Only support prefetch EnrichedEvent. Ignore {}.", event);
        continue;
      }

      if (event instanceof TabletInsertionEvent) {
        Tablet tablet = convertToTablet((TabletInsertionEvent) event);
        if (Objects.isNull(tablet)) {
          continue;
        }
        tablets.add(tablet);
        enrichedEvents.add((EnrichedEvent) event);
        if (tablets.size() >= limit) {
          break;
        }
      } else if (event instanceof PipeTsFileInsertionEvent) {
        for (TabletInsertionEvent tabletInsertionEvent :
            ((PipeTsFileInsertionEvent) event).toTabletInsertionEvents()) {
          Tablet tablet = convertToTablet(tabletInsertionEvent);
          if (Objects.isNull(tablet)) {
            continue;
          }
          tablets.add(tablet);
        }
        enrichedEvents.add((EnrichedEvent) event);
        if (tablets.size() >= limit) {
          break;
        }
      } else {
        // TODO:
        //  - PipeHeartbeatEvent: ignored? (may affect pipe metrics)
        //  - UserDefinedEnrichedEvent: ignored?
        //  - Others: events related to meta sync, safe to ignore
        LOGGER.warn("Subscription: Ignore EnrichedEvent {} when prefetching.", event);
      }
    }

    if (!tablets.isEmpty()) {
        String subscriptionCommitId = generateSubscriptionCommitId();
        SerializedEnrichedEvent enrichedEvent = new SerializedEnrichedEvent(
            new EnrichedTablets(topicName, tablets, subscriptionCommitId),
            enrichedEvents);

        prefetchingQueue.add(enrichedEvent);

        uncommittedEventsLock.writeLock().lock();
        uncommittedEvents.put(subscriptionCommitId, enrichedEvent);
        uncommittedEventsLock.writeLock().unlock();
      }
    }
  }

  private void serializeOnce() {
    SerializedEnrichedEvent event;
    try (ConcurrentIterableLinkedQueue<SerializedEnrichedEvent>.DynamicIterator iter =
        prefetchingQueue.iterateFromEarliest()) {
      // TODO: memory control
      while (Objects.nonNull(
          event =
              iter.next(
                  SubscriptionConfig.getInstance().getSubscriptionSerializeMaxBlockingTimeMs()))) {
        event.serialize();
      }
    }
  }

  /////////////////////////////// recycle ///////////////////////////////

  public void recycleUncommittedEvents() {
    try {
      uncommittedEventsLock.writeLock().lock();
      for (int i = 0; i < SubscriptionConfig.getInstance().getSubscriptionMaxEventsPerRecycling();
          ++i) {
        if (uncommittedEvents.isEmpty()) {
          break;
        }
        String subscriptionCommitId = uncommittedEvents.firstKey();
        SerializedEnrichedEvent event = uncommittedEvents.get(subscriptionCommitId);
        if (Objects.nonNull(event) && event.maybeExpired()) {
          LOGGER.info("Subscription: recycle the event with subscriptionCommitId {}",
              subscriptionCommitId);
          prefetchingQueue.add(event);
          uncommittedEvents.remove(subscriptionCommitId);
        }
      }
    } finally {
      uncommittedEventsLock.writeLock().unlock();
    }
  }

  /////////////////////////////// utility ///////////////////////////////

  private Tablet convertToTablet(TabletInsertionEvent tabletInsertionEvent) {
    if (tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent) {
      return ((PipeInsertNodeTabletInsertionEvent) tabletInsertionEvent).convertToTablet();
    } else if (tabletInsertionEvent instanceof PipeRawTabletInsertionEvent) {
      return ((PipeRawTabletInsertionEvent) tabletInsertionEvent).convertToTablet();
    }

    LOGGER.warn(
        "Subscription: Only support convert PipeInsertNodeTabletInsertionEvent or PipeRawTabletInsertionEvent to tablet. Ignore {}.",
        tabletInsertionEvent);
    return null;
  }

  private String generateSubscriptionCommitId() {
    return topicName + "_" + brokerId + "#" + subscriptionCommitIdGenerator.getAndIncrement();
  }
}
