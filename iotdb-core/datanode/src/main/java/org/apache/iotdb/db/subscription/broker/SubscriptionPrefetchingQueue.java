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

import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.task.connection.BoundedBlockingPendingQueue;
import org.apache.iotdb.commons.subscription.config.SubscriptionConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.agent.PipeAgent;
import org.apache.iotdb.db.pipe.event.UserDefinedEnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.subscription.timer.SubscriptionPollTimer;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.rpc.subscription.payload.EnrichedTablets;

import org.apache.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class SubscriptionPrefetchingQueue {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionBroker.class);

  private final String brokerId; // consumer group id
  private final String topicName;
  private final BoundedBlockingPendingQueue<Event> inputPendingQueue;

  private final Map<String, SerializedEnrichedEvent> uncommittedEvents;
  private final LinkedBlockingQueue<SerializedEnrichedEvent> prefetchingQueue;

  private final AtomicLong subscriptionCommitIdGenerator = new AtomicLong(0);

  public SubscriptionPrefetchingQueue(
      String brokerId, String topicName, BoundedBlockingPendingQueue<Event> inputPendingQueue) {
    this.brokerId = brokerId;
    this.topicName = topicName;
    this.inputPendingQueue = inputPendingQueue;
    this.uncommittedEvents = new ConcurrentHashMap<>();
    this.prefetchingQueue = new LinkedBlockingQueue<>();
  }

  /////////////////////////////// provided for SubscriptionBroker ///////////////////////////////

  public SerializedEnrichedEvent poll(SubscriptionPollTimer timer) {
    if (prefetchingQueue.isEmpty()) {
      prefetchOnce(SubscriptionConfig.getInstance().getSubscriptionMaxTabletsPerPrefetching());
      // without serializeOnce here
    }

    SerializedEnrichedEvent currentEvent;
    try {
      while (Objects.nonNull(
          currentEvent =
              prefetchingQueue.poll(
                  SubscriptionConfig.getInstance().getSubscriptionPollMaxBlockingTimeMs(),
                  TimeUnit.MILLISECONDS))) {
        if (currentEvent.isCommitted()) {
          continue;
        }
        // Re-enqueue the uncommitted event at the end of the queue.
        prefetchingQueue.add(currentEvent);
        // timeout control
        timer.update();
        if (timer.isExpired()) {
          break;
        }
        if (!currentEvent.pollable()) {
          continue;
        }
        currentEvent.recordLastPolledTimestamp();
        return currentEvent;
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.warn("Subscription: Interrupted while polling events.", e);
    }

    return null;
  }

  public void commit(List<String> subscriptionCommitIds) {
    for (String subscriptionCommitId : subscriptionCommitIds) {
      SerializedEnrichedEvent event = uncommittedEvents.get(subscriptionCommitId);
      if (Objects.isNull(event)) {
        LOGGER.warn(
            "Subscription: subscription commit id [{}] does not exist, it may have been committed or something unexpected happened",
            subscriptionCommitId);
        continue;
      }
      event.decreaseReferenceCount();
      event.recordCommittedTimestamp();
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
      SerializedEnrichedEvent enrichedEvent =
          new SerializedEnrichedEvent(
              new EnrichedTablets(topicName, tablets, subscriptionCommitId), enrichedEvents);
      uncommittedEvents.put(subscriptionCommitId, enrichedEvent); // before enqueuing the event
      prefetchingQueue.add(enrichedEvent);
    }
  }

  private void serializeOnce() {
    long size = prefetchingQueue.size();
    long count = 0;

    SerializedEnrichedEvent currentEvent;
    try {
      while (Objects.nonNull(
          currentEvent =
              prefetchingQueue.poll(
                  SubscriptionConfig.getInstance().getSubscriptionSerializeMaxBlockingTimeMs(),
                  TimeUnit.MILLISECONDS))) {
        if (currentEvent.isCommitted()) {
          continue;
        }
        // Re-enqueue the uncommitted event at the end of the queue.
        prefetchingQueue.add(currentEvent);
        // limit control
        if (count >= size) {
          break;
        }
        count++;
        // Serialize the uncommitted and pollable event.
        if (currentEvent.pollable()) {
          // No need to concern whether serialization is successful.
          currentEvent.serialize();
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.warn("Subscription: Interrupted while serializing events.", e);
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
    // subscription commit id format: {DataNodeId}#{RebootTimes}#{TopicName}_{BrokerId}#{Id}
    // Recording data node ID and reboot times to address potential stale commit IDs caused by
    // leader transfers or restarts.
    return IoTDBDescriptor.getInstance().getConfig().getDataNodeId()
        + "#"
        + PipeAgent.runtime().getRebootTimes()
        + "#"
        + topicName
        + "_"
        + brokerId
        + "#"
        + subscriptionCommitIdGenerator.getAndIncrement();
  }
}
