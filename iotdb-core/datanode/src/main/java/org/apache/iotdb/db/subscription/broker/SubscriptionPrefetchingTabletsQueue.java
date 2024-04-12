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
import org.apache.iotdb.db.pipe.event.UserDefinedEnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.db.subscription.timer.SubscriptionPollTimer;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.rpc.subscription.payload.common.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.common.SubscriptionRawMessage;
import org.apache.iotdb.rpc.subscription.payload.common.SubscriptionRawMessageType;
import org.apache.iotdb.rpc.subscription.payload.common.TabletsMessagePayload;
import org.apache.iotdb.tsfile.write.record.Tablet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class SubscriptionPrefetchingTabletsQueue extends SubscriptionPrefetchingQueue {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SubscriptionPrefetchingTabletsQueue.class);

  private final LinkedBlockingQueue<SubscriptionEvent> prefetchingQueue;

  public SubscriptionPrefetchingTabletsQueue(
      final String brokerId,
      final String topicName,
      final BoundedBlockingPendingQueue<Event> inputPendingQueue) {
    super(brokerId, topicName, inputPendingQueue);

    this.prefetchingQueue = new LinkedBlockingQueue<>();
  }

  @Override
  public SubscriptionEvent poll(final SubscriptionPollTimer timer) {
    if (prefetchingQueue.isEmpty()) {
      prefetchOnce(SubscriptionConfig.getInstance().getSubscriptionMaxTabletsPerPrefetching());
      // without serializeOnce here
    }

    SubscriptionEvent currentEvent;
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
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.warn("Subscription: Interrupted while polling events.", e);
    }

    return null;
  }

  @Override
  public void executePrefetch() {
    prefetchOnce(SubscriptionConfig.getInstance().getSubscriptionMaxTabletsPerPrefetching());
    serializeOnce();
  }

  // TODO: use org.apache.iotdb.db.pipe.resource.memory.PipeMemoryManager.calculateTabletSizeInBytes
  // for limit control
  private void prefetchOnce(final long limit) {
    final List<Tablet> tablets = new ArrayList<>();
    final List<EnrichedEvent> enrichedEvents = new ArrayList<>();

    Event event;
    while (Objects.nonNull(
        event = UserDefinedEnrichedEvent.maybeOf(inputPendingQueue.waitedPoll()))) {
      if (!(event instanceof EnrichedEvent)) {
        LOGGER.warn("Subscription: Only support prefetch EnrichedEvent. Ignore {}.", event);
        continue;
      }

      if (event instanceof TabletInsertionEvent) {
        final Tablet tablet = convertToTablet((TabletInsertionEvent) event);
        if (Objects.isNull(tablet)) {
          continue;
        }
        tablets.add(tablet);
        enrichedEvents.add((EnrichedEvent) event);
        if (tablets.size() >= limit) {
          break;
        }
      } else if (event instanceof PipeTsFileInsertionEvent) {
        for (final TabletInsertionEvent tabletInsertionEvent :
            ((PipeTsFileInsertionEvent) event).toTabletInsertionEvents()) {
          final Tablet tablet = convertToTablet(tabletInsertionEvent);
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
      final SubscriptionCommitContext commitContext = generateSubscriptionCommitContext();
      final SubscriptionEvent subscriptionEvent =
          new SubscriptionEvent(
              enrichedEvents,
              new SubscriptionRawMessage(
                  SubscriptionRawMessageType.TABLETS.getType(),
                  new TabletsMessagePayload(tablets),
                  commitContext));
      uncommittedEvents.put(commitContext, subscriptionEvent); // before enqueuing the event
      prefetchingQueue.add(subscriptionEvent);
    }
  }

  private void serializeOnce() {
    final long size = prefetchingQueue.size();
    long count = 0;

    SubscriptionEvent currentEvent;
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
          currentEvent.getMessage().trySerialize();
        }
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.warn("Subscription: Interrupted while serializing events.", e);
    }
  }

  /////////////////////////////// utility ///////////////////////////////

  private Tablet convertToTablet(final TabletInsertionEvent tabletInsertionEvent) {
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
}
