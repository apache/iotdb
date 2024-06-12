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
import org.apache.iotdb.commons.pipe.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.commons.subscription.config.SubscriptionConfig;
import org.apache.iotdb.db.pipe.event.UserDefinedEnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryManager;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.db.subscription.event.SubscriptionEventBinaryCache;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponse;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponseType;
import org.apache.iotdb.rpc.subscription.payload.poll.TabletsPayload;

import org.apache.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
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
      final UnboundedBlockingPendingQueue<Event> inputPendingQueue) {
    super(brokerId, topicName, inputPendingQueue);

    this.prefetchingQueue = new LinkedBlockingQueue<>();
  }

  @Override
  public SubscriptionEvent poll(final String consumerId) {
    if (prefetchingQueue.isEmpty()) {
      prefetchOnce();
    }

    final long size = prefetchingQueue.size();
    long count = 0;

    SubscriptionEvent currentEvent;
    try {
      while (count++ < size // limit control
          && Objects.nonNull(
              currentEvent =
                  prefetchingQueue.poll(
                      SubscriptionConfig.getInstance().getSubscriptionPollMaxBlockingTimeMs(),
                      TimeUnit.MILLISECONDS))) {
        if (currentEvent.isCommitted()) {
          continue;
        }
        if (!currentEvent.pollable()) {
          // Re-enqueue the uncommitted event at the end of the queue.
          prefetchingQueue.add(currentEvent);
          continue;
        }
        currentEvent.recordLastPolledConsumerId(consumerId);
        currentEvent.recordLastPolledTimestamp();
        // Re-enqueue the uncommitted event at the end of the queue.
        // This operation should be performed after recordLastPolledTimestamp to prevent multiple
        // consumers from consuming the same event.
        prefetchingQueue.add(currentEvent);
        return currentEvent;
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.warn(
          "Subscription: SubscriptionPrefetchingTabletsQueue {} interrupted while polling events.",
          this,
          e);
    }

    return null;
  }

  @Override
  public void executePrefetch() {
    prefetchOnce();
    serializeOnce();
  }

  private void prefetchOnce() {
    final List<Tablet> tablets = new ArrayList<>();
    final List<EnrichedEvent> enrichedEvents = new ArrayList<>();
    long calculatedTabletsSizeInBytes = 0;

    Event event;
    while (Objects.nonNull(
        event = UserDefinedEnrichedEvent.maybeOf(inputPendingQueue.waitedPoll()))) {
      if (!(event instanceof EnrichedEvent)) {
        LOGGER.warn(
            "Subscription: SubscriptionPrefetchingTabletsQueue {} only support prefetch EnrichedEvent. Ignore {}.",
            this,
            event);
        continue;
      }

      if (event instanceof TabletInsertionEvent) {
        final List<Tablet> currentTablets = convertToTablets((TabletInsertionEvent) event);
        if (currentTablets.isEmpty()) {
          continue;
        }
        tablets.addAll(currentTablets);
        calculatedTabletsSizeInBytes +=
            currentTablets.stream()
                .map((PipeMemoryManager::calculateTabletSizeInBytes))
                .reduce(Long::sum)
                .orElse(0L);
        enrichedEvents.add((EnrichedEvent) event);
      } else if (event instanceof PipeTsFileInsertionEvent) {
        for (final TabletInsertionEvent tabletInsertionEvent :
            ((PipeTsFileInsertionEvent) event).toTabletInsertionEvents()) {
          final List<Tablet> currentTablets = convertToTablets(tabletInsertionEvent);
          if (Objects.isNull(currentTablets)) {
            continue;
          }
          tablets.addAll(currentTablets);
          calculatedTabletsSizeInBytes +=
              currentTablets.stream()
                  .map((PipeMemoryManager::calculateTabletSizeInBytes))
                  .reduce(Long::sum)
                  .orElse(0L);
        }
        enrichedEvents.add((EnrichedEvent) event);
      } else {
        // TODO:
        //  - PipeHeartbeatEvent: ignored? (may affect pipe metrics)
        //  - UserDefinedEnrichedEvent: ignored?
        //  - Others: events related to meta sync, safe to ignore
        LOGGER.warn(
            "Subscription: SubscriptionPrefetchingTabletsQueue {} ignore EnrichedEvent {} when prefetching.",
            this,
            event);
      }

      if (tablets.size()
          >= SubscriptionConfig.getInstance().getSubscriptionMaxTabletsPerPrefetching()) {
        break;
      }

      if (calculatedTabletsSizeInBytes
          >= SubscriptionConfig.getInstance()
              .getSubscriptionMaxTabletsSizeInBytesPerPrefetching()) {
        break;
      }
    }

    if (!tablets.isEmpty()) {
      final SubscriptionCommitContext commitContext = generateSubscriptionCommitContext();
      final SubscriptionEvent subscriptionEvent =
          new SubscriptionEvent(
              enrichedEvents,
              new SubscriptionPollResponse(
                  SubscriptionPollResponseType.TABLETS.getType(),
                  new TabletsPayload(tablets, calculatedTabletsSizeInBytes),
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
      while (count++ < size // limit control
          && Objects.nonNull(
              currentEvent =
                  prefetchingQueue.poll(
                      SubscriptionConfig.getInstance().getSubscriptionSerializeMaxBlockingTimeMs(),
                      TimeUnit.MILLISECONDS))) {
        if (currentEvent.isCommitted()) {
          continue;
        }
        // Serialize the uncommitted and pollable event.
        if (currentEvent.pollable()) {
          // No need to concern whether serialization is successful.
          SubscriptionEventBinaryCache.getInstance().trySerialize(currentEvent);
        }
        // Re-enqueue the uncommitted event at the end of the queue.
        prefetchingQueue.add(currentEvent);
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.warn(
          "Subscription: SubscriptionPrefetchingTabletsQueue {} interrupted while serializing events.",
          this,
          e);
    }
  }

  /////////////////////////////// utility ///////////////////////////////

  private List<Tablet> convertToTablets(final TabletInsertionEvent tabletInsertionEvent) {
    if (tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent) {
      return ((PipeInsertNodeTabletInsertionEvent) tabletInsertionEvent).convertToTablets();
    } else if (tabletInsertionEvent instanceof PipeRawTabletInsertionEvent) {
      return Collections.singletonList(
          ((PipeRawTabletInsertionEvent) tabletInsertionEvent).convertToTablet());
    }

    LOGGER.warn(
        "Subscription: SubscriptionPrefetchingTabletsQueue {} only support convert PipeInsertNodeTabletInsertionEvent or PipeRawTabletInsertionEvent to tablet. Ignore {}.",
        this,
        tabletInsertionEvent);
    return Collections.emptyList();
  }
}
