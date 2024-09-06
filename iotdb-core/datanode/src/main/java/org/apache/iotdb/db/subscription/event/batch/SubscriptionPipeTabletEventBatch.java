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

package org.apache.iotdb.db.subscription.event.batch;

import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.subscription.config.SubscriptionConfig;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryWeightUtil;
import org.apache.iotdb.db.subscription.broker.SubscriptionPrefetchingTabletQueue;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.db.subscription.event.pipe.SubscriptionPipeTabletBatchEvents;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponse;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponseType;
import org.apache.iotdb.rpc.subscription.payload.poll.TabletsPayload;

import org.apache.tsfile.write.record.Tablet;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class SubscriptionPipeTabletEventBatch extends SubscriptionPipeEventBatch {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SubscriptionPipeTabletEventBatch.class);

  private static final long READ_TABLET_BUFFER_SIZE =
      SubscriptionConfig.getInstance().getSubscriptionReadTabletBufferSize();

  private final List<EnrichedEvent> enrichedEvents = new ArrayList<>();
  private final List<Tablet> tablets = new ArrayList<>();

  private long firstEventProcessingTime = Long.MIN_VALUE;
  private long totalBufferSize = 0;

  public SubscriptionPipeTabletEventBatch(
      final int regionId,
      final SubscriptionPrefetchingTabletQueue prefetchingQueue,
      final int maxDelayInMs,
      final long maxBatchSizeInBytes) {
    super(regionId, prefetchingQueue, maxDelayInMs, maxBatchSizeInBytes);
  }

  @Override
  public synchronized boolean onEvent(final Consumer<SubscriptionEvent> consumer) {
    if (shouldEmit()) {
      if (Objects.isNull(events)) {
        events = generateSubscriptionEvents();
      }
      if (Objects.nonNull(events)) {
        events.forEach(consumer);
        return true;
      }
      return false;
    }
    return false;
  }

  @Override
  public synchronized boolean onEvent(
      final @NonNull EnrichedEvent event, final Consumer<SubscriptionEvent> consumer) {
    if (event instanceof TabletInsertionEvent) {
      onEventInternal((TabletInsertionEvent) event); // no exceptions will be thrown
    }
    return onEvent(consumer);
  }

  @Override
  public synchronized void cleanUp() {
    // clear the reference count of events
    for (final EnrichedEvent enrichedEvent : enrichedEvents) {
      enrichedEvent.clearReferenceCount(this.getClass().getName());
    }
  }

  public synchronized void ack() {
    for (final EnrichedEvent enrichedEvent : enrichedEvents) {
      enrichedEvent.decreaseReferenceCount(this.getClass().getName(), true);
    }
  }

  /////////////////////////////// utility ///////////////////////////////

  private List<SubscriptionEvent> generateSubscriptionEvents() {
    if (tablets.isEmpty()) {
      return null;
    }

    final SubscriptionCommitContext commitContext =
        prefetchingQueue.generateSubscriptionCommitContext();
    final List<SubscriptionPollResponse> responses = new ArrayList<>();
    final List<Tablet> currentTablets = new ArrayList<>();
    long currentTotalBufferSize = 0;
    for (final Tablet tablet : tablets) {
      final long bufferSize = PipeMemoryWeightUtil.calculateTabletSizeInBytes(tablet);
      if (bufferSize > READ_TABLET_BUFFER_SIZE) {
        LOGGER.warn("Detect large tablet with {} byte(s).", bufferSize);
        responses.add(
            new SubscriptionPollResponse(
                SubscriptionPollResponseType.TABLETS.getType(),
                new TabletsPayload(Collections.singletonList(tablet), responses.size() + 1),
                commitContext));
        continue;
      }
      if (currentTotalBufferSize + bufferSize > READ_TABLET_BUFFER_SIZE) {
        responses.add(
            new SubscriptionPollResponse(
                SubscriptionPollResponseType.TABLETS.getType(),
                new TabletsPayload(new ArrayList<>(currentTablets), responses.size() + 1),
                commitContext));
        currentTablets.clear();
        currentTotalBufferSize = 0;
      }
      currentTablets.add(tablet);
      currentTotalBufferSize += bufferSize;
    }
    responses.add(
        new SubscriptionPollResponse(
            SubscriptionPollResponseType.TABLETS.getType(),
            new TabletsPayload(new ArrayList<>(currentTablets), -tablets.size()),
            commitContext));
    return Collections.singletonList(
        new SubscriptionEvent(new SubscriptionPipeTabletBatchEvents(this), responses));
  }

  private void onEventInternal(final TabletInsertionEvent event) {
    constructBatch(event);
    enrichedEvents.add((EnrichedEvent) event);
    if (firstEventProcessingTime == Long.MIN_VALUE) {
      firstEventProcessingTime = System.currentTimeMillis();
    }
  }

  private void constructBatch(final TabletInsertionEvent event) {
    final List<Tablet> currentTablets = convertToTablets(event);
    if (currentTablets.isEmpty()) {
      return;
    }
    tablets.addAll(currentTablets);
    totalBufferSize +=
        currentTablets.stream()
            .map(PipeMemoryWeightUtil::calculateTabletSizeInBytes)
            .reduce(Long::sum)
            .orElse(0L);
  }

  private boolean shouldEmit() {
    return totalBufferSize >= maxBatchSizeInBytes
        || System.currentTimeMillis() - firstEventProcessingTime >= maxDelayInMs;
  }

  private List<Tablet> convertToTablets(final TabletInsertionEvent tabletInsertionEvent) {
    if (tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent) {
      return ((PipeInsertNodeTabletInsertionEvent) tabletInsertionEvent).convertToTablets();
    } else if (tabletInsertionEvent instanceof PipeRawTabletInsertionEvent) {
      return Collections.singletonList(
          ((PipeRawTabletInsertionEvent) tabletInsertionEvent).convertToTablet());
    }

    LOGGER.warn(
        "SubscriptionPipeTabletEventBatch {} only support convert PipeInsertNodeTabletInsertionEvent or PipeRawTabletInsertionEvent to tablet. Ignore {}.",
        this,
        tabletInsertionEvent);
    return Collections.emptyList();
  }

  /////////////////////////////// stringify ///////////////////////////////

  @Override
  public String toString() {
    return "SubscriptionPipeTabletEventBatch" + this.coreReportMessage();
  }

  @Override
  protected Map<String, String> coreReportMessage() {
    final Map<String, String> coreReportMessage = super.coreReportMessage();
    coreReportMessage.put("enrichedEvents", formatEnrichedEvents(enrichedEvents, 4));
    coreReportMessage.put("size of tablets", String.valueOf(tablets.size()));
    coreReportMessage.put("firstEventProcessingTime", String.valueOf(firstEventProcessingTime));
    coreReportMessage.put("totalBufferSize", String.valueOf(totalBufferSize));
    return coreReportMessage;
  }

  private static String formatEnrichedEvents(
      final List<EnrichedEvent> enrichedEvents, final int threshold) {
    final List<String> eventMessageList =
        enrichedEvents.stream()
            .limit(threshold)
            .map(EnrichedEvent::coreReportMessage)
            .collect(Collectors.toList());
    if (eventMessageList.size() > threshold) {
      eventMessageList.add(
          String.format("omit the remaining %s event(s)...", eventMessageList.size() - threshold));
    }
    return eventMessageList.toString();
  }
}
