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
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryWeightUtil;
import org.apache.iotdb.db.subscription.agent.SubscriptionAgent;
import org.apache.iotdb.db.subscription.broker.SubscriptionPrefetchingTabletQueue;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.metrics.core.utils.IoTDBMovingAverage;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Meter;
import org.apache.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SubscriptionPipeTabletEventBatch extends SubscriptionPipeEventBatch
    implements Iterator<List<Tablet>> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SubscriptionPipeTabletEventBatch.class);

  private long firstEventProcessingTime = Long.MIN_VALUE;
  private long totalBufferSize = 0;

  private volatile Iterator<EnrichedEvent> enrichedEventsIterator;
  private volatile Iterator<TabletInsertionEvent> currentTabletInsertionEventsIterator;

  private final Meter insertNodeTabletInsertionEventSizeEstimator;
  private final Meter rawTabletInsertionEventSizeEstimator;

  public SubscriptionPipeTabletEventBatch(
      final int regionId,
      final SubscriptionPrefetchingTabletQueue prefetchingQueue,
      final int maxDelayInMs,
      final long maxBatchSizeInBytes) {
    super(regionId, prefetchingQueue, maxDelayInMs, maxBatchSizeInBytes);

    this.insertNodeTabletInsertionEventSizeEstimator =
        new Meter(new IoTDBMovingAverage(), Clock.defaultClock());
    this.rawTabletInsertionEventSizeEstimator =
        new Meter(new IoTDBMovingAverage(), Clock.defaultClock());
  }

  /////////////////////////////// ack & clean ///////////////////////////////

  @Override
  public synchronized void ack() {
    for (final EnrichedEvent enrichedEvent : enrichedEvents) {
      enrichedEvent.decreaseReferenceCount(this.getClass().getName(), true);
    }
  }

  @Override
  public synchronized void cleanUp() {
    // clear the reference count of events
    for (final EnrichedEvent enrichedEvent : enrichedEvents) {
      enrichedEvent.clearReferenceCount(this.getClass().getName());
    }
    enrichedEvents.clear();

    enrichedEventsIterator = null;
    currentTabletInsertionEventsIterator = null;
  }

  /////////////////////////////// utility ///////////////////////////////

  @Override
  protected void onTabletInsertionEvent(final TabletInsertionEvent event) {
    // update processing time
    if (firstEventProcessingTime == Long.MIN_VALUE) {
      firstEventProcessingTime = System.currentTimeMillis();
    }

    // update buffer size
    // TODO: more precise computation
    if (event instanceof PipeInsertNodeTabletInsertionEvent) {
      totalBufferSize += getEstimatedInsertNodeTabletInsertionEventSize();
    } else if (event instanceof PipeRawTabletInsertionEvent) {
      totalBufferSize += getEstimatedRawTabletInsertionEventSize();
    }
  }

  @Override
  protected void onTsFileInsertionEvent(final TsFileInsertionEvent event) {
    // update processing time
    if (firstEventProcessingTime == Long.MIN_VALUE) {
      firstEventProcessingTime = System.currentTimeMillis();
    }

    // update buffer size
    // TODO: more precise computation
    totalBufferSize += ((PipeTsFileInsertionEvent) event).getTsFile().length();
  }

  @Override
  protected List<SubscriptionEvent> generateSubscriptionEvents() {
    return Collections.singletonList(
        new SubscriptionEvent(this, prefetchingQueue.generateSubscriptionCommitContext()));
  }

  @Override
  protected boolean shouldEmit() {
    return totalBufferSize >= maxBatchSizeInBytes
        || System.currentTimeMillis() - firstEventProcessingTime >= maxDelayInMs;
  }

  private List<Tablet> convertToTablets(final TabletInsertionEvent tabletInsertionEvent) {
    if (tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent) {
      final List<Tablet> tablets =
          ((PipeInsertNodeTabletInsertionEvent) tabletInsertionEvent).convertToTablets();
      updateEstimatedInsertNodeTabletInsertionEventSize(
          tablets.stream()
              .map(PipeMemoryWeightUtil::calculateTabletSizeInBytes)
              .reduce(Long::sum)
              .orElse(0L));
      return tablets;
    } else if (tabletInsertionEvent instanceof PipeRawTabletInsertionEvent) {
      final Tablet tablet = ((PipeRawTabletInsertionEvent) tabletInsertionEvent).convertToTablet();
      updateEstimatedRawTabletInsertionEventSize(
          PipeMemoryWeightUtil.calculateTabletSizeInBytes(tablet));
      return Collections.singletonList(tablet);
    }

    LOGGER.warn(
        "SubscriptionPipeTabletEventBatch {} only support convert PipeInsertNodeTabletInsertionEvent or PipeRawTabletInsertionEvent to tablet. Ignore {}.",
        this,
        tabletInsertionEvent);
    return Collections.emptyList();
  }

  /////////////////////////////// estimator ///////////////////////////////

  private long getEstimatedInsertNodeTabletInsertionEventSize() {
    return Math.max(100L, (long) insertNodeTabletInsertionEventSizeEstimator.getOneMinuteRate());
  }

  private void updateEstimatedInsertNodeTabletInsertionEventSize(final long size) {
    insertNodeTabletInsertionEventSizeEstimator.mark(size);
  }

  private long getEstimatedRawTabletInsertionEventSize() {
    return Math.max(100L, (long) rawTabletInsertionEventSizeEstimator.getOneMinuteRate());
  }

  private void updateEstimatedRawTabletInsertionEventSize(final long size) {
    rawTabletInsertionEventSizeEstimator.mark(size);
  }

  /////////////////////////////// iterator ///////////////////////////////

  public void resetIterator() {
    enrichedEventsIterator = enrichedEvents.iterator();
  }

  @Override
  public boolean hasNext() {
    if (Objects.nonNull(currentTabletInsertionEventsIterator)) {
      if (currentTabletInsertionEventsIterator.hasNext()) {
        return true;
      } else {
        // reset
        currentTabletInsertionEventsIterator = null;
        return false;
      }
    }

    if (Objects.isNull(enrichedEventsIterator)) {
      return false;
    }

    if (enrichedEventsIterator.hasNext()) {
      return true;
    } else {
      // reset
      enrichedEventsIterator = null;
      return false;
    }
  }

  @Override
  public List<Tablet> next() {
    if (Objects.nonNull(currentTabletInsertionEventsIterator)) {
      if (currentTabletInsertionEventsIterator.hasNext()) {
        return convertToTablets(currentTabletInsertionEventsIterator.next());
      } else {
        currentTabletInsertionEventsIterator = null;
      }
    }

    if (Objects.isNull(enrichedEventsIterator)) {
      return null;
    }

    if (!enrichedEventsIterator.hasNext()) {
      return null;
    }

    final EnrichedEvent enrichedEvent = enrichedEventsIterator.next();
    if (enrichedEvent instanceof TsFileInsertionEvent) {
      if (Objects.nonNull(currentTabletInsertionEventsIterator)) {
        LOGGER.warn(
            "SubscriptionPipeTabletEventBatch {} override non-null currentTabletInsertionEventsIterator when iterating (broken invariant).",
            this);
      }
      currentTabletInsertionEventsIterator =
          ((PipeTsFileInsertionEvent) enrichedEvent)
              .toTabletInsertionEvents(SubscriptionAgent.receiver().remainingMs())
              .iterator();
      return next();
    } else if (enrichedEvent instanceof TabletInsertionEvent) {
      return convertToTablets((TabletInsertionEvent) enrichedEvent);
    } else {
      LOGGER.warn(
          "SubscriptionPipeTabletEventBatch {} ignore EnrichedEvent {} when iterating (broken invariant).",
          this,
          enrichedEvent);
      return null;
    }
  }

  /////////////////////////////// stringify ///////////////////////////////

  @Override
  public String toString() {
    return "SubscriptionPipeTabletEventBatch" + this.coreReportMessage();
  }

  @Override
  protected Map<String, String> coreReportMessage() {
    final Map<String, String> coreReportMessage = super.coreReportMessage();
    coreReportMessage.put("firstEventProcessingTime", String.valueOf(firstEventProcessingTime));
    coreReportMessage.put("totalBufferSize", String.valueOf(totalBufferSize));
    coreReportMessage.put(
        "estimatedInsertNodeTabletInsertionEventSize",
        String.valueOf(getEstimatedInsertNodeTabletInsertionEventSize()));
    coreReportMessage.put(
        "estimatedRawTabletInsertionEventSize",
        String.valueOf(getEstimatedRawTabletInsertionEventSize()));
    return coreReportMessage;
  }
}
