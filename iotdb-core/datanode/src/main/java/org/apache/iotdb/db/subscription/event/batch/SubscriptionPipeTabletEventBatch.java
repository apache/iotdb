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
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;

import org.apache.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SubscriptionPipeTabletEventBatch extends SubscriptionPipeEventBatch {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SubscriptionPipeTabletEventBatch.class);

  private volatile List<Tablet> tablets = new LinkedList<>();
  private long firstEventProcessingTime = Long.MIN_VALUE;
  private long totalBufferSize = 0;

  public SubscriptionPipeTabletEventBatch(
      final int regionId,
      final SubscriptionPrefetchingTabletQueue prefetchingQueue,
      final int maxDelayInMs,
      final long maxBatchSizeInBytes) {
    super(regionId, prefetchingQueue, maxDelayInMs, maxBatchSizeInBytes);
  }

  public LinkedList<Tablet> moveTablets() {
    if (Objects.isNull(tablets)) {
      tablets = new ArrayList<>();
      for (final EnrichedEvent enrichedEvent : enrichedEvents) {
        if (enrichedEvent instanceof TsFileInsertionEvent) {
          onTsFileInsertionEvent((TsFileInsertionEvent) enrichedEvent);
        } else if (enrichedEvent instanceof TabletInsertionEvent) {
          onTabletInsertionEvent((TabletInsertionEvent) enrichedEvent);
        } else {
          LOGGER.warn(
              "SubscriptionPipeTabletEventBatch {} ignore EnrichedEvent {} when moving.",
              this,
              enrichedEvent);
        }
      }
    }
    final LinkedList<Tablet> result = new LinkedList<>(tablets);
    firstEventProcessingTime = Long.MIN_VALUE;
    totalBufferSize = 0;
    tablets = null; // reset to null for gc & subsequent move
    return result;
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
    if (Objects.nonNull(tablets)) {
      tablets.clear();
    }
  }

  /////////////////////////////// utility ///////////////////////////////

  @Override
  protected void onTabletInsertionEvent(final TabletInsertionEvent event) {
    constructBatch(event);
    if (firstEventProcessingTime == Long.MIN_VALUE) {
      firstEventProcessingTime = System.currentTimeMillis();
    }
  }

  @Override
  protected void onTsFileInsertionEvent(final TsFileInsertionEvent event) {
    for (final TabletInsertionEvent tabletInsertionEvent :
        ((PipeTsFileInsertionEvent) event)
            .toTabletInsertionEvents(SubscriptionAgent.receiver().remainingMs())) {
      onTabletInsertionEvent(tabletInsertionEvent);
    }
  }

  @Override
  protected List<SubscriptionEvent> generateSubscriptionEvents() {
    if (tablets.isEmpty()) {
      return null;
    }

    return Collections.singletonList(
        new SubscriptionEvent(this, prefetchingQueue.generateSubscriptionCommitContext()));
  }

  @Override
  protected boolean shouldEmit() {
    return totalBufferSize >= maxBatchSizeInBytes
        || System.currentTimeMillis() - firstEventProcessingTime >= maxDelayInMs;
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
    coreReportMessage.put(
        "size of tablets",
        (Objects.nonNull(tablets) ? String.valueOf(tablets.size()) : "<unknown>"));
    coreReportMessage.put("firstEventProcessingTime", String.valueOf(firstEventProcessingTime));
    coreReportMessage.put("totalBufferSize", String.valueOf(totalBufferSize));
    return coreReportMessage;
  }
}
