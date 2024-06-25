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
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;

import org.apache.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class SubscriptionPipeTabletEventBatch implements SubscriptionPipeEventBatch {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SubscriptionPipeTabletEventBatch.class);

  private final List<EnrichedEvent> enrichedEvents = new ArrayList<>();
  private final List<Tablet> tablets = new ArrayList<>();

  private final int maxDelayInMs;
  private long firstEventProcessingTime = Long.MIN_VALUE;

  private final long maxBatchSizeInBytes;
  private long totalBufferSize = 0;

  public SubscriptionPipeTabletEventBatch(final int maxDelayInMs, final long maxBatchSizeInBytes) {
    this.maxDelayInMs = maxDelayInMs;
    this.maxBatchSizeInBytes = maxBatchSizeInBytes;
  }

  @Override
  public List<File> sealTsFiles() {
    return Collections.emptyList();
  }

  @Override
  public List<Tablet> sealTablets() {
    return tablets;
  }

  @Override
  public boolean shouldEmit() {
    return totalBufferSize >= maxBatchSizeInBytes
        || System.currentTimeMillis() - firstEventProcessingTime >= maxDelayInMs;
  }

  @Override
  public boolean onEvent(final EnrichedEvent event) {
    if (event instanceof TabletInsertionEvent) {
      final List<Tablet> currentTablets = convertToTablets((TabletInsertionEvent) event);
      if (currentTablets.isEmpty()) {
        return shouldEmit();
      }
      tablets.addAll(currentTablets);
      totalBufferSize +=
          currentTablets.stream()
              .map((PipeMemoryWeightUtil::calculateTabletSizeInBytes))
              .reduce(Long::sum)
              .orElse(0L);
      enrichedEvents.add(event);
      if (firstEventProcessingTime == Long.MIN_VALUE) {
        firstEventProcessingTime = System.currentTimeMillis();
      }
    } else if (event instanceof PipeTsFileInsertionEvent) {
      for (final TabletInsertionEvent tabletInsertionEvent :
          ((PipeTsFileInsertionEvent) event).toTabletInsertionEvents()) {
        final List<Tablet> currentTablets = convertToTablets(tabletInsertionEvent);
        if (Objects.isNull(currentTablets)) {
          continue;
        }
        tablets.addAll(currentTablets);
        totalBufferSize +=
            currentTablets.stream()
                .map((PipeMemoryWeightUtil::calculateTabletSizeInBytes))
                .reduce(Long::sum)
                .orElse(0L);
      }
      enrichedEvents.add(event);
      if (firstEventProcessingTime == Long.MIN_VALUE) {
        firstEventProcessingTime = System.currentTimeMillis();
      }
    }

    return shouldEmit();
  }

  @Override
  public void ack() {
    for (final EnrichedEvent enrichedEvent : enrichedEvents) {
      enrichedEvent.decreaseReferenceCount(this.getClass().getName(), true);
    }
  }

  @Override
  public void cleanup() {
    // clear the reference count of events
    for (final EnrichedEvent enrichedEvent : enrichedEvents) {
      enrichedEvent.clearReferenceCount(this.getClass().getName());
    }
  }

  private List<Tablet> convertToTablets(final TabletInsertionEvent tabletInsertionEvent) {
    if (tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent) {
      return ((PipeInsertNodeTabletInsertionEvent) tabletInsertionEvent).convertToTablets();
    } else if (tabletInsertionEvent instanceof PipeRawTabletInsertionEvent) {
      return Collections.singletonList(
          ((PipeRawTabletInsertionEvent) tabletInsertionEvent).convertToTablet());
    }

    LOGGER.warn(
        "Subscription: SubscriptionPipeTabletEventBatch {} only support convert PipeInsertNodeTabletInsertionEvent or PipeRawTabletInsertionEvent to tablet. Ignore {}.",
        this,
        tabletInsertionEvent);
    return Collections.emptyList();
  }
}
