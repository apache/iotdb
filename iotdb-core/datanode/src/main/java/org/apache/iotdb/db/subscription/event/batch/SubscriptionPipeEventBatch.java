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
import org.apache.iotdb.db.subscription.broker.SubscriptionPrefetchingQueue;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public abstract class SubscriptionPipeEventBatch {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionPipeEventBatch.class);

  private final int regionId;

  protected final SubscriptionPrefetchingQueue prefetchingQueue;
  protected final int maxDelayInMs;
  protected final long maxBatchSizeInBytes;

  protected volatile List<SubscriptionEvent> events = null;
  protected final List<EnrichedEvent> enrichedEvents = new ArrayList<>();

  protected SubscriptionPipeEventBatch(
      final int regionId,
      final SubscriptionPrefetchingQueue prefetchingQueue,
      final int maxDelayInMs,
      final long maxBatchSizeInBytes) {
    this.regionId = regionId;
    this.prefetchingQueue = prefetchingQueue;
    this.maxDelayInMs = maxDelayInMs;
    this.maxBatchSizeInBytes = maxBatchSizeInBytes;
  }

  /////////////////////////////// ack & clean ///////////////////////////////

  public abstract void ack();

  public abstract void cleanUp();

  /////////////////////////////// APIs ///////////////////////////////

  /**
   * @return {@code true} if there are subscription events consumed.
   */
  protected synchronized boolean onEvent(final Consumer<SubscriptionEvent> consumer)
      throws Exception {
    if (shouldEmit() && !enrichedEvents.isEmpty()) {
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

  /**
   * @return {@code true} if there are subscription events consumed.
   */
  protected synchronized boolean onEvent(
      final @NonNull EnrichedEvent event, final Consumer<SubscriptionEvent> consumer)
      throws Exception {
    if (event instanceof TabletInsertionEvent) {
      onTabletInsertionEvent((TabletInsertionEvent) event);
      enrichedEvents.add(event);
    } else if (event instanceof TsFileInsertionEvent) {
      onTsFileInsertionEvent((TsFileInsertionEvent) event);
      enrichedEvents.add(event);
    } else {
      LOGGER.warn(
          "SubscriptionPipeEventBatch {} ignore EnrichedEvent {} when batching.", this, event);
    }
    return onEvent(consumer);
  }

  /////////////////////////////// utility ///////////////////////////////

  protected abstract void onTabletInsertionEvent(final TabletInsertionEvent event);

  protected abstract void onTsFileInsertionEvent(final TsFileInsertionEvent event);

  protected abstract boolean shouldEmit();

  protected abstract List<SubscriptionEvent> generateSubscriptionEvents() throws Exception;

  /////////////////////////////// stringify ///////////////////////////////

  protected Map<String, String> coreReportMessage() {
    final Map<String, String> result = new HashMap<>();
    result.put("regionId", String.valueOf(regionId));
    result.put("prefetchingQueue", prefetchingQueue.coreReportMessage().toString());
    result.put("maxDelayInMs", String.valueOf(maxDelayInMs));
    result.put("maxBatchSizeInBytes", String.valueOf(maxBatchSizeInBytes));
    // omit subscription events here
    result.put("enrichedEvents", formatEnrichedEvents(enrichedEvents, 4));
    return result;
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

  //////////////////////////// APIs provided for metric framework ////////////////////////////

  public int getPipeEventCount() {
    return enrichedEvents.size();
  }
}
