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
import org.apache.iotdb.commons.subscription.config.SubscriptionConfig;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.db.subscription.event.batch.SubscriptionPipeTabletEventBatch;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class SubscriptionPrefetchingTabletQueue extends SubscriptionPrefetchingQueue {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SubscriptionPrefetchingTabletQueue.class);

  private static final int BATCH_MAX_DELAY_IN_MS =
      SubscriptionConfig.getInstance().getSubscriptionPrefetchTabletBatchMaxDelayInMs();
  private static final long BATCH_MAX_SIZE_IN_BYTES =
      SubscriptionConfig.getInstance().getSubscriptionPrefetchTabletBatchMaxSizeInBytes();

  private final AtomicReference<SubscriptionPipeTabletEventBatch> currentBatchRef =
      new AtomicReference<>();

  public SubscriptionPrefetchingTabletQueue(
      final String brokerId,
      final String topicName,
      final SubscriptionBlockingPendingQueue inputPendingQueue) {
    super(brokerId, topicName, inputPendingQueue);

    this.currentBatchRef.set(
        new SubscriptionPipeTabletEventBatch(this, BATCH_MAX_DELAY_IN_MS, BATCH_MAX_SIZE_IN_BYTES));
  }

  @Override
  public void cleanup() {
    super.cleanup();

    // clean up batch
    currentBatchRef.getAndUpdate(
        (batch) -> {
          if (Objects.nonNull(batch)) {
            batch.cleanup();
          }
          return null;
        });
  }

  /////////////////////////////// prefetch ///////////////////////////////

  @Override
  public void executePrefetch() {
    super.tryPrefetch(false);
    this.serializeEventsInQueue();
  }

  @Override
  protected boolean onEvent(final TabletInsertionEvent event) {
    return onEventInternal((EnrichedEvent) event);
  }

  @Override
  protected boolean onEvent(final PipeTsFileInsertionEvent event) {
    return onEventInternal(event);
  }

  @Override
  protected boolean onEvent() {
    return onEventInternal(null);
  }

  // missing synchronization can cause IoTDBSubscriptionSharingIT to lose data
  private synchronized boolean onEventInternal(@Nullable final EnrichedEvent event) {
    final AtomicBoolean result = new AtomicBoolean(false);
    currentBatchRef.getAndUpdate(
        (batch) -> {
          final List<SubscriptionEvent> evs = batch.onEvent(event);
          if (!evs.isEmpty()) {
            evs.forEach(
                (ev) -> {
                  uncommittedEvents.put(ev.getCommitContext(), ev); // before enqueuing the event
                  prefetchingQueue.add(ev);
                });
            result.set(true);
            return new SubscriptionPipeTabletEventBatch(
                this, BATCH_MAX_DELAY_IN_MS, BATCH_MAX_SIZE_IN_BYTES);
          }
          // If onEvent returns an empty list, one possibility is that the batch has already been
          // sealed, which would result in the failure of weakCompareAndSetVolatile to obtain the
          // most recent batch.
          return batch;
        });
    return result.get();
  }

  /**
   * serialize uncommitted and pollable events in {@link
   * SubscriptionPrefetchingQueue#prefetchingQueue}
   */
  private void serializeEventsInQueue() {
    final long size = prefetchingQueue.size();
    long count = 0;

    SubscriptionEvent event;
    try {
      while (count++ < size // limit control
          && Objects.nonNull(
              event =
                  prefetchingQueue.poll(
                      SubscriptionConfig.getInstance().getSubscriptionSerializeMaxBlockingTimeMs(),
                      TimeUnit.MILLISECONDS))) {
        if (event.isCommitted()) {
          event.cleanup();
          continue;
        }
        // Serialize the uncommitted and pollable event.
        if (event.pollable()) {
          // No need to concern whether serialization is successful.
          event.trySerializeCurrentResponse();
        }
        // Re-enqueue the uncommitted event at the end of the queue.
        prefetchingQueue.add(event);
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.warn(
          "Subscription: SubscriptionPrefetchingTabletQueue {} interrupted while serializing events.",
          this,
          e);
    }
  }

  /////////////////////////////// stringify ///////////////////////////////

  @Override
  public String toString() {
    return "SubscriptionPrefetchingTabletQueue" + this.coreReportMessage();
  }

  @Override
  protected Map<String, String> allReportMessage() {
    final Map<String, String> allReportMessage = super.allReportMessage();
    allReportMessage.put("currentBatch", currentBatchRef.toString());
    return allReportMessage;
  }
}
