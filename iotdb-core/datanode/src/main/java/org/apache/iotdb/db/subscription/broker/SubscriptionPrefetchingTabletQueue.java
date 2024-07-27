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
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponse;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponseType;
import org.apache.iotdb.rpc.subscription.payload.poll.TabletsPayload;

import org.apache.tsfile.utils.Pair;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
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

  /////////////////////////////// poll ///////////////////////////////

  public @NonNull SubscriptionEvent pollTablets(
      final String consumerId, final SubscriptionCommitContext commitContext, final int offset) {
    // 1. Extract current event and check it
    final SubscriptionEvent event =
        inFlightSubscriptionEventMap.compute(
            new Pair<>(consumerId, commitContext),
            (key, ev) -> {
              if (Objects.nonNull(ev) && ev.isCommitted()) {
                ev.cleanup();
                return null; // remove this entry
              }
              return ev;
            });

    if (Objects.isNull(event)) {
      final String errorMessage =
          String.format(
              "SubscriptionPrefetchingTsFileQueue %s is currently not transferring any TsFile to consumer %s, commit context: %s, offset: %s",
              this, consumerId, commitContext, offset);
      LOGGER.warn(errorMessage);
      return generateSubscriptionPollErrorResponse(errorMessage);
    }

    // check consumer id
    if (!Objects.equals(event.getLastPolledConsumerId(), consumerId)) {
      final String errorMessage =
          String.format(
              "inconsistent polled consumer id, current: %s, incoming: %s, commit context: %s, offset: %s, prefetching queue: %s",
              event.getLastPolledConsumerId(), consumerId, commitContext, offset, this);
      LOGGER.warn(errorMessage);
      return generateSubscriptionPollErrorResponse(errorMessage);
    }

    final SubscriptionPollResponse response = event.getCurrentResponse();
    final SubscriptionPollPayload payload = response.getPayload();

    // 2. Check previous response type and offset
    final short responseType = response.getResponseType();
    if (!SubscriptionPollResponseType.isValidatedResponseType(responseType)) {
      final String errorMessage = String.format("unexpected response type: %s", responseType);
      LOGGER.warn(errorMessage);
      return generateSubscriptionPollErrorResponse(errorMessage);
    }

    switch (SubscriptionPollResponseType.valueOf(responseType)) {
      case TABLETS:
        // check offset
        if (!Objects.equals(offset, ((TabletsPayload) payload).getNextOffset())) {
          final String errorMessage =
              String.format(
                  "inconsistent offset, current: %s, incoming: %s, consumer: %s, prefetching queue: %s",
                  ((TabletsPayload) payload).getNextOffset(), offset, consumerId, this);
          LOGGER.warn(errorMessage);
          return generateSubscriptionPollErrorResponse(errorMessage);
        }
        break;
      default:
        {
          final String errorMessage = String.format("unexpected response type: %s", responseType);
          LOGGER.warn(errorMessage);
          return generateSubscriptionPollErrorResponse(errorMessage);
        }
    }

    // 3. Poll next tablets
    try {
      event.fetchNextResponse();
    } catch (final Exception ignored) {
      // no exceptions will be thrown
    }

    event.recordLastPolledTimestamp();
    return event;
  }

  /////////////////////////////// prefetch ///////////////////////////////

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

  private boolean onEventInternal(@Nullable final EnrichedEvent event) {
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
