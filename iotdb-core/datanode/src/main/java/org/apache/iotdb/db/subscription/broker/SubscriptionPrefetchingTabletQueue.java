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
import org.apache.iotdb.db.subscription.agent.SubscriptionAgent;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponse;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponseType;
import org.apache.iotdb.rpc.subscription.payload.poll.TabletsPayload;

import org.apache.tsfile.utils.Pair;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class SubscriptionPrefetchingTabletQueue extends SubscriptionPrefetchingQueue {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SubscriptionPrefetchingTabletQueue.class);

  public SubscriptionPrefetchingTabletQueue(
      final String brokerId,
      final String topicName,
      final SubscriptionBlockingPendingQueue inputPendingQueue,
      final AtomicLong commitIdGenerator) {
    super(
        brokerId,
        topicName,
        inputPendingQueue,
        commitIdGenerator,
        SubscriptionConfig.getInstance().getSubscriptionPrefetchTabletBatchMaxDelayInMs(),
        SubscriptionConfig.getInstance().getSubscriptionPrefetchTabletBatchMaxSizeInBytes());
  }

  /////////////////////////////// poll ///////////////////////////////

  public SubscriptionEvent pollTablets(
      final String consumerId, final SubscriptionCommitContext commitContext, final int offset) {
    acquireReadLock();
    try {
      return isClosed() ? null : pollTabletsInternal(consumerId, commitContext, offset);
    } finally {
      releaseReadLock();
    }
  }

  private @NonNull SubscriptionEvent pollTabletsInternal(
      final String consumerId, final SubscriptionCommitContext commitContext, final int offset) {
    final AtomicReference<SubscriptionEvent> eventRef = new AtomicReference<>();
    inFlightEvents.compute(
        new Pair<>(consumerId, commitContext),
        (key, ev) -> {
          // 1. Extract current event and check it
          if (Objects.isNull(ev)) {
            final String errorMessage =
                String.format(
                    "SubscriptionPrefetchingTabletQueue %s is currently not transferring any tablet to consumer %s, commit context: %s, offset: %s",
                    this, consumerId, commitContext, offset);
            LOGGER.warn(errorMessage);
            eventRef.set(generateSubscriptionPollErrorResponse(errorMessage));
            return null;
          }

          if (ev.isCommitted()) {
            ev.cleanUp();
            final String errorMessage =
                String.format(
                    "outdated poll request after commit, consumer id: %s, commit context: %s, offset: %s, prefetching queue: %s",
                    consumerId, commitContext, offset, this);
            LOGGER.warn(errorMessage);
            eventRef.set(generateSubscriptionPollErrorResponse(errorMessage));
            return null; // remove this entry
          }

          // check consumer id
          if (!Objects.equals(ev.getLastPolledConsumerId(), consumerId)) {
            final String errorMessage =
                String.format(
                    "inconsistent polled consumer id, current: %s, incoming: %s, commit context: %s, offset: %s, prefetching queue: %s",
                    ev.getLastPolledConsumerId(), consumerId, commitContext, offset, this);
            LOGGER.warn(errorMessage);
            eventRef.set(generateSubscriptionPollErrorResponse(errorMessage));
            return ev;
          }

          final SubscriptionPollResponse response = ev.getCurrentResponse();
          final SubscriptionPollPayload payload = response.getPayload();

          // 2. Check previous response type and offset
          final short responseType = response.getResponseType();
          if (!SubscriptionPollResponseType.isValidatedResponseType(responseType)) {
            final String errorMessage = String.format("unexpected response type: %s", responseType);
            LOGGER.warn(errorMessage);
            eventRef.set(generateSubscriptionPollErrorResponse(errorMessage));
            return ev;
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
                eventRef.set(generateSubscriptionPollErrorResponse(errorMessage));
                return ev;
              }
              break;
            default:
              {
                final String errorMessage =
                    String.format("unexpected response type: %s", responseType);
                LOGGER.warn(errorMessage);
                eventRef.set(generateSubscriptionPollErrorResponse(errorMessage));
                return ev;
              }
          }

          // 3. Poll next tablets
          try {
            executeReceiverSubtask(
                () -> {
                  ev.fetchNextResponse(offset);
                  return null;
                },
                SubscriptionAgent.receiver().remainingMs());
            ev.recordLastPolledTimestamp();
            eventRef.set(ev);
          } catch (final Exception e) {
            final String errorMessage =
                String.format(
                    "exception occurred when fetching next response: %s, consumer id: %s, commit context: %s, offset: %s, prefetching queue: %s",
                    e, consumerId, commitContext, offset, this);
            LOGGER.warn(errorMessage);
            eventRef.set(generateSubscriptionPollErrorResponse(errorMessage));
          }

          return ev;
        });

    return eventRef.get();
  }

  /////////////////////////////// prefetch ///////////////////////////////

  @Override
  protected boolean onEvent(final TsFileInsertionEvent event) {
    return batches.onEvent((EnrichedEvent) event, this::enqueueEventToPrefetchingQueue);
  }

  /////////////////////////////// stringify ///////////////////////////////

  @Override
  public String toString() {
    return "SubscriptionPrefetchingTabletQueue" + this.coreReportMessage();
  }
}
