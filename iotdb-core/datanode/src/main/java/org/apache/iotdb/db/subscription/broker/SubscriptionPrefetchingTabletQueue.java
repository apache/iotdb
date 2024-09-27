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

import org.apache.iotdb.commons.subscription.config.SubscriptionConfig;
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
    // 1. Extract current event and check it
    final SubscriptionEvent event =
        inFlightEvents.compute(
            new Pair<>(consumerId, commitContext),
            (key, ev) -> {
              if (Objects.nonNull(ev) && ev.isCommitted()) {
                ev.cleanUp();
                return null; // remove this entry
              }
              return ev;
            });

    if (Objects.isNull(event)) {
      final String errorMessage =
          String.format(
              "SubscriptionPrefetchingTabletQueue %s is currently not transferring any tablet to consumer %s, commit context: %s, offset: %s",
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
  protected boolean onEvent(final TsFileInsertionEvent event) {
    LOGGER.warn(
        "Subscription: SubscriptionPrefetchingTabletQueue {} ignore TsFileInsertionEvent {} when prefetching.",
        this,
        event);
    return false;
  }

  /////////////////////////////// stringify ///////////////////////////////

  @Override
  public String toString() {
    return "SubscriptionPrefetchingTabletQueue" + this.coreReportMessage();
  }
}
