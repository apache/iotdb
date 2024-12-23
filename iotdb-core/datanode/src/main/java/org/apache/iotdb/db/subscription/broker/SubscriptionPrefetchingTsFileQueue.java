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
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.subscription.agent.SubscriptionAgent;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.db.subscription.event.pipe.SubscriptionPipeTsFilePlainEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.rpc.subscription.payload.poll.FileInitPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.FilePiecePayload;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponse;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponseType;

import org.apache.tsfile.utils.Pair;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class SubscriptionPrefetchingTsFileQueue extends SubscriptionPrefetchingQueue {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SubscriptionPrefetchingTsFileQueue.class);

  public SubscriptionPrefetchingTsFileQueue(
      final String brokerId,
      final String topicName,
      final SubscriptionBlockingPendingQueue inputPendingQueue,
      final AtomicLong commitIdGenerator) {
    super(
        brokerId,
        topicName,
        inputPendingQueue,
        commitIdGenerator,
        SubscriptionConfig.getInstance().getSubscriptionPrefetchTsFileBatchMaxDelayInMs(),
        SubscriptionConfig.getInstance().getSubscriptionPrefetchTsFileBatchMaxSizeInBytes());
  }

  /////////////////////////////// poll ///////////////////////////////

  public SubscriptionEvent pollTsFile(
      final String consumerId,
      final SubscriptionCommitContext commitContext,
      final long writingOffset) {
    acquireReadLock();
    try {
      return isClosed() ? null : pollTsFileInternal(consumerId, commitContext, writingOffset);
    } finally {
      releaseReadLock();
    }
  }

  public @NonNull SubscriptionEvent pollTsFileInternal(
      final String consumerId,
      final SubscriptionCommitContext commitContext,
      final long writingOffset) {
    final AtomicReference<SubscriptionEvent> eventRef = new AtomicReference<>();
    inFlightEvents.compute(
        new Pair<>(consumerId, commitContext),
        (key, ev) -> {
          // 1. Extract current event and check it
          if (Objects.isNull(ev)) {
            final String errorMessage =
                String.format(
                    "SubscriptionPrefetchingTsFileQueue %s is currently not transferring any file to consumer %s, commit context: %s, writing offset: %s",
                    this, consumerId, commitContext, writingOffset);
            LOGGER.warn(errorMessage);
            eventRef.set(generateSubscriptionPollErrorResponse(errorMessage));
            return null;
          }

          if (ev.isCommitted()) {
            ev.cleanUp();
            final String errorMessage =
                String.format(
                    "outdated poll request after commit, consumer id: %s, commit context: %s, writing offset: %s, prefetching queue: %s",
                    consumerId, commitContext, writingOffset, this);
            LOGGER.warn(errorMessage);
            eventRef.set(generateSubscriptionPollErrorResponse(errorMessage));
            return null; // remove this entry
          }

          // check consumer id
          if (!Objects.equals(ev.getLastPolledConsumerId(), consumerId)) {
            final String errorMessage =
                String.format(
                    "inconsistent polled consumer id, current: %s, incoming: %s, commit context: %s, writing offset: %s, prefetching queue: %s",
                    ev.getLastPolledConsumerId(), consumerId, commitContext, writingOffset, this);
            LOGGER.warn(errorMessage);
            eventRef.set(generateSubscriptionPollErrorResponse(errorMessage));
            return ev;
          }

          final SubscriptionPollResponse response = ev.getCurrentResponse();
          final SubscriptionPollPayload payload = response.getPayload();

          // 2. Check previous response type, file name and offset
          final short responseType = response.getResponseType();
          if (!SubscriptionPollResponseType.isValidatedResponseType(responseType)) {
            final String errorMessage = String.format("unexpected response type: %s", responseType);
            LOGGER.warn(errorMessage);
            eventRef.set(generateSubscriptionPollErrorResponse(errorMessage));
            return ev;
          }

          final String fileName = ev.getFileName();
          switch (SubscriptionPollResponseType.valueOf(responseType)) {
            case FILE_INIT:
              // check file name
              if (!Objects.equals(fileName, ((FileInitPayload) payload).getFileName())) {
                final String errorMessage =
                    String.format(
                        "inconsistent file name, current: %s, incoming: %s, consumer: %s, writing offset: %s, prefetching queue: %s",
                        ((FileInitPayload) payload).getFileName(),
                        fileName,
                        consumerId,
                        writingOffset,
                        this);
                LOGGER.warn(errorMessage);
                eventRef.set(generateSubscriptionPollErrorResponse(errorMessage));
                return ev;
              }
              // no need to check offset for resume from breakpoint
              break;
            case FILE_PIECE:
              // check file name
              if (!Objects.equals(fileName, ((FilePiecePayload) payload).getFileName())) {
                final String errorMessage =
                    String.format(
                        "inconsistent file name, current: %s, incoming: %s, consumer: %s, writing offset: %s, prefetching queue: %s",
                        ((FilePiecePayload) payload).getFileName(),
                        fileName,
                        consumerId,
                        writingOffset,
                        this);
                LOGGER.warn(errorMessage);
                eventRef.set(generateSubscriptionPollErrorResponse(errorMessage));
                return ev;
              }
              // check offset
              if (writingOffset != ((FilePiecePayload) payload).getNextWritingOffset()) {
                final String errorMessage =
                    String.format(
                        "inconsistent offset, current: %s, incoming: %s, consumer: %s, file name: %s, prefetching queue: %s",
                        ((FilePiecePayload) payload).getNextWritingOffset(),
                        writingOffset,
                        consumerId,
                        fileName,
                        this);
                LOGGER.warn(errorMessage);
                eventRef.set(generateSubscriptionPollErrorResponse(errorMessage));
                return ev;
              }
              break;
            case FILE_SEAL:
              {
                final String errorMessage =
                    String.format(
                        "poll after sealing, consumer: %s, file name: %s, writing offset: %s, prefetching queue: %s",
                        consumerId, fileName, writingOffset, this);
                LOGGER.warn(errorMessage);
                eventRef.set(generateSubscriptionPollErrorResponse(errorMessage));
                return ev;
              }
            default:
              {
                final String errorMessage =
                    String.format("unexpected response type: %s", responseType);
                LOGGER.warn(errorMessage);
                eventRef.set(generateSubscriptionPollErrorResponse(errorMessage));
                return ev;
              }
          }

          // 3. Poll tsfile piece or tsfile seal
          try {
            executeReceiverSubtask(
                () -> {
                  ev.fetchNextResponse(writingOffset);
                  return null;
                },
                SubscriptionAgent.receiver().remainingMs());
            ev.recordLastPolledTimestamp();
            eventRef.set(ev);
          } catch (final Exception e) {
            final String errorMessage =
                String.format(
                    "exception occurred when fetching next response: %s, consumer id: %s, commit context: %s, writing offset: %s, prefetching queue: %s",
                    e, consumerId, commitContext, writingOffset, this);
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
    final SubscriptionCommitContext commitContext = generateSubscriptionCommitContext();
    final SubscriptionEvent ev =
        new SubscriptionEvent(
            new SubscriptionPipeTsFilePlainEvent((PipeTsFileInsertionEvent) event),
            ((PipeTsFileInsertionEvent) event).getTsFile(),
            commitContext);
    super.enqueueEventToPrefetchingQueue(ev);
    return true;
  }

  /////////////////////////////// stringify ///////////////////////////////

  @Override
  public String toString() {
    return "SubscriptionPrefetchingTsFileQueue" + this.coreReportMessage();
  }
}
