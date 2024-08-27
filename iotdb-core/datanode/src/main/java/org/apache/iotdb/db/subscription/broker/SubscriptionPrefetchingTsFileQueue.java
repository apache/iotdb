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
              "SubscriptionPrefetchingTsFileQueue %s is currently not transferring any file to consumer %s, commit context: %s, writing offset: %s",
              this, consumerId, commitContext, writingOffset);
      LOGGER.warn(errorMessage);
      return generateSubscriptionPollErrorResponse(errorMessage);
    }

    // check consumer id
    if (!Objects.equals(event.getLastPolledConsumerId(), consumerId)) {
      final String errorMessage =
          String.format(
              "inconsistent polled consumer id, current: %s, incoming: %s, commit context: %s, writing offset: %s, prefetching queue: %s",
              event.getLastPolledConsumerId(), consumerId, commitContext, writingOffset, this);
      LOGGER.warn(errorMessage);
      return generateSubscriptionPollErrorResponse(errorMessage);
    }

    final SubscriptionPollResponse response = event.getCurrentResponse();
    final SubscriptionPollPayload payload = response.getPayload();

    // 2. Check previous response type, file name and offset
    final short responseType = response.getResponseType();
    if (!SubscriptionPollResponseType.isValidatedResponseType(responseType)) {
      final String errorMessage = String.format("unexpected response type: %s", responseType);
      LOGGER.warn(errorMessage);
      return generateSubscriptionPollErrorResponse(errorMessage);
    }

    final String fileName = event.getFileName();
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
          return generateSubscriptionPollErrorResponse(errorMessage);
        }
        // check offset
        if (writingOffset != 0) {
          final String errorMessage =
              String.format(
                  "inconsistent offset, current: %s, incoming: %s, consumer: %s, file name: %s, prefetching queue: %s",
                  0, writingOffset, consumerId, fileName, this);
          LOGGER.warn(errorMessage);
          return generateSubscriptionPollErrorResponse(errorMessage);
        }
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
          return generateSubscriptionPollErrorResponse(errorMessage);
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
          return generateSubscriptionPollErrorResponse(errorMessage);
        }
        break;
      case FILE_SEAL:
        {
          final String errorMessage =
              String.format(
                  "poll after sealing, consumer: %s, file name: %s, writing offset: %s, prefetching queue: %s",
                  consumerId, fileName, writingOffset, this);
          LOGGER.warn(errorMessage);
          return generateSubscriptionPollErrorResponse(errorMessage);
        }
      default:
        {
          final String errorMessage = String.format("unexpected response type: %s", responseType);
          LOGGER.warn(errorMessage);
          return generateSubscriptionPollErrorResponse(errorMessage);
        }
    }

    // 3. Poll tsfile piece or tsfile seal
    try {
      event.fetchNextResponse();
    } catch (final Exception e) {
      LOGGER.warn(
          "Exception occurred when SubscriptionPrefetchingTsFileQueue {} transferring file (with event {}) to consumer {}",
          this,
          event,
          consumerId,
          e);
      final String errorMessage =
          String.format(
              "Exception occurred when SubscriptionPrefetchingTsFileQueue %s transferring file (with event %s) to consumer %s: %s",
              this, event, consumerId, e);
      return generateSubscriptionPollErrorResponse(errorMessage);
    }

    event.recordLastPolledTimestamp();
    return event;
  }

  /////////////////////////////// prefetch ///////////////////////////////

  @Override
  protected boolean onEvent(final TsFileInsertionEvent event) {
    final SubscriptionCommitContext commitContext = generateSubscriptionCommitContext();
    final SubscriptionEvent ev =
        new SubscriptionEvent(
            new SubscriptionPipeTsFilePlainEvent((PipeTsFileInsertionEvent) event),
            new SubscriptionPollResponse(
                SubscriptionPollResponseType.FILE_INIT.getType(),
                new FileInitPayload(((PipeTsFileInsertionEvent) event).getTsFile().getName()),
                commitContext));
    super.enqueueEventToPrefetchingQueue(ev);
    return true;
  }

  /////////////////////////////// stringify ///////////////////////////////

  @Override
  public String toString() {
    return "SubscriptionPrefetchingTsFileQueue" + this.coreReportMessage();
  }
}
