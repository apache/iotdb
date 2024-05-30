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
import org.apache.iotdb.commons.pipe.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.db.pipe.event.UserDefinedEnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.subscription.event.SubscriptionTsFileEvent;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.rpc.subscription.payload.poll.ErrorPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.FileInitPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.FilePiecePayload;
import org.apache.iotdb.rpc.subscription.payload.poll.FileSealPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponse;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponseType;

import org.apache.tsfile.utils.Pair;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class SubscriptionPrefetchingTsFileQueue extends SubscriptionPrefetchingQueue {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SubscriptionPrefetchingTsFileQueue.class);

  private final Map<String, SubscriptionTsFileEvent> consumerIdToCurrentEventMap;

  public SubscriptionPrefetchingTsFileQueue(
      final String brokerId,
      final String topicName,
      final UnboundedBlockingPendingQueue<Event> inputPendingQueue) {
    super(brokerId, topicName, inputPendingQueue);

    this.consumerIdToCurrentEventMap = new ConcurrentHashMap<>();
  }

  @Override
  public SubscriptionTsFileEvent poll(final String consumerId) {
    if (hasUnPollableOnTheFlySubscriptionTsFileEvent(consumerId)) {
      return null;
    }

    final SubscriptionTsFileEvent pollableEvent =
        getPollableOnTheFlySubscriptionTsFileEvent(consumerId);
    if (Objects.nonNull(pollableEvent)) {
      return pollableEvent;
    }

    Event event;
    while (Objects.nonNull(
        event = UserDefinedEnrichedEvent.maybeOf(inputPendingQueue.waitedPoll()))) {
      if (event instanceof TabletInsertionEvent) {
        final String errorMessage =
            String.format(
                "A TabletInsertionEvent was pulled from topic %s which is formatted as TsFile by SubscriptionPrefetchingTsFileQueue %s. This event %s will be ignored. Please check the topic configuration.",
                topicName, this, event);
        LOGGER.warn(errorMessage);
        return generateSubscriptionPollErrorResponse(errorMessage, true);
      }

      if (!(event instanceof PipeTsFileInsertionEvent)) {
        LOGGER.warn(
            "Subscription: SubscriptionPrefetchingTsFileQueue {} only support poll PipeTsFileInsertionEvent. Ignore {}.",
            this,
            event);
        continue;
      }

      final PipeTsFileInsertionEvent tsFileInsertionEvent = (PipeTsFileInsertionEvent) event;
      final SubscriptionCommitContext commitContext = generateSubscriptionCommitContext();

      // update current event
      final SubscriptionTsFileEvent subscriptionEvent =
          SubscriptionTsFileEvent.generateSubscriptionTsFileEventWithInitPayload(
              tsFileInsertionEvent, commitContext);
      consumerIdToCurrentEventMap.put(consumerId, subscriptionEvent);

      subscriptionEvent.recordLastPolledConsumerId(consumerId);
      subscriptionEvent.recordLastPolledTimestamp();
      return subscriptionEvent;
    }

    return null;
  }

  public synchronized @NonNull SubscriptionTsFileEvent pollTsFile(
      final String consumerId, final String fileName, final long writingOffset) {
    // 1. Extract current event and check it
    final SubscriptionTsFileEvent event = consumerIdToCurrentEventMap.get(consumerId);
    if (Objects.isNull(event)) {
      final String errorMessage =
          String.format(
              "SubscriptionPrefetchingTsFileQueue %s is currently not transferring any TsFile to consumer %s, file name: %s, writing offset: %s",
              this, consumerId, fileName, writingOffset);
      LOGGER.warn(errorMessage);
      return generateSubscriptionPollErrorResponse(errorMessage);
    }

    if (event.isCommitted()) {
      consumerIdToCurrentEventMap.remove(consumerId);
      final String errorMessage =
          String.format(
              "SubscriptionEvent %s related to TsFile is committed, consumer: %s, writing offset: %s, prefetching queue: %s",
              event, consumerId, writingOffset, this);
      LOGGER.warn(errorMessage);
      return generateSubscriptionPollErrorResponse(errorMessage);
    }

    // check consumer id
    if (!Objects.equals(event.getLastPolledConsumerId(), consumerId)) {
      final String errorMessage =
          String.format(
              "inconsistent polled consumer id, current: %s, incoming: %s, file name: %s, writing offset: %s, prefetching queue: %s",
              event.getLastPolledConsumerId(), consumerId, fileName, writingOffset, this);
      LOGGER.warn(errorMessage);
      return generateSubscriptionPollErrorResponse(errorMessage);
    }

    final List<EnrichedEvent> enrichedEvents = event.getEnrichedEvents();
    final PipeTsFileInsertionEvent tsFileInsertionEvent =
        (PipeTsFileInsertionEvent) enrichedEvents.get(0);

    // check file name
    if (!fileName.startsWith(tsFileInsertionEvent.getTsFile().getName())) {
      final String errorMessage =
          String.format(
              "inconsistent file name, current: %s, incoming: %s, consumer: %s, writing offset: %s, prefetching queue: %s",
              tsFileInsertionEvent.getTsFile().getName(),
              fileName,
              consumerId,
              writingOffset,
              this);
      LOGGER.warn(errorMessage);
      return generateSubscriptionPollErrorResponse(errorMessage);
    }

    final SubscriptionPollResponse response = event.getResponse();
    final SubscriptionPollPayload payload = response.getPayload();

    // 2. Check previous response type, file name and offset
    final short responseType = response.getResponseType();
    if (!SubscriptionPollResponseType.isValidatedResponseType(responseType)) {
      final String errorMessage = String.format("unexpected response type: %s", responseType);
      LOGGER.warn(errorMessage);
      return generateSubscriptionPollErrorResponse(errorMessage);
    }

    switch (SubscriptionPollResponseType.valueOf(responseType)) {
      case FILE_INIT:
        // check file name
        if (!fileName.startsWith(((FileInitPayload) payload).getFileName())) {
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
          LOGGER.warn(
              "SubscriptionPrefetchingTsFileQueue {} set TsFile (with event {}) writing offset to {} for consumer {}",
              this,
              event,
              writingOffset,
              consumerId);
        }
        break;
      case FILE_PIECE:
        // check file name
        if (!fileName.startsWith(((FilePiecePayload) payload).getFileName())) {
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
          LOGGER.warn(
              "SubscriptionPrefetchingTsFileQueue {} set TsFile (with event {}) writing offset to {} for consumer {}",
              this,
              event,
              writingOffset,
              consumerId);
        }
        break;
      case FILE_SEAL:
        // check file name
        if (!fileName.startsWith(((FileSealPayload) payload).getFileName())) {
          final String errorMessage =
              String.format(
                  "inconsistent file name, current: %s, incoming: %s, consumer: %s, writing offset: %s, prefetching queue: %s",
                  ((FileSealPayload) payload).getFileName(),
                  fileName,
                  consumerId,
                  writingOffset,
                  this);
          LOGGER.warn(errorMessage);
          return generateSubscriptionPollErrorResponse(errorMessage);
        }

        LOGGER.warn(
            "SubscriptionPrefetchingTsFileQueue {} set TsFile (with event {}) writing offset to {} after transferring seal signal to consumer {}",
            this,
            event,
            writingOffset,
            consumerId);
        // mark uncommittable
        uncommittedEvents.remove(response.getCommitContext());
        break;
      default:
        final String errorMessage = String.format("unexpected response type: %s", responseType);
        LOGGER.warn(errorMessage);
        return generateSubscriptionPollErrorResponse(errorMessage);
    }

    // 3. Poll tsfile piece or tsfile seal
    return pollTsFile(consumerId, writingOffset, event);
  }

  private synchronized @NonNull SubscriptionTsFileEvent pollTsFile(
      final String consumerId, final long writingOffset, final SubscriptionTsFileEvent event) {
    Pair<SubscriptionTsFileEvent, Boolean> newEventWithCommittable =
        event.matchOrResetNext(writingOffset);
    if (Objects.isNull(newEventWithCommittable)) {
      try {
        newEventWithCommittable =
            event.generateSubscriptionTsFileEventWithPieceOrSealPayload(writingOffset);
      } catch (final IOException e) {
        final String errorMessage =
            String.format(
                "IOException occurred when SubscriptionPrefetchingTsFileQueue %s transferring TsFile (with event %s) to consumer %s: %s",
                this, event, consumerId, e);
        LOGGER.warn(errorMessage);
        return generateSubscriptionPollErrorResponse(errorMessage);
      }
    }

    // remove outdated event
    consumerIdToCurrentEventMap.remove(consumerId);

    // update current event
    final SubscriptionTsFileEvent newEvent = newEventWithCommittable.getLeft();
    consumerIdToCurrentEventMap.put(consumerId, newEvent);
    if (newEventWithCommittable.getRight()) {
      // mark committable
      uncommittedEvents.put(newEvent.getResponse().getCommitContext(), newEvent);
    }

    newEvent.recordLastPolledConsumerId(consumerId);
    newEvent.recordLastPolledTimestamp();
    return newEvent;
  }

  @Override
  public synchronized void executePrefetch() {
    consumerIdToCurrentEventMap.values().forEach(SubscriptionTsFileEvent::prefetchNext);
    consumerIdToCurrentEventMap.values().forEach(SubscriptionTsFileEvent::serializeNext);
  }

  /////////////////////////////// utility ///////////////////////////////

  private synchronized boolean hasUnPollableOnTheFlySubscriptionTsFileEvent(
      final String consumerId) {
    final SubscriptionTsFileEvent event = consumerIdToCurrentEventMap.get(consumerId);
    if (Objects.isNull(event)) {
      return false;
    }

    if (event.isCommitted()) {
      consumerIdToCurrentEventMap.remove(consumerId);
      return false;
    }

    if (!event.pollable()) {
      LOGGER.info(
          "SubscriptionPrefetchingTsFileQueue {} is currently transferring TsFile (with event {}) to consumer {}",
          this,
          event,
          consumerId);
      return true;
    }

    return false;
  }

  private synchronized SubscriptionTsFileEvent getPollableOnTheFlySubscriptionTsFileEvent(
      final String consumerId) {
    for (final Map.Entry<String, SubscriptionTsFileEvent> entry :
        consumerIdToCurrentEventMap.entrySet()) {
      final SubscriptionTsFileEvent currentEvent = entry.getValue();
      if (currentEvent.isCommitted()) {
        consumerIdToCurrentEventMap.remove(entry.getKey());
        continue;
      }

      if (!currentEvent.pollable()) {
        LOGGER.info(
            "SubscriptionPrefetchingTsFileQueue {} is currently transferring TsFile (with event {}) to consumer {}",
            this,
            currentEvent,
            entry.getKey());
        continue;
      }

      // uncommitted and pollable event

      // remove outdated event
      consumerIdToCurrentEventMap.remove(entry.getKey());

      // update current event
      final SubscriptionTsFileEvent newEvent =
          currentEvent.generateSubscriptionTsFileEventWithInitPayload();
      consumerIdToCurrentEventMap.put(consumerId, newEvent);

      newEvent.recordLastPolledConsumerId(consumerId);
      newEvent.recordLastPolledTimestamp();
      return newEvent;
    }

    return null;
  }

  private SubscriptionTsFileEvent generateSubscriptionPollErrorResponse(
      final String errorMessage, final boolean critical) {
    return new SubscriptionTsFileEvent(
        Collections.emptyList(),
        new SubscriptionPollResponse(
            SubscriptionPollResponseType.ERROR.getType(),
            new ErrorPayload(errorMessage, critical),
            super.generateInvalidSubscriptionCommitContext()));
  }

  private SubscriptionTsFileEvent generateSubscriptionPollErrorResponse(final String errorMessage) {
    // consider non-critical by default, meaning the client can retry
    return generateSubscriptionPollErrorResponse(errorMessage, false);
  }
}
