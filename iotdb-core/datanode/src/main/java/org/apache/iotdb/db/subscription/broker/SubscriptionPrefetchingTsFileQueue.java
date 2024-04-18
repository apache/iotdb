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
import org.apache.iotdb.commons.pipe.task.connection.BoundedBlockingPendingQueue;
import org.apache.iotdb.db.pipe.event.UserDefinedEnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.subscription.event.SubscriptionTsFileEvent;
import org.apache.iotdb.db.subscription.timer.SubscriptionPollTimer;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.rpc.subscription.payload.common.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.common.SubscriptionMessagePayload;
import org.apache.iotdb.rpc.subscription.payload.common.SubscriptionPolledMessage;
import org.apache.iotdb.rpc.subscription.payload.common.SubscriptionPolledMessageType;
import org.apache.iotdb.rpc.subscription.payload.common.TsFileErrorMessagePayload;
import org.apache.iotdb.rpc.subscription.payload.common.TsFileInitMessagePayload;
import org.apache.iotdb.rpc.subscription.payload.common.TsFilePieceMessagePayload;
import org.apache.iotdb.tsfile.utils.Pair;

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
      final BoundedBlockingPendingQueue<Event> inputPendingQueue) {
    super(brokerId, topicName, inputPendingQueue);

    this.consumerIdToCurrentEventMap = new ConcurrentHashMap<>();
  }

  @Override
  public SubscriptionTsFileEvent poll(final String consumerId, final SubscriptionPollTimer timer) {
    final SubscriptionTsFileEvent pollableEvent =
        getPollableOnTheFlySubscriptionTsFileEvent(consumerId);
    if (Objects.nonNull(pollableEvent)) {
      return pollableEvent;
    }

    Event event;
    while (Objects.nonNull(
        event = UserDefinedEnrichedEvent.maybeOf(inputPendingQueue.waitedPoll()))) {
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

  public @NonNull SubscriptionTsFileEvent pollTsFile(
      String consumerId, String fileName, long writingOffset) {
    // 1. Extract current event and check it
    final SubscriptionTsFileEvent event = consumerIdToCurrentEventMap.get(consumerId);
    if (Objects.isNull(event)) {
      final String errorMessage =
          String.format(
              "%s is currently not transferring any tsfile to consumer %s", this, consumerId);
      LOGGER.warn(errorMessage);
      return generateSubscriptionTsFileEventWithErrorMessage(errorMessage, false);
    }

    // check consumer id
    if (!Objects.equals(event.getLastPolledConsumerId(), consumerId)) {
      final String errorMessage =
          String.format(
              "inconsistent polled consumer id, current is %s, incoming is %s, prefetching queue: %s",
              event.getLastPolledConsumerId(), consumerId, this);
      LOGGER.warn(errorMessage);
      return generateSubscriptionTsFileEventWithErrorMessage(errorMessage, false);
    }

    final List<EnrichedEvent> enrichedEvents = event.getEnrichedEvents();
    final PipeTsFileInsertionEvent tsFileInsertionEvent =
        (PipeTsFileInsertionEvent) enrichedEvents.get(0);

    // check file name
    if (!Objects.equals(tsFileInsertionEvent.getTsFile().getName(), fileName)) {
      final String errorMessage =
          String.format(
              "inconsistent file name, current is %s, incoming is %s, prefetching queue: %s",
              tsFileInsertionEvent.getTsFile().getName(), fileName, this);
      LOGGER.warn(errorMessage);
      return generateSubscriptionTsFileEventWithErrorMessage(errorMessage, false);
    }

    final SubscriptionPolledMessage polledMessage = event.getMessage();
    final SubscriptionMessagePayload messagePayload = polledMessage.getMessagePayload();

    // 2. Check message type, file name and offset
    final short messageType = polledMessage.getMessageType();
    if (SubscriptionPolledMessageType.isValidatedMessageType(messageType)) {
      switch (SubscriptionPolledMessageType.valueOf(messageType)) {
        case TS_FILE_INIT:
          // check file name
          if (!Objects.equals(
              ((TsFileInitMessagePayload) messagePayload).getFileName(), fileName)) {
            final String errorMessage =
                String.format(
                    "inconsistent file name, current is %s, incoming is %s, prefetching queue: %s",
                    ((TsFileInitMessagePayload) messagePayload).getFileName(), fileName, this);
            LOGGER.warn(errorMessage);
            return generateSubscriptionTsFileEventWithErrorMessage(errorMessage, false);
          }
          // check offset
          if (writingOffset != 0) {
            LOGGER.warn("{} reset file {} offset to {}", this, fileName, writingOffset);
          }
          break;
        case TS_FILE_PIECE:
          // check file name
          if (!Objects.equals(
              ((TsFilePieceMessagePayload) messagePayload).getFileName(), fileName)) {
            final String errorMessage =
                String.format(
                    "inconsistent file name, current is %s, incoming is %s, prefetching queue: %s",
                    ((TsFilePieceMessagePayload) messagePayload).getFileName(), fileName, this);
            LOGGER.warn(errorMessage);
            return generateSubscriptionTsFileEventWithErrorMessage(errorMessage, false);
          }
          // check offset
          if (writingOffset
              != ((TsFilePieceMessagePayload) messagePayload).getNextWritingOffset()) {
            LOGGER.warn("{} reset file {} offset to {}", this, fileName, writingOffset);
          }
          break;
        case TS_FILE_SEAL:
          LOGGER.warn("{} reset file {} offset to {}", this, fileName, writingOffset);
          break;
        default:
          final String errorMessage = String.format("unexpected message type: %s", messageType);
          LOGGER.warn(errorMessage);
          return generateSubscriptionTsFileEventWithErrorMessage(errorMessage, false);
      }
    } else {
      final String errorMessage = String.format("unexpected message type: %s", messageType);
      LOGGER.warn(errorMessage);
      return generateSubscriptionTsFileEventWithErrorMessage(errorMessage, false);
    }

    // 3. Poll tsfile piece or tsfile seal
    return pollTsFile(consumerId, writingOffset, event);
  }

  private @NonNull SubscriptionTsFileEvent pollTsFile(
      String consumerId, long writingOffset, SubscriptionTsFileEvent event) {
    Pair<SubscriptionTsFileEvent, Boolean> newEventWithCommittable = event.matchNext(writingOffset);
    if (Objects.isNull(newEventWithCommittable)) {
      event.resetNext();
      try {
        newEventWithCommittable =
            event.generateSubscriptionTsFileEventWithPieceOrSealPayload(writingOffset);
      } catch (IOException e) {
        final String errorMessage =
            String.format(
                "IOException occurred when %s transferring tsfile to consumer %s: %s",
                this, consumerId, e.getMessage());
        LOGGER.warn(errorMessage);
        // assume retryable
        return generateSubscriptionTsFileEventWithErrorMessage(errorMessage, true);
      }
    }

    // remove outdated event
    consumerIdToCurrentEventMap.remove(consumerId);
    event.resetNext();

    // update current event
    final SubscriptionTsFileEvent newEvent = newEventWithCommittable.getLeft();
    consumerIdToCurrentEventMap.put(consumerId, newEvent);
    if (newEventWithCommittable.getRight()) {
      uncommittedEvents.put(newEvent.getMessage().getCommitContext(), newEvent);
    }

    newEvent.recordLastPolledConsumerId(consumerId);
    newEvent.recordLastPolledTimestamp();
    return newEvent;
  }

  @Override
  public void executePrefetch() {
    consumerIdToCurrentEventMap.values().forEach(SubscriptionTsFileEvent::prefetchNext);
    consumerIdToCurrentEventMap.values().forEach(SubscriptionTsFileEvent::serializeNext);
  }

  /////////////////////////////// utility ///////////////////////////////

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
            "{} is currently transferring tsfile (with event {}) to consumer {}.",
            this,
            currentEvent,
            entry.getKey());
        continue;
      }

      // uncommitted and pollable event

      // remove outdated event
      consumerIdToCurrentEventMap.remove(entry.getKey());
      currentEvent.resetNext();

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

  private SubscriptionTsFileEvent generateSubscriptionTsFileEventWithErrorMessage(
      final String errorMessage, final boolean retryable) {
    return new SubscriptionTsFileEvent(
        Collections.emptyList(),
        new SubscriptionPolledMessage(
            SubscriptionPolledMessageType.TS_FILE_ERROR.getType(),
            new TsFileErrorMessagePayload(errorMessage, retryable),
            super.generateInvalidSubscriptionCommitContext()));
  }
}
