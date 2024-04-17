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
import org.apache.iotdb.commons.subscription.config.SubscriptionConfig;
import org.apache.iotdb.db.pipe.event.UserDefinedEnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.db.subscription.timer.SubscriptionPollTimer;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.rpc.subscription.payload.common.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.common.SubscriptionMessagePayload;
import org.apache.iotdb.rpc.subscription.payload.common.SubscriptionPolledMessage;
import org.apache.iotdb.rpc.subscription.payload.common.SubscriptionPolledMessageType;
import org.apache.iotdb.rpc.subscription.payload.common.TsFileErrorMessagePayload;
import org.apache.iotdb.rpc.subscription.payload.common.TsFileInitMessagePayload;
import org.apache.iotdb.rpc.subscription.payload.common.TsFilePieceMessagePayload;
import org.apache.iotdb.rpc.subscription.payload.common.TsFileSealMessagePayload;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class SubscriptionPrefetchingTsFileQueue extends SubscriptionPrefetchingQueue {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SubscriptionPrefetchingTsFileQueue.class);

  private final Map<String, SubscriptionEvent> consumerIdToCurrentEvents;

  public SubscriptionPrefetchingTsFileQueue(
      final String brokerId,
      final String topicName,
      final BoundedBlockingPendingQueue<Event> inputPendingQueue) {
    super(brokerId, topicName, inputPendingQueue);

    this.consumerIdToCurrentEvents = new ConcurrentHashMap<>();
  }

  @Override
  public SubscriptionEvent poll(final String consumerId, final SubscriptionPollTimer timer) {
    final SubscriptionEvent currentEvent = consumerIdToCurrentEvents.get(consumerId);
    if (Objects.nonNull(currentEvent)) {
      LOGGER.info(
          "{} is currently transferring tsfile (with event {}) to consumer {}.",
          this,
          currentEvent,
          consumerId);
      return null;
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

      final SubscriptionEvent subscriptionEvent =
          new SubscriptionEvent(
              Collections.singletonList(tsFileInsertionEvent),
              new SubscriptionPolledMessage(
                  SubscriptionPolledMessageType.TS_FILE_INIT.getType(),
                  new TsFileInitMessagePayload(tsFileInsertionEvent.getTsFile().getName()),
                  commitContext));
      consumerIdToCurrentEvents.put(consumerId, subscriptionEvent);
      // don't allow commit now
      subscriptionEvent.recordLastPolledConsumerId(consumerId);
      subscriptionEvent.recordLastPolledTimestamp();
      return subscriptionEvent;
    }

    return null;
  }

  public @NonNull SubscriptionEvent pollTsFile(
      String consumerId, String fileName, long endWritingOffset) {
    // 1. Extract the current event and inspect it
    final SubscriptionEvent event = consumerIdToCurrentEvents.get(consumerId);
    if (Objects.isNull(event)) {
      final String errorMessage =
          String.format(
              "%s is currently not transferring any tsfile to consumer %s", this, consumerId);
      LOGGER.warn(errorMessage);
      return generateSubscriptionEventWithTsFileErrorMessage(errorMessage, false);
    }

    // check consumer id
    if (!consumerId.equals(event.getLastPolledConsumerId())) {
      final String errorMessage =
          String.format(
              "inconsistent polled consumer id, current is %s, incoming is %s, prefetching queue: %s",
              event.getLastPolledConsumerId(), consumerId, this);
      LOGGER.warn(errorMessage);
      return generateSubscriptionEventWithTsFileErrorMessage(errorMessage, false);
    }

    final List<EnrichedEvent> enrichedEvents = event.getEnrichedEvents();
    if (Objects.isNull(enrichedEvents) || enrichedEvents.size() != 1) {
      final String errorMessage =
          String.format(
              "unexpected enrichedEvents: %s, prefetching queue: %s", enrichedEvents, this);
      LOGGER.warn(errorMessage);
      return generateSubscriptionEventWithTsFileErrorMessage(errorMessage, false);
    }

    final PipeTsFileInsertionEvent tsFileInsertionEvent =
        (PipeTsFileInsertionEvent) enrichedEvents.get(0);
    if (Objects.isNull(tsFileInsertionEvent)) {
      final String errorMessage =
          String.format(
              "unexpected tsFileInsertionEvent: %s, prefetching queue: %s",
              tsFileInsertionEvent, this);
      LOGGER.warn(errorMessage);
      return generateSubscriptionEventWithTsFileErrorMessage(errorMessage, false);
    }

    final SubscriptionPolledMessage polledMessage = event.getMessage();
    if (Objects.isNull(polledMessage)) {
      final String errorMessage =
          String.format("unexpected polledMessage: %s, prefetching queue: %s", polledMessage, this);
      LOGGER.warn(errorMessage);
      return generateSubscriptionEventWithTsFileErrorMessage(errorMessage, false);
    }

    final SubscriptionMessagePayload messagePayload = polledMessage.getMessagePayload();
    if (Objects.isNull(messagePayload)) {
      final String errorMessage =
          String.format(
              "unexpected messagePayload: %s, prefetching queue: %s", messagePayload, this);
      LOGGER.warn(errorMessage);
      return generateSubscriptionEventWithTsFileErrorMessage(errorMessage, false);
    }

    final SubscriptionCommitContext commitContext = polledMessage.getCommitContext();
    if (Objects.isNull(commitContext)) {
      final String errorMessage =
          String.format("unexpected commitContext: %s, prefetching queue: %s", commitContext, this);
      LOGGER.warn(errorMessage);
      return generateSubscriptionEventWithTsFileErrorMessage(errorMessage, false);
    }

    // 2. Check message type, file name and offset
    final short messageType = polledMessage.getMessageType();
    if (SubscriptionPolledMessageType.isValidatedMessageType(messageType)) {
      switch (SubscriptionPolledMessageType.valueOf(messageType)) {
        case TS_FILE_INIT:
          // check file name
          if (!fileName.equals(((TsFileInitMessagePayload) messagePayload).getFileName())) {
            final String errorMessage =
                String.format(
                    "inconsistent file name, current is %s, incoming is %s, prefetching queue: %s",
                    ((TsFileInitMessagePayload) messagePayload).getFileName(), fileName, this);
            LOGGER.warn(errorMessage);
            return generateSubscriptionEventWithTsFileErrorMessage(errorMessage, false);
          }
          // check offset
          if (endWritingOffset != 0) {
            LOGGER.warn("{} reset file {} offset to {}", this, fileName, endWritingOffset);
          }
          break;
        case TS_FILE_PIECE:
          // check file name
          if (!fileName.equals(((TsFilePieceMessagePayload) messagePayload).getFileName())) {
            final String errorMessage =
                String.format(
                    "inconsistent file name, current is %s, incoming is %s, prefetching queue: %s",
                    ((TsFilePieceMessagePayload) messagePayload).getFileName(), fileName, this);
            LOGGER.warn(errorMessage);
            return generateSubscriptionEventWithTsFileErrorMessage(errorMessage, false);
          }
          // check offset
          if (endWritingOffset
              != ((TsFilePieceMessagePayload) messagePayload).getEndWritingOffset()) {
            LOGGER.warn("{} reset file {} offset to {}", this, fileName, endWritingOffset);
          }
          break;
        case TS_FILE_SEAL:
          LOGGER.warn("{} reset file {} offset to {}", this, fileName, endWritingOffset);
          uncommittedEvents.remove(commitContext);
          break;
        default:
          final String errorMessage = String.format("unexpected message type: %s", messageType);
          LOGGER.warn(errorMessage);
          return generateSubscriptionEventWithTsFileErrorMessage(errorMessage, false);
      }
    } else {
      final String errorMessage = String.format("unexpected message type: %s", messageType);
      LOGGER.warn(errorMessage);
      return generateSubscriptionEventWithTsFileErrorMessage(errorMessage, false);
    }

    // 3. Poll tsfile piece or tsfile seal
    final int readFileBufferSize =
        SubscriptionConfig.getInstance().getSubscriptionReadFileBufferSize();
    final byte[] readBuffer = new byte[readFileBufferSize];
    try (final RandomAccessFile reader =
        new RandomAccessFile(tsFileInsertionEvent.getTsFile(), "r")) {
      while (true) {
        reader.seek(endWritingOffset);
        final int readLength = reader.read(readBuffer);
        if (readLength == -1) {
          break;
        }

        final byte[] filePiece =
            readLength == readFileBufferSize
                ? readBuffer
                : Arrays.copyOfRange(readBuffer, 0, readLength);

        // poll tsfile piece
        final SubscriptionEvent newEvent =
            new SubscriptionEvent(
                Collections.singletonList(tsFileInsertionEvent),
                new SubscriptionPolledMessage(
                    SubscriptionPolledMessageType.TS_FILE_PIECE.getType(),
                    new TsFilePieceMessagePayload(
                        fileName, endWritingOffset + readLength, filePiece),
                    commitContext));

        consumerIdToCurrentEvents.put(consumerId, newEvent);
        // don't allow commit now
        newEvent.recordLastPolledConsumerId(consumerId);
        newEvent.recordLastPolledTimestamp();
        return newEvent;
      }

      // poll tsfile seal
      final SubscriptionEvent newEvent =
          new SubscriptionEvent(
              Collections.singletonList(tsFileInsertionEvent),
              new SubscriptionPolledMessage(
                  SubscriptionPolledMessageType.TS_FILE_SEAL.getType(),
                  new TsFileSealMessagePayload(fileName, tsFileInsertionEvent.getTsFile().length()),
                  commitContext));

      consumerIdToCurrentEvents.put(consumerId, newEvent);
      // allow commit now
      uncommittedEvents.put(commitContext, newEvent);
      newEvent.recordLastPolledConsumerId(consumerId);
      newEvent.recordLastPolledTimestamp();
      return newEvent;
    } catch (IOException e) {
      final String errorMessage =
          String.format(
              "IOException errored when %s transferring tsfile (with event %s) to consumer %s: %s",
              this, event, consumerId, e.getMessage());
      LOGGER.warn(errorMessage);
      // allow retry
      return generateSubscriptionEventWithTsFileErrorMessage(errorMessage, true);
    }
  }

  @Override
  public void executePrefetch() {
    // do nothing now
  }

  /////////////////////////////// utility ///////////////////////////////

  private SubscriptionEvent generateSubscriptionEventWithTsFileErrorMessage(
      final String errorMessage, final boolean retryable) {
    return new SubscriptionEvent(
        Collections.emptyList(),
        new SubscriptionPolledMessage(
            SubscriptionPolledMessageType.TS_FILE_ERROR.getType(),
            new TsFileErrorMessagePayload(errorMessage, retryable),
            super.generateInvalidSubscriptionCommitContext()));
  }
}
