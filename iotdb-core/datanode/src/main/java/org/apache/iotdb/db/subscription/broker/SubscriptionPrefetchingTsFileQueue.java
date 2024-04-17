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
import org.apache.iotdb.rpc.subscription.payload.common.TsFileInfoMessagePayload;
import org.apache.iotdb.rpc.subscription.payload.common.TsFilePieceMessagePayload;
import org.apache.iotdb.rpc.subscription.payload.common.TsFileSealMessagePayload;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public class SubscriptionPrefetchingTsFileQueue extends SubscriptionPrefetchingQueue {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SubscriptionPrefetchingTsFileQueue.class);

  private final AtomicReference<SubscriptionEvent> eventRef;

  public SubscriptionPrefetchingTsFileQueue(
      final String brokerId,
      final String topicName,
      final BoundedBlockingPendingQueue<Event> inputPendingQueue) {
    super(brokerId, topicName, inputPendingQueue);

    this.eventRef = new AtomicReference<>();
  }

  @Override
  public synchronized SubscriptionEvent poll(final SubscriptionPollTimer timer) {
    if (Objects.nonNull(eventRef.get())) {
      return null;
    }

    Event event;
    while (Objects.nonNull(
        event = UserDefinedEnrichedEvent.maybeOf(inputPendingQueue.waitedPoll()))) {
      if (!(event instanceof PipeTsFileInsertionEvent)) {
        LOGGER.warn(
            "Subscription: SubscriptionPrefetchingTsFileQueue only support poll PipeTsFileInsertionEvent. Ignore {}.",
            event);
        continue;
      }

      final PipeTsFileInsertionEvent tsFileInsertionEvent = (PipeTsFileInsertionEvent) event;
      final SubscriptionCommitContext commitContext = generateSubscriptionCommitContext();

      final SubscriptionEvent subscriptionEvent =
          new SubscriptionEvent(
              Collections.singletonList(tsFileInsertionEvent),
              new SubscriptionPolledMessage(
                  SubscriptionPolledMessageType.TS_FILE_INFO.getType(),
                  new TsFileInfoMessagePayload(tsFileInsertionEvent.getTsFile().getName()),
                  commitContext));
      eventRef.set(subscriptionEvent);
      // don't allow commit now
      return subscriptionEvent;
    }

    return null;
  }

  public SubscriptionEvent pollTsFile(String fileName, long endWritingOffset) {
    final SubscriptionEvent event = eventRef.get();
    if (Objects.isNull(event)) {
      return null;
    }

    final List<EnrichedEvent> enrichedEvents = event.getEnrichedEvents();
    if (Objects.isNull(enrichedEvents) || enrichedEvents.size() != 1) {
      return null;
    }

    final PipeTsFileInsertionEvent tsFileInsertionEvent =
        (PipeTsFileInsertionEvent) enrichedEvents.get(0);
    if (Objects.isNull(tsFileInsertionEvent)) {
      return null;
    }

    final SubscriptionPolledMessage polledMessage = event.getMessage();
    if (Objects.isNull(polledMessage)) {
      return null;
    }

    final SubscriptionMessagePayload messagePayload = polledMessage.getMessagePayload();
    if (Objects.isNull(messagePayload)) {
      return null;
    }

    final SubscriptionCommitContext commitContext = polledMessage.getCommitContext();
    if (Objects.isNull(commitContext)) {
      return null;
    }

    final short messageType = polledMessage.getMessageType();
    if (SubscriptionPolledMessageType.isValidatedMessageType(messageType)) {
      switch (SubscriptionPolledMessageType.valueOf(messageType)) {
        case TS_FILE_INFO:
          // check file name
          if (!fileName.equals(((TsFileInfoMessagePayload) messagePayload).getFileName())) {
            LOGGER.warn(
                "inconsistent file name, current is {}, incoming is {}",
                fileName,
                ((TsFileInfoMessagePayload) messagePayload).getFileName());
            return null;
          }
          // check offset
          if (endWritingOffset != 0) {
            LOGGER.warn("reset file {} offset to {}", fileName, endWritingOffset);
          }
          break;
        case TS_FILE_PIECE:
          // check file name
          if (!fileName.equals(((TsFilePieceMessagePayload) messagePayload).getFileName())) {
            LOGGER.warn(
                "inconsistent file name, current is {}, incoming is {}",
                fileName,
                ((TsFilePieceMessagePayload) messagePayload).getFileName());
            return null;
          }
          // check offset
          if (endWritingOffset
              != ((TsFilePieceMessagePayload) messagePayload).getEndWritingOffset()) {
            LOGGER.warn("reset file {} offset to {}", fileName, endWritingOffset);
          }
          break;
        case TS_FILE_SEAL:
          LOGGER.warn("reset file {} offset to {}", fileName, endWritingOffset);
          uncommittedEvents.remove(commitContext);
          break;
        default:
          LOGGER.warn("unexpected message type: {}", messageType);
          return null;
      }
    } else {
      LOGGER.warn("unexpected message type: {}", messageType);
      return null;
    }

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

        eventRef.set(newEvent);
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
      eventRef.set(newEvent);

      // allow commit now
      uncommittedEvents.put(commitContext, newEvent);
      return newEvent;
    } catch (IOException e) {
      LOGGER.warn(e.getMessage());
    }

    return null;
  }

  @Override
  public void executePrefetch() {
    // do nothing now
  }

  void resetEventRef() {
    eventRef.set(null);
  }
}
