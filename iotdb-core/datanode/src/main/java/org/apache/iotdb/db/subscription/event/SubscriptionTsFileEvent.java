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

package org.apache.iotdb.db.subscription.event;

import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.subscription.config.SubscriptionConfig;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
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
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public class SubscriptionTsFileEvent extends SubscriptionEvent {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionTsFileEvent.class);

  private final AtomicReference<Pair<SubscriptionTsFileEvent, Boolean>> nextEventWithCommittableRef;

  public SubscriptionTsFileEvent(
      final List<EnrichedEvent> enrichedEvents, final SubscriptionPollResponse response) {
    super(enrichedEvents, response);

    this.nextEventWithCommittableRef = new AtomicReference<>();
  }

  public void prefetchNext() {
    nextEventWithCommittableRef.getAndUpdate(
        (nextEventWithCommittable) -> {
          if (Objects.nonNull(nextEventWithCommittable)) {
            // prefetch recursively
            nextEventWithCommittable.getLeft().prefetchNext();
            return nextEventWithCommittable;
          }

          final SubscriptionPollResponse polledMessage = this.getResponse();
          final short responseType = polledMessage.getResponseType();
          final SubscriptionPollPayload eventPayload = polledMessage.getPayload();
          if (!SubscriptionPollResponseType.isValidatedResponseType(responseType)) {
            LOGGER.warn("unexpected response type: {}", responseType);
            return null;
          }

          switch (SubscriptionPollResponseType.valueOf(responseType)) {
            case FILE_INIT:
              try {
                return generateSubscriptionTsFileEventWithPieceOrSealPayload(0);
              } catch (final IOException e) {
                LOGGER.warn(
                    "IOException occurred when prefetching next SubscriptionTsFileEvent, current SubscriptionTsFileEvent: {}",
                    this,
                    e);
                return null;
              }
            case FILE_PIECE:
              try {
                return generateSubscriptionTsFileEventWithPieceOrSealPayload(
                    ((FilePiecePayload) eventPayload).getNextWritingOffset());
              } catch (final IOException e) {
                LOGGER.warn(
                    "IOException occurred when prefetching next SubscriptionTsFileEvent, current SubscriptionTsFileEvent: {}",
                    this,
                    e);
                return null;
              }
            case FILE_SEAL:
              // not need to prefetch
              return null;
            default:
              LOGGER.warn("unexpected message type: {}", responseType);
              return null;
          }
        });
  }

  public void serializeNext() {
    nextEventWithCommittableRef.getAndUpdate(
        (nextEventWithCommittable) -> {
          if (Objects.nonNull(nextEventWithCommittable)) {
            SubscriptionEventBinaryCache.getInstance()
                .trySerialize(nextEventWithCommittable.getLeft());
            // serialize recursively
            nextEventWithCommittable.getLeft().serializeNext();
            return nextEventWithCommittable;
          }

          return null;
        });
  }

  public Pair<@NonNull SubscriptionTsFileEvent, Boolean> matchOrResetNext(
      final long writingOffset) {
    return nextEventWithCommittableRef.getAndUpdate(
        (nextEventWithCommittable) -> {
          if (Objects.isNull(nextEventWithCommittable)) {
            return null;
          }

          final SubscriptionPollResponse polledMessage = this.getResponse();
          final short responseType = polledMessage.getResponseType();
          final SubscriptionPollPayload eventPayload = polledMessage.getPayload();
          if (!SubscriptionPollResponseType.isValidatedResponseType(responseType)) {
            LOGGER.warn("unexpected response type: {}", responseType);
            return null;
          }

          switch (SubscriptionPollResponseType.valueOf(responseType)) {
            case FILE_INIT:
              if (Objects.equals(writingOffset, 0)) {
                return nextEventWithCommittable;
              }
              // reset next SubscriptionTsFileEvent
              return null;
            case FILE_PIECE:
              if (Objects.equals(
                  writingOffset, ((FilePiecePayload) eventPayload).getNextWritingOffset())) {
                return nextEventWithCommittable;
              }
              // reset next SubscriptionTsFileEvent
              return null;
            case FILE_SEAL:
              return null;
            default:
              LOGGER.warn("unexpected message type: {}", responseType);
              return null;
          }
        });
  }

  public static SubscriptionTsFileEvent generateSubscriptionTsFileEventWithInitPayload(
      final PipeTsFileInsertionEvent tsFileInsertionEvent,
      final SubscriptionCommitContext commitContext) {
    return new SubscriptionTsFileEvent(
        Collections.singletonList(tsFileInsertionEvent),
        new SubscriptionPollResponse(
            SubscriptionPollResponseType.FILE_INIT.getType(),
            new FileInitPayload(tsFileInsertionEvent.getTsFile().getName()),
            commitContext));
  }

  public SubscriptionTsFileEvent generateSubscriptionTsFileEventWithInitPayload() {
    return generateSubscriptionTsFileEventWithInitPayload(
        (PipeTsFileInsertionEvent) this.getEnrichedEvents().get(0),
        this.getResponse().getCommitContext());
  }

  public @NonNull Pair<@NonNull SubscriptionTsFileEvent, Boolean>
      generateSubscriptionTsFileEventWithPieceOrSealPayload(final long writingOffset)
          throws IOException {
    final PipeTsFileInsertionEvent tsFileInsertionEvent =
        (PipeTsFileInsertionEvent) this.getEnrichedEvents().get(0);
    final SubscriptionCommitContext commitContext = this.getResponse().getCommitContext();

    final int readFileBufferSize =
        SubscriptionConfig.getInstance().getSubscriptionReadFileBufferSize();
    final byte[] readBuffer = new byte[readFileBufferSize];
    try (final RandomAccessFile reader =
        new RandomAccessFile(tsFileInsertionEvent.getTsFile(), "r")) {
      while (true) {
        reader.seek(writingOffset);
        final int readLength = reader.read(readBuffer);
        if (readLength == -1) {
          break;
        }

        final byte[] filePiece =
            readLength == readFileBufferSize
                ? readBuffer
                : Arrays.copyOfRange(readBuffer, 0, readLength);

        // generate subscription tsfile event with piece payload
        return new Pair<>(
            new SubscriptionTsFileEvent(
                Collections.singletonList(tsFileInsertionEvent),
                new SubscriptionPollResponse(
                    SubscriptionPollResponseType.FILE_PIECE.getType(),
                    new FilePiecePayload(
                        tsFileInsertionEvent.getTsFile().getName(),
                        writingOffset + readLength,
                        filePiece),
                    commitContext)),
            false);
      }

      // generate subscription tsfile event with seal payload
      return new Pair<>(
          new SubscriptionTsFileEvent(
              Collections.singletonList(tsFileInsertionEvent),
              new SubscriptionPollResponse(
                  SubscriptionPollResponseType.FILE_SEAL.getType(),
                  new FileSealPayload(
                      tsFileInsertionEvent.getTsFile().getName(),
                      tsFileInsertionEvent.getTsFile().length()),
                  commitContext)),
          true);
    }
  }

  @Override
  public void resetByteBuffer(final boolean recursive) {
    super.resetByteBuffer(recursive);
    if (recursive) {
      nextEventWithCommittableRef.getAndUpdate(
          (nextEventWithCommittable) -> {
            if (Objects.isNull(nextEventWithCommittable)) {
              return null;
            }
            // reset recursively
            SubscriptionEventBinaryCache.getInstance()
                .resetByteBuffer(nextEventWithCommittable.getLeft(), true);
            return nextEventWithCommittable;
          });
    }
  }
}
