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
import org.apache.iotdb.db.subscription.event.batch.SubscriptionPipeTsFileEventBatch;
import org.apache.iotdb.db.subscription.event.pipe.SubscriptionPipeTsFilePlainEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.rpc.subscription.payload.poll.FileInitPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.FilePiecePayload;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponse;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponseType;

import com.google.common.collect.ImmutableSet;
import org.apache.tsfile.utils.Pair;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class SubscriptionPrefetchingTsFileQueue extends SubscriptionPrefetchingQueue {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SubscriptionPrefetchingTsFileQueue.class);

  private static final int BATCH_MAX_DELAY_IN_MS =
      SubscriptionConfig.getInstance().getSubscriptionPrefetchTsFileBatchMaxDelayInMs();
  private static final long BATCH_MAX_SIZE_IN_BYTES =
      SubscriptionConfig.getInstance().getSubscriptionPrefetchTsFileBatchMaxSizeInBytes();

  // <consumer id, commit context> -> event
  private final Map<Pair<String, SubscriptionCommitContext>, SubscriptionEvent>
      inFlightSubscriptionEventMap;

  private final AtomicReference<SubscriptionPipeTsFileEventBatch> currentBatchRef =
      new AtomicReference<>();

  public SubscriptionPrefetchingTsFileQueue(
      final String brokerId,
      final String topicName,
      final SubscriptionBlockingPendingQueue inputPendingQueue) {
    super(brokerId, topicName, inputPendingQueue);

    this.inFlightSubscriptionEventMap = new ConcurrentHashMap<>();
    this.currentBatchRef.set(
        new SubscriptionPipeTsFileEventBatch(this, BATCH_MAX_DELAY_IN_MS, BATCH_MAX_SIZE_IN_BYTES));
  }

  @Override
  public void cleanup() {
    super.cleanup();

    // clean up events in inFlightSubscriptionEventMap
    // TODO: consider new events after cleaning up
    inFlightSubscriptionEventMap.values().forEach(SubscriptionEvent::cleanup);
    inFlightSubscriptionEventMap.clear();

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

  @Override
  public SubscriptionEvent poll(final String consumerId) {
    final SubscriptionEvent event = super.poll(consumerId);
    if (Objects.nonNull(event)) {
      inFlightSubscriptionEventMap.put(new Pair<>(consumerId, event.getCommitContext()), event);
    }

    return event;
  }

  public @NonNull SubscriptionEvent pollTsFile(
      final String consumerId,
      final SubscriptionCommitContext commitContext,
      final long writingOffset) {
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
              "SubscriptionPrefetchingTsFileQueue %s is currently not transferring any TsFile to consumer %s, commit context: %s, writing offset: %s",
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
          "Exception occurred when SubscriptionPrefetchingTsFileQueue {} transferring TsFile (with event {}) to consumer {}",
          this,
          event,
          consumerId,
          e);
      final String errorMessage =
          String.format(
              "Exception occurred when SubscriptionPrefetchingTsFileQueue %s transferring TsFile (with event %s) to consumer %s: %s",
              this, event, consumerId, e);
      return generateSubscriptionPollErrorResponse(errorMessage);
    }

    event.recordLastPolledConsumerId(consumerId);
    event.recordLastPolledTimestamp();
    return event;
  }

  /////////////////////////////// prefetch ///////////////////////////////

  @Override
  public void executePrefetch() {
    super.tryPrefetch(false);

    // Iterate on the snapshot of the key set, NOTE:
    // 1. Ignore entries added during iteration.
    // 2. For entries deleted by other threads during iteration, just check if the value is null.
    for (final Pair<String, SubscriptionCommitContext> pair :
        ImmutableSet.copyOf(inFlightSubscriptionEventMap.keySet())) {
      inFlightSubscriptionEventMap.compute(
          pair,
          (key, ev) -> {
            if (Objects.isNull(ev)) {
              return null;
            }

            // clean up committed event
            if (ev.isCommitted()) {
              ev.cleanup();
              return null; // remove this entry
            }

            // nack pollable event
            if (ev.pollable()) {
              ev.nack();
              return null; // remove this entry
            }

            // prefetch and serialize remaining subscription events
            // NOTE: Since the compute call for the same key is atomic and will be executed
            // serially, the current prefetch and serialize operations are safe.
            try {
              ev.prefetchRemainingResponses();
              ev.trySerializeRemainingResponses();
            } catch (final IOException ignored) {
            }

            return ev;
          });
    }
  }

  @Override
  protected boolean onEvent(final TabletInsertionEvent event) {
    return onEventInternal(event);
  }

  @Override
  protected boolean onEvent(final PipeTsFileInsertionEvent event) {
    final SubscriptionCommitContext commitContext = generateSubscriptionCommitContext();
    final SubscriptionEvent subscriptionEvent =
        new SubscriptionEvent(
            new SubscriptionPipeTsFilePlainEvent(event),
            new SubscriptionPollResponse(
                SubscriptionPollResponseType.FILE_INIT.getType(),
                new FileInitPayload(event.getTsFile().getName()),
                commitContext));
    uncommittedEvents.put(commitContext, subscriptionEvent); // before enqueuing the event
    prefetchingQueue.add(subscriptionEvent);
    return true;
  }

  @Override
  protected boolean onEvent() {
    return onEventInternal(null);
  }

  private boolean onEventInternal(@Nullable final TabletInsertionEvent event) {
    final AtomicBoolean result = new AtomicBoolean(false);
    currentBatchRef.getAndUpdate(
        (batch) -> {
          try {
            final List<SubscriptionEvent> evs = batch.onEvent(event);
            if (!evs.isEmpty()) {
              evs.forEach(
                  (ev) -> {
                    uncommittedEvents.put(ev.getCommitContext(), ev); // before enqueuing the event
                    prefetchingQueue.add(ev);
                  });
              result.set(true);
              return new SubscriptionPipeTsFileEventBatch(
                  this, BATCH_MAX_DELAY_IN_MS, BATCH_MAX_SIZE_IN_BYTES);
            }
            // If onEvent returns an empty list, one possibility is that the batch has already been
            // sealed, which would result in the failure of weakCompareAndSetVolatile to obtain the
            // most recent batch.
            return batch;
          } catch (final Exception e) {
            LOGGER.warn(
                "Exception occurred when SubscriptionPrefetchingTsFileQueue {} sealing TsFiles from batch {}",
                this,
                batch,
                e);
            return batch;
          }
        });
    return result.get();
  }

  /////////////////////////////// commit ///////////////////////////////

  /**
   * @return the corresponding event if ack successfully, otherwise {@code null}
   */
  @Override
  public SubscriptionEvent ack(
      final String consumerId, final SubscriptionCommitContext commitContext) {
    final SubscriptionEvent event = super.ack(consumerId, commitContext);
    if (Objects.nonNull(event)) {
      inFlightSubscriptionEventMap.compute(
          new Pair<>(consumerId, commitContext),
          (key, ev) -> {
            if (Objects.nonNull(ev) && Objects.equals(commitContext, ev.getCommitContext())) {
              return null; // remove this entry
            }
            return ev;
          });
      return event;
    }
    return null;
  }

  /**
   * @return the corresponding event if nack successfully, otherwise {@code null}
   */
  @Override
  public SubscriptionEvent nack(
      final String consumerId, final SubscriptionCommitContext commitContext) {
    final SubscriptionEvent event = super.nack(consumerId, commitContext);
    if (Objects.nonNull(event)) {
      inFlightSubscriptionEventMap.compute(
          new Pair<>(consumerId, commitContext),
          (key, ev) -> {
            if (Objects.nonNull(ev) && Objects.equals(commitContext, ev.getCommitContext())) {
              return null; // remove this entry
            }
            return ev;
          });
      return event;
    }
    return null;
  }

  /////////////////////////////// utility ///////////////////////////////

  private SubscriptionEvent generateSubscriptionPollErrorResponse(final String errorMessage) {
    // consider non-critical by default, meaning the client can retry
    return super.generateSubscriptionPollErrorResponse(errorMessage, false);
  }

  /////////////////////////////// stringify ///////////////////////////////

  @Override
  public String toString() {
    return "SubscriptionPrefetchingTsFileQueue" + this.coreReportMessage();
  }

  @Override
  protected Map<String, String> allReportMessage() {
    final Map<String, String> allReportMessage = super.allReportMessage();
    allReportMessage.put("inFlightSubscriptionEventMap", inFlightSubscriptionEventMap.toString());
    allReportMessage.put("currentBatch", currentBatchRef.toString());
    return allReportMessage;
  }
}
