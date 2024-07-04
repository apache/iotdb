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

import org.apache.iotdb.commons.pipe.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.commons.subscription.config.SubscriptionConfig;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.db.subscription.event.batch.SubscriptionPipeTsFileEventBatch;
import org.apache.iotdb.db.subscription.event.pipe.SubscriptionPipeTsFileBatchEvents;
import org.apache.iotdb.db.subscription.event.pipe.SubscriptionPipeTsFilePlainEvent;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.rpc.subscription.payload.poll.FileInitPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.FilePiecePayload;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponse;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponseType;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class SubscriptionPrefetchingTsFileQueue extends SubscriptionPrefetchingQueue {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SubscriptionPrefetchingTsFileQueue.class);

  private static final int BATCH_MAX_DELAY_IN_MS =
      SubscriptionConfig.getInstance().getSubscriptionPrefetchTsFileBatchMaxDelayInMs();
  private static final long BATCH_MAX_SIZE_IN_BYTES =
      SubscriptionConfig.getInstance().getSubscriptionPrefetchTsFileBatchMaxSizeInBytes();

  private final Map<String, SubscriptionEvent> consumerIdToSubscriptionEventMap;

  private final AtomicReference<SubscriptionPipeTsFileEventBatch> currentBatchRef =
      new AtomicReference<>();

  public SubscriptionPrefetchingTsFileQueue(
      final String brokerId,
      final String topicName,
      final UnboundedBlockingPendingQueue<Event> inputPendingQueue) {
    super(brokerId, topicName, inputPendingQueue);

    this.consumerIdToSubscriptionEventMap = new ConcurrentHashMap<>();
    this.currentBatchRef.set(
        new SubscriptionPipeTsFileEventBatch(BATCH_MAX_DELAY_IN_MS, BATCH_MAX_SIZE_IN_BYTES));
  }

  @Override
  public void cleanup() {
    super.cleanup();

    // clean up events in consumerIdToCurrentEventMap
    consumerIdToSubscriptionEventMap.values().forEach(SubscriptionEvent::cleanup);
    consumerIdToSubscriptionEventMap.clear();

    // clean up batch
    currentBatchRef.getAndUpdate(
        (batch) -> {
          if (Objects.nonNull(batch)) {
            batch.cleanup();
          }
          return null;
        });
  }

  @Override
  public SubscriptionEvent poll(final String consumerId) {
    if (hasUnPollableOnTheFlySubscriptionTsFileEvent(consumerId)) {
      return null;
    }

    final SubscriptionEvent pollableEvent = getPollableOnTheFlySubscriptionTsFileEvent(consumerId);
    if (Objects.nonNull(pollableEvent)) {
      return pollableEvent;
    }

    // At this point, the event that might be polled should not have been polled by any consumer.
    final SubscriptionEvent event = super.poll(consumerId);
    if (Objects.nonNull(event)) {
      consumerIdToSubscriptionEventMap.put(consumerId, event);
    }

    return event;
  }

  public synchronized @NonNull SubscriptionEvent pollTsFile(
      final String consumerId, final String fileName, final long writingOffset) {
    // 1. Extract current event and check it
    final SubscriptionEvent event = consumerIdToSubscriptionEventMap.get(consumerId);
    if (Objects.isNull(event)) {
      final String errorMessage =
          String.format(
              "SubscriptionPrefetchingTsFileQueue %s is currently not transferring any TsFile to consumer %s, file name: %s, writing offset: %s",
              this, consumerId, fileName, writingOffset);
      LOGGER.warn(errorMessage);
      return generateSubscriptionPollErrorResponse(errorMessage);
    }

    if (event.isCommitted()) {
      event.cleanup();
      consumerIdToSubscriptionEventMap.remove(consumerId);
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

    final File tsFile = event.getTsFile();

    // check file name
    if (!fileName.startsWith(tsFile.getName())) {
      final String errorMessage =
          String.format(
              "inconsistent file name, current: %s, incoming: %s, consumer: %s, writing offset: %s, prefetching queue: %s",
              tsFile.getName(), fileName, consumerId, writingOffset, this);
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
      final String errorMessage =
          String.format(
              "Exception occurred when SubscriptionPrefetchingTsFileQueue %s transferring TsFile (with event %s) to consumer %s: %s",
              this, event, consumerId, e);
      LOGGER.warn(errorMessage);
      return generateSubscriptionPollErrorResponse(errorMessage);
    }

    event.recordLastPolledConsumerId(consumerId);
    event.recordLastPolledTimestamp();
    return event;
  }

  @Override
  protected boolean onEvent(final TabletInsertionEvent event) {
    final AtomicBoolean result = new AtomicBoolean(false);
    currentBatchRef.getAndUpdate(
        (batch) -> {
          try {
            if (batch.onEvent(event)) {
              sealBatch(batch);
              result.set(true);
              return new SubscriptionPipeTsFileEventBatch(
                  BATCH_MAX_DELAY_IN_MS, BATCH_MAX_SIZE_IN_BYTES);
            }
            return batch;
          } catch (final Exception e) {
            LOGGER.warn(
                "Exception occurred when SubscriptionPrefetchingTsFileQueue {} sealing tsFiles from batch",
                this,
                e);
            return batch;
          }
        });
    return result.get();
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
  protected boolean trySealBatch() {
    final AtomicBoolean result = new AtomicBoolean(false);
    currentBatchRef.getAndUpdate(
        (batch) -> {
          try {
            if (batch.shouldEmit()) {
              sealBatch(batch);
              result.set(true);
              return new SubscriptionPipeTsFileEventBatch(
                  BATCH_MAX_DELAY_IN_MS, BATCH_MAX_SIZE_IN_BYTES);
            }
            return batch;
          } catch (final Exception e) {
            LOGGER.warn(
                "Exception occurred when SubscriptionPrefetchingTsFileQueue {} sealing TsFile from batch",
                this,
                e);
            return batch;
          }
        });
    return result.get();
  }

  private void sealBatch(final SubscriptionPipeTsFileEventBatch batch) throws Exception {
    final List<File> tsFiles = batch.sealTsFiles();
    final AtomicInteger referenceCount = new AtomicInteger(tsFiles.size());
    for (final File tsFile : tsFiles) {
      final SubscriptionCommitContext commitContext = generateSubscriptionCommitContext();
      final SubscriptionEvent subscriptionEvent =
          new SubscriptionEvent(
              new SubscriptionPipeTsFileBatchEvents(batch, tsFile, referenceCount),
              new SubscriptionPollResponse(
                  SubscriptionPollResponseType.FILE_INIT.getType(),
                  new FileInitPayload(tsFile.getName()),
                  commitContext));
      uncommittedEvents.put(commitContext, subscriptionEvent); // before enqueuing the event
      prefetchingQueue.add(subscriptionEvent);
    }
  }

  @Override
  public synchronized void executePrefetch() {
    tryPrefetch();

    // prefetch remaining subscription events based on {@link consumerIdToSubscriptionEventMap}
    for (final SubscriptionEvent event : consumerIdToSubscriptionEventMap.values()) {
      try {
        event.prefetchRemainingResponses();
        event.trySerializeRemainingResponses();
      } catch (final IOException ignored) {
      }
    }
  }

  /////////////////////////////// utility ///////////////////////////////

  private synchronized boolean hasUnPollableOnTheFlySubscriptionTsFileEvent(
      final String consumerId) {
    final SubscriptionEvent event = consumerIdToSubscriptionEventMap.get(consumerId);
    if (Objects.isNull(event)) {
      return false;
    }

    if (event.isCommitted()) {
      event.cleanup();
      consumerIdToSubscriptionEventMap.remove(consumerId);
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

  private synchronized SubscriptionEvent getPollableOnTheFlySubscriptionTsFileEvent(
      final String consumerId) {
    for (final Map.Entry<String, SubscriptionEvent> entry :
        consumerIdToSubscriptionEventMap.entrySet()) {
      final SubscriptionEvent event = entry.getValue();
      if (event.isCommitted()) {
        event.cleanup();
        consumerIdToSubscriptionEventMap.remove(entry.getKey());
        continue;
      }

      if (!event.pollable()) {
        continue;
      }

      // uncommitted and pollable event

      consumerIdToSubscriptionEventMap.remove(entry.getKey());

      event.resetCurrentResponseIndex();
      consumerIdToSubscriptionEventMap.put(consumerId, event);

      event.recordLastPolledConsumerId(consumerId);
      event.recordLastPolledTimestamp();
      return event;
    }

    return null;
  }

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
    allReportMessage.put(
        "consumerIdToSubscriptionEventMap", consumerIdToSubscriptionEventMap.toString());
    allReportMessage.put("currentBatch", currentBatchRef.toString());
    return allReportMessage;
  }
}
