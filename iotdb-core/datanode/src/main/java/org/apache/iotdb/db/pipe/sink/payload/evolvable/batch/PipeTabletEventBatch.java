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

package org.apache.iotdb.db.pipe.sink.payload.evolvable.batch;

import org.apache.iotdb.commons.exception.pipe.PipeRuntimeOutOfMemoryCriticalException;
import org.apache.iotdb.commons.pipe.agent.task.progress.CommitterKey;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeTabletMemoryBlock;
import org.apache.iotdb.db.pipe.sink.protocol.thrift.async.IoTDBDataRegionAsyncSink;
import org.apache.iotdb.db.storageengine.dataregion.wal.exception.WALPipeException;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public abstract class PipeTabletEventBatch implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTabletEventBatch.class);
  private final long maxBatchSizeInBytes;

  protected final List<EnrichedEvent> events = new ArrayList<>();
  protected final TriLongConsumer recordMetric;

  private final int maxDelayInMs;
  private long firstEventProcessingTime = Long.MIN_VALUE;

  protected long totalBufferSize = 0;
  private final PipeTabletMemoryBlock allocatedMemoryBlock;
  private boolean shouldEmitOnMemoryPressure = false;

  protected volatile boolean isClosed = false;

  protected PipeTabletEventBatch(
      final int maxDelayInMs,
      final long requestMaxBatchSizeInBytes,
      final TriLongConsumer recordMetric) {
    this.maxDelayInMs = maxDelayInMs;

    // limit in buffer size
    this.maxBatchSizeInBytes = requestMaxBatchSizeInBytes;
    this.allocatedMemoryBlock =
        PipeDataNodeResourceManager.memory().forceAllocateForTabletWithRetry(0);
    if (recordMetric != null) {
      this.recordMetric = recordMetric;
    } else {
      this.recordMetric =
          (timeInterval, bufferSize, events) -> {
            // do nothing
          };
    }
  }

  /**
   * Try offer {@link Event} into batch if the given {@link Event} is not duplicated.
   *
   * @param event the given {@link Event}
   * @return {@code true} if the batch can be transferred
   */
  public synchronized boolean onEvent(final TabletInsertionEvent event)
      throws WALPipeException, IOException {
    if (isClosed || !(event instanceof EnrichedEvent)) {
      return false;
    }

    // The deduplication logic here is to avoid the accumulation of
    // the same event in a batch when retrying.
    if (events.isEmpty() || !Objects.equals(events.get(events.size() - 1), event)) {
      // We increase the reference count for this event to determine if the event may be released.
      if (((EnrichedEvent) event)
          .increaseReferenceCount(PipeTransferBatchReqBuilder.class.getName())) {

        final int previousEventsSize = events.size();
        final long previousTotalBufferSize = totalBufferSize;
        final boolean previousMemoryPressureState = shouldEmitOnMemoryPressure;
        final Object batchState = captureBatchState();

        try {
          if (constructBatch(event)) {
            events.add((EnrichedEvent) event);
            if (firstEventProcessingTime == Long.MIN_VALUE) {
              firstEventProcessingTime = System.currentTimeMillis();
            }
          } else {
            ((EnrichedEvent) event)
                .decreaseReferenceCount(PipeTransferBatchReqBuilder.class.getName(), true);
          }
        } catch (final Exception e) {
          try {
            rollbackBatchState(batchState);
          } catch (final Exception rollbackException) {
            e.addSuppressed(rollbackException);
          }

          // A failed constructBatch must not retain a partial payload or its memory reservation,
          // even when older events are already buffered in this batch.
          if (totalBufferSize != previousTotalBufferSize) {
            // Shrinking never needs to wait. This path still holds the batch monitor, so a
            // blocking resize here would recreate the same lock cycle as a failed append.
            PipeDataNodeResourceManager.memory()
                .tryResize(allocatedMemoryBlock, previousTotalBufferSize);
          }
          totalBufferSize = previousTotalBufferSize;
          shouldEmitOnMemoryPressure =
              previousMemoryPressureState
                  || (e instanceof PipeRuntimeOutOfMemoryCriticalException && !events.isEmpty());
          if (events.size() > previousEventsSize) {
            events.subList(previousEventsSize, events.size()).clear();
          }

          // If the event is not added to the batch, we need to decrease the reference count.
          ((EnrichedEvent) event)
              .decreaseReferenceCount(PipeTransferBatchReqBuilder.class.getName(), false);
          // Will cause a retry
          throw e;
        }
      } else {
        LOGGER.warn(DataNodePipeMessages.CANNOT_INCREASE_REFERENCE_COUNT_FOR_EVENT_IGNORE, event);
      }
    }

    return shouldEmit();
  }

  /**
   * Added an {@link TabletInsertionEvent} into batch.
   *
   * @param event the {@link TabletInsertionEvent} in batch
   * @return {@code true} if the event is retained by this batch, {@code false} if the event is
   *     consumed but produces no batched payload. If there are failure encountered, just throw
   *     exceptions and do not return {@code false} here.
   */
  protected abstract boolean constructBatch(final TabletInsertionEvent event)
      throws WALPipeException, IOException;

  /** Captures subclass payload state before constructing one event. */
  protected Object captureBatchState() {
    return null;
  }

  /** Restores subclass payload state after a failed event construction. */
  protected void rollbackBatchState(final Object state) {}

  protected void increaseTotalBufferSizeAndUpdateMemoryBlock(final long bufferSize) {
    if (bufferSize <= 0) {
      return;
    }

    final long newTotalBufferSize = Math.min(totalBufferSize + bufferSize, maxBatchSizeInBytes);
    final PipeMemoryManager memoryManager = PipeDataNodeResourceManager.memory();
    if (!memoryManager.tryResize(allocatedMemoryBlock, newTotalBufferSize)) {
      // Existing events must be emitted before this event is retried. Do not wait here: onEvent()
      // is called while both the request builder and this batch are locked.
      shouldEmitOnMemoryPressure = !events.isEmpty();
      throw new PipeRuntimeOutOfMemoryCriticalException(
          String.format(
              DataNodePipeMessages
                  .PIPE_EXCEPTION_FORCERESIZE_FAILED_TO_ALLOCATE_MEMORY_AFTER_D_RETRIES_TOTAL_8C6948BC,
              0,
              memoryManager.getTotalNonFloatingMemorySizeInBytes(),
              memoryManager.getUsedMemorySizeInBytes(),
              newTotalBufferSize - totalBufferSize));
    }
    totalBufferSize = newTotalBufferSize;
  }

  protected void releaseAllocatedMemoryBlock() {
    PipeDataNodeResourceManager.memory().forceResize(allocatedMemoryBlock, 0);
  }

  protected void clearBatchData() {}

  /** Close resources owned by a batch that will not be reused. */
  protected void closeBatchData() {
    clearBatchData();
  }

  public boolean shouldEmit() {
    if (events.isEmpty()) {
      return false;
    }

    final long diff = System.currentTimeMillis() - firstEventProcessingTime;
    if (shouldEmitOnMemoryPressure
        || totalBufferSize >= maxBatchSizeInBytes
        || diff >= maxDelayInMs) {
      recordMetric.accept(diff, totalBufferSize, events.size());
      return true;
    }
    return false;
  }

  public synchronized void onSuccess() {
    events.clear();
    try {
      clearBatchData();
    } finally {
      resetMemoryUsage();
    }
  }

  /**
   * Close a detached asynchronous batch after its event references have been handed to handlers or
   * a retry queue. Unlike {@link #close()}, this method does not release those references.
   */
  public synchronized void closeAfterEventTransfer() {
    events.clear();
    close();
  }

  @Override
  public synchronized void close() {
    if (isClosed) {
      return;
    }
    isClosed = true;

    try {
      clearEventsReferenceCount(PipeTabletEventBatch.class.getName());
    } finally {
      events.clear();
      try {
        closeBatchData();
      } finally {
        try {
          resetMemoryUsage();
        } finally {
          allocatedMemoryBlock.close();
        }
      }
    }
  }

  /**
   * Discard all events of the given pipe. This method only clears the reference count of the
   * events. If some events remain, cached batch data is kept unchanged for simplicity.
   */
  public synchronized void discardEventsOfPipe(
      final String pipeNameToDrop, final long creationTimeToDrop, final int regionId) {
    discardEventsOfPipe(new CommitterKey(pipeNameToDrop, creationTimeToDrop, regionId, -1));
  }

  public synchronized void discardEventsOfPipe(final CommitterKey committerKey) {
    final boolean hasDiscardedEvents =
        events.removeIf(
            event -> {
              if (isEventFromPipe(event, committerKey)) {
                event.clearReferenceCount(IoTDBDataRegionAsyncSink.class.getName());
                return true;
              }
              return false;
            });
    if (hasDiscardedEvents && events.isEmpty()) {
      clearBatchData();
      resetMemoryUsage();
    }
  }

  private void resetMemoryUsage() {
    totalBufferSize = 0;
    shouldEmitOnMemoryPressure = false;

    releaseAllocatedMemoryBlock();

    firstEventProcessingTime = Long.MIN_VALUE;
  }

  private static boolean isEventFromPipe(
      final EnrichedEvent event, final CommitterKey committerKey) {
    return committerKey.getPipeName().equals(event.getPipeName())
        && committerKey.getCreationTime() == event.getCreationTime()
        && committerKey.getRegionId() == event.getRegionId()
        && (committerKey.getRestartTimes() < 0 || committerKey.equals(event.getCommitterKey()));
  }

  public synchronized void decreaseEventsReferenceCount(
      final String holderMessage, final boolean shouldReport) {
    events.forEach(event -> event.decreaseReferenceCount(holderMessage, shouldReport));
  }

  private void clearEventsReferenceCount(final String holderMessage) {
    events.forEach(event -> event.clearReferenceCount(holderMessage));
  }

  public List<EnrichedEvent> deepCopyEvents() {
    return new ArrayList<>(events);
  }

  public boolean isEmpty() {
    return events.isEmpty();
  }

  @FunctionalInterface
  public interface TriLongConsumer {
    void accept(long l1, long l2, long l3);
  }
}
