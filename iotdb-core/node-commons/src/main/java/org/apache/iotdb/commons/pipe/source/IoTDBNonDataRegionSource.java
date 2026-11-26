/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.commons.pipe.source;

import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.HybridProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MetaProgressIndex;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.pipe.datastructure.pattern.IoTDBPipePatternOperations;
import org.apache.iotdb.commons.pipe.datastructure.pattern.PipePattern;
import org.apache.iotdb.commons.pipe.datastructure.queue.ConcurrentIterableLinkedQueue;
import org.apache.iotdb.commons.pipe.datastructure.queue.listening.AbstractPipeListeningQueue;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.event.PipeSnapshotEvent;
import org.apache.iotdb.commons.pipe.event.PipeWritePlanEvent;
import org.apache.iotdb.commons.pipe.event.ProgressReportEvent;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeExtractorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class IoTDBNonDataRegionSource extends IoTDBSource {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBNonDataRegionSource.class);

  protected IoTDBPipePatternOperations pipePattern;

  private List<PipeSnapshotEvent> historicalEvents = new LinkedList<>();
  // A fixed size initialized only when the historicalEvents are first
  // filled. Used only for metric framework.
  private int historicalEventsCount = 0;

  private ConcurrentIterableLinkedQueue<Event>.DynamicIterator iterator;
  private PipeSnapshotEvent currentSnapshotEvent;
  private final Deque<PipeWritePlanEvent> pendingEvents = new ArrayDeque<>();
  private ProgressIndex pendingEventsProgressIndex;

  // If close() is called, hasBeenClosed will be set to true even if the extractor is started again.
  // If the extractor is closed, it should not be started again. This is to avoid the case that
  // the extractor is closed and then be reused by processor.
  protected final AtomicBoolean hasBeenClosed = new AtomicBoolean(false);
  private final AtomicBoolean hasWarnedUnexpectedHybridProgressIndex = new AtomicBoolean(false);

  protected abstract AbstractPipeListeningQueue getListeningQueue();

  @Override
  public void customize(
      final PipeParameters parameters, final PipeExtractorRuntimeConfiguration configuration)
      throws Exception {
    super.customize(parameters, configuration);

    final PipePattern pattern = PipePattern.parsePipePatternFromSourceParameters(parameters);
    if (!(pattern instanceof IoTDBPipePatternOperations
        && (((IoTDBPipePatternOperations) pattern).isPrefixOrFullPath()))) {
      throw new IllegalArgumentException(
          String.format(
              "The path pattern %s is not valid for the source. Only prefix or full path is allowed.",
              pattern.getPattern()));
    }
    pipePattern = (IoTDBPipePatternOperations) pattern;
  }

  @Override
  public void start() throws Exception {
    if (hasBeenStarted.get() || hasBeenClosed.get()) {
      return;
    }

    final ProgressIndex progressIndex = pipeTaskMeta.getProgressIndex();
    warnIfUnexpectedHybridProgressIndex(progressIndex);
    final MetaProgressIndex metaProgressIndex = extractMetaProgressIndex(progressIndex);
    final long nextIndex =
        Objects.isNull(metaProgressIndex)
                // If the index is invalid, the queue is seen as cleared before and thus
                // needs snapshot re-transferring
                || !getListeningQueue().isGivenNextIndexValid(metaProgressIndex.getIndex() + 1)
            ? getNextIndexAfterSnapshot()
            : metaProgressIndex.getIndex() + 1;
    iterator = getListeningQueue().newIterator(nextIndex);
    super.start();
  }

  private long getNextIndexAfterSnapshot() {
    long nextIndex;
    if (needTransferSnapshot()) {
      nextIndex = findSnapshot(true);
      if (nextIndex == Long.MIN_VALUE) {
        triggerSnapshot();
        nextIndex = findSnapshot(false);
        if (nextIndex == Long.MIN_VALUE) {
          throw new PipeException("Cannot get the newest snapshot after triggering one.");
        }
      }
    } else {
      // This will listen to the newest element after the iterator is created
      // Mainly used for alter/deletion sync
      nextIndex = Long.MAX_VALUE;
    }
    return nextIndex;
  }

  private long findSnapshot(final boolean mayClear) {
    final Pair<Long, List<PipeSnapshotEvent>> queueTailIndex2Snapshots =
        getListeningQueue().findAvailableSnapshots(mayClear);
    final long nextIndex =
        Objects.nonNull(queueTailIndex2Snapshots.getLeft())
                && queueTailIndex2Snapshots.getLeft() != Long.MIN_VALUE
            ? queueTailIndex2Snapshots.getLeft()
            : Long.MIN_VALUE;
    historicalEvents = new LinkedList<>(queueTailIndex2Snapshots.getRight());
    historicalEventsCount = historicalEvents.size();
    return nextIndex;
  }

  protected abstract boolean needTransferSnapshot();

  protected abstract void triggerSnapshot();

  protected abstract boolean canSkipSnapshotPrivilegeCheck(final PipeSnapshotEvent event);

  protected abstract void initSnapshotGenerator(final PipeSnapshotEvent event)
      throws IOException, IllegalPathException;

  protected abstract boolean hasNextEventInCurrentSnapshot() throws Exception;

  protected abstract PipeWritePlanEvent getNextEventInCurrentSnapshot();

  private boolean hasNextSnapshotPlan() throws Exception {
    final boolean hasNext = hasNextEventInCurrentSnapshot();
    if (!hasNext) {
      currentSnapshotEvent = null;
    }
    return hasNext;
  }

  protected abstract List<PipeWritePlanEvent> trimRealtimeEventByPrivilege(
      final PipeWritePlanEvent event);

  @Override
  public EnrichedEvent supply() throws Exception {
    if (hasBeenClosed.get()) {
      return null;
    }

    // Delayed start
    // In schema region: to avoid pipe start is called when schema region is unready
    // In config region: to avoid triggering snapshot under a consensus write causing deadlock
    if (!hasBeenStarted.get()) {
      start();
      // Failed to start, due to sudden switch of schema leader
      // Simply return
      if (!hasBeenStarted.get()) {
        return null;
      }
    }

    if (!pendingEvents.isEmpty()) {
      return supplyPendingEvent();
    }

    PipeWritePlanEvent realtimeEvent =
        hasNextSnapshotPlan() ? getNextEventInCurrentSnapshot() : null;

    // Historical
    while (Objects.isNull(realtimeEvent) && !historicalEvents.isEmpty()) {
      final PipeSnapshotEvent historicalEvent =
          (PipeSnapshotEvent)
              historicalEvents
                  .remove(0)
                  .shallowCopySelfAndBindPipeTaskMetaForProgressReport(
                      pipeName,
                      creationTime,
                      pipeTaskMeta,
                      pipePattern,
                      Long.MIN_VALUE,
                      Long.MAX_VALUE);

      if (canSkipSnapshotPrivilegeCheck(historicalEvent)) {
        if (historicalEvents.isEmpty()) {
          historicalEvent.bindProgressIndex(new MetaProgressIndex(iterator.getNextIndex() - 1));
        }
        historicalEvent.increaseReferenceCount(IoTDBNonDataRegionSource.class.getName());
        bindSourceAuthentication(historicalEvent);
        confineHistoricalEventTransferTypes(historicalEvent);
        return historicalEvent;
      }

      currentSnapshotEvent = historicalEvent;
      initSnapshotGenerator(historicalEvent);
      if (hasNextSnapshotPlan()) {
        realtimeEvent = getNextEventInCurrentSnapshot();
      }
    }

    final boolean shouldBindIndex = historicalEvents.isEmpty() && !hasNextSnapshotPlan();

    // Realtime
    if (Objects.isNull(realtimeEvent)) {
      realtimeEvent = (PipeWritePlanEvent) iterator.next(getMaxBlockingTimeMs());
    }
    if (Objects.isNull(realtimeEvent)) {
      return null;
    }

    realtimeEvent = trimRealtimeEventByPipePattern(realtimeEvent).orElse(null);
    if (Objects.isNull(realtimeEvent)
        || (!isForwardingPipeRequests && realtimeEvent.isGeneratedByPipe())) {
      final ProgressReportEvent event =
          new ProgressReportEvent(pipeName, creationTime, pipeTaskMeta);
      if (shouldBindIndex) {
        event.bindProgressIndex(new MetaProgressIndex(iterator.getNextIndex() - 1));
      }
      event.increaseReferenceCount(IoTDBNonDataRegionSource.class.getName());
      return event;
    }

    for (final PipeWritePlanEvent trimmedEvent : trimRealtimeEventByPrivilege(realtimeEvent)) {
      if (isTypeListened(trimmedEvent)
          && (isForwardingPipeRequests || !trimmedEvent.isGeneratedByPipe())) {
        pendingEvents.addLast(trimmedEvent);
      }
    }
    if (pendingEvents.isEmpty()) {
      final ProgressReportEvent event =
          new ProgressReportEvent(pipeName, creationTime, pipeTaskMeta);
      if (shouldBindIndex) {
        event.bindProgressIndex(new MetaProgressIndex(iterator.getNextIndex() - 1));
      }
      event.increaseReferenceCount(IoTDBNonDataRegionSource.class.getName());
      return event;
    }

    pendingEventsProgressIndex =
        shouldBindIndex ? new MetaProgressIndex(iterator.getNextIndex() - 1) : null;
    return supplyPendingEvent();
  }

  private PipeWritePlanEvent supplyPendingEvent() {
    final PipeWritePlanEvent event =
        (PipeWritePlanEvent)
            pendingEvents
                .getFirst()
                .shallowCopySelfAndBindPipeTaskMetaForProgressReport(
                    pipeName,
                    creationTime,
                    pipeTaskMeta,
                    pipePattern,
                    Long.MIN_VALUE,
                    Long.MAX_VALUE);
    if (pendingEvents.size() == 1 && Objects.nonNull(pendingEventsProgressIndex)) {
      event.bindProgressIndex(pendingEventsProgressIndex);
    }
    event.increaseReferenceCount(IoTDBNonDataRegionSource.class.getName());
    bindSourceAuthentication(event);
    pendingEvents.removeFirst();
    if (pendingEvents.isEmpty()) {
      pendingEventsProgressIndex = null;
    }
    return event;
  }

  protected abstract long getMaxBlockingTimeMs();

  // The trimmed event shall be non-null.
  protected abstract Optional<PipeWritePlanEvent> trimRealtimeEventByPipePattern(
      final PipeWritePlanEvent event);

  protected abstract boolean isTypeListened(final PipeWritePlanEvent event);

  protected abstract void confineHistoricalEventTransferTypes(final PipeSnapshotEvent event);

  @Override
  public void close() throws Exception {
    getListeningQueue().returnIterator(iterator);
    historicalEvents.clear();
    currentSnapshotEvent = null;
    pendingEvents.clear();
    pendingEventsProgressIndex = null;
  }

  //////////////////////////// APIs provided for metric framework ////////////////////////////

  public long getUnTransferredEventCount() {
    if (Objects.isNull(pipeTaskMeta)) {
      return 0L;
    }
    return getUnTransferredEventCount(getListeningQueue());
  }

  /** Calculate metrics from a captured queue without looking it up again during cleanup. */
  protected long getUnTransferredEventCount(final AbstractPipeListeningQueue listeningQueue) {
    if (Objects.isNull(pipeTaskMeta) || Objects.isNull(listeningQueue)) {
      return 0L;
    }
    final ProgressIndex progressIndex = pipeTaskMeta.getProgressIndex();
    warnIfUnexpectedHybridProgressIndex(progressIndex);
    final MetaProgressIndex metaProgressIndex = extractMetaProgressIndex(progressIndex);
    return Objects.nonNull(metaProgressIndex)
        ? listeningQueue.getTailIndex() - metaProgressIndex.getIndex() - 1
        : listeningQueue.getSize() + historicalEventsCount;
  }

  private static MetaProgressIndex extractMetaProgressIndex(final ProgressIndex progressIndex) {
    return Objects.isNull(progressIndex)
        ? null
        : progressIndex.getProgressIndexByType(MetaProgressIndex.class).orElse(null);
  }

  private void warnIfUnexpectedHybridProgressIndex(final ProgressIndex progressIndex) {
    if (Objects.nonNull(progressIndex)
        && progressIndex.getProgressIndexByType(HybridProgressIndex.class).isPresent()
        && hasWarnedUnexpectedHybridProgressIndex.compareAndSet(false, true)) {
      LOGGER.warn(
          "Pipe {}@{} encountered an unexpected HybridProgressIndex in {}. Progress index: {}.",
          pipeName,
          creationTime,
          getClass().getSimpleName(),
          progressIndex);
    }
  }
}
