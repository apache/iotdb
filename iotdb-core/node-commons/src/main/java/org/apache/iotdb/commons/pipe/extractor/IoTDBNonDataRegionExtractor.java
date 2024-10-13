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

package org.apache.iotdb.commons.pipe.extractor;

import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MetaProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.pipe.datastructure.pattern.IoTDBTreePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TreePattern;
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

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class IoTDBNonDataRegionExtractor extends IoTDBExtractor {

  protected IoTDBTreePattern pipePattern;

  private List<PipeSnapshotEvent> historicalEvents = new LinkedList<>();
  // A fixed size initialized only when the historicalEvents are first
  // filled. Used only for metric framework.
  private int historicalEventsCount = 0;

  private ConcurrentIterableLinkedQueue<Event>.DynamicIterator iterator;

  // If close() is called, hasBeenClosed will be set to true even if the extractor is started again.
  // If the extractor is closed, it should not be started again. This is to avoid the case that
  // the extractor is closed and then be reused by processor.
  protected final AtomicBoolean hasBeenClosed = new AtomicBoolean(false);

  protected abstract AbstractPipeListeningQueue getListeningQueue();

  @Override
  public void customize(
      final PipeParameters parameters, final PipeExtractorRuntimeConfiguration configuration)
      throws Exception {
    super.customize(parameters, configuration);

    final TreePattern pattern = TreePattern.parsePipePatternFromSourceParameters(parameters);
    if (!(pattern instanceof IoTDBTreePattern
        && (((IoTDBTreePattern) pattern).isPrefix()
            || ((IoTDBTreePattern) pattern).isFullPath()))) {
      throw new IllegalArgumentException(
          String.format(
              "The path pattern %s is not valid for the source. Only prefix or full path is allowed.",
              pattern.getPattern()));
    }
    pipePattern = (IoTDBTreePattern) pattern;
  }

  @Override
  public void start() throws Exception {
    if (hasBeenStarted.get() || hasBeenClosed.get()) {
      return;
    }

    final ProgressIndex progressIndex = pipeTaskMeta.getProgressIndex();
    final long nextIndex =
        progressIndex instanceof MinimumProgressIndex
                // If the index is invalid, the queue is seen as cleared before and thus
                // needs snapshot re-transferring
                || !getListeningQueue()
                    .isGivenNextIndexValid(((MetaProgressIndex) progressIndex).getIndex() + 1)
            ? getNextIndexAfterSnapshot()
            : ((MetaProgressIndex) progressIndex).getIndex() + 1;
    iterator = getListeningQueue().newIterator(nextIndex);
    super.start();
  }

  private long getNextIndexAfterSnapshot() {
    long nextIndex;
    if (needTransferSnapshot()) {
      nextIndex = findSnapshot();
      if (nextIndex == Long.MIN_VALUE) {
        triggerSnapshot();
        nextIndex = findSnapshot();
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

  private long findSnapshot() {
    final Pair<Long, List<PipeSnapshotEvent>> queueTailIndex2Snapshots =
        getListeningQueue().findAvailableSnapshots();
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

    // Historical
    if (!historicalEvents.isEmpty()) {
      final PipeSnapshotEvent historicalEvent =
          (PipeSnapshotEvent)
              historicalEvents
                  .remove(0)
                  .shallowCopySelfAndBindPipeTaskMetaForProgressReport(
                      pipeName,
                      creationTime,
                      pipeTaskMeta,
                      pipePattern,
                      null,
                      Long.MIN_VALUE,
                      Long.MAX_VALUE);

      if (historicalEvents.isEmpty()) {
        // We only report progress for the last snapshot event.
        historicalEvent.bindProgressIndex(new MetaProgressIndex(iterator.getNextIndex() - 1));
      }

      historicalEvent.increaseReferenceCount(IoTDBNonDataRegionExtractor.class.getName());
      // We allow to send the events with empty transferred types to make the last
      // event commit and report its progress
      confineHistoricalEventTransferTypes(historicalEvent);
      return historicalEvent;
    }

    // Realtime
    PipeWritePlanEvent realtimeEvent = (PipeWritePlanEvent) iterator.next(getMaxBlockingTimeMs());
    if (Objects.isNull(realtimeEvent)) {
      return null;
    }

    realtimeEvent = trimRealtimeEventByPipePattern(realtimeEvent).orElse(null);
    if (Objects.isNull(realtimeEvent)
        || !isTypeListened(realtimeEvent)
        || (!isForwardingPipeRequests && realtimeEvent.isGeneratedByPipe())) {
      final ProgressReportEvent event =
          new ProgressReportEvent(
              pipeName,
              creationTime,
              pipeTaskMeta,
              pipePattern,
              null,
              Long.MIN_VALUE,
              Long.MAX_VALUE);
      event.bindProgressIndex(new MetaProgressIndex(iterator.getNextIndex() - 1));
      event.increaseReferenceCount(IoTDBNonDataRegionExtractor.class.getName());
      return event;
    }

    realtimeEvent =
        (PipeWritePlanEvent)
            realtimeEvent.shallowCopySelfAndBindPipeTaskMetaForProgressReport(
                pipeName,
                creationTime,
                pipeTaskMeta,
                pipePattern,
                null,
                Long.MIN_VALUE,
                Long.MAX_VALUE);
    realtimeEvent.bindProgressIndex(new MetaProgressIndex(iterator.getNextIndex() - 1));
    realtimeEvent.increaseReferenceCount(IoTDBNonDataRegionExtractor.class.getName());
    return realtimeEvent;
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
  }

  //////////////////////////// APIs provided for metric framework ////////////////////////////

  public long getUnTransferredEventCount() {
    return !(pipeTaskMeta.getProgressIndex() instanceof MinimumProgressIndex)
        ? getListeningQueue().getTailIndex()
            - ((MetaProgressIndex) pipeTaskMeta.getProgressIndex()).getIndex()
            - 1
        : getListeningQueue().getSize() + historicalEventsCount;
  }
}
