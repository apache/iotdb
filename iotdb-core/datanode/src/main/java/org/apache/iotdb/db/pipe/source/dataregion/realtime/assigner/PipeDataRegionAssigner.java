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

package org.apache.iotdb.db.pipe.source.dataregion.realtime.assigner;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.event.ProgressReportEvent;
import org.apache.iotdb.commons.pipe.metric.PipeEventCounter;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.pipe.consensus.deletion.DeletionResource;
import org.apache.iotdb.db.pipe.consensus.deletion.DeletionResourceManager;
import org.apache.iotdb.db.pipe.event.common.deletion.PipeDeleteDataNodeEvent;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeEvent;
import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeEventFactory;
import org.apache.iotdb.db.pipe.metric.overview.PipeDataNodeSinglePipeMetrics;
import org.apache.iotdb.db.pipe.metric.source.PipeAssignerMetrics;
import org.apache.iotdb.db.pipe.metric.source.PipeDataRegionEventCounter;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.PipeRealtimeDataRegionSource;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.matcher.CachedSchemaPatternMatcher;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.matcher.PipeDataRegionMatcher;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;

import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

public class PipeDataRegionAssigner implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeDataRegionAssigner.class);
  private static final AtomicLong ASSIGNER_EPOCH_GENERATOR = new AtomicLong(0);

  /**
   * The {@link PipeDataRegionMatcher} is used to match the event with the source based on the
   * pattern.
   */
  private final PipeDataRegionMatcher matcher;

  /** The {@link DisruptorQueue} is used to assign the event to the source. */
  private final DisruptorQueue disruptor;

  private final int dataRegionId;
  private final long assignerEpoch = ASSIGNER_EPOCH_GENERATOR.incrementAndGet();
  private final AtomicLong publishedDataGeneration = new AtomicLong(0);
  private final AtomicLong publicationFailureEpoch = new AtomicLong(0);
  private final AtomicLong completionInvalidationEpoch = new AtomicLong(0);
  private final ReentrantLock publicationLock = new ReentrantLock();

  private Boolean isTableModel;

  private volatile int listenToTsFileSourceCount = 0;
  private volatile int listenToInsertNodeSourceCount = 0;

  private final PipeEventCounter eventCounter = new PipeDataRegionEventCounter();
  private int inFlightPublishCount = 0;

  public int getDataRegionId() {
    return dataRegionId;
  }

  public PipeDataRegionAssigner(final int dataRegionId) {
    this.matcher = new CachedSchemaPatternMatcher();
    this.disruptor = new DisruptorQueue(dataRegionId, this::assignToSource, this::onAssignedHook);
    this.dataRegionId = dataRegionId;
    PipeAssignerMetrics.getInstance().register(this);

    final DataRegion dataRegion =
        StorageEngine.getInstance().getDataRegion(new DataRegionId(dataRegionId));
    if (Objects.nonNull(dataRegion)) {
      final String databaseName = dataRegion.getDatabaseName();
      if (Objects.nonNull(databaseName)) {
        isTableModel = PathUtils.isTableModelDatabase(databaseName);
      }
    }
  }

  public void publishToAssign(final PipeRealtimeEvent event) {
    publicationLock.lock();
    try {
      final EnrichedEvent innerEvent = event.getEvent();
      final boolean isDataEvent = !(innerEvent instanceof PipeHeartbeatEvent);
      if (isDataEvent) {
        publishedDataGeneration.incrementAndGet();
      }
      publishToAssignInternal(event, isDataEvent);
    } finally {
      publicationLock.unlock();
    }
  }

  public void publishDataEventToAssign(final Supplier<PipeRealtimeEvent> eventSupplier) {
    publishDataEventToAssign(eventSupplier, false);
  }

  public void publishInsertDataEventToAssign(final Supplier<PipeRealtimeEvent> eventSupplier) {
    publishDataEventToAssign(eventSupplier, true);
  }

  private void publishDataEventToAssign(
      final Supplier<PipeRealtimeEvent> eventSupplier, final boolean invalidateCompletionBarrier) {
    publicationLock.lock();
    try {
      publishedDataGeneration.incrementAndGet();
      if (invalidateCompletionBarrier) {
        completionInvalidationEpoch.incrementAndGet();
      }
      final PipeRealtimeEvent event;
      try {
        event = eventSupplier.get();
      } catch (final RuntimeException | Error e) {
        markPublicationFailed();
        throw e;
      }
      if (event != null) {
        publishToAssignInternal(event, true);
      } else {
        markPublicationFailed();
      }
    } finally {
      publicationLock.unlock();
    }
  }

  private void publishToAssignInternal(final PipeRealtimeEvent event, final boolean isDataEvent) {
    final EnrichedEvent innerEvent = event.getEvent();
    if (!event.increaseReferenceCount(PipeDataRegionAssigner.class.getName())) {
      if (isDataEvent) {
        markPublicationFailed();
      }
      LOGGER.warn(DataNodePipeMessages.THE_REFERENCE_COUNT_OF_THE_REALTIME_EVENT, event);
      return;
    }

    eventCounter.increaseEventCount(innerEvent);
    if (innerEvent instanceof PipeHeartbeatEvent) {
      ((PipeHeartbeatEvent) innerEvent).onPublished();
    }

    boolean shouldReleaseDirectly = false;
    boolean isPublished = false;
    try {
      if (innerEvent instanceof PipeHeartbeatEvent) {
        final PipeHeartbeatEvent heartbeatEvent = (PipeHeartbeatEvent) innerEvent;
        if (heartbeatEvent.isCompletionBarrier()) {
          heartbeatEvent.bindCompletionBarrier(assignerEpoch, publishedDataGeneration.get());
        }
      }

      synchronized (this) {
        if (disruptor.isClosed()) {
          shouldReleaseDirectly = true;
        } else {
          inFlightPublishCount++;
        }
      }

      if (!shouldReleaseDirectly) {
        try {
          isPublished = disruptor.publishOrDrop(event);
        } finally {
          synchronized (this) {
            inFlightPublishCount--;
            if (inFlightPublishCount == 0) {
              notifyAll();
            }
          }
        }
      }
    } catch (final RuntimeException | Error e) {
      if (isDataEvent) {
        markPublicationFailed();
      }
      throw e;
    }

    if (shouldReleaseDirectly || !isPublished) {
      if (isDataEvent) {
        markPublicationFailed();
      }
      onAssignedHook(event);
    }
  }

  private void markPublicationFailed() {
    publicationFailureEpoch.incrementAndGet();
  }

  /**
   * Advances the published data generation for a data event that was ignored before publication.
   * This deliberately keeps the current completion token valid: full-flush close callbacks may be
   * ignored when no source listens to TsFile events, but their generations still need to be covered
   * by the barrier for that flush.
   */
  public void invalidateCompletion() {
    invalidateCompletion(false);
  }

  /**
   * Advances the published data generation and invalidates the current completion token for an
   * ignored insert. The insert may create a working TsFile processor that the in-progress full
   * flush does not cover, so its barrier must not be published.
   */
  public void invalidateCompletionBarrier() {
    invalidateCompletion(true);
  }

  private void invalidateCompletion(final boolean invalidateCompletionBarrier) {
    publicationLock.lock();
    try {
      publishedDataGeneration.incrementAndGet();
      if (invalidateCompletionBarrier) {
        completionInvalidationEpoch.incrementAndGet();
      }
    } finally {
      publicationLock.unlock();
    }
  }

  /**
   * Atomically advances the generation, invalidates any prior token, and returns the token for a
   * new full flush. The corresponding completion barrier is accepted only if no insert invalidates
   * this token before the flush finishes.
   */
  public CompletionToken invalidateCompletionAndGetToken() {
    publicationLock.lock();
    try {
      publishedDataGeneration.incrementAndGet();
      return new CompletionToken(assignerEpoch, completionInvalidationEpoch.incrementAndGet());
    } finally {
      publicationLock.unlock();
    }
  }

  public boolean publishCompletionBarrier(final CompletionToken token) {
    publicationLock.lock();
    try {
      if (token == null
          || token.assignerEpoch != assignerEpoch
          || token.completionInvalidationEpoch != completionInvalidationEpoch.get()) {
        return false;
      }
      publishToAssignInternal(
          PipeRealtimeEventFactory.createCompletionBarrierEvent(dataRegionId), false);
      return true;
    } finally {
      publicationLock.unlock();
    }
  }

  public static final class CompletionToken {

    private final long assignerEpoch;
    private final long completionInvalidationEpoch;

    private CompletionToken(final long assignerEpoch, final long completionInvalidationEpoch) {
      this.assignerEpoch = assignerEpoch;
      this.completionInvalidationEpoch = completionInvalidationEpoch;
    }
  }

  private void onAssignedHook(final PipeRealtimeEvent realtimeEvent) {
    realtimeEvent.gcSchemaInfo();
    realtimeEvent.decreaseReferenceCount(PipeDataRegionAssigner.class.getName(), false);

    final EnrichedEvent innerEvent = realtimeEvent.getEvent();
    if (innerEvent instanceof PipeHeartbeatEvent) {
      ((PipeHeartbeatEvent) innerEvent).onAssigned();
    }

    eventCounter.decreaseEventCount(innerEvent);
  }

  private void assignToSource(
      final PipeRealtimeEvent event, final long sequence, final boolean endOfBatch) {
    if (disruptor.isClosed()) {
      return;
    }

    try {
      assignToSourceInternal(event);
    } catch (final RuntimeException | Error e) {
      if (!(event.getEvent() instanceof PipeHeartbeatEvent)) {
        markPublicationFailed();
      }
      throw e;
    }
  }

  private void assignToSourceInternal(final PipeRealtimeEvent event) {
    final Pair<Set<PipeRealtimeDataRegionSource>, Set<PipeRealtimeDataRegionSource>>
        matchedAndUnmatched = matcher.match(event);

    matchedAndUnmatched
        .getLeft()
        .forEach(
            source -> {
              if (disruptor.isClosed()) {
                return;
              }

              if (event.getEvent().isGeneratedByPipe() && !source.isForwardingPipeRequests()) {
                final ProgressReportEvent reportEvent =
                    new ProgressReportEvent(
                        source.getPipeName(), source.getCreationTime(), source.getPipeTaskMeta());
                reportEvent.bindProgressIndex(event.getProgressIndex());
                if (!reportEvent.increaseReferenceCount(PipeDataRegionAssigner.class.getName())) {
                  markPublicationFailed();
                  LOGGER.warn(
                      DataNodePipeMessages.THE_REFERENCE_COUNT_OF_THE_EVENT_CANNOT, reportEvent);
                  return;
                }
                source.extract(PipeRealtimeEventFactory.createRealtimeEvent(reportEvent));
                return;
              }

              final PipeRealtimeEvent copiedEvent =
                  event.shallowCopySelfAndBindPipeTaskMetaForProgressReport(
                      source.getPipeName(),
                      source.getCreationTime(),
                      source.getPipeTaskMeta(),
                      source.getTreePattern(),
                      source.getTablePattern(),
                      String.valueOf(source.getUserId()),
                      source.getUserName(),
                      source.getCliHostname(),
                      source.isSkipIfNoPrivileges(),
                      source.getRealtimeDataExtractionStartTime(),
                      source.getRealtimeDataExtractionEndTime());
              final EnrichedEvent innerEvent = copiedEvent.getEvent();

              if (innerEvent instanceof PipeHeartbeatEvent
                  && ((PipeHeartbeatEvent) innerEvent).isCompletionBarrier()) {
                ((PipeHeartbeatEvent) innerEvent)
                    .bindCompletionSource(source.getCompletionSourceId());
              }

              if (innerEvent instanceof PipeTsFileInsertionEvent) {
                final PipeTsFileInsertionEvent tsFileInsertionEvent =
                    (PipeTsFileInsertionEvent) innerEvent;
                tsFileInsertionEvent.bindTsFileDedupScopeID(source.getTsFileDedupScopeID());
                tsFileInsertionEvent.setTsFileParser(source.getTsFileParser());
                tsFileInsertionEvent.disableMod4NonTransferPipes(source.isShouldTransferModFile());
              }

              if (innerEvent instanceof PipeDeleteDataNodeEvent) {
                final PipeDeleteDataNodeEvent deleteDataNodeEvent =
                    (PipeDeleteDataNodeEvent) innerEvent;
                final DeletionResourceManager manager =
                    DeletionResourceManager.getInstance(source.getDataRegionId());
                // increase deletion resource's reference and bind real deleteEvent
                if (Objects.nonNull(manager)
                    && DeletionResource.isDeleteNodeGeneratedInLocalByIoTV2(
                        deleteDataNodeEvent.getDeleteDataNode())) {
                  deleteDataNodeEvent.setDeletionResource(
                      manager.getDeletionResource(
                          ((PipeDeleteDataNodeEvent) event.getEvent()).getDeleteDataNode()));
                }
              }

              if (!copiedEvent.increaseReferenceCount(PipeDataRegionAssigner.class.getName())) {
                if (!(event.getEvent() instanceof PipeHeartbeatEvent)) {
                  markPublicationFailed();
                }
                LOGGER.warn(
                    DataNodePipeMessages.THE_REFERENCE_COUNT_OF_THE_EVENT_CANNOT, copiedEvent);
                return;
              }
              source.extract(copiedEvent);
            });

    matchedAndUnmatched
        .getRight()
        .forEach(
            source -> {
              if (disruptor.isClosed()) {
                return;
              }

              final EnrichedEvent innerEvent = event.getEvent();
              if (innerEvent instanceof TabletInsertionEvent
                  || innerEvent instanceof TsFileInsertionEvent) {
                final ProgressReportEvent reportEvent =
                    new ProgressReportEvent(
                        source.getPipeName(), source.getCreationTime(), source.getPipeTaskMeta());
                reportEvent.bindProgressIndex(event.getProgressIndex());
                if (!reportEvent.increaseReferenceCount(PipeDataRegionAssigner.class.getName())) {
                  markPublicationFailed();
                  LOGGER.warn(
                      DataNodePipeMessages.THE_REFERENCE_COUNT_OF_THE_EVENT_CANNOT, reportEvent);
                  return;
                }
                source.extract(PipeRealtimeEventFactory.createRealtimeEvent(reportEvent));
              }
            });
  }

  public synchronized void startAssignTo(final PipeRealtimeDataRegionSource source) {
    PipeDataNodeSinglePipeMetrics.getInstance().register(source, this);
    try {
      matcher.register(source);
    } catch (final RuntimeException | Error e) {
      PipeDataNodeSinglePipeMetrics.getInstance().deregister(source, this);
      throw e;
    }
    if (source.isNeedListenToTsFile()) {
      listenToTsFileSourceCount++;
    }
    if (source.isNeedListenToInsertNode()) {
      listenToInsertNodeSourceCount++;
    }
    logSourceAssignmentChange("registered", source);
  }

  public synchronized void stopAssignTo(final PipeRealtimeDataRegionSource source) {
    matcher.deregister(source);
    PipeDataNodeSinglePipeMetrics.getInstance().deregister(source, this);
    if (source.isNeedListenToTsFile()) {
      listenToTsFileSourceCount--;
    }
    if (source.isNeedListenToInsertNode()) {
      listenToInsertNodeSourceCount--;
    }
    logSourceAssignmentChange("deregistered", source);
  }

  public boolean shouldListenToTsFile() {
    return listenToTsFileSourceCount > 0;
  }

  public boolean shouldListenToInsertNode() {
    return listenToInsertNodeSourceCount > 0;
  }

  public void invalidateCache() {
    matcher.invalidateCache();
  }

  public boolean notMoreSourceNeededToBeAssigned() {
    return matcher.getRegisterCount() == 0;
  }

  /**
   * Clear the matcher and disruptor. The method {@link PipeDataRegionAssigner#publishToAssign}
   * should not be used after calling this method.
   */
  @Override
  // use synchronized here for completely preventing reference count leaks under extreme thread
  // scheduling when closing
  public synchronized void close() {
    PipeAssignerMetrics.getInstance().deregister(dataRegionId);

    boolean interrupted = false;
    disruptor.closeInput();
    while (inFlightPublishCount > 0) {
      try {
        wait();
      } catch (final InterruptedException e) {
        interrupted = true;
        LOGGER.warn(
            DataNodePipeMessages
                .PIPE_LOG_INTERRUPTED_WHILE_WAITING_FOR_IN_FLIGHT_PUBLISHES_TO_FINISH_C8E3757B,
            dataRegionId);
      }
    }

    final long startTime = System.currentTimeMillis();
    disruptor.shutdown();
    matcher.clear();
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
    LOGGER.info(
        DataNodePipeMessages.PIPE_ASSIGNER_ON_DATA_REGION_SHUTDOWN_INTERNAL,
        dataRegionId,
        System.currentTimeMillis() - startTime);
  }

  public int getTabletInsertionEventCount() {
    return eventCounter.getTabletInsertionEventCount();
  }

  public int getTsFileInsertionEventCount() {
    return eventCounter.getTsFileInsertionEventCount();
  }

  public int getPipeHeartbeatEventCount() {
    return eventCounter.getPipeHeartbeatEventCount();
  }

  private void logSourceAssignmentChange(
      final String action, final PipeRealtimeDataRegionSource source) {
    LOGGER.info(
        DataNodePipeMessages
            .PIPE_LOG_PIPE_REALTIME_SOURCE_ON_DATA_REGION_LISTENTOTSFILE_LISTENTOINSERTNODE_A02E1552,
        source.getPipeName(),
        source.getCreationTime(),
        action,
        dataRegionId,
        source.isNeedListenToTsFile(),
        source.isNeedListenToInsertNode(),
        matcher.getRegisterCount(),
        listenToTsFileSourceCount,
        listenToInsertNodeSourceCount);
  }

  public Boolean isTableModel() {
    return isTableModel;
  }

  public long getAssignerEpoch() {
    return assignerEpoch;
  }

  public long getPublishedDataGeneration() {
    return publishedDataGeneration.get();
  }

  public long getPublicationFailureEpoch() {
    return publicationFailureEpoch.get();
  }
}
