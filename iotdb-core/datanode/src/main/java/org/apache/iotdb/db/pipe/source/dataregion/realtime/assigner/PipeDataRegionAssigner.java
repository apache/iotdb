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

public class PipeDataRegionAssigner implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeDataRegionAssigner.class);

  /**
   * The {@link PipeDataRegionMatcher} is used to match the event with the source based on the
   * pattern.
   */
  private final PipeDataRegionMatcher matcher;

  /** The {@link DisruptorQueue} is used to assign the event to the source. */
  private final DisruptorQueue disruptor;

  private final int dataRegionId;

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
    if (!event.increaseReferenceCount(PipeDataRegionAssigner.class.getName())) {
      LOGGER.warn(DataNodePipeMessages.THE_REFERENCE_COUNT_OF_THE_REALTIME_EVENT, event);
      return;
    }

    final EnrichedEvent innerEvent = event.getEvent();
    eventCounter.increaseEventCount(innerEvent);
    if (innerEvent instanceof PipeHeartbeatEvent) {
      ((PipeHeartbeatEvent) innerEvent).onPublished();
    }

    synchronized (this) {
      if (disruptor.isClosed()) {
        onAssignedHook(event);
        return;
      }
      inFlightPublishCount++;
    }

    boolean isPublished = false;
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

    if (!isPublished) {
      onAssignedHook(event);
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

              if (innerEvent instanceof PipeTsFileInsertionEvent) {
                final PipeTsFileInsertionEvent tsFileInsertionEvent =
                    (PipeTsFileInsertionEvent) innerEvent;
                tsFileInsertionEvent.bindTsFileDedupScopeID(source.getTsFileDedupScopeID());
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
                  LOGGER.warn(
                      DataNodePipeMessages.THE_REFERENCE_COUNT_OF_THE_EVENT_CANNOT, reportEvent);
                  return;
                }
                source.extract(PipeRealtimeEventFactory.createRealtimeEvent(reportEvent));
              }
            });
  }

  public synchronized void startAssignTo(final PipeRealtimeDataRegionSource source) {
    matcher.register(source);
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
}
