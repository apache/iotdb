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
import org.apache.iotdb.consensus.pipe.PipeConsensus;
import org.apache.iotdb.db.consensus.DataRegionConsensusImpl;
import org.apache.iotdb.db.pipe.consensus.ReplicateProgressDataNodeManager;
import org.apache.iotdb.db.pipe.consensus.deletion.DeletionResource;
import org.apache.iotdb.db.pipe.consensus.deletion.DeletionResourceManager;
import org.apache.iotdb.db.pipe.event.common.deletion.PipeDeleteDataNodeEvent;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeEvent;
import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeEventFactory;
import org.apache.iotdb.db.pipe.metric.source.PipeAssignerMetrics;
import org.apache.iotdb.db.pipe.metric.source.PipeDataRegionEventCounter;
import org.apache.iotdb.db.pipe.processor.pipeconsensus.PipeConsensusProcessor;
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
   * The {@link PipeDataRegionMatcher} is used to match the event with the extractor based on the
   * pattern.
   */
  private final PipeDataRegionMatcher matcher;

  /** The {@link DisruptorQueue} is used to assign the event to the extractor. */
  private final DisruptorQueue disruptor;

  private final int dataRegionId;

  private Boolean isTableModel;

  private final PipeEventCounter eventCounter = new PipeDataRegionEventCounter();

  public int getDataRegionId() {
    return dataRegionId;
  }

  public PipeDataRegionAssigner(final int dataRegionId) {
    this.matcher = new CachedSchemaPatternMatcher();
    this.disruptor = new DisruptorQueue(this::assignToExtractor, this::onAssignedHook);
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
      LOGGER.warn(
          "The reference count of the realtime event {} cannot be increased, skipping it.", event);
      return;
    }

    final EnrichedEvent innerEvent = event.getEvent();
    eventCounter.increaseEventCount(innerEvent);
    if (innerEvent instanceof PipeHeartbeatEvent) {
      ((PipeHeartbeatEvent) innerEvent).onPublished();
    }

    // use synchronized here for completely preventing reference count leaks under extreme thread
    // scheduling when closing
    synchronized (this) {
      if (!disruptor.isClosed()) {
        disruptor.publish(event);
      } else {
        onAssignedHook(event);
      }
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

  private void assignToExtractor(
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
                      "The reference count of the event {} cannot be increased, skipping it.",
                      reportEvent);
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
              // if using IoTV2, assign a replicateIndex for this realtime event
              if (DataRegionConsensusImpl.getInstance() instanceof PipeConsensus
                  && PipeConsensusProcessor.isShouldReplicate(innerEvent)) {
                innerEvent.setReplicateIndexForIoTV2(
                    ReplicateProgressDataNodeManager.assignReplicateIndexForIoTV2(
                        source.getPipeName()));
                LOGGER.debug(
                    "[{}]Set {} for realtime event {}",
                    source.getPipeName(),
                    innerEvent.getReplicateIndexForIoTV2(),
                    innerEvent);
              }

              if (innerEvent instanceof PipeTsFileInsertionEvent) {
                final PipeTsFileInsertionEvent tsFileInsertionEvent =
                    (PipeTsFileInsertionEvent) innerEvent;
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
                    "The reference count of the event {} cannot be increased, skipping it.",
                    copiedEvent);
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
                      "The reference count of the event {} cannot be increased, skipping it.",
                      reportEvent);
                  return;
                }
                source.extract(PipeRealtimeEventFactory.createRealtimeEvent(reportEvent));
              }
            });
  }

  public void startAssignTo(final PipeRealtimeDataRegionSource extractor) {
    matcher.register(extractor);
  }

  public void stopAssignTo(final PipeRealtimeDataRegionSource extractor) {
    matcher.deregister(extractor);
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

    final long startTime = System.currentTimeMillis();
    disruptor.shutdown();
    matcher.clear();
    LOGGER.info(
        "Pipe: Assigner on data region {} shutdown internal disruptor within {} ms",
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

  public Boolean isTableModel() {
    return isTableModel;
  }
}
