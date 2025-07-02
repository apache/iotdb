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

package org.apache.iotdb.db.pipe.extractor.dataregion.realtime.assigner;

import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.event.ProgressReportEvent;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeEvent;
import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeEventFactory;
import org.apache.iotdb.db.pipe.extractor.dataregion.realtime.PipeRealtimeDataRegionExtractor;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicReference;

public class PipeDataRegionAssigner implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeDataRegionAssigner.class);

  private static final PipeConfig PIPE_CONFIG = PipeConfig.getInstance();

  private final String dataRegionId;
  protected final Set<PipeRealtimeDataRegionExtractor> extractors = new CopyOnWriteArraySet<>();

  private int counter = 0;

  private final AtomicReference<ProgressIndex> maxProgressIndexForRealtimeEvent =
      new AtomicReference<>(MinimumProgressIndex.INSTANCE);

  public String getDataRegionId() {
    return dataRegionId;
  }

  public PipeDataRegionAssigner(final String dataRegionId) {
    this.dataRegionId = dataRegionId;
  }

  public void assignToExtractor(final PipeRealtimeEvent event) {
    if (event.getEvent() instanceof PipeHeartbeatEvent) {
      ((PipeHeartbeatEvent) event.getEvent()).onPublished();
    }

    extractors.forEach(
        extractor -> {
          if (event.getEvent().isGeneratedByPipe() && !extractor.isForwardingPipeRequests()) {
            // The frequency of progress reports is limited by the counter, while progress
            // reports to TsFileInsertionEvent are not limited.
            if (!(event.getEvent() instanceof TsFileInsertionEvent)) {
              if (counter < PIPE_CONFIG.getPipeNonForwardingEventsProgressReportInterval()) {
                counter++;
                return;
              }
              counter = 0;
            }

            final ProgressReportEvent reportEvent =
                new ProgressReportEvent(
                    extractor.getPipeName(),
                    extractor.getCreationTime(),
                    extractor.getPipeTaskMeta(),
                    extractor.getPipePattern(),
                    extractor.getRealtimeDataExtractionStartTime(),
                    extractor.getRealtimeDataExtractionEndTime());
            reportEvent.bindProgressIndex(event.getProgressIndex());
            if (!reportEvent.increaseReferenceCount(PipeDataRegionAssigner.class.getName())) {
              LOGGER.warn(
                  "The reference count of the event {} cannot be increased, skipping it.",
                  reportEvent);
              return;
            }
            extractor.extract(PipeRealtimeEventFactory.createRealtimeEvent(reportEvent));
            return;
          }

          final PipeRealtimeEvent copiedEvent =
              event.shallowCopySelfAndBindPipeTaskMetaForProgressReport(
                  extractor.getPipeName(),
                  extractor.getCreationTime(),
                  extractor.getPipeTaskMeta(),
                  extractor.getPipePattern(),
                  extractor.getRealtimeDataExtractionStartTime(),
                  extractor.getRealtimeDataExtractionEndTime());
          final EnrichedEvent innerEvent = copiedEvent.getEvent();
          if (innerEvent instanceof PipeTsFileInsertionEvent) {
            final PipeTsFileInsertionEvent tsFileInsertionEvent =
                (PipeTsFileInsertionEvent) innerEvent;
            tsFileInsertionEvent.disableMod4NonTransferPipes(extractor.isShouldTransferModFile());
          }

          if (innerEvent instanceof PipeTsFileInsertionEvent
              || innerEvent instanceof PipeInsertNodeTabletInsertionEvent) {
            bindOrUpdateProgressIndexForRealtimeEvent(copiedEvent);
          }

          if (!copiedEvent.increaseReferenceCount(PipeDataRegionAssigner.class.getName())) {
            LOGGER.warn(
                "The reference count of the event {} cannot be increased, skipping it.",
                copiedEvent);
            return;
          }
          extractor.extract(copiedEvent);
        });
  }

  private void bindOrUpdateProgressIndexForRealtimeEvent(final PipeRealtimeEvent event) {
    if (PipeTsFileEpochProgressIndexKeeper.getInstance()
        .isProgressIndexAfterOrEquals(
            dataRegionId,
            event.getTsFileEpoch().getFilePath(),
            getProgressIndex4RealtimeEvent(event))) {
      event.bindProgressIndex(maxProgressIndexForRealtimeEvent.get());
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "Data region {} bind {} to event {} because it was flushed prematurely.",
            dataRegionId,
            maxProgressIndexForRealtimeEvent,
            event.coreReportMessage());
      }
    } else {
      maxProgressIndexForRealtimeEvent.updateAndGet(
          index ->
              index.updateToMinimumEqualOrIsAfterProgressIndex(
                  getProgressIndex4RealtimeEvent(event)));
    }
  }

  private ProgressIndex getProgressIndex4RealtimeEvent(final PipeRealtimeEvent event) {
    return event.getEvent() instanceof PipeTsFileInsertionEvent
        ? ((PipeTsFileInsertionEvent) event.getEvent()).forceGetProgressIndex()
        : event.getProgressIndex();
  }

  public void startAssignTo(final PipeRealtimeDataRegionExtractor extractor) {
    extractors.add(extractor);
  }

  public void stopAssignTo(final PipeRealtimeDataRegionExtractor extractor) {
    extractors.remove(extractor);
  }

  public boolean notMoreExtractorNeededToBeAssigned() {
    return extractors.isEmpty();
  }

  @Override
  // use synchronized here for completely preventing reference count leaks under extreme thread
  // scheduling when closing
  public synchronized void close() {
    final long startTime = System.currentTimeMillis();
    extractors.clear();
    LOGGER.info(
        "Pipe: Assigner on data region {} shutdown internal disruptor within {} ms",
        dataRegionId,
        System.currentTimeMillis() - startTime);
  }
}
