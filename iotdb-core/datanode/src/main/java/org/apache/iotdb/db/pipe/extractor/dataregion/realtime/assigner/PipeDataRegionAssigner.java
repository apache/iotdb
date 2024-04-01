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

import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeEvent;
import org.apache.iotdb.db.pipe.extractor.dataregion.realtime.PipeRealtimeDataRegionExtractor;
import org.apache.iotdb.db.pipe.metric.PipeAssignerMetrics;
import org.apache.iotdb.db.pipe.pattern.CachedSchemaPatternMatcher;
import org.apache.iotdb.db.pipe.pattern.PipeDataRegionMatcher;

import java.io.Closeable;

public class PipeDataRegionAssigner implements Closeable {

  /**
   * The {@link PipeDataRegionMatcher} is used to match the event with the extractor based on the
   * pattern.
   */
  private final PipeDataRegionMatcher matcher;

  /** The {@link DisruptorQueue} is used to assign the event to the extractor. */
  private final DisruptorQueue disruptor;

  private final String dataRegionId;

  public String getDataRegionId() {
    return dataRegionId;
  }

  public PipeDataRegionAssigner(String dataRegionId) {
    this.matcher = new CachedSchemaPatternMatcher();
    this.disruptor = new DisruptorQueue(this::assignToExtractor);
    this.dataRegionId = dataRegionId;
    PipeAssignerMetrics.getInstance().register(this);
  }

  public void publishToAssign(PipeRealtimeEvent event) {
    event.increaseReferenceCount(PipeDataRegionAssigner.class.getName());

    disruptor.publish(event);

    if (event.getEvent() instanceof PipeHeartbeatEvent) {
      ((PipeHeartbeatEvent) event.getEvent()).onPublished();
    }
  }

  public void assignToExtractor(PipeRealtimeEvent event, long sequence, boolean endOfBatch) {
    matcher
        .match(event)
        .forEach(
            extractor -> {
              if (event.getEvent().isGeneratedByPipe() && !extractor.isForwardingPipeRequests()) {
                return;
              }

              final PipeRealtimeEvent copiedEvent =
                  event.shallowCopySelfAndBindPipeTaskMetaForProgressReport(
                      extractor.getPipeName(),
                      extractor.getPipeTaskMeta(),
                      extractor.getPipePattern(),
                      extractor.getRealtimeDataExtractionStartTime(),
                      extractor.getRealtimeDataExtractionEndTime());
              final EnrichedEvent innerEvent = copiedEvent.getEvent();
              if (innerEvent instanceof PipeTsFileInsertionEvent) {
                ((PipeTsFileInsertionEvent) innerEvent)
                    .disableMod4NonTransferPipes(extractor.isShouldTransferModFile());
              }

              copiedEvent.increaseReferenceCount(PipeDataRegionAssigner.class.getName());
              extractor.extract(copiedEvent);

              if (innerEvent instanceof PipeHeartbeatEvent) {
                ((PipeHeartbeatEvent) innerEvent).bindPipeName(extractor.getPipeName());
                ((PipeHeartbeatEvent) innerEvent).onAssigned();
              }
            });
    event.gcSchemaInfo();
    event.decreaseReferenceCount(PipeDataRegionAssigner.class.getName(), false);
  }

  public void startAssignTo(PipeRealtimeDataRegionExtractor extractor) {
    matcher.register(extractor);
  }

  public void stopAssignTo(PipeRealtimeDataRegionExtractor extractor) {
    matcher.deregister(extractor);
  }

  public boolean notMoreExtractorNeededToBeAssigned() {
    return matcher.getRegisterCount() == 0;
  }

  /**
   * Clear the matcher and disruptor. The method {@link PipeDataRegionAssigner#publishToAssign}
   * should not be used after calling this method.
   */
  @Override
  public void close() {
    PipeAssignerMetrics.getInstance().deregister(dataRegionId);
    matcher.clear();
    disruptor.clear();
  }

  public int getTabletInsertionEventCount() {
    return disruptor.getTabletInsertionEventCount();
  }

  public int getTsFileInsertionEventCount() {
    return disruptor.getTsFileInsertionEventCount();
  }

  public int getPipeHeartbeatEventCount() {
    return disruptor.getPipeHeartbeatEventCount();
  }
}
