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

package org.apache.iotdb.db.pipe.extractor.realtime.assigner;

import org.apache.iotdb.db.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeEvent;
import org.apache.iotdb.db.pipe.extractor.realtime.PipeRealtimeDataRegionExtractor;
import org.apache.iotdb.db.pipe.extractor.realtime.matcher.CachedSchemaPatternMatcher;
import org.apache.iotdb.db.pipe.extractor.realtime.matcher.PipeDataRegionMatcher;
import org.apache.iotdb.db.pipe.metric.PipeDataRegionAssignerMetrics;

import java.util.concurrent.atomic.AtomicInteger;

public class PipeDataRegionAssigner {

  /** The matcher is used to match the event with the extractor based on the pattern. */
  private final PipeDataRegionMatcher matcher;

  /** The disruptor is used to assign the event to the extractor. */
  private final DisruptorQueue disruptor;

  private final String dataRegionId;

  private final AtomicInteger tabletInsertionEventCount = new AtomicInteger(0);

  private final AtomicInteger tsFileInsertionEventCount = new AtomicInteger(0);

  private final AtomicInteger pipeHeartbeatEventCount = new AtomicInteger(0);

  public Integer getTsFileInsertionEventCount() {
    return tsFileInsertionEventCount.get();
  }

  public Integer getTabletInsertionEventCount() {
    return tabletInsertionEventCount.get();
  }

  public Integer getPipeHeartbeatEventCount() {
    return pipeHeartbeatEventCount.get();
  }

  public String getDataRegionId() {
    return dataRegionId;
  }

  public PipeDataRegionAssigner(String dataRegionId) {
    this.matcher = new CachedSchemaPatternMatcher();
    this.disruptor = new DisruptorQueue(this::assignToExtractor);
    this.dataRegionId = dataRegionId;
    PipeDataRegionAssignerMetrics.getInstance().register(this);
  }

  public void publishToAssign(PipeRealtimeEvent event) {
    event.increaseReferenceCount(PipeDataRegionAssigner.class.getName());

    disruptor.publish(event);

    if (event.getEvent() instanceof PipeHeartbeatEvent) {
      ((PipeHeartbeatEvent) event.getEvent()).onPublished();
    }

    EnrichedEvent innerEvent = event.getEvent();
    if (innerEvent instanceof PipeHeartbeatEvent) {
      pipeHeartbeatEventCount.getAndIncrement();
    } else if (innerEvent instanceof PipeInsertNodeTabletInsertionEvent) {
      tabletInsertionEventCount.getAndIncrement();
    } else if (innerEvent instanceof PipeTsFileInsertionEvent) {
      tsFileInsertionEventCount.getAndIncrement();
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
                      extractor.getPipeTaskMeta(), extractor.getPattern());

              copiedEvent.increaseReferenceCount(PipeDataRegionAssigner.class.getName());
              extractor.extract(copiedEvent);

              final EnrichedEvent innerEvent = copiedEvent.getEvent();
              if (innerEvent instanceof PipeHeartbeatEvent) {
                ((PipeHeartbeatEvent) innerEvent).bindPipeName(extractor.getPipeName());
                ((PipeHeartbeatEvent) innerEvent).onAssigned();
              }
            });
    event.gcSchemaInfo();
    event.decreaseReferenceCount(PipeDataRegionAssigner.class.getName(), false);

    EnrichedEvent innerEvent = event.getEvent();
    if (innerEvent instanceof PipeHeartbeatEvent) {
      pipeHeartbeatEventCount.getAndDecrement();
    } else if (innerEvent instanceof PipeInsertNodeTabletInsertionEvent) {
      tabletInsertionEventCount.getAndDecrement();
    } else if (innerEvent instanceof PipeTsFileInsertionEvent) {
      tsFileInsertionEventCount.getAndDecrement();
    }
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
   * Clear the matcher and disruptor. The method publishToAssign should be work after calling this
   * method.
   */
  public void gc() {
    matcher.clear();
    disruptor.clear();
  }
}
