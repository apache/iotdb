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

package org.apache.iotdb.db.pipe.event.realtime;

import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.db.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.extractor.realtime.epoch.TsFileEpoch;

import java.util.Map;

/**
 * PipeRealtimeEvent is an event that decorates the EnrichedEvent with the information of
 * TsFileEpoch and schema info. It only exists in the realtime event extractor.
 */
public class PipeRealtimeEvent extends EnrichedEvent {

  private final EnrichedEvent event;
  private final TsFileEpoch tsFileEpoch;

  private Map<String, String[]> device2Measurements;

  public PipeRealtimeEvent(
      EnrichedEvent event,
      TsFileEpoch tsFileEpoch,
      Map<String, String[]> device2Measurements,
      String pattern) {
    this(event, tsFileEpoch, device2Measurements, null, pattern, Long.MIN_VALUE, Long.MAX_VALUE);
  }

  public PipeRealtimeEvent(
      EnrichedEvent event,
      TsFileEpoch tsFileEpoch,
      Map<String, String[]> device2Measurements,
      PipeTaskMeta pipeTaskMeta,
      String pattern,
      long startTime,
      long endTime) {
    // pipeTaskMeta is used to report the progress of the event, the PipeRealtimeEvent
    // is only used in the realtime event extractor, which does not need to report the progress
    // of the event, so the pipeTaskMeta is always null.
    super(event != null ? event.getPipeName() : null, pipeTaskMeta, pattern, startTime, endTime);

    this.event = event;
    this.tsFileEpoch = tsFileEpoch;
    this.device2Measurements = device2Measurements;
  }

  public EnrichedEvent getEvent() {
    return event;
  }

  public TsFileEpoch getTsFileEpoch() {
    return tsFileEpoch;
  }

  public Map<String, String[]> getSchemaInfo() {
    return device2Measurements;
  }

  public void gcSchemaInfo() {
    device2Measurements = null;
  }

  @Override
  public boolean increaseReferenceCount(String holderMessage) {
    // This method must be overridden, otherwise during the real-time data extraction stage, the
    // current PipeRealtimeEvent rather than the member variable EnrichedEvent will increase
    // the reference count, resulting in errors in the reference count of the EnrichedEvent
    // contained in this PipeRealtimeEvent during the processor and connector stages.
    return event.increaseReferenceCount(holderMessage);
  }

  @Override
  public boolean internallyIncreaseResourceReferenceCount(String holderMessage) {
    return event.internallyIncreaseResourceReferenceCount(holderMessage);
  }

  @Override
  public boolean decreaseReferenceCount(String holderMessage, boolean shouldReport) {
    // This method must be overridden, otherwise during the real-time data extraction stage, the
    // current PipeRealtimeEvent rather than the member variable EnrichedEvent will decrease
    // the reference count, resulting in errors in the reference count of the EnrichedEvent
    // contained in this PipeRealtimeEvent during the processor and connector stages.
    return event.decreaseReferenceCount(holderMessage, shouldReport);
  }

  @Override
  public boolean clearReferenceCount(String holderMessage) {
    // This method must be overridden, otherwise during the real-time data extraction stage, the
    // current PipeRealtimeEvent rather than the member variable EnrichedEvent will clear
    // the reference count.
    return event.clearReferenceCount(holderMessage);
  }

  @Override
  public boolean internallyDecreaseResourceReferenceCount(String holderMessage) {
    return event.internallyDecreaseResourceReferenceCount(holderMessage);
  }

  @Override
  public ProgressIndex getProgressIndex() {
    return event.getProgressIndex();
  }

  /**
   * If pipe's pattern is database-level, then no need to parse event by pattern cause pipes are
   * data-region-level.
   */
  @Override
  public void skipParsingPattern() {
    event.skipParsingPattern();
  }

  @Override
  public void skipParsingTime() {
    event.skipParsingTime();
  }

  @Override
  public PipeRealtimeEvent shallowCopySelfAndBindPipeTaskMetaForProgressReport(
      String pipeName, PipeTaskMeta pipeTaskMeta, String pattern, long startTime, long endTime) {
    return new PipeRealtimeEvent(
        event.shallowCopySelfAndBindPipeTaskMetaForProgressReport(
            pipeName, pipeTaskMeta, pattern, startTime, endTime),
        this.tsFileEpoch,
        this.device2Measurements,
        pipeTaskMeta,
        pattern,
        startTime,
        endTime);
  }

  @Override
  public boolean isGeneratedByPipe() {
    return event.isGeneratedByPipe();
  }

  @Override
  public boolean isEventTimeOverlappedWithTimeRange() {
    return event.isEventTimeOverlappedWithTimeRange();
  }

  @Override
  public String toString() {
    return "PipeRealtimeEvent{"
        + "event="
        + event
        + ", tsFileEpoch="
        + tsFileEpoch
        + ", device2Measurements="
        + device2Measurements
        + '}';
  }
}
