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

package org.apache.iotdb.db.pipe.core.event.realtime;

import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.db.pipe.core.event.EnrichedEvent;
import org.apache.iotdb.pipe.api.event.Event;

import java.util.Map;

/**
 * PipeRealtimeCollectEvent is an event that decorates the EnrichedEvent with the information of
 * TsFileEpoch and schema info. It only exists in the realtime event collector.
 */
public class PipeRealtimeCollectEvent extends EnrichedEvent {

  private final EnrichedEvent event;
  private final TsFileEpoch tsFileEpoch;

  private Map<String, String[]> device2Measurements;

  public PipeRealtimeCollectEvent(
      EnrichedEvent event,
      TsFileEpoch tsFileEpoch,
      Map<String, String[]> device2Measurements,
      String pattern) {
    // pipeTaskMeta is used to report the progress of the event, the PipeRealtimeCollectEvent
    // is only used in the realtime event collector, which does not need to report the progress
    // of the event, so the pipeTaskMeta is always null.
    super(null, pattern);

    this.event = event;
    this.tsFileEpoch = tsFileEpoch;
    this.device2Measurements = device2Measurements;
  }

  public PipeRealtimeCollectEvent(
      EnrichedEvent event,
      TsFileEpoch tsFileEpoch,
      Map<String, String[]> device2Measurements,
      PipeTaskMeta pipeTaskMeta) {
    // pipeTaskMeta is used to report the progress of the event, the PipeRealtimeCollectEvent
    // is only used in the realtime event collector, which does not need to report the progress
    // of the event, so the pipeTaskMeta is always null.
    super(pipeTaskMeta);

    this.event = event;
    this.tsFileEpoch = tsFileEpoch;
    this.device2Measurements = device2Measurements;
  }

  public Event getEvent() {
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
    // This method must be overridden, otherwise during the real-time data collection stage, the
    // current PipeRealtimeCollectEvent rather than the member variable EnrichedEvent will increase
    // the reference count, resulting in errors in the reference count of the EnrichedEvent
    // contained in this PipeRealtimeCollectEvent during the processor and connector stages.
    return event.increaseReferenceCount(holderMessage);
  }

  @Override
  public boolean increaseResourceReferenceCount(String holderMessage) {
    return event.increaseResourceReferenceCount(holderMessage);
  }

  @Override
  public boolean decreaseReferenceCount(String holderMessage) {
    // This method must be overridden, otherwise during the real-time data collection stage, the
    // current PipeRealtimeCollectEvent rather than the member variable EnrichedEvent will increase
    // the reference count, resulting in errors in the reference count of the EnrichedEvent
    // contained in this PipeRealtimeCollectEvent during the processor and connector stages.
    return event.decreaseReferenceCount(holderMessage);
  }

  @Override
  public boolean decreaseResourceReferenceCount(String holderMessage) {
    return event.decreaseResourceReferenceCount(holderMessage);
  }

  @Override
  public ProgressIndex getProgressIndex() {
    return event.getProgressIndex();
  }

  @Override
  public PipeRealtimeCollectEvent shallowCopySelfAndBindPipeTaskMetaForProgressReport(
      PipeTaskMeta pipeTaskMeta, String pattern) {
    return new PipeRealtimeCollectEvent(
        event.shallowCopySelfAndBindPipeTaskMetaForProgressReport(pipeTaskMeta, pattern),
        this.tsFileEpoch,
        this.device2Measurements,
        pattern,
        pipeTaskMeta);
  }

  @Override
  public String toString() {
    return "PipeRealtimeCollectEvent{"
        + "event="
        + event
        + ", tsFileEpoch="
        + tsFileEpoch
        + ", device2Measurements="
        + device2Measurements
        + '}';
  }
}
