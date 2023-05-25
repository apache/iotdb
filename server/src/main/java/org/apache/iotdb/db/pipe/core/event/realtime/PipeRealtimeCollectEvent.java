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

import org.apache.iotdb.db.pipe.core.event.EnrichedEvent;
import org.apache.iotdb.pipe.api.event.Event;

import java.util.Map;

public class PipeRealtimeCollectEvent implements Event, EnrichedEvent {

  private final Event event;
  private final TsFileEpoch tsFileEpoch;

  private Map<String, String[]> device2Measurements;

  public PipeRealtimeCollectEvent(
      Event event, TsFileEpoch tsFileEpoch, Map<String, String[]> device2Measurements) {
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
    return !(event instanceof EnrichedEvent)
        || ((EnrichedEvent) event).increaseReferenceCount(holderMessage);
  }

  @Override
  public boolean decreaseReferenceCount(String holderMessage) {
    return !(event instanceof EnrichedEvent)
        || ((EnrichedEvent) event).decreaseReferenceCount(holderMessage);
  }

  @Override
  public int getReferenceCount() {
    return event instanceof EnrichedEvent ? ((EnrichedEvent) event).getReferenceCount() : 0;
  }

  @Override
  public void setPattern(String pathPattern) {
    if (event instanceof EnrichedEvent) {
      ((EnrichedEvent) event).setPattern(pathPattern);
    }
  }

  @Override
  public String getPattern() {
    if (event instanceof EnrichedEvent) {
      return ((EnrichedEvent) event).getPattern();
    }
    return null;
  }

  @Override
  public String toString() {
    return "PipeRealtimeCollectEvent{" + "event=" + event + ", tsFileEpoch=" + tsFileEpoch + '}';
  }
}
