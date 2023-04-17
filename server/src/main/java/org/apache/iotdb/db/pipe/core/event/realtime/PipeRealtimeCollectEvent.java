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

public class PipeRealtimeCollectEvent implements Event {
  private final EnrichedEvent event;
  private final Map<String, String[]> device2Measurements;
  private final TsFileEpoch tsFileEpoch;

  public PipeRealtimeCollectEvent(
      EnrichedEvent event, Map<String, String[]> device2Measurements, TsFileEpoch tsFileEpoch) {
    this.event = event;
    this.device2Measurements = device2Measurements;
    this.tsFileEpoch = tsFileEpoch;
  }

  public EnrichedEvent getEvent() {
    return event;
  }

  public Map<String, String[]> getSchemaInfo() {
    return device2Measurements;
  }

  public void clearSchemaInfo() {
    device2Measurements.clear();
  }

  public TsFileEpoch getTsFileEpoch() {
    return tsFileEpoch;
  }

  @Override
  public String toString() {
    return "PipeRealtimeCollectEvent{" + "event=" + event + ", tsFileEpoch=" + tsFileEpoch + '}';
  }
}
