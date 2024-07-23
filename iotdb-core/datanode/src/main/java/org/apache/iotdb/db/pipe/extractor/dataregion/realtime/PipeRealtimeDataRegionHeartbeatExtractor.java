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

package org.apache.iotdb.db.pipe.extractor.dataregion.realtime;

import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeEvent;
import org.apache.iotdb.pipe.api.event.Event;

public class PipeRealtimeDataRegionHeartbeatExtractor extends PipeRealtimeDataRegionExtractor {

  @Override
  public Event supply() {
    PipeRealtimeEvent realtimeEvent = (PipeRealtimeEvent) pendingQueue.directPoll();

    while (realtimeEvent != null) {
      Event suppliedEvent = null;

      // only supply PipeHeartbeatEvent
      if (realtimeEvent.getEvent() instanceof PipeHeartbeatEvent) {
        suppliedEvent = supplyHeartbeat(realtimeEvent);
      }

      realtimeEvent.decreaseReferenceCount(
          PipeRealtimeDataRegionHeartbeatExtractor.class.getName(), false);

      if (suppliedEvent != null) {
        return suppliedEvent;
      }

      realtimeEvent = (PipeRealtimeEvent) pendingQueue.directPoll();
    }

    return null;
  }

  @Override
  protected void doExtract(final PipeRealtimeEvent event) {
    // only extract PipeHeartbeatEvent
    if (event.getEvent() instanceof PipeHeartbeatEvent) {
      extractHeartbeat(event);
    } else {
      event.decreaseReferenceCount(PipeRealtimeDataRegionHeartbeatExtractor.class.getName(), false);
    }
  }

  @Override
  public boolean isNeedListenToTsFile() {
    return false;
  }

  @Override
  public boolean isNeedListenToInsertNode() {
    return false;
  }

  @Override
  public String toString() {
    return "PipeRealtimeDataRegionHeartbeatExtractor{"
        + "dataRegionId='"
        + dataRegionId
        + '\''
        + '}';
  }
}
