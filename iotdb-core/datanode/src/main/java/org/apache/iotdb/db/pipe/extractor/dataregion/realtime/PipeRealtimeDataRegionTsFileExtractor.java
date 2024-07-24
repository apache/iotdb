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

import org.apache.iotdb.commons.exception.pipe.PipeRuntimeNonCriticalException;
import org.apache.iotdb.commons.pipe.event.ProgressReportEvent;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.schema.PipeSchemaRegionWritePlanEvent;
import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeEvent;
import org.apache.iotdb.db.pipe.extractor.dataregion.realtime.epoch.TsFileEpoch;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipeRealtimeDataRegionTsFileExtractor extends PipeRealtimeDataRegionExtractor {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeRealtimeDataRegionTsFileExtractor.class);

  @Override
  protected void doExtract(PipeRealtimeEvent event) {
    if (event.getEvent() instanceof PipeHeartbeatEvent) {
      extractHeartbeat(event);
      return;
    }

    if (event.getEvent() instanceof PipeSchemaRegionWritePlanEvent) {
      extractDirectly(event);
      return;
    }

    event.getTsFileEpoch().migrateState(this, state -> TsFileEpoch.State.USING_TSFILE);

    if (!(event.getEvent() instanceof TsFileInsertionEvent)) {
      event.decreaseReferenceCount(PipeRealtimeDataRegionTsFileExtractor.class.getName(), false);
      return;
    }

    if (!pendingQueue.waitedOffer(event)) {
      // This would not happen, but just in case.
      // Pending is unbounded, so it should never reach capacity.
      final String errorMessage =
          String.format(
              "extract: pending queue of PipeRealtimeDataRegionTsFileExtractor %s "
                  + "has reached capacity, discard TsFile event %s, current state %s",
              this, event, event.getTsFileEpoch().getState(this));
      LOGGER.error(errorMessage);
      PipeDataNodeAgent.runtime()
          .report(pipeTaskMeta, new PipeRuntimeNonCriticalException(errorMessage));

      // Ignore the event.
      event.decreaseReferenceCount(PipeRealtimeDataRegionTsFileExtractor.class.getName(), false);
    }
  }

  @Override
  public boolean isNeedListenToTsFile() {
    return shouldExtractInsertion;
  }

  @Override
  public boolean isNeedListenToInsertNode() {
    return false;
  }

  @Override
  public Event supply() {
    PipeRealtimeEvent realtimeEvent = (PipeRealtimeEvent) pendingQueue.directPoll();

    while (realtimeEvent != null) {
      Event suppliedEvent = null;

      if (realtimeEvent.getEvent() instanceof PipeHeartbeatEvent) {
        suppliedEvent = supplyHeartbeat(realtimeEvent);
      } else if (realtimeEvent.getEvent() instanceof PipeSchemaRegionWritePlanEvent
          || realtimeEvent.getEvent() instanceof ProgressReportEvent) {
        suppliedEvent = supplyDirectly(realtimeEvent);
      } else if (realtimeEvent.increaseReferenceCount(
          PipeRealtimeDataRegionTsFileExtractor.class.getName())) {
        suppliedEvent = realtimeEvent.getEvent();
      } else {
        // if the event's reference count can not be increased, it means the data represented by
        // this event is not reliable anymore. the data has been lost. we simply discard this event
        // and report the exception to PipeRuntimeAgent.
        final String errorMessage =
            String.format(
                "Event %s can not be supplied because "
                    + "the reference count can not be increased, "
                    + "the data represented by this event is lost",
                realtimeEvent.getEvent());
        LOGGER.error(errorMessage);
        PipeDataNodeAgent.runtime()
            .report(pipeTaskMeta, new PipeRuntimeNonCriticalException(errorMessage));
      }

      realtimeEvent.decreaseReferenceCount(
          PipeRealtimeDataRegionTsFileExtractor.class.getName(), false);

      if (suppliedEvent != null) {
        return suppliedEvent;
      }

      realtimeEvent = (PipeRealtimeEvent) pendingQueue.directPoll();
    }

    // means the pending queue is empty.
    return null;
  }
}
