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

package org.apache.iotdb.db.pipe.extractor.realtime;

import org.apache.iotdb.commons.exception.pipe.PipeRuntimeNonCriticalException;
import org.apache.iotdb.db.pipe.agent.PipeAgent;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeEvent;
import org.apache.iotdb.db.pipe.extractor.realtime.epoch.TsFileEpoch;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipeRealtimeDataRegionLogExtractor extends PipeRealtimeDataRegionExtractor {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeRealtimeDataRegionLogExtractor.class);

  @Override
  protected void doExtract(PipeRealtimeEvent event) {
    final Event eventToExtract = event.getEvent();

    if (eventToExtract instanceof TabletInsertionEvent) {
      extractTabletInsertion(event);
    } else if (eventToExtract instanceof TsFileInsertionEvent) {
      extractTsFileInsertion(event);
    } else if (eventToExtract instanceof PipeHeartbeatEvent) {
      extractHeartbeat(event);
    } else {
      throw new UnsupportedOperationException(
          String.format(
              "Unsupported event type %s for log realtime extractor %s",
              eventToExtract.getClass(), this));
    }
  }

  private void extractTabletInsertion(PipeRealtimeEvent event) {
    event.getTsFileEpoch().migrateState(this, state -> TsFileEpoch.State.USING_TABLET);

    if (!pendingQueue.waitedOffer(event)) {
      // this would not happen, but just in case.
      // pendingQueue is unbounded, so it should never reach capacity.
      final String errorMessage =
          String.format(
              "extract: pending queue of PipeRealtimeDataRegionLogExtractor %s "
                  + "has reached capacity, discard tablet event %s, current state %s",
              this, event, event.getTsFileEpoch().getState(this));
      LOGGER.error(errorMessage);
      PipeAgent.runtime().report(pipeTaskMeta, new PipeRuntimeNonCriticalException(errorMessage));

      // ignore this event.
      event.decreaseReferenceCount(PipeRealtimeDataRegionLogExtractor.class.getName(), false);
    }
  }

  private void extractTsFileInsertion(PipeRealtimeEvent event) {
    if (!(((PipeTsFileInsertionEvent) event.getEvent()).getIsLoaded()
        || event.getTsFileEpoch().getMinTime()
            > ((PipeTsFileInsertionEvent) event.getEvent()).getFileStartTime())) {
      // If the tsFile's data can be represented by insertNodes, ignore this event.
      event.decreaseReferenceCount(PipeRealtimeDataRegionLogExtractor.class.getName(), false);
      return;
    }

    event.getTsFileEpoch().migrateState(this, state -> TsFileEpoch.State.USING_TSFILE);

    if (!pendingQueue.waitedOffer(event)) {
      // this would not happen, but just in case.
      // pendingQueue is unbounded, so it should never reach capacity.
      final String errorMessage =
          String.format(
              "extract: pending queue of PipeRealtimeDataRegionLogExtractor %s "
                  + "has reached capacity, discard loaded tsFile event %s, current state %s",
              this, event, event.getTsFileEpoch().getState(this));
      LOGGER.error(errorMessage);
      PipeAgent.runtime().report(pipeTaskMeta, new PipeRuntimeNonCriticalException(errorMessage));

      // ignore this event.
      event.decreaseReferenceCount(PipeRealtimeDataRegionLogExtractor.class.getName(), false);
    }
  }

  private void extractHeartbeat(PipeRealtimeEvent event) {
    // Record the pending queue size before trying to put heartbeatEvent into queue
    ((PipeHeartbeatEvent) event.getEvent()).recordExtractorQueueSize(pendingQueue);

    Event lastEvent = pendingQueue.peekLast();
    if (lastEvent instanceof PipeRealtimeEvent
        && ((PipeRealtimeEvent) lastEvent).getEvent() instanceof PipeHeartbeatEvent
        && (((PipeHeartbeatEvent) ((PipeRealtimeEvent) lastEvent).getEvent()).isShouldPrintMessage()
            || !((PipeHeartbeatEvent) event.getEvent()).isShouldPrintMessage())) {
      // If the last event in the pending queue is a heartbeat event, we should not extract any more
      // heartbeat events to avoid OOM when the pipe is stopped.
      // Besides, the printable event has higher priority to stay in queue to enable metrics report.
      event.decreaseReferenceCount(PipeRealtimeDataRegionLogExtractor.class.getName(), false);
      return;
    }

    if (!pendingQueue.waitedOffer(event)) {
      // this would not happen, but just in case.
      // pendingQueue is unbounded, so it should never reach capacity.
      LOGGER.error(
          "extract: pending queue of PipeRealtimeDataRegionLogExtractor {} "
              + "has reached capacity, discard heartbeat event {}",
          this,
          event);

      // Do not report exception since the PipeHeartbeatEvent doesn't affect the correction of
      // pipe progress.

      // ignore this event.
      event.decreaseReferenceCount(PipeRealtimeDataRegionLogExtractor.class.getName(), false);
    }
  }

  @Override
  public boolean isNeedListenToTsFile() {
    // Only listen to tsFiles that can't be represented by insertNodes
    return true;
  }

  @Override
  public boolean isNeedListenToInsertNode() {
    return true;
  }

  @Override
  public Event doSupply() {
    PipeRealtimeEvent realtimeEvent = (PipeRealtimeEvent) pendingQueue.directPoll();

    while (realtimeEvent != null) {
      Event suppliedEvent = null;

      if (realtimeEvent.increaseReferenceCount(
          PipeRealtimeDataRegionLogExtractor.class.getName())) {
        suppliedEvent = realtimeEvent.getEvent();
      } else {
        // if the event's reference count can not be increased, it means the data represented by
        // this event is not reliable anymore. the data has been lost. we simply discard this event
        // and report the exception to PipeRuntimeAgent.
        final String errorMessage =
            String.format(
                "Tablet Event %s can not be supplied because "
                    + "the reference count can not be increased, "
                    + "the data represented by this event is lost",
                realtimeEvent.getEvent());
        LOGGER.error(errorMessage);
        PipeAgent.runtime().report(pipeTaskMeta, new PipeRuntimeNonCriticalException(errorMessage));
      }

      realtimeEvent.decreaseReferenceCount(
          PipeRealtimeDataRegionLogExtractor.class.getName(), false);

      if (suppliedEvent != null) {
        return suppliedEvent;
      }

      realtimeEvent = (PipeRealtimeEvent) pendingQueue.directPoll();
    }

    // means the pending queue is empty.
    return null;
  }
}
