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
import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeEvent;
import org.apache.iotdb.db.pipe.extractor.realtime.epoch.TsFileEpoch;
import org.apache.iotdb.db.pipe.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipeRealtimeDataRegionLogExtractor extends PipeRealtimeDataRegionExtractor {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeRealtimeDataRegionLogExtractor.class);

  // This queue is used to store pending events extracted by the method extract(). The method
  // supply() will poll events from this queue and send them to the next pipe plugin.
  private final UnboundedBlockingPendingQueue<Event> pendingQueue;

  public PipeRealtimeDataRegionLogExtractor() {
    this.pendingQueue = new UnboundedBlockingPendingQueue<>();
  }

  @Override
  public void extract(PipeRealtimeEvent event) {
    if (event.getEvent() instanceof PipeHeartbeatEvent) {
      extractHeartbeat(event);
      return;
    }

    event.getTsFileEpoch().migrateState(this, state -> TsFileEpoch.State.USING_TABLET);

    if (!(event.getEvent() instanceof TabletInsertionEvent)) {
      event.decreaseReferenceCount(PipeRealtimeDataRegionLogExtractor.class.getName());
      return;
    }

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
      event.decreaseReferenceCount(PipeRealtimeDataRegionLogExtractor.class.getName());
    }
  }

  private void extractHeartbeat(PipeRealtimeEvent event) {
    if (!pendingQueue.waitedOffer(event)) {
      // this would not happen, but just in case.
      // pendingQueue is unbounded, so it should never reach capacity.
      LOGGER.error(
          "extract: pending queue of PipeRealtimeDataRegionTsFileExtractor {} "
              + "has reached capacity, discard heartbeat event {}",
          this,
          event);

      // Do not report exception since the PipeHeartbeatEvent doesn't affect the correction of
      // pipe progress.

      // ignore this event.
      event.decreaseReferenceCount(PipeRealtimeDataRegionLogExtractor.class.getName());
    }
  }

  @Override
  public boolean isNeedListenToTsFile() {
    return false;
  }

  @Override
  public boolean isNeedListenToInsertNode() {
    return true;
  }

  @Override
  public Event supply() {
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

      realtimeEvent.decreaseReferenceCount(PipeRealtimeDataRegionLogExtractor.class.getName());

      if (suppliedEvent != null) {
        return suppliedEvent;
      }

      realtimeEvent = (PipeRealtimeEvent) pendingQueue.directPoll();
    }

    // means the pending queue is empty.
    return null;
  }

  @Override
  public void close() throws Exception {
    super.close();
    pendingQueue.clear();
  }
}
