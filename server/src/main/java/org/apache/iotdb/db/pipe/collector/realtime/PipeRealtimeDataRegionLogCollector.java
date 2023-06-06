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

package org.apache.iotdb.db.pipe.collector.realtime;

import org.apache.iotdb.commons.exception.pipe.PipeRuntimeNonCriticalException;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.db.pipe.agent.PipeAgent;
import org.apache.iotdb.db.pipe.collector.realtime.epoch.TsFileEpoch;
import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeCollectEvent;
import org.apache.iotdb.db.pipe.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipeRealtimeDataRegionLogCollector extends PipeRealtimeDataRegionCollector {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeRealtimeDataRegionLogCollector.class);

  // This queue is used to store pending events collected by the method collect(). The method
  // supply() will poll events from this queue and send them to the next pipe plugin.
  private final UnboundedBlockingPendingQueue<Event> pendingQueue;

  public PipeRealtimeDataRegionLogCollector(
      PipeTaskMeta pipeTaskMeta, UnboundedBlockingPendingQueue<Event> pendingQueue) {
    super(pipeTaskMeta);
    this.pendingQueue = pendingQueue;
  }

  @Override
  public void collect(PipeRealtimeCollectEvent event) {
    event.getTsFileEpoch().migrateState(this, state -> TsFileEpoch.State.USING_TABLET);

    if (!(event.getEvent() instanceof TabletInsertionEvent)) {
      return;
    }

    if (!pendingQueue.offer(event)) {
      LOGGER.warn(
          String.format(
              "collect: pending queue of PipeRealtimeDataRegionLogCollector %s has reached capacity, discard tablet event %s, current state %s",
              this, event, event.getTsFileEpoch().getState(this)));
      // this would not happen, but just in case.
      // ListenableUnblockingPendingQueue is unbounded, so it should never reach capacity.
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
    PipeRealtimeCollectEvent collectEvent = (PipeRealtimeCollectEvent) pendingQueue.poll();

    while (collectEvent != null) {
      Event suppliedEvent = null;

      if (collectEvent.increaseReferenceCount(PipeRealtimeDataRegionLogCollector.class.getName())) {
        suppliedEvent = collectEvent.getEvent();
      } else {
        // if the event's reference count can not be increased, it means the data represented by
        // this event is not reliable anymore. the data has been lost. we simply discard this event
        // and report the exception to PipeRuntimeAgent.
        final String errorMessage =
            String.format(
                "Tablet Event %s can not be supplied because the reference count can not be increased, "
                    + "the data represented by this event is lost",
                collectEvent.getEvent());
        LOGGER.warn(errorMessage);
        PipeAgent.runtime().report(pipeTaskMeta, new PipeRuntimeNonCriticalException(errorMessage));
      }

      collectEvent.decreaseReferenceCount(PipeRealtimeDataRegionLogCollector.class.getName());
      if (suppliedEvent != null) {
        return suppliedEvent;
      }

      collectEvent = (PipeRealtimeCollectEvent) pendingQueue.poll();
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
