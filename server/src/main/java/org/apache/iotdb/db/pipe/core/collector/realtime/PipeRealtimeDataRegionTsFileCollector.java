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

package org.apache.iotdb.db.pipe.core.collector.realtime;

import org.apache.iotdb.db.pipe.agent.PipeAgent;
import org.apache.iotdb.db.pipe.core.event.realtime.PipeRealtimeCollectEvent;
import org.apache.iotdb.db.pipe.core.event.realtime.TsFileEpoch;
import org.apache.iotdb.db.pipe.task.queue.ListenableUnblockingPendingQueue;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeRuntimeNonCriticalException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipeRealtimeDataRegionTsFileCollector extends PipeRealtimeDataRegionCollector {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeRealtimeDataRegionTsFileCollector.class);

  // TODO: memory control
  // This queue is used to store pending events collected by the method collect(). The method
  // supply() will poll events from this queue and send them to the next pipe plugin.
  private final ListenableUnblockingPendingQueue<Event> pendingQueue;

  public PipeRealtimeDataRegionTsFileCollector(
      ListenableUnblockingPendingQueue<Event> pendingQueue) {
    this.pendingQueue = pendingQueue;
  }

  @Override
  public void collect(PipeRealtimeCollectEvent event) {
    if (event.getEvent() instanceof TsFileInsertionEvent) {
      event.getTsFileEpoch().migrateState(this, state -> TsFileEpoch.State.USING_TSFILE);
      if (!pendingQueue.offer(event)) {
        LOGGER.warn(
            String.format(
                "Pending Queue of TsFile Realtime Collector %s has reached capacity, discard TsFile Event %s, current state %s",
                this, event, event.getTsFileEpoch().getState(this)));
        // this would not happen, but just in case.
        // ListenableUnblockingPendingQueue is unbounded, so it should never reach capacity.
        // TODO: memory control when elements in queue are too many.
      }
    }
  }

  @Override
  public Event supply() {
    PipeRealtimeCollectEvent collectEvent = (PipeRealtimeCollectEvent) pendingQueue.poll();

    while (collectEvent != null) {
      Event suppliedEvent = null;

      if (collectEvent.increaseReferenceCount(
          PipeRealtimeDataRegionTsFileCollector.class.getName())) {
        suppliedEvent = collectEvent.getEvent();
      } else {
        // if the event's reference count can not be increased, it means the data represented by
        // this event is not reliable anymore. the data has been lost. we simply discard this event
        // and report the exception to PipeRuntimeAgent.
        final String errorMessage =
            String.format(
                "TsFile Event %s can not be supplied because the reference count can not be increased, "
                    + "the data represented by this event is lost",
                collectEvent.getEvent());
        PipeAgent.runtime().report(new PipeRuntimeNonCriticalException(errorMessage));
      }

      collectEvent.decreaseReferenceCount(PipeRealtimeDataRegionTsFileCollector.class.getName());
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
