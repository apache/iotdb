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

import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.db.pipe.agent.PipeAgent;
import org.apache.iotdb.db.pipe.core.event.realtime.PipeRealtimeCollectEvent;
import org.apache.iotdb.db.pipe.core.event.realtime.TsFileEpoch;
import org.apache.iotdb.db.pipe.task.queue.ListenableUnboundedBlockingPendingQueue;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeRuntimeNonCriticalException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: make this collector as a builtin pipe plugin. register it in BuiltinPipePlugin.
public class PipeRealtimeDataRegionHybridCollector extends PipeRealtimeDataRegionCollector {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeRealtimeDataRegionHybridCollector.class);

  // TODO: memory control
  // This queue is used to store pending events collected by the method collect(). The method
  // supply() will poll events from this queue and send them to the next pipe plugin.
  private final ListenableUnboundedBlockingPendingQueue<Event> pendingQueue;

  public PipeRealtimeDataRegionHybridCollector(
      PipeTaskMeta pipeTaskMeta, ListenableUnboundedBlockingPendingQueue<Event> pendingQueue) {
    super(pipeTaskMeta);
    this.pendingQueue = pendingQueue;
  }

  @Override
  public void collect(PipeRealtimeCollectEvent event) {
    final Event eventToCollect = event.getEvent();

    if (eventToCollect instanceof TabletInsertionEvent) {
      collectTabletInsertion(event);
    } else if (eventToCollect instanceof TsFileInsertionEvent) {
      collectTsFileInsertion(event);
    } else {
      throw new UnsupportedOperationException(
          String.format(
              "Unsupported event type %s for Hybrid Realtime Collector %s",
              eventToCollect.getClass(), this));
    }
  }

  @Override
  public boolean isNeedListenToTsFile() {
    return true;
  }

  @Override
  public boolean isNeedListenToInsertNode() {
    return true;
  }

  private void collectTabletInsertion(PipeRealtimeCollectEvent event) {
    if (isApproachingCapacity()) {
      event.getTsFileEpoch().migrateState(this, state -> TsFileEpoch.State.USING_TSFILE);
      // if the pending queue is approaching capacity, we should not collect any more tablet events.
      // all the data represented by the tablet events should be carried by the following tsfile
      // event.
      return;
    }

    if (!event.getTsFileEpoch().getState(this).equals(TsFileEpoch.State.USING_TSFILE)) {
      pendingQueue.offer(event);
    }
  }

  private void collectTsFileInsertion(PipeRealtimeCollectEvent event) {
    event
        .getTsFileEpoch()
        .migrateState(
            this,
            state ->
                state.equals(TsFileEpoch.State.EMPTY) ? TsFileEpoch.State.USING_TSFILE : state);

    if (!pendingQueue.offer(event)) {
      LOGGER.warn(
          String.format(
              "Pending Queue of Hybrid Realtime Collector %s has reached capacity, discard TsFile Event %s, current state %s",
              this, event, event.getTsFileEpoch().getState(this)));
      // this would not happen, but just in case.
      // ListenableUnblockingPendingQueue is unbounded, so it should never reach capacity.
      // TODO: memory control when elements in queue are too many.
    }
  }

  private boolean isApproachingCapacity() {
    return pendingQueue.size()
        >= PipeConfig.getInstance().getRealtimeCollectorPendingQueueTabletLimit();
  }

  @Override
  public Event supply() {
    PipeRealtimeCollectEvent collectEvent = (PipeRealtimeCollectEvent) pendingQueue.poll();

    while (collectEvent != null) {
      Event suppliedEvent;

      // used to judge type of event, not directly for supplying.
      final Event eventToSupply = collectEvent.getEvent();
      if (eventToSupply instanceof TabletInsertionEvent) {
        suppliedEvent = supplyTabletInsertion(collectEvent);
      } else if (eventToSupply instanceof TsFileInsertionEvent) {
        suppliedEvent = supplyTsFileInsertion(collectEvent);
      } else {
        throw new UnsupportedOperationException(
            String.format(
                "Unsupported event type %s for Hybrid Realtime Collector %s to supply.",
                eventToSupply.getClass(), this));
      }

      collectEvent.decreaseReferenceCount(PipeRealtimeDataRegionHybridCollector.class.getName());
      if (suppliedEvent != null) {
        return suppliedEvent;
      }

      collectEvent = (PipeRealtimeCollectEvent) pendingQueue.poll();
    }

    // means the pending queue is empty.
    return null;
  }

  private Event supplyTabletInsertion(PipeRealtimeCollectEvent event) {
    event
        .getTsFileEpoch()
        .migrateState(
            this,
            state ->
                (state.equals(TsFileEpoch.State.EMPTY)) ? TsFileEpoch.State.USING_TABLET : state);

    if (event.getTsFileEpoch().getState(this).equals(TsFileEpoch.State.USING_TABLET)) {
      if (event.increaseReferenceCount(PipeRealtimeDataRegionHybridCollector.class.getName())) {
        return event.getEvent();
      } else {
        // if the event's reference count can not be increased, it means the data represented by
        // this event is not reliable anymore. but the data represented by this event
        // has been carried by the following tsfile event, so we can just discard this event.
        event.getTsFileEpoch().migrateState(this, state -> TsFileEpoch.State.USING_TSFILE);
        return null;
      }
    }
    // if the state is USING_TSFILE, discard the event and poll the next one.
    return null;
  }

  private Event supplyTsFileInsertion(PipeRealtimeCollectEvent event) {
    event
        .getTsFileEpoch()
        .migrateState(
            this,
            state -> {
              // this would not happen, but just in case.
              if (state.equals(TsFileEpoch.State.EMPTY)) {
                LOGGER.warn(
                    String.format("EMPTY TsFileEpoch when supplying TsFile Event %s", event));
                return TsFileEpoch.State.USING_TSFILE;
              }
              return state;
            });

    if (event.getTsFileEpoch().getState(this).equals(TsFileEpoch.State.USING_TSFILE)) {
      if (event.increaseReferenceCount(PipeRealtimeDataRegionHybridCollector.class.getName())) {
        return event.getEvent();
      } else {
        // if the event's reference count can not be increased, it means the data represented by
        // this event is not reliable anymore. the data has been lost. we simply discard this event
        // and report the exception to PipeRuntimeAgent.
        final String errorMessage =
            String.format(
                "TsFile Event %s can not be supplied because the reference count can not be increased, "
                    + "the data represented by this event is lost",
                event.getEvent());
        PipeAgent.runtime().report(pipeTaskMeta, new PipeRuntimeNonCriticalException(errorMessage));
        return null;
      }
    }
    // if the state is USING_TABLET, discard the event and poll the next one.
    return null;
  }

  @Override
  public void close() throws Exception {
    super.close();
    pendingQueue.clear();
  }
}
