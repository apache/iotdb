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
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.db.pipe.agent.PipeAgent;
import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeEvent;
import org.apache.iotdb.db.pipe.extractor.realtime.epoch.TsFileEpoch;
import org.apache.iotdb.db.pipe.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipeRealtimeDataRegionHybridExtractor extends PipeRealtimeDataRegionExtractor {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeRealtimeDataRegionHybridExtractor.class);

  // This queue is used to store pending events extracted by the method extract(). The method
  // supply() will poll events from this queue and send them to the next pipe plugin.
  private final UnboundedBlockingPendingQueue<Event> pendingQueue;

  public PipeRealtimeDataRegionHybridExtractor() {
    this.pendingQueue = new UnboundedBlockingPendingQueue<>();
  }

  @Override
  public void extract(PipeRealtimeEvent event) {
    final Event eventToExtract = event.getEvent();

    if (eventToExtract instanceof TabletInsertionEvent) {
      extractTabletInsertion(event);
    } else if (eventToExtract instanceof TsFileInsertionEvent) {
      extractTsFileInsertion(event);
    } else {
      throw new UnsupportedOperationException(
          String.format(
              "Unsupported event type %s for hybrid realtime extractor %s",
              eventToExtract.getClass(), this));
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

  private void extractTabletInsertion(PipeRealtimeEvent event) {
    if (isApproachingCapacity()) {
      // if the pending queue is approaching capacity, we should not extract any more tablet events.
      // all the data represented by the tablet events should be carried by the following tsfile
      // event.
      event.getTsFileEpoch().migrateState(this, state -> TsFileEpoch.State.USING_TSFILE);
    }

    final TsFileEpoch.State state = event.getTsFileEpoch().getState(this);
    switch (state) {
      case USING_TSFILE:
        // Ignore the tablet event.
        event.decreaseReferenceCount(PipeRealtimeDataRegionHybridExtractor.class.getName());
        break;
      case EMPTY:
      case USING_TABLET:
        if (!pendingQueue.waitedOffer(event)) {
          // this would not happen, but just in case.
          // pendingQueue is unbounded, so it should never reach capacity.
          final String errorMessage =
              String.format(
                  "extractTabletInsertion: pending queue of PipeRealtimeDataRegionHybridExtractor %s "
                      + "has reached capacity, discard tablet event %s, current state %s",
                  this, event, event.getTsFileEpoch().getState(this));
          LOGGER.error(errorMessage);
          PipeAgent.runtime()
              .report(pipeTaskMeta, new PipeRuntimeNonCriticalException(errorMessage));

          // Ignore the tablet event.
          event.decreaseReferenceCount(PipeRealtimeDataRegionHybridExtractor.class.getName());
        }
        break;
      default:
        throw new UnsupportedOperationException(
            String.format(
                "Unsupported state %s for hybrid realtime extractor %s",
                state, PipeRealtimeDataRegionHybridExtractor.class.getName()));
    }
  }

  private void extractTsFileInsertion(PipeRealtimeEvent event) {
    event
        .getTsFileEpoch()
        .migrateState(
            this,
            state ->
                state.equals(TsFileEpoch.State.EMPTY) ? TsFileEpoch.State.USING_TSFILE : state);

    final TsFileEpoch.State state = event.getTsFileEpoch().getState(this);
    switch (state) {
      case EMPTY:
      case USING_TSFILE:
        if (!pendingQueue.waitedOffer(event)) {
          // this would not happen, but just in case.
          // pendingQueue is unbounded, so it should never reach capacity.
          final String errorMessage =
              String.format(
                  "extractTsFileInsertion: pending queue of PipeRealtimeDataRegionHybridExtractor %s "
                      + "has reached capacity, discard TsFile event %s, current state %s",
                  this, event, event.getTsFileEpoch().getState(this));
          LOGGER.error(errorMessage);
          PipeAgent.runtime()
              .report(pipeTaskMeta, new PipeRuntimeNonCriticalException(errorMessage));

          // Ignore the tsfile event.
          event.decreaseReferenceCount(PipeRealtimeDataRegionHybridExtractor.class.getName());
        }
        break;
      case USING_TABLET:
        // All the tablet events have been extracted, so we can ignore the tsfile event.
        event.decreaseReferenceCount(PipeRealtimeDataRegionHybridExtractor.class.getName());
        break;
      default:
        throw new UnsupportedOperationException(
            String.format(
                "Unsupported state %s for hybrid realtime extractor %s",
                state, PipeRealtimeDataRegionHybridExtractor.class.getName()));
    }
  }

  private boolean isApproachingCapacity() {
    return pendingQueue.size()
        >= PipeConfig.getInstance().getPipeExtractorPendingQueueTabletLimit();
  }

  @Override
  public Event supply() {
    PipeRealtimeEvent realtimeEvent = (PipeRealtimeEvent) pendingQueue.directPoll();

    while (realtimeEvent != null) {
      Event suppliedEvent;

      // used to judge type of event, not directly for supplying.
      final Event eventToSupply = realtimeEvent.getEvent();
      if (eventToSupply instanceof TabletInsertionEvent) {
        suppliedEvent = supplyTabletInsertion(realtimeEvent);
      } else if (eventToSupply instanceof TsFileInsertionEvent) {
        suppliedEvent = supplyTsFileInsertion(realtimeEvent);
      } else {
        throw new UnsupportedOperationException(
            String.format(
                "Unsupported event type %s for hybrid realtime extractor %s to supply.",
                eventToSupply.getClass(), this));
      }

      realtimeEvent.decreaseReferenceCount(PipeRealtimeDataRegionHybridExtractor.class.getName());

      if (suppliedEvent != null) {
        return suppliedEvent;
      }

      realtimeEvent = (PipeRealtimeEvent) pendingQueue.directPoll();
    }

    // means the pending queue is empty.
    return null;
  }

  private Event supplyTabletInsertion(PipeRealtimeEvent event) {
    event
        .getTsFileEpoch()
        .migrateState(
            this,
            state ->
                (state.equals(TsFileEpoch.State.EMPTY)) ? TsFileEpoch.State.USING_TABLET : state);

    if (event.getTsFileEpoch().getState(this).equals(TsFileEpoch.State.USING_TABLET)) {
      if (event.increaseReferenceCount(PipeRealtimeDataRegionHybridExtractor.class.getName())) {
        return event.getEvent();
      } else {
        // if the event's reference count can not be increased, it means the data represented by
        // this event is not reliable anymore. but the data represented by this event
        // has been carried by the following tsfile event, so we can just discard this event.
        event.getTsFileEpoch().migrateState(this, state -> TsFileEpoch.State.USING_TSFILE);
        LOGGER.warn(
            "Discard tablet event {} because it is not reliable anymore. "
                + "Change the state of TsFileEpoch to USING_TSFILE.",
            event);
        return null;
      }
    }
    // if the state is USING_TSFILE, discard the event and poll the next one.
    return null;
  }

  private Event supplyTsFileInsertion(PipeRealtimeEvent event) {
    event
        .getTsFileEpoch()
        .migrateState(
            this,
            state -> {
              // this would not happen, but just in case.
              if (state.equals(TsFileEpoch.State.EMPTY)) {
                LOGGER.error(
                    String.format("EMPTY TsFileEpoch when supplying TsFile Event %s", event));
                return TsFileEpoch.State.USING_TSFILE;
              }
              return state;
            });

    if (event.getTsFileEpoch().getState(this).equals(TsFileEpoch.State.USING_TSFILE)) {
      if (event.increaseReferenceCount(PipeRealtimeDataRegionHybridExtractor.class.getName())) {
        return event.getEvent();
      } else {
        // if the event's reference count can not be increased, it means the data represented by
        // this event is not reliable anymore. the data has been lost. we simply discard this event
        // and report the exception to PipeRuntimeAgent.
        final String errorMessage =
            String.format(
                "TsFile Event %s can not be supplied because "
                    + "the reference count can not be increased, "
                    + "the data represented by this event is lost",
                event.getEvent());
        LOGGER.error(errorMessage);
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
