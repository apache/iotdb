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
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.agent.PipeAgent;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeEvent;
import org.apache.iotdb.db.pipe.extractor.realtime.epoch.TsFileEpoch;
import org.apache.iotdb.db.pipe.resource.PipeResourceManager;
import org.apache.iotdb.db.storageengine.dataregion.wal.WALManager;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PipeRealtimeDataRegionHybridExtractor extends PipeRealtimeDataRegionExtractor {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeRealtimeDataRegionHybridExtractor.class);

  private volatile boolean isStartedToSupply = false;
  private final ReentrantReadWriteLock tsFileStateLock = new ReentrantReadWriteLock();
  private final AtomicInteger eventCollectorQueueTsFileSize = new AtomicInteger(0);

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
    tsFileStateLock.readLock().lock();
    try {
      event
          .getTsFileEpoch()
          .migrateState(
              this,
              state ->
                  (state.equals(TsFileEpoch.State.EMPTY)) ? TsFileEpoch.State.USING_TABLET : state);
      if ((!isStartedToSupply
              || mayWalSizeReachThrottleThreshold()
              || tooManyWALPinned()
              || isTsFileEventCountInQueueExceededLimit())
          && !event.getTsFileEpoch().getState(this).equals(TsFileEpoch.State.USING_TABLET_FORCE)) {
        // In the following 3 cases, we should not extract any more tablet events. all the data
        // represented by the tablet events should be carried by the following tsfile event:
        //  1. The historical extractor has not consumed all the data.
        //  2. HybridExtractor will first try to do extraction in log mode, and then choose log or
        //  tsfile mode to continue extracting, but if (leader data regions num * Wal size) >
        // (maximum
        //  size of wal buffer), the write operation will be throttled, so we should not extract any
        //  more tablet events.
        //  3. The number of tsfile events in the pending queue has exceeded the limit.
        event.getTsFileEpoch().migrateState(this, state -> TsFileEpoch.State.USING_TSFILE);
      }
    } finally {
      tsFileStateLock.readLock().unlock();
    }

    final TsFileEpoch.State state = event.getTsFileEpoch().getState(this);
    switch (state) {
      case USING_TSFILE:
        // Ignore the tablet event.
        event.decreaseReferenceCount(PipeRealtimeDataRegionHybridExtractor.class.getName(), false);
        break;
      case EMPTY:
      case USING_TABLET:
      case USING_TABLET_FORCE:
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
          event.decreaseReferenceCount(
              PipeRealtimeDataRegionHybridExtractor.class.getName(), false);
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
    tsFileStateLock.writeLock().lock();
    try {
      event
          .getTsFileEpoch()
          .migrateState(
              this,
              state ->
                  state.equals(TsFileEpoch.State.USING_TABLET)
                      ? TsFileEpoch.State.USING_TABLET_FORCE
                      : TsFileEpoch.State.USING_TSFILE);
    } finally {
      tsFileStateLock.writeLock().unlock();
    }

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
          event.decreaseReferenceCount(
              PipeRealtimeDataRegionHybridExtractor.class.getName(), false);
        }
        break;
      case USING_TABLET:
      case USING_TABLET_FORCE:
        // All the tablet events have been extracted, so we can ignore the tsFile event.
        event.decreaseReferenceCount(PipeRealtimeDataRegionHybridExtractor.class.getName(), false);
        break;
      default:
        throw new UnsupportedOperationException(
            String.format(
                "Unsupported state %s for hybrid realtime extractor %s",
                state, PipeRealtimeDataRegionHybridExtractor.class.getName()));
    }
  }

  private void extractHeartbeat(PipeRealtimeEvent event) {
    // Bind extractor so that the heartbeat event can later inform the extractor of queue size
    ((PipeHeartbeatEvent) event.getEvent()).bindExtractor(this);

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
      event.decreaseReferenceCount(PipeRealtimeDataRegionHybridExtractor.class.getName(), false);
      return;
    }

    if (!pendingQueue.waitedOffer(event)) {
      // this would not happen, but just in case.
      // pendingQueue is unbounded, so it should never reach capacity.
      LOGGER.error(
          "extract: pending queue of PipeRealtimeDataRegionHybridExtractor {} "
              + "has reached capacity, discard heartbeat event {}",
          this,
          event);

      // Do not report exception since the PipeHeartbeatEvent doesn't affect the correction of
      // pipe progress.

      // ignore this event.
      event.decreaseReferenceCount(PipeRealtimeDataRegionLogExtractor.class.getName(), false);
    }
  }

  private boolean mayWalSizeReachThrottleThreshold() {
    return 3 * WALManager.getInstance().getTotalDiskUsage()
        > IoTDBDescriptor.getInstance().getConfig().getThrottleThreshold();
  }

  private boolean tooManyWALPinned() {
    return PipeResourceManager.wal().getApproximatePinnedWALCount()
        > Math.max(1, PipeAgent.task().getLeaderDataRegionCount())
            * PipeConfig.getInstance().getPipeMaxAllowedPendingTsFileEpochPerDataRegion();
  }

  private boolean isTsFileEventCountInQueueExceededLimit() {
    return pendingQueue.getTsFileInsertionEventCount() + eventCollectorQueueTsFileSize.get()
        >= PipeConfig.getInstance().getPipeMaxAllowedPendingTsFileEpochPerDataRegion();
  }

  public void informEventCollectorQueueTsFileSize(int queueSize) {
    eventCollectorQueueTsFileSize.set(queueSize);
  }

  @Override
  public Event supply() {
    isStartedToSupply = true;

    PipeRealtimeEvent realtimeEvent = (PipeRealtimeEvent) pendingQueue.directPoll();

    while (realtimeEvent != null) {
      Event suppliedEvent;

      // used to judge type of event, not directly for supplying.
      final Event eventToSupply = realtimeEvent.getEvent();
      if (eventToSupply instanceof TabletInsertionEvent) {
        suppliedEvent = supplyTabletInsertion(realtimeEvent);
      } else if (eventToSupply instanceof TsFileInsertionEvent) {
        suppliedEvent = supplyTsFileInsertion(realtimeEvent);
      } else if (eventToSupply instanceof PipeHeartbeatEvent) {
        suppliedEvent = supplyHeartbeat(realtimeEvent);
      } else {
        throw new UnsupportedOperationException(
            String.format(
                "Unsupported event type %s for hybrid realtime extractor %s to supply.",
                eventToSupply.getClass(), this));
      }

      realtimeEvent.decreaseReferenceCount(
          PipeRealtimeDataRegionHybridExtractor.class.getName(), false);

      if (suppliedEvent != null) {
        return suppliedEvent;
      }

      realtimeEvent = (PipeRealtimeEvent) pendingQueue.directPoll();
    }

    // means the pending queue is empty.
    return null;
  }

  private Event supplyTabletInsertion(PipeRealtimeEvent event) {
    if (event.getTsFileEpoch().getState(this).equals(TsFileEpoch.State.USING_TABLET)
        || event.getTsFileEpoch().getState(this).equals(TsFileEpoch.State.USING_TABLET_FORCE)) {
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

  private Event supplyHeartbeat(PipeRealtimeEvent event) {
    if (event.increaseReferenceCount(PipeRealtimeDataRegionHybridExtractor.class.getName())) {
      return event.getEvent();
    } else {
      // this would not happen, but just in case.
      LOGGER.error(
          "Heartbeat Event {} can not be supplied because "
              + "the reference count can not be increased",
          event.getEvent());

      // Do not report exception since the PipeHeartbeatEvent doesn't affect the correction of pipe
      // progress.

      return null;
    }
  }
}
