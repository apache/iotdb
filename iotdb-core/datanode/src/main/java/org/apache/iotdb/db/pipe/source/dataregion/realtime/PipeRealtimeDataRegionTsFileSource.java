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

package org.apache.iotdb.db.pipe.source.dataregion.realtime;

import org.apache.iotdb.commons.exception.pipe.PipeRuntimeNonCriticalException;
import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.commons.pipe.event.ProgressReportEvent;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.event.common.deletion.PipeDeleteDataNodeEvent;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeEvent;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.assigner.PipeTsFileEpochProgressIndexKeeper;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.epoch.TsFileEpoch;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeExtractorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.PriorityQueue;
import java.util.Queue;

public class PipeRealtimeDataRegionTsFileSource extends PipeRealtimeDataRegionSource {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeRealtimeDataRegionTsFileSource.class);

  private boolean isRegionLevelDowngradingEnabled =
      PipeSourceConstant.EXTRACTOR_REALTIME_REGION_LEVEL_DOWNGRADING_DEFAULT_VALUE;
  private final Object tsFileOrderingLock = new Object();
  private final Queue<PipeRealtimeEvent> orderedTsFileEvents =
      new PriorityQueue<>(
          (event1, event2) ->
              compareTsFileEpochsByQueryPriority(event1.getTsFileEpoch(), event2.getTsFileEpoch()));
  private final Deque<PipeRealtimeEvent> bufferedEvents = new ArrayDeque<>();
  private TsFileEpoch inFlightTsFileEpoch;

  @Override
  public void validate(final PipeParameterValidator validator) throws Exception {
    super.validate(validator);
    validator
        .validateAttributeValueRange(
            PipeSourceConstant.EXTRACTOR_REALTIME_REGION_LEVEL_DOWNGRADING_KEY,
            true,
            Boolean.TRUE.toString(),
            Boolean.FALSE.toString())
        .validateAttributeValueRange(
            PipeSourceConstant.SOURCE_REALTIME_REGION_LEVEL_DOWNGRADING_KEY,
            true,
            Boolean.TRUE.toString(),
            Boolean.FALSE.toString());
  }

  @Override
  public void customize(
      final PipeParameters parameters, final PipeExtractorRuntimeConfiguration configuration)
      throws Exception {
    super.customize(parameters, configuration);
    isRegionLevelDowngradingEnabled =
        parameters.getBooleanOrDefault(
            Arrays.asList(
                PipeSourceConstant.EXTRACTOR_REALTIME_REGION_LEVEL_DOWNGRADING_KEY,
                PipeSourceConstant.SOURCE_REALTIME_REGION_LEVEL_DOWNGRADING_KEY),
            PipeSourceConstant.EXTRACTOR_REALTIME_REGION_LEVEL_DOWNGRADING_DEFAULT_VALUE);
  }

  @Override
  protected void doExtract(final PipeRealtimeEvent event) {
    if (isRegionLevelDowngradingEnabled) {
      synchronized (tsFileOrderingLock) {
        if (isClosed.get()) {
          event.decreaseReferenceCount(PipeRealtimeDataRegionTsFileSource.class.getName(), false);
          return;
        }
        doExtractInternal(event);
      }
      return;
    }
    doExtractInternal(event);
  }

  private void doExtractInternal(final PipeRealtimeEvent event) {
    if (event.getEvent() instanceof PipeHeartbeatEvent) {
      extractHeartbeat(event);
      return;
    }

    if (event.getEvent() instanceof PipeDeleteDataNodeEvent) {
      pendingQueue.offer(event);
      return;
    }

    event.getTsFileEpoch().migrateState(this, state -> TsFileEpoch.State.USING_TSFILE);
    PipeTsFileEpochProgressIndexKeeper.getInstance()
        .registerProgressIndex(
            dataRegionId, getTsFileDedupScopeID(), event.getTsFileEpoch().getResource());

    if (!(event.getEvent() instanceof TsFileInsertionEvent)) {
      event.decreaseReferenceCount(PipeRealtimeDataRegionTsFileSource.class.getName(), false);
      return;
    }

    pendingQueue.offer(event);

    event.getTsFileEpoch().clearState(this);
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
  protected void extractProgressReportEvent(final PipeRealtimeEvent event) {
    if (isRegionLevelDowngradingEnabled) {
      synchronized (tsFileOrderingLock) {
        if (isClosed.get()) {
          event.decreaseReferenceCount(PipeRealtimeDataRegionTsFileSource.class.getName(), false);
          return;
        }
        super.extractProgressReportEvent(event);
      }
      return;
    }
    super.extractProgressReportEvent(event);
  }

  @Override
  public void close() throws Exception {
    try {
      super.close();
    } finally {
      synchronized (tsFileOrderingLock) {
        PipeRealtimeEvent event;
        while ((event = orderedTsFileEvents.poll()) != null) {
          event.clearReferenceCount(PipeRealtimeDataRegionTsFileSource.class.getName());
        }
        while ((event = bufferedEvents.pollFirst()) != null) {
          event.clearReferenceCount(PipeRealtimeDataRegionTsFileSource.class.getName());
        }
        inFlightTsFileEpoch = null;
      }
    }
  }

  @Override
  public Event supply() {
    if (isRegionLevelDowngradingEnabled) {
      synchronized (tsFileOrderingLock) {
        if (isClosed.get() || inFlightTsFileEpoch != null) {
          return null;
        }

        // Include every observed, unsent TsFile before selecting the next one. Non-file events
        // stay behind the files so they cannot advance progress past an unfinished file.
        PipeRealtimeEvent pendingEvent;
        while ((pendingEvent = (PipeRealtimeEvent) pendingQueue.directPoll()) != null) {
          if (pendingEvent.getEvent() instanceof TsFileInsertionEvent) {
            orderedTsFileEvents.offer(pendingEvent);
          } else {
            bufferedEvents.offerLast(pendingEvent);
          }
        }
        return supplyInternal();
      }
    }
    return supplyInternal();
  }

  private PipeRealtimeEvent pollNextEvent() {
    if (!isRegionLevelDowngradingEnabled) {
      return (PipeRealtimeEvent) pendingQueue.directPoll();
    }
    return orderedTsFileEvents.isEmpty() ? bufferedEvents.pollFirst() : orderedTsFileEvents.poll();
  }

  private Event supplyInternal() {
    PipeRealtimeEvent realtimeEvent = pollNextEvent();

    while (realtimeEvent != null) {
      Event suppliedEvent = null;

      if (realtimeEvent.getEvent() instanceof PipeHeartbeatEvent) {
        suppliedEvent = supplyHeartbeat(realtimeEvent);
      } else if (realtimeEvent.getEvent() instanceof PipeDeleteDataNodeEvent
          || realtimeEvent.getEvent() instanceof ProgressReportEvent) {
        suppliedEvent = supplyDirectly(realtimeEvent);
      } else if (realtimeEvent.increaseReferenceCount(
          PipeRealtimeDataRegionTsFileSource.class.getName())) {
        if (isRegionLevelDowngradingEnabled) {
          inFlightTsFileEpoch = realtimeEvent.getTsFileEpoch();
          final TsFileEpoch suppliedTsFileEpoch = inFlightTsFileEpoch;
          final PipeTsFileInsertionEvent tsFileInsertionEvent =
              (PipeTsFileInsertionEvent) realtimeEvent.getEvent();
          final Runnable onCompletedHook = () -> clearInFlightTsFileEpoch(suppliedTsFileEpoch);
          tsFileInsertionEvent.addOnTransferredHook(onCompletedHook);
          tsFileInsertionEvent.addOnDiscardedHook(onCompletedHook);
        }
        suppliedEvent = realtimeEvent.getEvent();
      } else {
        // if the event's reference count can not be increased, it means the data represented by
        // this event is not reliable anymore. the data has been lost. we simply discard this event
        // and report the exception to PipeRuntimeAgent.
        final String errorMessage =
            String.format(
                DataNodePipeMessages.EVENT_CAN_NOT_BE_SUPPLIED_BECAUSE_DATA_IS_LOST,
                realtimeEvent.getEvent());
        LOGGER.error(errorMessage);
        PipeDataNodeAgent.runtime()
            .report(pipeTaskMeta, new PipeRuntimeNonCriticalException(errorMessage));
        PipeTsFileEpochProgressIndexKeeper.getInstance()
            .eliminateProgressIndex(
                dataRegionId,
                getTsFileDedupScopeID(),
                realtimeEvent.getTsFileEpoch().getFilePath());
      }

      realtimeEvent.decreaseReferenceCount(
          PipeRealtimeDataRegionTsFileSource.class.getName(), false);

      if (suppliedEvent != null) {
        suppliedEvent = assignReplicateIndexIfNeeded(realtimeEvent, suppliedEvent);
        maySkipIndex4Event(realtimeEvent);
        return suppliedEvent;
      }

      realtimeEvent = pollNextEvent();
    }

    // means the pending queue is empty.
    return null;
  }

  private void clearInFlightTsFileEpoch(final TsFileEpoch tsFileEpoch) {
    synchronized (tsFileOrderingLock) {
      if (inFlightTsFileEpoch == tsFileEpoch) {
        inFlightTsFileEpoch = null;
      }
    }
  }
}
