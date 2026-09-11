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
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.commons.pipe.event.ProgressReportEvent;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.event.common.deletion.PipeDeleteDataNodeEvent;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeEvent;
import org.apache.iotdb.db.pipe.metric.overview.PipeDataNodeRemainingEventAndTimeOperator;
import org.apache.iotdb.db.pipe.metric.overview.PipeDataNodeSinglePipeMetrics;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.assigner.PipeTsFileEpochProgressIndexKeeper;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.epoch.TsFileEpoch;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeExtractorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class PipeRealtimeDataRegionHybridSource extends PipeRealtimeDataRegionSource {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeRealtimeDataRegionHybridSource.class);

  private boolean isRegionLevelDowngradingEnabled =
      PipeSourceConstant.EXTRACTOR_REALTIME_REGION_LEVEL_DOWNGRADING_DEFAULT_VALUE;
  private final Object regionLevelDowngradingLock = new Object();

  private final Set<TsFileEpoch> activeTsFileEpochs =
      Collections.newSetFromMap(new ConcurrentHashMap<>());
  private final Set<TsFileEpoch> degradedTsFileEpochs =
      Collections.newSetFromMap(new ConcurrentHashMap<>());
  private final Deque<TsFileEpoch> regionLevelDegradedTsFileEpochs = new ArrayDeque<>();
  private final Deque<PipeRealtimeEvent> eventsBeforeRegionLevelDowngrading = new ArrayDeque<>();
  private final Deque<PipeRealtimeEvent> regionLevelBufferedEvents = new ArrayDeque<>();

  private volatile boolean isRegionLevelDegraded = false;
  private TsFileEpoch regionLevelTailTsFileEpoch = null;
  private boolean canSupplyEventsBeforeRegionLevelDowngrading = false;
  private int inFlightTsFileCount = 0;

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
      synchronized (regionLevelDowngradingLock) {
        if (isClosed.get()) {
          event.decreaseReferenceCount(PipeRealtimeDataRegionHybridSource.class.getName(), false);
          return;
        }
        doExtractInternal(event);
      }
      return;
    }
    doExtractInternal(event);
  }

  private void doExtractInternal(final PipeRealtimeEvent event) {
    final Event eventToExtract = event.getEvent();

    if (eventToExtract instanceof TabletInsertionEvent) {
      extractTabletInsertion(event);
    } else if (eventToExtract instanceof TsFileInsertionEvent) {
      extractTsFileInsertion(event);
      event.getTsFileEpoch().clearState(this);
    } else if (eventToExtract instanceof PipeHeartbeatEvent) {
      extractHeartbeat(event);
    } else if (eventToExtract instanceof PipeDeleteDataNodeEvent) {
      pendingQueue.offer(event);
    } else {
      throw new UnsupportedOperationException(
          String.format(
              DataNodePipeMessages
                  .PIPE_EXCEPTION_UNSUPPORTED_EVENT_TYPE_S_FOR_HYBRID_REALTIME_EXTRACTOR_S_9C4F4C82,
              eventToExtract.getClass(),
              this));
    }
  }

  @Override
  public boolean isNeedListenToTsFile() {
    return shouldExtractInsertion;
  }

  @Override
  public boolean isNeedListenToInsertNode() {
    return shouldExtractInsertion;
  }

  private void extractTabletInsertion(final PipeRealtimeEvent event) {
    markTsFileEpochActive(event.getTsFileEpoch());

    if (isRegionLevelDowngradingEnabled
        && isRegionLevelDegraded
        && degradedTsFileEpochs.contains(event.getTsFileEpoch())) {
      event.getTsFileEpoch().migrateState(this, currentState -> TsFileEpoch.State.USING_TSFILE);
      PipeTsFileEpochProgressIndexKeeper.getInstance()
          .registerProgressIndex(
              dataRegionId, getTsFileDedupScopeID(), event.getTsFileEpoch().getResource());
      event.decreaseReferenceCount(PipeRealtimeDataRegionHybridSource.class.getName(), false);
      return;
    }

    TsFileEpoch.State state;

    if (isRegionLevelDowngradingEnabled && isRegionLevelDegraded) {
      // Retain only the newest epoch as a realtime tail. Once another epoch arrives, the previous
      // tail is downgraded to bound the buffered tablet memory to roughly one TsFile.
      prepareRegionLevelTailTsFileEpochUnderLock(event.getTsFileEpoch());
      if (canNotUseTabletAnymore(event)) {
        if (regionLevelTailTsFileEpoch == event.getTsFileEpoch()) {
          promoteRegionLevelTailTsFileEpochUnderLock();
          bufferPendingEventsForRegionLevelExitUnderLock();
          rebalanceRegionLevelBufferedEventsUnderLock();
        }
        event.getTsFileEpoch().migrateState(this, currentState -> TsFileEpoch.State.USING_TSFILE);
        PipeTsFileEpochProgressIndexKeeper.getInstance()
            .registerProgressIndex(
                dataRegionId, getTsFileDedupScopeID(), event.getTsFileEpoch().getResource());
        markTsFileEpochDegradedFromExtraction(event.getTsFileEpoch());
        event.decreaseReferenceCount(PipeRealtimeDataRegionHybridSource.class.getName(), false);
        return;
      }
      event
          .getTsFileEpoch()
          .migrateState(
              this,
              currentState ->
                  currentState == TsFileEpoch.State.EMPTY
                      ? TsFileEpoch.State.USING_TABLET
                      : currentState);
    } else if (canNotUseTabletAnymore(event)) {
      event.getTsFileEpoch().migrateState(this, curState -> TsFileEpoch.State.USING_TSFILE);
      PipeTsFileEpochProgressIndexKeeper.getInstance()
          .registerProgressIndex(
              dataRegionId, getTsFileDedupScopeID(), event.getTsFileEpoch().getResource());
    } else {
      event
          .getTsFileEpoch()
          .migrateState(
              this,
              curState -> {
                switch (curState) {
                  case USING_BOTH:
                  case USING_TSFILE:
                    return TsFileEpoch.State.USING_BOTH;
                  case EMPTY:
                  case USING_TABLET:
                  default:
                    return TsFileEpoch.State.USING_TABLET;
                }
              });
    }

    state = event.getTsFileEpoch().getState(this);
    if (state == TsFileEpoch.State.USING_TSFILE || state == TsFileEpoch.State.USING_BOTH) {
      markTsFileEpochDegradedFromExtraction(event.getTsFileEpoch());
    }
    switch (state) {
      case USING_TSFILE:
        // Ignore the tablet event.
        event.decreaseReferenceCount(PipeRealtimeDataRegionHybridSource.class.getName(), false);
        break;
      case USING_BOTH:
        if (isRegionLevelDowngradingEnabled && isRegionLevelDegraded) {
          event.decreaseReferenceCount(PipeRealtimeDataRegionHybridSource.class.getName(), false);
          break;
        }
        // USING_BOTH indicates that there are discarded events previously. In this case, we need
        // to delay the progress report to the TsFile event, to avoid losing data.
        event.skipReportOnCommit();
        pendingQueue.offer(event);
        break;
      case EMPTY:
      case USING_TABLET:
        pendingQueue.offer(event);
        break;
      default:
        throw new UnsupportedOperationException(
            String.format(
                DataNodePipeMessages
                    .PIPE_EXCEPTION_UNSUPPORTED_STATE_S_FOR_HYBRID_REALTIME_EXTRACTOR_S_43BD62C2,
                state,
                PipeRealtimeDataRegionHybridSource.class.getName()));
    }
  }

  private void extractTsFileInsertion(final PipeRealtimeEvent event) {
    markTsFileEpochActive(event.getTsFileEpoch());

    // Notice that, if the tsFile is partially extracted because the pipe is not opened before, the
    // former data won't be extracted
    event
        .getTsFileEpoch()
        .migrateState(
            this,
            state -> {
              switch (state) {
                case EMPTY:
                  return ((PipeTsFileInsertionEvent) event.getEvent()).isLoaded()
                      ? TsFileEpoch.State.USING_TSFILE
                      : TsFileEpoch.State.USING_TABLET;
                case USING_TABLET:
                  return TsFileEpoch.State.USING_TABLET;
                case USING_TSFILE:
                  return TsFileEpoch.State.USING_TSFILE;
                case USING_BOTH:
                default:
                  return canNotUseTabletAnymore(event)
                      ? TsFileEpoch.State.USING_TSFILE
                      : TsFileEpoch.State.USING_BOTH;
              }
            });

    final TsFileEpoch.State state = event.getTsFileEpoch().getState(this);
    if (state == TsFileEpoch.State.USING_BOTH
        || (isRegionLevelDowngradingEnabled
            && isRegionLevelDegraded
            && state == TsFileEpoch.State.USING_TSFILE)) {
      markTsFileEpochDegradedFromExtraction(event.getTsFileEpoch());
    }
    switch (state) {
      case USING_TABLET:
        if (isRegionLevelDowngradingEnabled && isRegionLevelDegraded) {
          pendingQueue.offer(event);
          return;
        }
        // If the state is USING_TABLET, discard the event
        PipeTsFileEpochProgressIndexKeeper.getInstance()
            .eliminateProgressIndex(
                dataRegionId, getTsFileDedupScopeID(), event.getTsFileEpoch().getFilePath());
        event.decreaseReferenceCount(PipeRealtimeDataRegionHybridSource.class.getName(), false);
        clearTsFileEpoch(event.getTsFileEpoch());
        return;
      case EMPTY:
      case USING_TSFILE:
      case USING_BOTH:
        pendingQueue.offer(event);
        break;
      default:
        throw new UnsupportedOperationException(
            String.format(
                DataNodePipeMessages
                    .PIPE_EXCEPTION_UNSUPPORTED_STATE_S_FOR_HYBRID_REALTIME_EXTRACTOR_S_43BD62C2,
                state,
                PipeRealtimeDataRegionHybridSource.class.getName()));
    }
  }

  private void markTsFileEpochActive(final TsFileEpoch tsFileEpoch) {
    synchronized (regionLevelDowngradingLock) {
      activeTsFileEpochs.add(tsFileEpoch);
      reportTsFileEpochDegradedStatusUnderLock();
    }
  }

  private void markTsFileEpochDegraded(final TsFileEpoch tsFileEpoch) {
    markTsFileEpochDegraded(tsFileEpoch, false);
  }

  private void markTsFileEpochDegradedFromExtraction(final TsFileEpoch tsFileEpoch) {
    markTsFileEpochDegraded(tsFileEpoch, true);
  }

  private void markTsFileEpochDegraded(
      final TsFileEpoch tsFileEpoch, final boolean shouldPreservePendingEvents) {
    synchronized (regionLevelDowngradingLock) {
      final boolean wasRegionLevelDegraded = isRegionLevelDegraded;
      if (isRegionLevelDowngradingEnabled
          && shouldPreservePendingEvents
          && !wasRegionLevelDegraded) {
        PipeRealtimeEvent pendingEvent;
        while ((pendingEvent = (PipeRealtimeEvent) pendingQueue.directPoll()) != null) {
          eventsBeforeRegionLevelDowngrading.offerLast(pendingEvent);
        }
        canSupplyEventsBeforeRegionLevelDowngrading = true;
      }
      if (regionLevelTailTsFileEpoch == tsFileEpoch) {
        regionLevelTailTsFileEpoch = null;
      }
      markTsFileEpochDegradedUnderLock(tsFileEpoch);
      if (isRegionLevelDowngradingEnabled) {
        // A downgrade discovered while supplying an event happens after all remaining pending
        // events. Track those events as the possible realtime tail before rebalancing the queues.
        if (!wasRegionLevelDegraded && !shouldPreservePendingEvents) {
          bufferPendingEventsAndTrackRegionLevelTailUnderLock();
        }
        rebalanceRegionLevelBufferedEventsUnderLock();
      }
    }
  }

  private void markTsFileEpochDegradedUnderLock(final TsFileEpoch tsFileEpoch) {
    activeTsFileEpochs.add(tsFileEpoch);
    if (degradedTsFileEpochs.add(tsFileEpoch) && isRegionLevelDowngradingEnabled) {
      regionLevelDegradedTsFileEpochs.offerLast(tsFileEpoch);
    }
    if (isRegionLevelDowngradingEnabled) {
      isRegionLevelDegraded = true;
    }
    reportTsFileEpochDegradedStatusUnderLock();
  }

  private void prepareRegionLevelTailTsFileEpochUnderLock(final TsFileEpoch tsFileEpoch) {
    if (regionLevelTailTsFileEpoch == tsFileEpoch) {
      return;
    }

    if (regionLevelTailTsFileEpoch != null) {
      promoteRegionLevelTailTsFileEpochUnderLock();
      bufferPendingEventsForRegionLevelExitUnderLock();
      rebalanceRegionLevelBufferedEventsUnderLock();
    }
    regionLevelTailTsFileEpoch = tsFileEpoch;
  }

  private void promoteRegionLevelTailTsFileEpochUnderLock() {
    final TsFileEpoch tsFileEpoch = regionLevelTailTsFileEpoch;
    if (tsFileEpoch == null) {
      return;
    }

    regionLevelTailTsFileEpoch = null;
    tsFileEpoch.migrateState(this, state -> TsFileEpoch.State.USING_TSFILE);
    PipeTsFileEpochProgressIndexKeeper.getInstance()
        .registerProgressIndex(dataRegionId, getTsFileDedupScopeID(), tsFileEpoch.getResource());
    markTsFileEpochDegradedUnderLock(tsFileEpoch);
  }

  private void bufferPendingEventsAndTrackRegionLevelTailUnderLock() {
    PipeRealtimeEvent pendingEvent;
    while ((pendingEvent = (PipeRealtimeEvent) pendingQueue.directPoll()) != null) {
      if (pendingEvent.getEvent() instanceof TabletInsertionEvent
          && !degradedTsFileEpochs.contains(pendingEvent.getTsFileEpoch())) {
        if (regionLevelTailTsFileEpoch != null
            && regionLevelTailTsFileEpoch != pendingEvent.getTsFileEpoch()) {
          promoteRegionLevelTailTsFileEpochUnderLock();
        }
        regionLevelTailTsFileEpoch = pendingEvent.getTsFileEpoch();
      }
      regionLevelBufferedEvents.offerLast(pendingEvent);
    }
  }

  private void rebalanceRegionLevelBufferedEventsUnderLock() {
    bufferPendingEventsForRegionLevelExitUnderLock();

    final TsFileEpoch nextDegradedTsFileEpoch = regionLevelDegradedTsFileEpochs.peekFirst();
    final Deque<PipeRealtimeEvent> retainedEvents = new ArrayDeque<>();
    boolean nextDegradedTsFileEventPromoted = false;
    PipeRealtimeEvent bufferedEvent;
    while ((bufferedEvent = regionLevelBufferedEvents.pollFirst()) != null) {
      if (bufferedEvent.getEvent() instanceof TabletInsertionEvent
          && degradedTsFileEpochs.contains(bufferedEvent.getTsFileEpoch())) {
        bufferedEvent.decreaseReferenceCount(
            PipeRealtimeDataRegionHybridSource.class.getName(), false);
      } else if (!nextDegradedTsFileEventPromoted
          && bufferedEvent.getEvent() instanceof TsFileInsertionEvent
          && bufferedEvent.getTsFileEpoch() == nextDegradedTsFileEpoch) {
        pendingQueue.offer(bufferedEvent);
        nextDegradedTsFileEventPromoted = true;
      } else {
        retainedEvents.offerLast(bufferedEvent);
      }
    }
    regionLevelBufferedEvents.addAll(retainedEvents);
  }

  private void clearTsFileEpoch(final TsFileEpoch tsFileEpoch) {
    synchronized (regionLevelDowngradingLock) {
      clearTsFileEpochUnderLock(tsFileEpoch);
    }
  }

  private void clearTsFileEpochAfterCommit(final TsFileEpoch tsFileEpoch) {
    synchronized (regionLevelDowngradingLock) {
      if (isClosed.get()) {
        return;
      }
      if (inFlightTsFileCount > 0) {
        --inFlightTsFileCount;
      }
      clearTsFileEpochUnderLock(tsFileEpoch);

      // The decision whether newer writes can resume the realtime path must be made at the exact
      // point when the last currently degraded TsFile is committed. Otherwise, writes arriving
      // between the commit and the next supply call would still be unnecessarily downgraded.
      if (isRegionLevelDegraded
          && inFlightTsFileCount == 0
          && degradedTsFileEpochs.isEmpty()
          && eventsBeforeRegionLevelDowngrading.isEmpty()) {
        bufferPendingEventsForRegionLevelExitUnderLock();
        tryExitRegionLevelDowngrading(false);
      }
    }
  }

  private void bufferPendingEventsForRegionLevelExitUnderLock() {
    PipeRealtimeEvent event;
    while ((event = (PipeRealtimeEvent) pendingQueue.directPoll()) != null) {
      regionLevelBufferedEvents.offerLast(event);
    }
  }

  private void clearTsFileEpochUnderLock(final TsFileEpoch tsFileEpoch) {
    activeTsFileEpochs.remove(tsFileEpoch);
    degradedTsFileEpochs.remove(tsFileEpoch);
    regionLevelDegradedTsFileEpochs.remove(tsFileEpoch);
    if (regionLevelTailTsFileEpoch == tsFileEpoch) {
      regionLevelTailTsFileEpoch = null;
    }
    if (isRegionLevelDowngradingEnabled && isRegionLevelDegraded && inFlightTsFileCount == 0) {
      rebalanceRegionLevelBufferedEventsUnderLock();
    }
    reportTsFileEpochDegradedStatusUnderLock();
  }

  private void clearRegionLevelBufferedEventsUnderLock() {
    clearBufferedEventsUnderLock(eventsBeforeRegionLevelDowngrading);
    clearBufferedEventsUnderLock(regionLevelBufferedEvents);
  }

  private void clearBufferedEventsUnderLock(final Deque<PipeRealtimeEvent> events) {
    PipeRealtimeEvent event;
    while ((event = events.pollFirst()) != null) {
      event.clearReferenceCount(PipeRealtimeDataRegionHybridSource.class.getName());
    }
  }

  private void reportTsFileEpochDegradedStatusUnderLock() {
    if (isRegionLevelDowngradingEnabled && isRegionLevelDegraded) {
      PipeDataNodeAgent.task()
          .setPipeTsFileEpochDegraded(pipeName, creationTime, dataRegionId, true);
    } else if (activeTsFileEpochs.isEmpty()) {
      PipeDataNodeAgent.task().clearPipeTsFileEpochDegraded(pipeName, creationTime, dataRegionId);
    } else {
      PipeDataNodeAgent.task()
          .setPipeTsFileEpochDegraded(
              pipeName,
              creationTime,
              dataRegionId,
              isRegionLevelDowngradingEnabled ? false : !degradedTsFileEpochs.isEmpty());
    }
  }

  @Override
  public void close() throws Exception {
    try {
      // Do not hold regionLevelDowngradingLock while waiting for the assigner to stop. An event
      // already being assigned may need the same lock to finish extraction.
      super.close();
    } finally {
      synchronized (regionLevelDowngradingLock) {
        clearRegionLevelBufferedEventsUnderLock();
        activeTsFileEpochs.clear();
        degradedTsFileEpochs.clear();
        regionLevelDegradedTsFileEpochs.clear();
        isRegionLevelDegraded = false;
        regionLevelTailTsFileEpoch = null;
        canSupplyEventsBeforeRegionLevelDowngrading = false;
        inFlightTsFileCount = 0;
        PipeDataNodeAgent.task().clearPipeTsFileEpochDegraded(pipeName, creationTime, dataRegionId);
      }
    }
  }

  @Override
  protected void extractProgressReportEvent(final PipeRealtimeEvent event) {
    if (isRegionLevelDowngradingEnabled) {
      synchronized (regionLevelDowngradingLock) {
        if (isClosed.get()) {
          event.decreaseReferenceCount(PipeRealtimeDataRegionHybridSource.class.getName(), false);
          return;
        }
        super.extractProgressReportEvent(event);
      }
      return;
    }
    super.extractProgressReportEvent(event);
  }

  // If the insertNode's memory has reached the dangerous threshold, we should not extract any
  // tablets.
  private boolean canNotUseTabletAnymore(final PipeRealtimeEvent event) {
    final long floatingMemoryUsageInByte =
        PipeDataNodeAgent.task().getFloatingMemoryUsageInByte(pipeName, creationTime);
    final long pipeCount = PipeDataNodeAgent.task().getPipeCount();
    long totalFloatingMemorySizeInBytes =
        PipeDataNodeResourceManager.memory().getTotalFloatingMemorySizeInBytes();
    // If the occupied memory has reached the max, it may cause a large latency to the receiver due
    // to queuing. To reduce the latency, we lower the memory limit forcibly in the single tsFile
    // since the tsFile is doomed to be transferred, then more downgrading will just cause more
    // latency to a few points and will greatly reduce the incoming latencies.
    if (PipeConfig.getInstance().getPipeRealtimeForceDowngradingEnabled()
        && !event.maySourceOnlyUseTablets(this)) {
      totalFloatingMemorySizeInBytes =
          (long)
              ((double) totalFloatingMemorySizeInBytes
                  * PipeConfig.getInstance().getPipeRealtimeForceDowngradingProportion());
    }
    final boolean mayInsertNodeMemoryReachDangerousThreshold =
        floatingMemoryUsageInByte * pipeCount >= totalFloatingMemorySizeInBytes;
    if (mayInsertNodeMemoryReachDangerousThreshold && event.maySourceOnlyUseTablets(this)) {
      final PipeDataNodeRemainingEventAndTimeOperator operator =
          PipeDataNodeSinglePipeMetrics.getInstance().remainingEventAndTimeOperatorMap.get(pipeID);
      LOGGER.info(
          DataNodePipeMessages.PIPE_TASK_CANNOTUSETABLETANYMORE_FOR_TSFILE_THE_MEMORY,
          pipeName,
          dataRegionId,
          event.getTsFileEpoch().getFilePath(),
          floatingMemoryUsageInByte,
          totalFloatingMemorySizeInBytes / pipeCount,
          Optional.ofNullable(operator)
              .map(PipeDataNodeRemainingEventAndTimeOperator::getInsertNodeEventCount)
              .orElse(0));
    }
    return mayInsertNodeMemoryReachDangerousThreshold;
  }

  @Override
  public Event supply() {
    if (isRegionLevelDowngradingEnabled) {
      synchronized (regionLevelDowngradingLock) {
        return isRegionLevelDegraded ? supplyRegionLevelDegradedInternal() : supplyInternal();
      }
    }
    return supplyInternal();
  }

  private Event supplyInternal() {
    PipeRealtimeEvent realtimeEvent = (PipeRealtimeEvent) pendingQueue.directPoll();

    while (realtimeEvent != null) {
      final Event suppliedEvent = supplyExtractedEvent(realtimeEvent);
      if (suppliedEvent != null) {
        return suppliedEvent;
      }

      if (isRegionLevelDowngradingEnabled && isRegionLevelDegraded) {
        return supplyRegionLevelDegradedInternal();
      }

      realtimeEvent = (PipeRealtimeEvent) pendingQueue.directPoll();
    }

    // Means the pending queue is empty.
    return null;
  }

  private Event supplyExtractedEvent(final PipeRealtimeEvent realtimeEvent) {
    Event suppliedEvent;

    // Used to judge the type of the event, not directly for supplying.
    final Event eventToSupply = realtimeEvent.getEvent();
    if (eventToSupply instanceof TabletInsertionEvent) {
      suppliedEvent = supplyTabletInsertion(realtimeEvent);
    } else if (eventToSupply instanceof TsFileInsertionEvent) {
      suppliedEvent = supplyTsFileInsertion(realtimeEvent);
    } else if (eventToSupply instanceof PipeHeartbeatEvent) {
      suppliedEvent = supplyHeartbeat(realtimeEvent);
    } else if (eventToSupply instanceof PipeDeleteDataNodeEvent
        || eventToSupply instanceof ProgressReportEvent) {
      suppliedEvent = supplyDirectly(realtimeEvent);
    } else {
      throw new UnsupportedOperationException(
          String.format(
              DataNodePipeMessages
                  .PIPE_EXCEPTION_UNSUPPORTED_EVENT_TYPE_S_FOR_HYBRID_REALTIME_EXTRACTOR_S_474BAAC2,
              eventToSupply.getClass(),
              this));
    }

    realtimeEvent.decreaseReferenceCount(PipeRealtimeDataRegionHybridSource.class.getName(), false);

    if (suppliedEvent != null) {
      suppliedEvent = assignReplicateIndexIfNeeded(realtimeEvent, suppliedEvent);
      maySkipIndex4Event(realtimeEvent);
    }
    return suppliedEvent;
  }

  private Event supplyRegionLevelDegradedInternal() {
    // Once region-level downgrading starts, only one TsFile is allowed to be in flight. Events of
    // newer epochs are buffered until all currently degraded TsFiles are committed downstream.
    if (inFlightTsFileCount > 0) {
      return null;
    }

    final Event eventBeforeDowngrading = supplyEventsBeforeRegionLevelDowngradingInternal();
    if (eventBeforeDowngrading != null) {
      return eventBeforeDowngrading;
    }

    PipeRealtimeEvent realtimeEvent = (PipeRealtimeEvent) pendingQueue.directPoll();
    while (realtimeEvent != null) {
      final Event eventToSupply = realtimeEvent.getEvent();
      if (eventToSupply instanceof TabletInsertionEvent) {
        if (degradedTsFileEpochs.contains(realtimeEvent.getTsFileEpoch())) {
          realtimeEvent.decreaseReferenceCount(
              PipeRealtimeDataRegionHybridSource.class.getName(), false);
        } else {
          regionLevelBufferedEvents.offerLast(realtimeEvent);
        }
      } else if (eventToSupply instanceof TsFileInsertionEvent) {
        final TsFileEpoch.State state = realtimeEvent.getTsFileEpoch().getState(this);
        if (degradedTsFileEpochs.contains(realtimeEvent.getTsFileEpoch())
            || state == TsFileEpoch.State.USING_TSFILE
            || state == TsFileEpoch.State.USING_BOTH) {
          markTsFileEpochDegraded(realtimeEvent.getTsFileEpoch());
          if (regionLevelDegradedTsFileEpochs.peekFirst() != realtimeEvent.getTsFileEpoch()) {
            regionLevelBufferedEvents.offerLast(realtimeEvent);
            realtimeEvent = (PipeRealtimeEvent) pendingQueue.directPoll();
            continue;
          }
          final Event suppliedEvent = supplyExtractedEvent(realtimeEvent);
          if (suppliedEvent != null) {
            return suppliedEvent;
          }
        } else {
          regionLevelBufferedEvents.offerLast(realtimeEvent);
        }
      } else {
        regionLevelBufferedEvents.offerLast(realtimeEvent);
      }

      realtimeEvent = (PipeRealtimeEvent) pendingQueue.directPoll();
    }

    return tryExitRegionLevelDowngrading(true);
  }

  private Event supplyEventsBeforeRegionLevelDowngradingInternal() {
    PipeRealtimeEvent realtimeEvent;
    while ((realtimeEvent = eventsBeforeRegionLevelDowngrading.pollFirst()) != null) {
      final Event eventToSupply = realtimeEvent.getEvent();

      if (canSupplyEventsBeforeRegionLevelDowngrading) {
        if (eventToSupply instanceof TabletInsertionEvent
            && degradedTsFileEpochs.contains(realtimeEvent.getTsFileEpoch())) {
          realtimeEvent.decreaseReferenceCount(
              PipeRealtimeDataRegionHybridSource.class.getName(), false);
          continue;
        }

        final Event suppliedEvent = supplyExtractedEvent(realtimeEvent);
        if (suppliedEvent != null) {
          return suppliedEvent;
        }
        if (eventToSupply instanceof TabletInsertionEvent
            && degradedTsFileEpochs.contains(realtimeEvent.getTsFileEpoch())) {
          canSupplyEventsBeforeRegionLevelDowngrading = false;
        }
        continue;
      }

      if (eventToSupply instanceof TabletInsertionEvent) {
        if (degradedTsFileEpochs.contains(realtimeEvent.getTsFileEpoch())) {
          realtimeEvent.decreaseReferenceCount(
              PipeRealtimeDataRegionHybridSource.class.getName(), false);
        } else {
          regionLevelBufferedEvents.offerLast(realtimeEvent);
        }
      } else if (eventToSupply instanceof TsFileInsertionEvent) {
        final TsFileEpoch.State state = realtimeEvent.getTsFileEpoch().getState(this);
        if (degradedTsFileEpochs.contains(realtimeEvent.getTsFileEpoch())
            || state == TsFileEpoch.State.USING_TSFILE
            || state == TsFileEpoch.State.USING_BOTH) {
          markTsFileEpochDegraded(realtimeEvent.getTsFileEpoch());
          if (regionLevelDegradedTsFileEpochs.peekFirst() != realtimeEvent.getTsFileEpoch()) {
            regionLevelBufferedEvents.offerLast(realtimeEvent);
            continue;
          }
          final Event suppliedEvent = supplyExtractedEvent(realtimeEvent);
          if (suppliedEvent != null) {
            return suppliedEvent;
          }
        } else {
          regionLevelBufferedEvents.offerLast(realtimeEvent);
        }
      } else {
        regionLevelBufferedEvents.offerLast(realtimeEvent);
      }
    }

    canSupplyEventsBeforeRegionLevelDowngrading = false;
    return null;
  }

  private Event tryExitRegionLevelDowngrading(final boolean shouldSupplyAfterTransition) {
    if (!degradedTsFileEpochs.isEmpty()) {
      // Some degraded epochs are still waiting for their TsFile to be flushed.
      return null;
    }

    if (regionLevelTailTsFileEpoch != null
        && regionLevelBufferedEvents.stream()
            .filter(event -> event.getTsFileEpoch() == regionLevelTailTsFileEpoch)
            .filter(event -> event.getEvent() instanceof TabletInsertionEvent)
            .anyMatch(event -> event.getEvent().isReleased())) {
      promoteRegionLevelTailTsFileEpochUnderLock();
      rebalanceRegionLevelBufferedEventsUnderLock();
      return shouldSupplyAfterTransition ? supplyRegionLevelDegradedInternal() : null;
    }

    isRegionLevelDegraded = false;
    regionLevelTailTsFileEpoch = null;
    PipeRealtimeEvent bufferedEvent;
    while ((bufferedEvent = regionLevelBufferedEvents.pollFirst()) != null) {
      pendingQueue.offer(bufferedEvent);
    }
    reportTsFileEpochDegradedStatusUnderLock();
    return shouldSupplyAfterTransition ? supplyInternal() : null;
  }

  private Event supplyTabletInsertion(final PipeRealtimeEvent event) {
    if (event.increaseReferenceCount(PipeRealtimeDataRegionHybridSource.class.getName())) {
      return event.getEvent();
    } else {
      // If the event's reference count can not be increased, it means the data represented by
      // this event is not reliable anymore. but the data represented by this event
      // has been carried by the following tsfile event, so we can just discard this event.
      event
          .getTsFileEpoch()
          .migrateState(
              this,
              state ->
                  isRegionLevelDowngradingEnabled
                      ? TsFileEpoch.State.USING_TSFILE
                      : TsFileEpoch.State.USING_BOTH);
      markTsFileEpochDegraded(event.getTsFileEpoch());
      LOGGER.warn(DataNodePipeMessages.DISCARD_TABLET_EVENT_BECAUSE_IT_IS_NOT, event);
      return null;
    }
  }

  private Event supplyTsFileInsertion(final PipeRealtimeEvent event) {
    if (isRegionLevelDowngradingEnabled
        && event.getTsFileEpoch().getState(this) == TsFileEpoch.State.USING_TABLET) {
      PipeTsFileEpochProgressIndexKeeper.getInstance()
          .eliminateProgressIndex(
              dataRegionId, getTsFileDedupScopeID(), event.getTsFileEpoch().getFilePath());
      clearTsFileEpoch(event.getTsFileEpoch());
      return null;
    }

    if (event.increaseReferenceCount(PipeRealtimeDataRegionHybridSource.class.getName())) {
      if (isRegionLevelDowngradingEnabled) {
        ++inFlightTsFileCount;
        final PipeTsFileInsertionEvent tsFileInsertionEvent =
            (PipeTsFileInsertionEvent) event.getEvent();
        final Runnable clearTsFileEpochHook =
            () -> clearTsFileEpochAfterCommit(event.getTsFileEpoch());
        tsFileInsertionEvent.addOnTransferredHook(clearTsFileEpochHook);
        tsFileInsertionEvent.addOnDiscardedHook(clearTsFileEpochHook);
      } else {
        clearTsFileEpoch(event.getTsFileEpoch());
      }
      return event.getEvent();
    } else {
      // If the event's reference count can not be increased, it means the data represented by
      // this event is not reliable anymore. the data has been lost. we simply discard this
      // event and report the exception to PipeRuntimeAgent.
      final String errorMessage =
          String.format(
              DataNodePipeMessages.EVENT_CAN_NOT_BE_SUPPLIED_BECAUSE_DATA_IS_LOST,
              event.getEvent());
      LOGGER.error(errorMessage);
      PipeDataNodeAgent.runtime()
          .report(pipeTaskMeta, new PipeRuntimeNonCriticalException(errorMessage));
      PipeTsFileEpochProgressIndexKeeper.getInstance()
          .eliminateProgressIndex(
              dataRegionId, getTsFileDedupScopeID(), event.getTsFileEpoch().getFilePath());
      clearTsFileEpoch(event.getTsFileEpoch());
      return null;
    }
  }
}
