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

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeNonCriticalException;
import org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskExtractorRuntimeEnvironment;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.pattern.PipePattern;
import org.apache.iotdb.commons.pipe.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.db.pipe.agent.PipeAgent;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeEvent;
import org.apache.iotdb.db.pipe.extractor.dataregion.DataRegionListeningFilter;
import org.apache.iotdb.db.pipe.extractor.dataregion.realtime.listener.PipeInsertionDataNodeListener;
import org.apache.iotdb.db.pipe.extractor.dataregion.realtime.listener.PipeTimePartitionListener;
import org.apache.iotdb.db.pipe.metric.PipeDataRegionEventCounter;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.utils.DateTimeUtils;
import org.apache.iotdb.pipe.api.PipeExtractor;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeExtractorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.exception.PipeParameterNotValidException;

import org.apache.tsfile.utils.Pair;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_END_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_MODS_ENABLE_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_MODS_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_START_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_END_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_MODS_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_START_TIME_KEY;

public abstract class PipeRealtimeDataRegionExtractor implements PipeExtractor {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeRealtimeDataRegionExtractor.class);

  protected String pipeName;
  protected String dataRegionId;
  protected PipeTaskMeta pipeTaskMeta;

  protected boolean shouldExtractInsertion;
  protected boolean shouldExtractDeletion;

  protected PipePattern pipePattern;
  private boolean isDbNameCoveredByPattern = false;

  protected long realtimeDataExtractionStartTime = Long.MIN_VALUE; // Event time
  protected long realtimeDataExtractionEndTime = Long.MAX_VALUE; // Event time

  private boolean disableSkippingTimeParse = false;
  private long startTimePartitionIdLowerBound; // calculated by realtimeDataExtractionStartTime
  private long endTimePartitionIdUpperBound; // calculated by realtimeDataExtractionEndTime

  // This variable is used to record the upper and lower bounds that the time partition ID
  // corresponding to this data region has ever reached. It may be updated by
  // PipeTimePartitionListener.
  private final AtomicReference<Pair<Long, Long>> dataRegionTimePartitionIdBound =
      new AtomicReference<>();

  protected boolean isForwardingPipeRequests;

  private boolean shouldTransferModFile; // Whether to transfer mods

  // This queue is used to store pending events extracted by the method extract(). The method
  // supply() will poll events from this queue and send them to the next pipe plugin.
  protected final UnboundedBlockingPendingQueue<Event> pendingQueue =
      new UnboundedBlockingPendingQueue<>(new PipeDataRegionEventCounter());

  protected final AtomicBoolean isClosed = new AtomicBoolean(false);

  private String taskID;

  protected PipeRealtimeDataRegionExtractor() {
    // Do nothing
  }

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    final PipeParameters parameters = validator.getParameters();

    try {
      realtimeDataExtractionStartTime =
          parameters.hasAnyAttributes(SOURCE_START_TIME_KEY, EXTRACTOR_START_TIME_KEY)
              ? DateTimeUtils.convertTimestampOrDatetimeStrToLongWithDefaultZone(
                  parameters.getStringByKeys(SOURCE_START_TIME_KEY, EXTRACTOR_START_TIME_KEY))
              : Long.MIN_VALUE;
      realtimeDataExtractionEndTime =
          parameters.hasAnyAttributes(SOURCE_END_TIME_KEY, EXTRACTOR_END_TIME_KEY)
              ? DateTimeUtils.convertTimestampOrDatetimeStrToLongWithDefaultZone(
                  parameters.getStringByKeys(SOURCE_END_TIME_KEY, EXTRACTOR_END_TIME_KEY))
              : Long.MAX_VALUE;
      if (realtimeDataExtractionStartTime > realtimeDataExtractionEndTime) {
        throw new PipeParameterNotValidException(
            String.format(
                "%s or %s should be less than or equal to %s or %s.",
                SOURCE_START_TIME_KEY,
                EXTRACTOR_START_TIME_KEY,
                SOURCE_END_TIME_KEY,
                EXTRACTOR_END_TIME_KEY));
      }
    } catch (Exception e) {
      // compatible with the current validation framework
      throw new PipeParameterNotValidException(e.getMessage());
    }
  }

  @Override
  public void customize(PipeParameters parameters, PipeExtractorRuntimeConfiguration configuration)
      throws Exception {
    final PipeTaskExtractorRuntimeEnvironment environment =
        (PipeTaskExtractorRuntimeEnvironment) configuration.getRuntimeEnvironment();

    final Pair<Boolean, Boolean> insertionDeletionListeningOptionPair =
        DataRegionListeningFilter.parseInsertionDeletionListeningOptionPair(parameters);
    shouldExtractInsertion = insertionDeletionListeningOptionPair.getLeft();
    shouldExtractDeletion = insertionDeletionListeningOptionPair.getRight();

    pipeName = environment.getPipeName();
    dataRegionId = String.valueOf(environment.getRegionId());
    pipeTaskMeta = environment.getPipeTaskMeta();

    // Metrics related to TsFileEpoch are managed in PipeExtractorMetrics. These metrics are
    // indexed by the taskID of IoTDBDataRegionExtractor. To avoid PipeRealtimeDataRegionExtractor
    // holding a reference to IoTDBDataRegionExtractor, the taskID should be constructed to
    // match that of IoTDBDataRegionExtractor.
    final long creationTime = environment.getCreationTime();
    taskID = pipeName + "_" + dataRegionId + "_" + creationTime;

    pipePattern = PipePattern.parsePipePatternFromSourceParameters(parameters);

    final DataRegion dataRegion =
        StorageEngine.getInstance().getDataRegion(new DataRegionId(environment.getRegionId()));
    if (dataRegion != null) {
      final String databaseName = dataRegion.getDatabaseName();
      if (databaseName != null) {
        isDbNameCoveredByPattern = pipePattern.coversDb(databaseName);
      }
    }

    startTimePartitionIdLowerBound =
        (realtimeDataExtractionStartTime % TimePartitionUtils.getTimePartitionInterval() == 0)
            ? TimePartitionUtils.getTimePartitionId(realtimeDataExtractionStartTime)
            : TimePartitionUtils.getTimePartitionId(realtimeDataExtractionStartTime) + 1;
    endTimePartitionIdUpperBound =
        (realtimeDataExtractionEndTime % TimePartitionUtils.getTimePartitionInterval() == 0)
            ? TimePartitionUtils.getTimePartitionId(realtimeDataExtractionEndTime)
            : TimePartitionUtils.getTimePartitionId(realtimeDataExtractionEndTime) - 1;

    isForwardingPipeRequests =
        parameters.getBooleanOrDefault(
            Arrays.asList(
                PipeExtractorConstant.EXTRACTOR_FORWARDING_PIPE_REQUESTS_KEY,
                PipeExtractorConstant.SOURCE_FORWARDING_PIPE_REQUESTS_KEY),
            PipeExtractorConstant.EXTRACTOR_FORWARDING_PIPE_REQUESTS_DEFAULT_VALUE);

    shouldTransferModFile =
        parameters.getBooleanOrDefault(
            Arrays.asList(SOURCE_MODS_ENABLE_KEY, EXTRACTOR_MODS_ENABLE_KEY),
            EXTRACTOR_MODS_ENABLE_DEFAULT_VALUE || shouldExtractDeletion);
  }

  @Override
  public void start() throws Exception {
    PipeTimePartitionListener.getInstance().startListen(dataRegionId, this);
    PipeInsertionDataNodeListener.getInstance().startListenAndAssign(dataRegionId, this);
  }

  @Override
  public void close() throws Exception {
    if (Objects.nonNull(dataRegionId)) {
      PipeInsertionDataNodeListener.getInstance().stopListenAndAssign(dataRegionId, this);
      PipeTimePartitionListener.getInstance().stopListen(dataRegionId, this);
    }

    synchronized (isClosed) {
      clearPendingQueue();
      isClosed.set(true);
    }
  }

  private void clearPendingQueue() {
    final List<Event> eventsToDrop = new ArrayList<>(pendingQueue.size());

    // processor stage is closed later than extractor stage, {@link supply()} may be called after
    // processor stage is closed. To avoid concurrent issues, we should clear the pending queue
    // before clearing all the reference count of the events in the pending queue.
    pendingQueue.forEach(eventsToDrop::add);
    pendingQueue.clear();

    eventsToDrop.forEach(
        event -> {
          if (event instanceof EnrichedEvent) {
            ((EnrichedEvent) event)
                .clearReferenceCount(PipeRealtimeDataRegionExtractor.class.getName());
          }
        });
  }

  /** @param event the {@link Event} from the {@link StorageEngine} */
  public final void extract(PipeRealtimeEvent event) {
    if (isDbNameCoveredByPattern) {
      event.skipParsingPattern();
    }

    if (!disableSkippingTimeParse && Objects.nonNull(dataRegionTimePartitionIdBound.get())) {
      if (isDataRegionTimePartitionCoveredByTimeRange()) {
        event.skipParsingTime();
      } else {
        // Since we only record the upper and lower bounds that time partition has ever reached, if
        // the time partition cannot be covered by the time range during query, it will not be
        // possible later.
        disableSkippingTimeParse = true;
      }
    }

    // 1. Check if time parsing is necessary. If not, it means that the timestamps of the data
    // contained in this event are definitely within the time range [start time, end time].
    // Otherwise,
    // 2. Check if the timestamps of the data contained in this event intersect with the time range.
    // If there is no intersection, it indicates that this data will be filtered out by the
    // extractor, and the extract process is skipped.
    if (!event.shouldParseTime() || event.getEvent().mayEventTimeOverlappedWithTimeRange()) {
      doExtract(event);
    } else {
      event.decreaseReferenceCount(PipeRealtimeDataRegionExtractor.class.getName(), false);
    }

    synchronized (isClosed) {
      if (isClosed.get()) {
        clearPendingQueue();
      }
    }
  }

  protected abstract void doExtract(PipeRealtimeEvent event);

  protected void extractHeartbeat(PipeRealtimeEvent event) {
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
      event.decreaseReferenceCount(PipeRealtimeDataRegionExtractor.class.getName(), false);
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

      // Do not report exception since the PipeHeartbeatEvent doesn't affect
      // the correction of pipe progress.

      // ignore this event.
      event.decreaseReferenceCount(PipeRealtimeDataRegionExtractor.class.getName(), false);
    }
  }

  protected void extractDeletion(PipeRealtimeEvent event) {
    if (!pendingQueue.waitedOffer(event)) {
      // This would not happen, but just in case.
      // Pending is unbounded, so it should never reach capacity.
      final String errorMessage =
          String.format(
              "extract: pending queue of %s %s "
                  + "has reached capacity, discard deletion event %s",
              this.getClass().getSimpleName(), this, event);
      LOGGER.error(errorMessage);
      PipeAgent.runtime().report(pipeTaskMeta, new PipeRuntimeNonCriticalException(errorMessage));

      // Ignore the event.
      event.decreaseReferenceCount(PipeRealtimeDataRegionExtractor.class.getName(), false);
    }
  }

  protected Event supplyHeartbeat(PipeRealtimeEvent event) {
    if (event.increaseReferenceCount(PipeRealtimeDataRegionExtractor.class.getName())) {
      return event.getEvent();
    } else {
      // this would not happen, but just in case.
      LOGGER.error(
          "Heartbeat Event {} can not be supplied because "
              + "the reference count can not be increased",
          event.getEvent());

      // Do not report exception since the PipeHeartbeatEvent doesn't affect
      // the correction of pipe progress.

      return null;
    }
  }

  protected Event supplyDeletion(PipeRealtimeEvent event) {
    if (event.increaseReferenceCount(PipeRealtimeDataRegionExtractor.class.getName())) {
      return event.getEvent();
    } else {
      // if the event's reference count can not be increased, it means the data represented by
      // this event is not reliable anymore. the data has been lost. we simply discard this
      // event and report the exception to PipeRuntimeAgent.
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

  public final String getPipeName() {
    return pipeName;
  }

  public final PipeTaskMeta getPipeTaskMeta() {
    return pipeTaskMeta;
  }

  public final boolean shouldExtractInsertion() {
    return shouldExtractInsertion;
  }

  public final boolean shouldExtractDeletion() {
    return shouldExtractDeletion;
  }

  public final String getPatternString() {
    return pipePattern != null ? pipePattern.getPattern() : null;
  }

  public final PipePattern getPipePattern() {
    return pipePattern;
  }

  public final long getRealtimeDataExtractionStartTime() {
    return realtimeDataExtractionStartTime;
  }

  public final long getRealtimeDataExtractionEndTime() {
    return realtimeDataExtractionEndTime;
  }

  public void setDataRegionTimePartitionIdBound(@NonNull Pair<Long, Long> timePartitionIdBound) {
    LOGGER.info(
        "PipeRealtimeDataRegionExtractor({}) observed data region {} time partition growth, recording time partition id bound: {}.",
        taskID,
        dataRegionId,
        timePartitionIdBound);
    dataRegionTimePartitionIdBound.set(timePartitionIdBound);
  }

  private boolean isDataRegionTimePartitionCoveredByTimeRange() {
    final Pair<Long, Long> timePartitionIdBound = dataRegionTimePartitionIdBound.get();
    return startTimePartitionIdLowerBound <= timePartitionIdBound.left
        && timePartitionIdBound.right <= endTimePartitionIdUpperBound;
  }

  public final boolean isForwardingPipeRequests() {
    return isForwardingPipeRequests;
  }

  public abstract boolean isNeedListenToTsFile();

  public abstract boolean isNeedListenToInsertNode();

  public final boolean isShouldTransferModFile() {
    return shouldTransferModFile;
  }

  @Override
  public String toString() {
    return "PipeRealtimeDataRegionExtractor{"
        + "pipePattern='"
        + pipePattern
        + '\''
        + ", dataRegionId='"
        + dataRegionId
        + '\''
        + '}';
  }

  //////////////////////////// APIs provided for metric framework ////////////////////////////

  public int getTabletInsertionEventCount() {
    return pendingQueue.getTabletInsertionEventCount();
  }

  public int getTsFileInsertionEventCount() {
    return pendingQueue.getTsFileInsertionEventCount();
  }

  public int getPipeHeartbeatEventCount() {
    return pendingQueue.getPipeHeartbeatEventCount();
  }

  public String getTaskID() {
    return taskID;
  }
}
