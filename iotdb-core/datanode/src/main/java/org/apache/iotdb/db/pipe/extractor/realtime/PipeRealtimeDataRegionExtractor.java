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

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskExtractorRuntimeEnvironment;
import org.apache.iotdb.commons.pipe.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.db.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeEvent;
import org.apache.iotdb.db.pipe.extractor.realtime.listener.PipeInsertionDataNodeListener;
import org.apache.iotdb.db.pipe.extractor.realtime.listener.PipeTimePartitionListener;
import org.apache.iotdb.db.pipe.metric.PipeDataRegionEventCounter;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.rescon.memory.TimePartitionManager;
import org.apache.iotdb.db.utils.DateTimeUtils;
import org.apache.iotdb.pipe.api.PipeExtractor;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeExtractorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.exception.PipeParameterNotValidException;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_PATTERN_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_END_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_PATTERN_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_START_TIME_KEY;

public abstract class PipeRealtimeDataRegionExtractor implements PipeExtractor {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeRealtimeDataRegionExtractor.class);

  protected String pipeName;
  protected String dataRegionId;
  protected PipeTaskMeta pipeTaskMeta;

  protected String pattern;
  private boolean isDbNameCoveredByPattern = false;

  protected long realtimeDataExtractionStartTime = Long.MIN_VALUE; // Event time
  protected long realtimeDataExtractionEndTime = Long.MAX_VALUE; // Event time

  private final AtomicBoolean enableSkippingTimeParseByTimePartition = new AtomicBoolean(false);
  private boolean disableSkippingTimeParse = false;
  private long startTimePartitionIdLowerBound; // calculated by realtimeDataExtractionStartTime
  private long endTimePartitionIdUpperBound; // calculated by realtimeDataExtractionEndTime

  // This variable is used to record the upper and lower bounds that the time partition ID
  // corresponding to this data region has ever reached. It may be updated by
  // PipeTimePartitionListener.
  private final AtomicReference<Pair<Long, Long>> dataRegionTimePartitionIdBound =
      new AtomicReference<>();

  protected boolean isForwardingPipeRequests;

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
    PipeParameters parameters = validator.getParameters();

    try {
      realtimeDataExtractionStartTime =
          parameters.hasAnyAttributes(SOURCE_START_TIME_KEY)
              ? DateTimeUtils.convertDatetimeStrToLong(
                  parameters.getStringByKeys(SOURCE_START_TIME_KEY), ZoneId.systemDefault())
              : Long.MIN_VALUE;
      realtimeDataExtractionEndTime =
          parameters.hasAnyAttributes(SOURCE_END_TIME_KEY)
              ? DateTimeUtils.convertDatetimeStrToLong(
                  parameters.getStringByKeys(SOURCE_END_TIME_KEY), ZoneId.systemDefault())
              : Long.MAX_VALUE;
      if (realtimeDataExtractionStartTime > realtimeDataExtractionEndTime) {
        throw new PipeParameterNotValidException(
            String.format(
                "%s should be less than or equal to %s.",
                SOURCE_START_TIME_KEY, SOURCE_END_TIME_KEY));
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

    pipeName = environment.getPipeName();
    dataRegionId = String.valueOf(environment.getRegionId());
    pipeTaskMeta = environment.getPipeTaskMeta();

    // Metrics related to TsFileEpoch are managed in PipeExtractorMetrics. These metrics are
    // indexed by the taskID of IoTDBDataRegionExtractor. To avoid PipeRealtimeDataRegionExtractor
    // holding a reference to IoTDBDataRegionExtractor, the taskID should be constructed to
    // match that of IoTDBDataRegionExtractor.
    long creationTime = environment.getCreationTime();
    taskID = pipeName + "_" + dataRegionId + "_" + creationTime;

    pattern =
        parameters.getStringOrDefault(
            Arrays.asList(EXTRACTOR_PATTERN_KEY, SOURCE_PATTERN_KEY),
            PipeExtractorConstant.EXTRACTOR_PATTERN_DEFAULT_VALUE);
    final DataRegion dataRegion =
        StorageEngine.getInstance().getDataRegion(new DataRegionId(environment.getRegionId()));
    if (dataRegion != null) {
      final String databaseName = dataRegion.getDatabaseName();
      if (databaseName != null
          && pattern.length() <= databaseName.length()
          && databaseName.startsWith(pattern)) {
        isDbNameCoveredByPattern = true;
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

    final Pair<Long, Long> timePartitionIdBound =
        TimePartitionManager.getInstance()
            .getTimePartitionIdBound(new DataRegionId(Integer.parseInt(dataRegionId)));
    if (Objects.nonNull(timePartitionIdBound)) {
      setDataRegionTimePartitionIdBound(timePartitionIdBound);
    } else {
      LOGGER.warn(
          "Something unexpected happened when PipeRealtimeDataRegionExtractor({}) obtaining time partition id bound on data region {}, set enableTimeParseSkipByTimePartition to false.",
          taskID,
          dataRegionId);
      enableSkippingTimeParseByTimePartition.set(false);
    }

    isForwardingPipeRequests =
        parameters.getBooleanOrDefault(
            Arrays.asList(
                PipeExtractorConstant.EXTRACTOR_FORWARDING_PIPE_REQUESTS_KEY,
                PipeExtractorConstant.SOURCE_FORWARDING_PIPE_REQUESTS_KEY),
            PipeExtractorConstant.EXTRACTOR_FORWARDING_PIPE_REQUESTS_DEFAULT_VALUE);
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

  /** @param event the event from the storage engine */
  public final void extract(PipeRealtimeEvent event) {
    if (isDbNameCoveredByPattern) {
      event.skipParsingPattern();
    }

    if (!disableSkippingTimeParse && enableSkippingTimeParseByTimePartition.get()) {
      if (isDataRegionTimePartitionCoveredByTimeRange()) {
        event.skipParsingTime();
      } else {
        // Since we only record the upper and lower bounds that time partition have ever reached, if
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
    if (!event.shouldParseTime() || event.getEvent().isEventTimeOverlappedWithTimeRange()) {
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

  public abstract boolean isNeedListenToTsFile();

  public abstract boolean isNeedListenToInsertNode();

  public final String getPattern() {
    return pattern;
  }

  public final long getRealtimeDataExtractionStartTime() {
    return realtimeDataExtractionStartTime;
  }

  public final long getRealtimeDataExtractionEndTime() {
    return realtimeDataExtractionEndTime;
  }

  public void setDataRegionTimePartitionIdBound(Pair<Long, Long> timePartitionIdBound) {
    LOGGER.info(
        "PipeRealtimeDataRegionExtractor({}) observed data region {} time partition growth, recording partition id bound: {}, set enableTimeParseSkipByTimePartition to true.",
        taskID,
        dataRegionId,
        timePartitionIdBound);
    dataRegionTimePartitionIdBound.set(timePartitionIdBound);
    enableSkippingTimeParseByTimePartition.set(true);
  }

  private boolean isDataRegionTimePartitionCoveredByTimeRange() {
    Pair<Long, Long> timePartitionIdBound = dataRegionTimePartitionIdBound.get();
    return Objects.nonNull(timePartitionIdBound)
        && startTimePartitionIdLowerBound <= timePartitionIdBound.left
        && timePartitionIdBound.right <= endTimePartitionIdUpperBound;
  }

  public final boolean isForwardingPipeRequests() {
    return isForwardingPipeRequests;
  }

  public final String getPipeName() {
    return pipeName;
  }

  public final PipeTaskMeta getPipeTaskMeta() {
    return pipeTaskMeta;
  }

  @Override
  public String toString() {
    return "PipeRealtimeDataRegionExtractor{"
        + "pattern='"
        + pattern
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
