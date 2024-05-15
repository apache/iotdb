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

package org.apache.iotdb.db.pipe.extractor.dataregion.historical;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.StateProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.TimeWindowStateProgressIndex;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskExtractorRuntimeEnvironment;
import org.apache.iotdb.commons.pipe.pattern.PipePattern;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.extractor.dataregion.DataRegionListeningFilter;
import org.apache.iotdb.db.pipe.resource.PipeResourceManager;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;
import org.apache.iotdb.db.utils.DateTimeUtils;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeExtractorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.exception.PipeParameterNotValidException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_END_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_HISTORY_ENABLE_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_HISTORY_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_HISTORY_END_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_HISTORY_LOOSE_RANGE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_HISTORY_START_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_MODS_ENABLE_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_MODS_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_START_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_END_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_HISTORY_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_HISTORY_END_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_HISTORY_LOOSE_RANGE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_HISTORY_START_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_MODS_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_START_TIME_KEY;

public class PipeHistoricalDataRegionTsFileExtractor implements PipeHistoricalDataRegionExtractor {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeHistoricalDataRegionTsFileExtractor.class);

  private static final Map<Integer, Long> DATA_REGION_ID_TO_PIPE_FLUSHED_TIME_MAP = new HashMap<>();
  private static final long PIPE_MIN_FLUSH_INTERVAL_IN_MS = 2000;

  private String pipeName;
  private PipeTaskMeta pipeTaskMeta;
  private ProgressIndex startIndex;

  private int dataRegionId;

  private PipePattern pipePattern;
  private boolean isDbNameCoveredByPattern = false;

  private boolean isHistoricalExtractorEnabled = false;

  private long historicalDataExtractionStartTime = Long.MIN_VALUE; // Event time
  private long historicalDataExtractionEndTime = Long.MAX_VALUE; // Event time

  private long historicalDataExtractionTimeLowerBound; // Arrival time

  private boolean sloppyTimeRange; // true to disable time range filter after extraction

  private boolean shouldExtractInsertion;

  private boolean shouldTransferModFile; // Whether to transfer mods

  private Queue<TsFileResource> pendingQueue;

  @Override
  public void validate(PipeParameterValidator validator) {
    final PipeParameters parameters = validator.getParameters();

    if (parameters.hasAnyAttributes(
        SOURCE_START_TIME_KEY,
        EXTRACTOR_START_TIME_KEY,
        SOURCE_END_TIME_KEY,
        EXTRACTOR_END_TIME_KEY)) {
      isHistoricalExtractorEnabled = true;

      try {
        historicalDataExtractionStartTime =
            parameters.hasAnyAttributes(SOURCE_START_TIME_KEY, EXTRACTOR_START_TIME_KEY)
                ? DateTimeUtils.convertTimestampOrDatetimeStrToLongWithDefaultZone(
                    parameters.getStringByKeys(SOURCE_START_TIME_KEY, EXTRACTOR_START_TIME_KEY))
                : Long.MIN_VALUE;
        historicalDataExtractionEndTime =
            parameters.hasAnyAttributes(SOURCE_END_TIME_KEY, EXTRACTOR_END_TIME_KEY)
                ? DateTimeUtils.convertTimestampOrDatetimeStrToLongWithDefaultZone(
                    parameters.getStringByKeys(SOURCE_END_TIME_KEY, EXTRACTOR_END_TIME_KEY))
                : Long.MAX_VALUE;
        if (historicalDataExtractionStartTime > historicalDataExtractionEndTime) {
          throw new PipeParameterNotValidException(
              String.format(
                  "%s or %s should be less than or equal to %s or %s.",
                  SOURCE_START_TIME_KEY,
                  EXTRACTOR_START_TIME_KEY,
                  SOURCE_END_TIME_KEY,
                  EXTRACTOR_END_TIME_KEY));
        }
        return;
      } catch (Exception e) {
        // compatible with the current validation framework
        throw new PipeParameterNotValidException(e.getMessage());
      }
    }

    // Historical data extraction is enabled in the following cases:
    // 1. System restarts the pipe. If the pipe is restarted but historical data extraction is not
    // enabled, the pipe will lose some historical data.
    // 2. User may set the EXTRACTOR_HISTORY_START_TIME and EXTRACTOR_HISTORY_END_TIME without
    // enabling the historical data extraction, which may affect the realtime data extraction.
    isHistoricalExtractorEnabled =
        parameters.getBooleanOrDefault(
                SystemConstant.RESTART_KEY, SystemConstant.RESTART_DEFAULT_VALUE)
            || parameters.getBooleanOrDefault(
                Arrays.asList(EXTRACTOR_HISTORY_ENABLE_KEY, SOURCE_HISTORY_ENABLE_KEY),
                EXTRACTOR_HISTORY_ENABLE_DEFAULT_VALUE);

    try {
      historicalDataExtractionStartTime =
          isHistoricalExtractorEnabled
                  && parameters.hasAnyAttributes(
                      EXTRACTOR_HISTORY_START_TIME_KEY, SOURCE_HISTORY_START_TIME_KEY)
              ? DateTimeUtils.convertTimestampOrDatetimeStrToLongWithDefaultZone(
                  parameters.getStringByKeys(
                      EXTRACTOR_HISTORY_START_TIME_KEY, SOURCE_HISTORY_START_TIME_KEY))
              : Long.MIN_VALUE;
      historicalDataExtractionEndTime =
          isHistoricalExtractorEnabled
                  && parameters.hasAnyAttributes(
                      EXTRACTOR_HISTORY_END_TIME_KEY, SOURCE_HISTORY_END_TIME_KEY)
              ? DateTimeUtils.convertTimestampOrDatetimeStrToLongWithDefaultZone(
                  parameters.getStringByKeys(
                      EXTRACTOR_HISTORY_END_TIME_KEY, SOURCE_HISTORY_END_TIME_KEY))
              : Long.MAX_VALUE;
      if (historicalDataExtractionStartTime > historicalDataExtractionEndTime) {
        throw new PipeParameterNotValidException(
            String.format(
                "%s (%s) should be less than or equal to %s (%s).",
                EXTRACTOR_HISTORY_START_TIME_KEY,
                SOURCE_HISTORY_START_TIME_KEY,
                EXTRACTOR_HISTORY_END_TIME_KEY,
                SOURCE_HISTORY_END_TIME_KEY));
      }

      shouldTransferModFile =
          parameters.getBooleanOrDefault(
              Arrays.asList(SOURCE_MODS_ENABLE_KEY, EXTRACTOR_MODS_ENABLE_KEY),
              EXTRACTOR_MODS_ENABLE_DEFAULT_VALUE
                  || // Should extract deletion
                  DataRegionListeningFilter.parseInsertionDeletionListeningOptionPair(parameters)
                      .getRight());
    } catch (Exception e) {
      // Compatible with the current validation framework
      throw new PipeParameterNotValidException(e.getMessage());
    }
  }

  @Override
  public void customize(PipeParameters parameters, PipeExtractorRuntimeConfiguration configuration)
      throws IllegalPathException {
    shouldExtractInsertion =
        DataRegionListeningFilter.parseInsertionDeletionListeningOptionPair(parameters).getLeft();
    // Do nothing if only extract deletion
    if (!shouldExtractInsertion) {
      return;
    }

    final PipeTaskExtractorRuntimeEnvironment environment =
        (PipeTaskExtractorRuntimeEnvironment) configuration.getRuntimeEnvironment();

    pipeName = environment.getPipeName();
    pipeTaskMeta = environment.getPipeTaskMeta();
    startIndex = environment.getPipeTaskMeta().getProgressIndex();

    dataRegionId = environment.getRegionId();
    synchronized (DATA_REGION_ID_TO_PIPE_FLUSHED_TIME_MAP) {
      DATA_REGION_ID_TO_PIPE_FLUSHED_TIME_MAP.putIfAbsent(dataRegionId, 0L);
    }

    pipePattern = PipePattern.parsePipePatternFromSourceParameters(parameters);

    final DataRegion dataRegion =
        StorageEngine.getInstance().getDataRegion(new DataRegionId(environment.getRegionId()));
    if (Objects.nonNull(dataRegion)) {
      final String databaseName = dataRegion.getDatabaseName();
      if (Objects.nonNull(databaseName)) {
        isDbNameCoveredByPattern = pipePattern.coversDb(databaseName);
      }
    }

    // Enable historical extractor by default
    historicalDataExtractionTimeLowerBound =
        isHistoricalExtractorEnabled
            ? Long.MIN_VALUE
            // We define the realtime data as the data generated after the creation time
            // of the pipe from user's perspective. But we still need to use
            // PipeHistoricalDataRegionExtractor to extract the realtime data generated between the
            // creation time of the pipe and the time when the pipe starts, because those data
            // can not be listened by PipeRealtimeDataRegionExtractor, and should be extracted by
            // PipeHistoricalDataRegionExtractor from implementation perspective.
            : environment.getCreationTime();

    // Only invoke flushDataRegionAllTsFiles() when the pipe runs in the realtime only mode.
    // realtime only mode -> (historicalDataExtractionTimeLowerBound != Long.MIN_VALUE)
    //
    // Ensure that all data in the data region is flushed to disk before extracting data.
    // This ensures the generation time of all newly generated TsFiles (realtime data) after the
    // invocation of flushDataRegionAllTsFiles() is later than the creationTime of the pipe
    // (historicalDataExtractionTimeLowerBound).
    //
    // Note that: the generation time of the TsFile is the time when the TsFile is created, not
    // the time when the data is flushed to the TsFile.
    //
    // Then we can use the generation time of the TsFile to determine whether the data in the
    // TsFile should be extracted by comparing the generation time of the TsFile with the
    // historicalDataExtractionTimeLowerBound when starting the pipe in realtime only mode.
    //
    // If we don't invoke flushDataRegionAllTsFiles() in the realtime only mode, the data generated
    // between the creation time of the pipe the time when the pipe starts will be lost.
    if (historicalDataExtractionTimeLowerBound != Long.MIN_VALUE) {
      synchronized (DATA_REGION_ID_TO_PIPE_FLUSHED_TIME_MAP) {
        final long lastFlushedByPipeTime =
            DATA_REGION_ID_TO_PIPE_FLUSHED_TIME_MAP.get(dataRegionId);
        if (System.currentTimeMillis() - lastFlushedByPipeTime >= PIPE_MIN_FLUSH_INTERVAL_IN_MS) {
          flushDataRegionAllTsFiles();
          DATA_REGION_ID_TO_PIPE_FLUSHED_TIME_MAP.replace(dataRegionId, System.currentTimeMillis());
        }
      }
    }

    sloppyTimeRange =
        Arrays.stream(
                parameters
                    .getStringOrDefault(
                        Arrays.asList(
                            EXTRACTOR_HISTORY_LOOSE_RANGE_KEY, SOURCE_HISTORY_LOOSE_RANGE_KEY),
                        "")
                    .split(","))
            .map(String::trim)
            .map(String::toLowerCase)
            .collect(Collectors.toSet())
            .contains("time");

    LOGGER.info(
        "Pipe {}@{}: historical data extraction time range, start time {}({}), end time {}({}), sloppy time range {}",
        pipeName,
        dataRegionId,
        DateTimeUtils.convertLongToDate(historicalDataExtractionStartTime),
        historicalDataExtractionStartTime,
        DateTimeUtils.convertLongToDate(historicalDataExtractionEndTime),
        historicalDataExtractionEndTime,
        sloppyTimeRange);
  }

  private void flushDataRegionAllTsFiles() {
    final DataRegion dataRegion =
        StorageEngine.getInstance().getDataRegion(new DataRegionId(dataRegionId));
    if (Objects.isNull(dataRegion)) {
      return;
    }

    dataRegion.writeLock("Pipe: create historical TsFile extractor");
    try {
      dataRegion.syncCloseAllWorkingTsFileProcessors();
    } finally {
      dataRegion.writeUnlock();
    }
  }

  @Override
  public synchronized void start() {
    if (!shouldExtractInsertion) {
      return;
    }

    final DataRegion dataRegion =
        StorageEngine.getInstance().getDataRegion(new DataRegionId(dataRegionId));
    if (Objects.isNull(dataRegion)) {
      pendingQueue = new ArrayDeque<>();
      return;
    }

    dataRegion.writeLock("Pipe: start to extract historical TsFile");
    final long startHistoricalExtractionTime = System.currentTimeMillis();
    try {
      LOGGER.info("Pipe {}@{}: start to flush data region", pipeName, dataRegionId);
      synchronized (DATA_REGION_ID_TO_PIPE_FLUSHED_TIME_MAP) {
        final long lastFlushedByPipeTime =
            DATA_REGION_ID_TO_PIPE_FLUSHED_TIME_MAP.get(dataRegionId);
        if (System.currentTimeMillis() - lastFlushedByPipeTime >= PIPE_MIN_FLUSH_INTERVAL_IN_MS) {
          dataRegion.syncCloseAllWorkingTsFileProcessors();
          DATA_REGION_ID_TO_PIPE_FLUSHED_TIME_MAP.replace(dataRegionId, System.currentTimeMillis());
          LOGGER.info(
              "Pipe {}@{}: finish to flush data region, took {} ms",
              pipeName,
              dataRegionId,
              System.currentTimeMillis() - startHistoricalExtractionTime);
        } else {
          LOGGER.info(
              "Pipe {}@{}: skip to flush data region, last flushed time {} ms ago",
              pipeName,
              dataRegionId,
              System.currentTimeMillis() - lastFlushedByPipeTime);
        }
      }

      final TsFileManager tsFileManager = dataRegion.getTsFileManager();
      tsFileManager.readLock();
      try {
        final int originalSequenceTsFileCount = tsFileManager.size(true);
        final int originalUnsequenceTsFileCount = tsFileManager.size(false);
        final List<TsFileResource> resourceList =
            new ArrayList<>(originalSequenceTsFileCount + originalUnsequenceTsFileCount);
        LOGGER.info(
            "Pipe {}@{}: start to extract historical TsFile, original sequence file count {}, "
                + "original unsequence file count {}, start progress index {}",
            pipeName,
            dataRegionId,
            originalSequenceTsFileCount,
            originalUnsequenceTsFileCount,
            startIndex);

        final Collection<TsFileResource> sequenceTsFileResources =
            tsFileManager.getTsFileList(true).stream()
                .filter(
                    resource ->
                        // Some resource may not be closed due to the control of
                        // PIPE_MIN_FLUSH_INTERVAL_IN_MS. We simply ignore them.
                        !resource.isClosed()
                            || mayTsFileContainUnprocessedData(resource)
                                && isTsFileResourceOverlappedWithTimeRange(resource)
                                && isTsFileGeneratedAfterExtractionTimeLowerBound(resource))
                .collect(Collectors.toList());
        resourceList.addAll(sequenceTsFileResources);

        final Collection<TsFileResource> unsequenceTsFileResources =
            tsFileManager.getTsFileList(false).stream()
                .filter(
                    resource ->
                        // Some resource may not be closed due to the control of
                        // PIPE_MIN_FLUSH_INTERVAL_IN_MS. We simply ignore them.
                        !resource.isClosed()
                            || mayTsFileContainUnprocessedData(resource)
                                && isTsFileResourceOverlappedWithTimeRange(resource)
                                && isTsFileGeneratedAfterExtractionTimeLowerBound(resource))
                .collect(Collectors.toList());
        resourceList.addAll(unsequenceTsFileResources);

        resourceList.forEach(
            resource -> {
              // Pin the resource, in case the file is removed by compaction or anything.
              // Will unpin it after the PipeTsFileInsertionEvent is created and pinned.
              try {
                PipeResourceManager.tsfile().pinTsFileResource(resource, shouldTransferModFile);
              } catch (IOException e) {
                LOGGER.warn("Pipe: failed to pin TsFileResource {}", resource.getTsFilePath());
              }
            });

        resourceList.sort(
            (o1, o2) ->
                startIndex instanceof TimeWindowStateProgressIndex
                    ? Long.compare(o1.getFileStartTime(), o2.getFileStartTime())
                    : o1.getMaxProgressIndex().topologicalCompareTo(o2.getMaxProgressIndex()));
        pendingQueue = new ArrayDeque<>(resourceList);

        LOGGER.info(
            "Pipe {}@{}: finish to extract historical TsFile, extracted sequence file count {}/{}, "
                + "extracted unsequence file count {}/{}, extracted file count {}/{}, took {} ms",
            pipeName,
            dataRegionId,
            sequenceTsFileResources.size(),
            originalSequenceTsFileCount,
            unsequenceTsFileResources.size(),
            originalUnsequenceTsFileCount,
            resourceList.size(),
            originalSequenceTsFileCount + originalUnsequenceTsFileCount,
            System.currentTimeMillis() - startHistoricalExtractionTime);
      } finally {
        tsFileManager.readUnlock();
      }
    } finally {
      dataRegion.writeUnlock();
    }
  }

  private boolean mayTsFileContainUnprocessedData(TsFileResource resource) {
    if (startIndex instanceof TimeWindowStateProgressIndex) {
      // The resource is closed thus the TsFileResource#getFileEndTime() is safe to use
      return ((TimeWindowStateProgressIndex) startIndex).getMinTime() <= resource.getFileEndTime();
    }

    if (startIndex instanceof StateProgressIndex) {
      // Some different tsFiles may share the same max progressIndex, thus tsFiles with an
      // "equals" max progressIndex must be transmitted to avoid data loss
      final ProgressIndex innerProgressIndex =
          ((StateProgressIndex) startIndex).getInnerProgressIndex();
      return !innerProgressIndex.isAfter(resource.getMaxProgressIndexAfterClose())
          && !innerProgressIndex.equals(resource.getMaxProgressIndexAfterClose());
    }

    // Some different tsFiles may share the same max progressIndex, thus tsFiles with an
    // "equals" max progressIndex must be transmitted to avoid data loss
    return !startIndex.isAfter(resource.getMaxProgressIndexAfterClose());
  }

  private boolean isTsFileResourceOverlappedWithTimeRange(TsFileResource resource) {
    return !(resource.getFileEndTime() < historicalDataExtractionStartTime
        || historicalDataExtractionEndTime < resource.getFileStartTime());
  }

  private boolean isTsFileResourceCoveredByTimeRange(TsFileResource resource) {
    return historicalDataExtractionStartTime <= resource.getFileStartTime()
        && historicalDataExtractionEndTime >= resource.getFileEndTime();
  }

  private boolean isTsFileGeneratedAfterExtractionTimeLowerBound(TsFileResource resource) {
    try {
      return historicalDataExtractionTimeLowerBound
          <= TsFileNameGenerator.getTsFileName(resource.getTsFile().getName()).getTime();
    } catch (IOException e) {
      LOGGER.warn(
          "Pipe {}@{}: failed to get the generation time of TsFile {}, extract it anyway"
              + " (historical data extraction time lower bound: {})",
          pipeName,
          dataRegionId,
          resource.getTsFilePath(),
          historicalDataExtractionTimeLowerBound,
          e);
      // If failed to get the generation time of the TsFile, we will extract the data in the TsFile
      // anyway.
      return true;
    }
  }

  @Override
  public synchronized Event supply() {
    if (Objects.isNull(pendingQueue)) {
      return null;
    }

    final TsFileResource resource = pendingQueue.poll();
    if (resource == null) {
      return null;
    }

    final PipeTsFileInsertionEvent event =
        new PipeTsFileInsertionEvent(
            resource,
            shouldTransferModFile,
            false,
            false,
            pipeName,
            pipeTaskMeta,
            pipePattern,
            historicalDataExtractionStartTime,
            historicalDataExtractionEndTime);
    if (isDbNameCoveredByPattern) {
      event.skipParsingPattern();
    }

    if (sloppyTimeRange || isTsFileResourceCoveredByTimeRange(resource)) {
      event.skipParsingTime();
    }

    event.increaseReferenceCount(PipeHistoricalDataRegionTsFileExtractor.class.getName());
    try {
      PipeResourceManager.tsfile().unpinTsFileResource(resource);
    } catch (IOException e) {
      LOGGER.warn(
          "Pipe {}@{}: failed to unpin TsFileResource after creating event, original path: {}",
          pipeName,
          dataRegionId,
          resource.getTsFilePath());
    }

    return event;
  }

  public synchronized boolean hasConsumedAll() {
    return Objects.isNull(pendingQueue) || pendingQueue.isEmpty();
  }

  @Override
  public int getPendingQueueSize() {
    return Objects.nonNull(pendingQueue) ? pendingQueue.size() : 0;
  }

  @Override
  public synchronized void close() {
    if (Objects.nonNull(pendingQueue)) {
      pendingQueue.forEach(
          resource -> {
            try {
              PipeResourceManager.tsfile().unpinTsFileResource(resource);
            } catch (IOException e) {
              LOGGER.warn(
                  "Pipe {}@{}: failed to unpin TsFileResource after dropping pipe, original path: {}",
                  pipeName,
                  dataRegionId,
                  resource.getTsFilePath());
            }
          });
      pendingQueue.clear();
      pendingQueue = null;
    }
  }
}
