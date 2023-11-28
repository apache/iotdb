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

package org.apache.iotdb.db.pipe.extractor.historical;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.db.pipe.config.plugin.env.PipeTaskExtractorRuntimeEnvironment;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_HISTORY_ENABLE_DEFAULT_VALUE;
import static org.apache.iotdb.db.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_HISTORY_ENABLE_KEY;
import static org.apache.iotdb.db.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_HISTORY_END_TIME_KEY;
import static org.apache.iotdb.db.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_HISTORY_LOOSE_RANGE_KEY;
import static org.apache.iotdb.db.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_HISTORY_START_TIME_KEY;
import static org.apache.iotdb.db.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_PATTERN_DEFAULT_VALUE;
import static org.apache.iotdb.db.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_PATTERN_KEY;
import static org.apache.iotdb.db.pipe.config.constant.PipeExtractorConstant.SOURCE_HISTORY_ENABLE_KEY;
import static org.apache.iotdb.db.pipe.config.constant.PipeExtractorConstant.SOURCE_HISTORY_END_TIME_KEY;
import static org.apache.iotdb.db.pipe.config.constant.PipeExtractorConstant.SOURCE_HISTORY_LOOSE_RANGE_KEY;
import static org.apache.iotdb.db.pipe.config.constant.PipeExtractorConstant.SOURCE_HISTORY_START_TIME_KEY;
import static org.apache.iotdb.db.pipe.config.constant.PipeExtractorConstant.SOURCE_PATTERN_KEY;

public class PipeHistoricalDataRegionTsFileExtractor implements PipeHistoricalDataRegionExtractor {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeHistoricalDataRegionTsFileExtractor.class);

  private static final Map<Integer, Long> DATA_REGION_ID_TO_PIPE_FLUSHED_TIME_MAP = new HashMap<>();
  private static final long PIPE_MIN_FLUSH_INTERVAL_IN_MS = 2000;

  private String pipeName;
  private PipeTaskMeta pipeTaskMeta;
  private ProgressIndex startIndex;

  private int dataRegionId;

  private String pattern;
  private boolean isDbNameCoveredByPattern = false;

  private long historicalDataExtractionStartTime; // Event time
  private long historicalDataExtractionEndTime; // Event time

  private long historicalDataExtractionTimeLowerBound; // Arrival time

  private boolean sloppyTimeRange; // true to disable time range filter after extraction

  private Queue<TsFileResource> pendingQueue;

  @Override
  public void validate(PipeParameterValidator validator) {
    // Do nothing
  }

  @Override
  public void customize(
      PipeParameters parameters, PipeExtractorRuntimeConfiguration configuration) {
    final PipeTaskExtractorRuntimeEnvironment environment =
        (PipeTaskExtractorRuntimeEnvironment) configuration.getRuntimeEnvironment();

    pipeName = environment.getPipeName();
    pipeTaskMeta = environment.getPipeTaskMeta();
    startIndex = environment.getPipeTaskMeta().getProgressIndex();

    dataRegionId = environment.getRegionId();
    synchronized (DATA_REGION_ID_TO_PIPE_FLUSHED_TIME_MAP) {
      DATA_REGION_ID_TO_PIPE_FLUSHED_TIME_MAP.putIfAbsent(dataRegionId, 0L);
    }

    pattern =
        parameters.getStringOrDefault(
            Arrays.asList(EXTRACTOR_PATTERN_KEY, SOURCE_PATTERN_KEY),
            EXTRACTOR_PATTERN_DEFAULT_VALUE);
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

    // User may set the EXTRACTOR_HISTORY_START_TIME and EXTRACTOR_HISTORY_END_TIME without
    // enabling the historical data extraction, which may affect the realtime data extraction.
    final boolean isHistoricalExtractorEnabledByUser =
        parameters.getBooleanOrDefault(
            Arrays.asList(EXTRACTOR_HISTORY_ENABLE_KEY, SOURCE_HISTORY_ENABLE_KEY),
            EXTRACTOR_HISTORY_ENABLE_DEFAULT_VALUE);
    historicalDataExtractionStartTime =
        isHistoricalExtractorEnabledByUser
                && parameters.hasAnyAttributes(
                    EXTRACTOR_HISTORY_START_TIME_KEY, SOURCE_HISTORY_START_TIME_KEY)
            ? DateTimeUtils.convertDatetimeStrToLong(
                parameters.getStringByKeys(
                    EXTRACTOR_HISTORY_START_TIME_KEY, SOURCE_HISTORY_START_TIME_KEY),
                ZoneId.systemDefault())
            : Long.MIN_VALUE;
    historicalDataExtractionEndTime =
        isHistoricalExtractorEnabledByUser
                && parameters.hasAnyAttributes(
                    EXTRACTOR_HISTORY_END_TIME_KEY, SOURCE_HISTORY_END_TIME_KEY)
            ? DateTimeUtils.convertDatetimeStrToLong(
                parameters.getStringByKeys(
                    EXTRACTOR_HISTORY_END_TIME_KEY, SOURCE_HISTORY_END_TIME_KEY),
                ZoneId.systemDefault())
            : Long.MAX_VALUE;

    // Enable historical extractor by default
    historicalDataExtractionTimeLowerBound =
        parameters.getBooleanOrDefault(
                Arrays.asList(EXTRACTOR_HISTORY_ENABLE_KEY, SOURCE_HISTORY_ENABLE_KEY),
                EXTRACTOR_HISTORY_ENABLE_DEFAULT_VALUE)
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
        "historical data extraction time range, start time {}({}), end time {}({}), sloppy time range {}",
        DateTimeUtils.convertLongToDate(historicalDataExtractionStartTime),
        historicalDataExtractionStartTime,
        DateTimeUtils.convertLongToDate(historicalDataExtractionEndTime),
        historicalDataExtractionEndTime,
        sloppyTimeRange);
  }

  private void flushDataRegionAllTsFiles() {
    final DataRegion dataRegion =
        StorageEngine.getInstance().getDataRegion(new DataRegionId(dataRegionId));
    if (dataRegion == null) {
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
    final DataRegion dataRegion =
        StorageEngine.getInstance().getDataRegion(new DataRegionId(dataRegionId));
    if (dataRegion == null) {
      pendingQueue = new ArrayDeque<>();
      return;
    }

    dataRegion.writeLock("Pipe: start to extract historical TsFile");
    final long startHistoricalExtractionTime = System.currentTimeMillis();
    try {
      synchronized (DATA_REGION_ID_TO_PIPE_FLUSHED_TIME_MAP) {
        final long lastFlushedByPipeTime =
            DATA_REGION_ID_TO_PIPE_FLUSHED_TIME_MAP.get(dataRegionId);
        if (System.currentTimeMillis() - lastFlushedByPipeTime >= PIPE_MIN_FLUSH_INTERVAL_IN_MS) {
          dataRegion.syncCloseAllWorkingTsFileProcessors();
          DATA_REGION_ID_TO_PIPE_FLUSHED_TIME_MAP.replace(dataRegionId, System.currentTimeMillis());
        }
      }

      final TsFileManager tsFileManager = dataRegion.getTsFileManager();
      tsFileManager.readLock();
      try {
        final List<TsFileResource> resourceList =
            new ArrayList<>(tsFileManager.size(true) + tsFileManager.size(false));

        final Collection<TsFileResource> sequenceTsFileResources =
            tsFileManager.getTsFileList(true).stream()
                .filter(
                    resource ->
                        // Some resource may be not closed due to the control of
                        // PIPE_MIN_FLUSH_INTERVAL_IN_MS. We simply ignore them.
                        resource.isClosed()
                            && !startIndex.isAfter(resource.getMaxProgressIndexAfterClose())
                            && isTsFileResourceOverlappedWithTimeRange(resource)
                            && isTsFileGeneratedAfterExtractionTimeLowerBound(resource))
                .collect(Collectors.toList());
        resourceList.addAll(sequenceTsFileResources);

        final Collection<TsFileResource> unsequenceTsFileResources =
            tsFileManager.getTsFileList(false).stream()
                .filter(
                    resource ->
                        // Some resource may be not closed due to the control of
                        // PIPE_MIN_FLUSH_INTERVAL_IN_MS. We simply ignore them.
                        resource.isClosed()
                            && !startIndex.isAfter(resource.getMaxProgressIndexAfterClose())
                            && isTsFileResourceOverlappedWithTimeRange(resource)
                            && isTsFileGeneratedAfterExtractionTimeLowerBound(resource))
                .collect(Collectors.toList());
        resourceList.addAll(unsequenceTsFileResources);

        resourceList.forEach(
            resource -> {
              // Pin the resource, in case the file is removed by compaction or anything.
              // Will unpin it after the PipeTsFileInsertionEvent is created and pinned.
              try {
                PipeResourceManager.tsfile().pinTsFileResource(resource);
              } catch (IOException e) {
                LOGGER.warn("Pipe: failed to pin TsFileResource {}", resource.getTsFilePath());
              }
            });

        resourceList.sort(
            (o1, o2) -> o1.getMaxProgressIndex().topologicalCompareTo(o2.getMaxProgressIndex()));
        pendingQueue = new ArrayDeque<>(resourceList);

        LOGGER.info(
            "Pipe: start to extract historical TsFile, data region {}, "
                + "sequence file count {}, unsequence file count {}, historical extraction time {} ms",
            dataRegionId,
            sequenceTsFileResources.size(),
            unsequenceTsFileResources.size(),
            System.currentTimeMillis() - startHistoricalExtractionTime);
      } finally {
        tsFileManager.readUnlock();
      }
    } finally {
      dataRegion.writeUnlock();
    }
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
          String.format(
              "failed to get the generation time of TsFile %s, extract it anyway",
              resource.getTsFilePath()),
          e);
      // If failed to get the generation time of the TsFile, we will extract the data in the TsFile
      // anyway.
      return true;
    }
  }

  @Override
  public synchronized Event supply() {
    if (pendingQueue == null) {
      return null;
    }
    TsFileResource resource = pendingQueue.poll();
    if (resource == null) {
      return null;
    }

    final PipeTsFileInsertionEvent event =
        new PipeTsFileInsertionEvent(
            resource,
            false,
            false,
            pipeName,
            pipeTaskMeta,
            pattern,
            historicalDataExtractionStartTime,
            historicalDataExtractionEndTime,
            !sloppyTimeRange && !isTsFileResourceCoveredByTimeRange(resource));
    if (isDbNameCoveredByPattern) {
      event.skipParsingPattern();
    }

    event.increaseReferenceCount(PipeHistoricalDataRegionTsFileExtractor.class.getName());
    try {
      PipeResourceManager.tsfile().unpinTsFileResource(resource);
    } catch (IOException e) {
      LOGGER.warn(
          "Pipe: failed to unpin TsFileResource after creating event, original path: {}",
          resource.getTsFilePath());
    }

    return event;
  }

  public synchronized boolean hasConsumedAll() {
    return pendingQueue != null && pendingQueue.isEmpty();
  }

  @Override
  public int getPendingQueueSize() {
    return pendingQueue != null ? pendingQueue.size() : 0;
  }

  @Override
  public synchronized void close() {
    if (pendingQueue != null) {
      pendingQueue.forEach(
          resource -> {
            try {
              PipeResourceManager.tsfile().unpinTsFileResource(resource);
            } catch (IOException e) {
              LOGGER.warn(
                  "Pipe: failed to unpin TsFileResource after dropping pipe, original path: {}",
                  resource.getTsFilePath());
            }
          });
      pendingQueue.clear();
      pendingQueue = null;
    }
  }
}
