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
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.DataRegion;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.pipe.config.plugin.env.PipeTaskExtractorRuntimeEnvironment;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
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
import java.util.Queue;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_HISTORY_ENABLE_KEY;
import static org.apache.iotdb.db.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_HISTORY_END_TIME;
import static org.apache.iotdb.db.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_HISTORY_START_TIME;
import static org.apache.iotdb.db.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_PATTERN_DEFAULT_VALUE;
import static org.apache.iotdb.db.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_PATTERN_KEY;

public class PipeHistoricalDataRegionTsFileExtractor implements PipeHistoricalDataRegionExtractor {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeHistoricalDataRegionTsFileExtractor.class);

  private PipeTaskMeta pipeTaskMeta;
  private ProgressIndex startIndex;

  private int dataRegionId;

  private String pattern;

  private long historicalDataExtractionStartTime; // event time
  private long historicalDataExtractionEndTime; // event time

  private long historicalDataExtractionTimeLowerBound; // arrival time

  private Queue<PipeTsFileInsertionEvent> pendingQueue;

  @Override
  public void validate(PipeParameterValidator validator) {
    // do nothing
  }

  @Override
  public void customize(
      PipeParameters parameters, PipeExtractorRuntimeConfiguration configuration) {
    final PipeTaskExtractorRuntimeEnvironment environment =
        (PipeTaskExtractorRuntimeEnvironment) configuration.getRuntimeEnvironment();

    pipeTaskMeta = environment.getPipeTaskMeta();
    startIndex = environment.getPipeTaskMeta().getProgressIndex();

    dataRegionId = environment.getRegionId();

    pattern = parameters.getStringOrDefault(EXTRACTOR_PATTERN_KEY, EXTRACTOR_PATTERN_DEFAULT_VALUE);

    // user may set the EXTRACTOR_HISTORY_START_TIME and EXTRACTOR_HISTORY_END_TIME without
    // enabling the historical data extraction, which may affect the realtime data extraction.
    final boolean isHistoricalExtractorEnabledByUser =
        parameters.getBooleanOrDefault(EXTRACTOR_HISTORY_ENABLE_KEY, true);
    historicalDataExtractionStartTime =
        isHistoricalExtractorEnabledByUser && parameters.hasAttribute(EXTRACTOR_HISTORY_START_TIME)
            ? DateTimeUtils.convertDatetimeStrToLong(
                parameters.getString(EXTRACTOR_HISTORY_START_TIME), ZoneId.systemDefault())
            : Long.MIN_VALUE;
    historicalDataExtractionEndTime =
        isHistoricalExtractorEnabledByUser && parameters.hasAttribute(EXTRACTOR_HISTORY_END_TIME)
            ? DateTimeUtils.convertDatetimeStrToLong(
                parameters.getString(EXTRACTOR_HISTORY_END_TIME), ZoneId.systemDefault())
            : Long.MAX_VALUE;

    // enable historical extractor by default
    historicalDataExtractionTimeLowerBound =
        parameters.getBooleanOrDefault(EXTRACTOR_HISTORY_ENABLE_KEY, true)
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
      flushDataRegionAllTsFiles();
    }
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
    try {
      dataRegion.syncCloseAllWorkingTsFileProcessors();

      final TsFileManager tsFileManager = dataRegion.getTsFileManager();
      tsFileManager.readLock();
      try {
        pendingQueue = new ArrayDeque<>(tsFileManager.size(true) + tsFileManager.size(false));
        pendingQueue.addAll(
            tsFileManager.getTsFileList(true).stream()
                .filter(
                    resource ->
                        !startIndex.isAfter(resource.getMaxProgressIndexAfterClose())
                            && isTsFileResourceOverlappedWithTimeRange(resource)
                            && isTsFileGeneratedAfterExtractionTimeLowerBound(resource))
                .map(
                    resource ->
                        new PipeTsFileInsertionEvent(
                            resource,
                            pipeTaskMeta,
                            pattern,
                            historicalDataExtractionStartTime,
                            historicalDataExtractionEndTime))
                .collect(Collectors.toList()));
        pendingQueue.addAll(
            tsFileManager.getTsFileList(false).stream()
                .filter(
                    resource ->
                        !startIndex.isAfter(resource.getMaxProgressIndexAfterClose())
                            && isTsFileResourceOverlappedWithTimeRange(resource)
                            && isTsFileGeneratedAfterExtractionTimeLowerBound(resource))
                .map(
                    resource ->
                        new PipeTsFileInsertionEvent(
                            resource,
                            pipeTaskMeta,
                            pattern,
                            historicalDataExtractionStartTime,
                            historicalDataExtractionEndTime))
                .collect(Collectors.toList()));
        pendingQueue.forEach(
            event ->
                event.increaseReferenceCount(
                    PipeHistoricalDataRegionTsFileExtractor.class.getName()));
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
  public Event supply() {
    if (pendingQueue == null) {
      return null;
    }

    return pendingQueue.poll();
  }

  public synchronized boolean hasConsumedAll() {
    return pendingQueue != null && pendingQueue.isEmpty();
  }

  @Override
  public void close() {
    if (pendingQueue != null) {
      pendingQueue.clear();
      pendingQueue = null;
    }
  }
}
