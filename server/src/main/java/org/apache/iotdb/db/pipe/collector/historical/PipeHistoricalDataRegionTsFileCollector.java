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

package org.apache.iotdb.db.pipe.collector.historical;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.DataRegion;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.pipe.config.PipeCollectorConstant;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.utils.DateTimeUtils;
import org.apache.iotdb.pipe.api.customizer.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.PipeParameters;
import org.apache.iotdb.pipe.api.customizer.collector.PipeCollectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.event.Event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.pipe.config.PipeCollectorConstant.COLLECTOR_HISTORY_ENABLE_KEY;
import static org.apache.iotdb.db.pipe.config.PipeCollectorConstant.COLLECTOR_HISTORY_END_TIME;
import static org.apache.iotdb.db.pipe.config.PipeCollectorConstant.COLLECTOR_HISTORY_START_TIME;
import static org.apache.iotdb.db.pipe.config.PipeCollectorConstant.DATA_REGION_KEY;

public class PipeHistoricalDataRegionTsFileCollector extends PipeHistoricalDataRegionCollector {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeHistoricalDataRegionTsFileCollector.class);

  private final PipeTaskMeta pipeTaskMeta;
  private final ProgressIndex startIndex;

  private int dataRegionId;

  private String pattern;

  private final long historicalDataCollectionTimeLowerBound; // arrival time
  private long historicalDataCollectionStartTime; // event time
  private long historicalDataCollectionEndTime; // event time

  private Queue<PipeTsFileInsertionEvent> pendingQueue;

  public PipeHistoricalDataRegionTsFileCollector(
      PipeTaskMeta pipeTaskMeta, long historicalDataCollectionTimeLowerBound) {
    this.pipeTaskMeta = pipeTaskMeta;
    this.startIndex = pipeTaskMeta.getProgressIndex();

    this.historicalDataCollectionTimeLowerBound = historicalDataCollectionTimeLowerBound;
  }

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    validator.validateRequiredAttribute(DATA_REGION_KEY);
  }

  @Override
  public void customize(
      PipeParameters parameters, PipeCollectorRuntimeConfiguration configuration) {
    dataRegionId = parameters.getInt(DATA_REGION_KEY);

    pattern =
        parameters.getStringOrDefault(
            PipeCollectorConstant.COLLECTOR_PATTERN_KEY,
            PipeCollectorConstant.COLLECTOR_PATTERN_DEFAULT_VALUE);

    // user may set the COLLECTOR_HISTORY_START_TIME and COLLECTOR_HISTORY_END_TIME without
    // enabling the historical data collection, which may affect the realtime data collection.
    final boolean isHistoricalCollectorEnabledByUser =
        parameters.getBooleanOrDefault(COLLECTOR_HISTORY_ENABLE_KEY, true);
    historicalDataCollectionStartTime =
        isHistoricalCollectorEnabledByUser && parameters.hasAttribute(COLLECTOR_HISTORY_START_TIME)
            ? DateTimeUtils.convertDatetimeStrToLong(
                parameters.getString(COLLECTOR_HISTORY_START_TIME), ZoneId.systemDefault())
            : Long.MIN_VALUE;
    historicalDataCollectionEndTime =
        isHistoricalCollectorEnabledByUser && parameters.hasAttribute(COLLECTOR_HISTORY_END_TIME)
            ? DateTimeUtils.convertDatetimeStrToLong(
                parameters.getString(COLLECTOR_HISTORY_END_TIME), ZoneId.systemDefault())
            : Long.MAX_VALUE;

    // Only invoke flushDataRegionAllTsFiles() when the pipe runs in the realtime only mode.
    // realtime only mode -> (historicalDataCollectionTimeLowerBound != Long.MIN_VALUE)
    //
    // Ensure that all data in the data region is flushed to disk before collecting data.
    // This ensures the generation time of all newly generated TsFiles (realtime data) after the
    // invocation of flushDataRegionAllTsFiles() is later than the creationTime of the pipe
    // (historicalDataCollectionTimeLowerBound).
    //
    // Note that: the generation time of the TsFile is the time when the TsFile is created, not
    // the time when the data is flushed to the TsFile.
    //
    // Then we can use the generation time of the TsFile to determine whether the data in the
    // TsFile should be collected by comparing the generation time of the TsFile with the
    // historicalDataCollectionTimeLowerBound when starting the pipe in realtime only mode.
    //
    // If we don't invoke flushDataRegionAllTsFiles() in the realtime only mode, the data generated
    // between the creation time of the pipe the time when the pipe starts will be lost.
    if (historicalDataCollectionTimeLowerBound != Long.MIN_VALUE) {
      flushDataRegionAllTsFiles();
    }
  }

  private void flushDataRegionAllTsFiles() {
    final DataRegion dataRegion =
        StorageEngine.getInstance().getDataRegion(new DataRegionId(dataRegionId));
    if (dataRegion == null) {
      return;
    }

    dataRegion.writeLock("Pipe: create historical TsFile collector");
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

    dataRegion.writeLock("Pipe: start to collect historical TsFile");
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
                            && isTsFileGeneratedAfterCollectionTimeLowerBound(resource))
                .map(
                    resource ->
                        new PipeTsFileInsertionEvent(
                            resource,
                            pipeTaskMeta,
                            pattern,
                            historicalDataCollectionStartTime,
                            historicalDataCollectionEndTime))
                .collect(Collectors.toList()));
        pendingQueue.addAll(
            tsFileManager.getTsFileList(false).stream()
                .filter(
                    resource ->
                        !startIndex.isAfter(resource.getMaxProgressIndexAfterClose())
                            && isTsFileResourceOverlappedWithTimeRange(resource)
                            && isTsFileGeneratedAfterCollectionTimeLowerBound(resource))
                .map(
                    resource ->
                        new PipeTsFileInsertionEvent(
                            resource,
                            pipeTaskMeta,
                            pattern,
                            historicalDataCollectionStartTime,
                            historicalDataCollectionEndTime))
                .collect(Collectors.toList()));
        pendingQueue.forEach(
            event ->
                event.increaseReferenceCount(
                    PipeHistoricalDataRegionTsFileCollector.class.getName()));
      } finally {
        tsFileManager.readUnlock();
      }
    } finally {
      dataRegion.writeUnlock();
    }
  }

  private boolean isTsFileResourceOverlappedWithTimeRange(TsFileResource resource) {
    return historicalDataCollectionStartTime <= resource.getFileEndTime()
        || resource.getFileStartTime() <= historicalDataCollectionEndTime;
  }

  private boolean isTsFileGeneratedAfterCollectionTimeLowerBound(TsFileResource resource) {
    try {
      return historicalDataCollectionTimeLowerBound
          <= TsFileNameGenerator.getTsFileName(resource.getTsFile().getName()).getTime();
    } catch (IOException e) {
      LOGGER.warn(
          String.format(
              "failed to get the generation time of TsFile %s, collect it anyway",
              resource.getTsFilePath()),
          e);
      // If failed to get the generation time of the TsFile, we will collect the data in the TsFile
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
