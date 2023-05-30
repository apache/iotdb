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

package org.apache.iotdb.db.pipe.core.collector.historical;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.DataRegion;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.pipe.core.event.impl.PipeTsFileInsertionEvent;
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

import static org.apache.iotdb.db.pipe.config.PipeCollectorConstant.COLLECTOR_HISTORY_END_TIME;
import static org.apache.iotdb.db.pipe.config.PipeCollectorConstant.COLLECTOR_HISTORY_START_TIME;
import static org.apache.iotdb.db.pipe.config.PipeCollectorConstant.DATA_REGION_KEY;

public class PipeHistoricalDataRegionTsFileCollector extends PipeHistoricalDataRegionCollector {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeHistoricalDataRegionTsFileCollector.class);

  private final PipeTaskMeta pipeTaskMeta;
  private final ProgressIndex startIndex;

  private int dataRegionId;

  private long historicalDataStartTime;
  private long historicalDataEndTime;

  private final long historicalDataGenerationStartTime;

  private Queue<PipeTsFileInsertionEvent> pendingQueue;

  public PipeHistoricalDataRegionTsFileCollector(
      long historicalDataGenerationStartTime, PipeTaskMeta pipeTaskMeta) {
    this.historicalDataGenerationStartTime = historicalDataGenerationStartTime;
    this.pipeTaskMeta = pipeTaskMeta;
    this.startIndex = pipeTaskMeta.getProgressIndex();
  }

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    validator.validateRequiredAttribute(DATA_REGION_KEY);
  }

  @Override
  public void customize(
      PipeParameters parameters, PipeCollectorRuntimeConfiguration configuration) {
    dataRegionId = parameters.getInt(DATA_REGION_KEY);
    historicalDataStartTime =
        parameters.hasAttribute(COLLECTOR_HISTORY_START_TIME)
            ? DateTimeUtils.convertDatetimeStrToLong(
                parameters.getString(COLLECTOR_HISTORY_START_TIME), ZoneId.systemDefault())
            : Long.MIN_VALUE;
    historicalDataEndTime =
        parameters.hasAttribute(COLLECTOR_HISTORY_END_TIME)
            ? DateTimeUtils.convertDatetimeStrToLong(
                parameters.getString(COLLECTOR_HISTORY_END_TIME), ZoneId.systemDefault())
            : Long.MAX_VALUE;

    flushDataRegionAllTsFile();
  }

  private void flushDataRegionAllTsFile() {
    final DataRegion dataRegion =
        StorageEngine.getInstance().getDataRegion(new DataRegionId(dataRegionId));
    if (dataRegion == null) {
      return;
    }

    dataRegion.writeLock("Pipe: create historical TsFile collector");
    try {
      // Ensure that the generation time of the pipe matches the generation time of the TsFile, so
      // that in possible realtime-only mode, data can be correctly obtained by pipe generation
      // time.
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
                            && isTsFileGeneratedAfterRequiredTime(resource))
                .map(resource -> new PipeTsFileInsertionEvent(resource, pipeTaskMeta))
                .collect(Collectors.toList()));
        pendingQueue.addAll(
            tsFileManager.getTsFileList(false).stream()
                .filter(
                    resource ->
                        !startIndex.isAfter(resource.getMaxProgressIndexAfterClose())
                            && isTsFileResourceOverlappedWithTimeRange(resource)
                            && isTsFileGeneratedAfterRequiredTime(resource))
                .map(resource -> new PipeTsFileInsertionEvent(resource, pipeTaskMeta))
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
    return !(resource.getFileEndTime() < historicalDataStartTime
        || historicalDataEndTime < resource.getFileStartTime());
  }

  private boolean isTsFileGeneratedAfterRequiredTime(TsFileResource resource) {
    try {
      return historicalDataGenerationStartTime
          <= TsFileNameGenerator.getTsFileName(resource.getTsFile().getName()).getTime();
    } catch (IOException e) {
      LOGGER.warn(
          String.format("Get generation time from TsFile %s error.", resource.getTsFilePath()), e);
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
