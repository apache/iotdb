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
import org.apache.iotdb.db.pipe.config.PipeCollectorConstant;
import org.apache.iotdb.db.pipe.core.event.impl.PipeTsFileInsertionEvent;
import org.apache.iotdb.pipe.api.PipeCollector;
import org.apache.iotdb.pipe.api.customizer.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.PipeParameters;
import org.apache.iotdb.pipe.api.customizer.collector.PipeCollectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.event.Event;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.stream.Collectors;

public class PipeHistoricalDataRegionTsFileCollector implements PipeCollector {

  private final PipeTaskMeta pipeTaskMeta;
  private final ProgressIndex startIndex;
  private int dataRegionId;

  private Queue<PipeTsFileInsertionEvent> pendingQueue;

  public PipeHistoricalDataRegionTsFileCollector(PipeTaskMeta pipeTaskMeta) {
    this.pipeTaskMeta = pipeTaskMeta;
    this.startIndex = pipeTaskMeta.getProgressIndex();
  }

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    validator.validateRequiredAttribute(PipeCollectorConstant.DATA_REGION_KEY);
  }

  @Override
  public void customize(
      PipeParameters parameters, PipeCollectorRuntimeConfiguration configuration) {
    dataRegionId = parameters.getInt(PipeCollectorConstant.DATA_REGION_KEY);
  }

  @Override
  public synchronized void start() {
    final DataRegion dataRegion =
        StorageEngine.getInstance().getDataRegion(new DataRegionId(dataRegionId));
    if (dataRegion == null) {
      pendingQueue = new ArrayDeque<>();
      return;
    }

    dataRegion.writeLock("Pipe: collect historical TsFile");
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
                        resource.getMaxProgressIndexAfterClose() == null
                            || !startIndex.isAfter(resource.getMaxProgressIndexAfterClose()))
                .map(resource -> new PipeTsFileInsertionEvent(resource, pipeTaskMeta))
                .collect(Collectors.toList()));
        pendingQueue.addAll(
            tsFileManager.getTsFileList(false).stream()
                .filter(
                    resource ->
                        resource.getMaxProgressIndexAfterClose() == null
                            || !startIndex.isAfter(resource.getMaxProgressIndexAfterClose()))
                .map(resource -> new PipeTsFileInsertionEvent(resource, pipeTaskMeta))
                .collect(Collectors.toList()));
        pendingQueue.forEach(
            event -> {
              event.increaseReferenceCount(PipeHistoricalDataRegionTsFileCollector.class.getName());
            });
      } finally {
        tsFileManager.readUnlock();
      }
    } finally {
      dataRegion.writeUnlock();
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
