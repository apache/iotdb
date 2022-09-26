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

package org.apache.iotdb.db.engine.alter;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.db.engine.alter.log.AlteringLogAnalyzer;
import org.apache.iotdb.db.engine.alter.log.AlteringLogger;
import org.apache.iotdb.db.engine.cache.AlteringRecordsCache;
import org.apache.iotdb.db.engine.compaction.CompactionTaskManager;
import org.apache.iotdb.db.engine.compaction.task.CompactionTaskSummary;
import org.apache.iotdb.db.engine.storagegroup.DataRegion;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.dataregion.StorageGroupManager;
import org.apache.iotdb.db.exception.StorageEngineException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

/** Ensure that alterLock locks RewriteTimeseriesTask for all Dataregions */
public class RewriteTimeseriesTaskManager {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);

  private final CompactionTaskManager compactionTaskManager = CompactionTaskManager.getInstance();

  private AlteringRecordsCache alteringRecordsCache = AlteringRecordsCache.getInstance();

  private RewriteTimeseriesTaskManager() {}

  public void rewriteTimeseries(DataRegion[] dataRegions, StorageGroupManager storageGroupManager)
      throws Exception {

    if (storageGroupManager == null || dataRegions == null || dataRegions.length <= 0) {
      return;
    }
    if (storageGroupManager.isAlterLocked()) {
      throw new StorageEngineException("Rewrite is unfinished, try again later");
    }
    // Avoid creating too many threads as a result of pressure testing
    storageGroupManager.alterLock();
    try {
      Thread thread =
          new Thread(
              () -> {
                DataRegion dataRegion = dataRegions[0];
                if (dataRegion == null) {
                  return;
                }
                String storageGroupName = dataRegion.getStorageGroupName();
                storageGroupManager.alterLock();
                try {
                  // rewrite DataRegions
                  doRewrite(dataRegions);
                } catch (Exception e) {
                  LOGGER.error("RewriteTimeseriesTaskManager Error", e);
                } finally {
                  alteringRecordsCache.clear(storageGroupName);
                  storageGroupManager.alterUnlock();
                }
              },
              this.getClass().getSimpleName());
      thread.start();
    } finally {
      storageGroupManager.alterUnlock();
    }
  }

  private void doRewrite(DataRegion[] dataRegions) throws IOException {
    List<RewriteTimeseriesTask> tasks = new ArrayList<>();
    for (DataRegion dataRegion : dataRegions) {
      if (dataRegion != null) {
        List<Long> timePartitions = dataRegion.getTimePartitions();
        if (timePartitions != null && timePartitions.size() > 0) {
          // alter.log analyzer
          File logFile =
              SystemFileFactory.INSTANCE.getFile(
                  dataRegion.getStorageGroupSysDir(), AlteringLogger.ALTERING_LOG_NAME);
          if (!logFile.exists()) {
            // there is no altering timeseries in the DataRegion
            continue;
          }
          AlteringLogAnalyzer analyzer = new AlteringLogAnalyzer(logFile);
          analyzer.analyzer();
          // clear begin
          if (!analyzer.isClearBegin()) {
            AlteringLogger.clearBegin(logFile);
          }
          TsFileManager tsFileManager = dataRegion.getTsFileManager();
          // Create tasks grouped by DataRegion TimePartition
          // Share CompactionTaskManager
          try {
            RewriteTimeseriesTask task =
                new RewriteTimeseriesTask(
                    dataRegion.getStorageGroupName(),
                    dataRegion.getDataRegionId(),
                    timePartitions,
                    tsFileManager,
                    CompactionTaskManager.currentTaskNum,
                    tsFileManager.getNextCompactionTaskId(),
                    analyzer.isClearBegin(),
                    analyzer.getDoneFiles(),
                    logFile);
            boolean b = compactionTaskManager.addTaskToWaitingQueue(task);
            if (!b) {
              // addTaskToWaitingQueue failed
              continue;
            }
            tasks.add(task);
          } catch (Exception e) {
            LOGGER.error("addTaskTOWaitingQueue failed", e);
          }
        }
      }
    }

    // wait until all proccess done
    tasks.forEach(
        task -> {
          String logKey = task.getStorageGroupName() + "-" + task.getDataRegionId();
          try {
            Future<CompactionTaskSummary> future =
                compactionTaskManager.getCompactionTaskFutureCheckStatusMayBlock(task);
            boolean isFinished = false;
            if (future != null) {
              CompactionTaskSummary summary = future.get();
              if (summary == null) {
                LOGGER.info("[rewriteTimeseries] {} task summary is null", logKey);
                return;
              }
              isFinished = summary.isFinished();
            }
            LOGGER.info(
                "[rewriteTimeseries] {} task isFinished:{}",
                logKey,
                future == null ? task.isTaskFinished() : isFinished);
          } catch (Exception e) {
            LOGGER.error("rewrite future failed " + logKey, e);
          }
        });
  }

  public static RewriteTimeseriesTaskManager getInstance() {
    return RewriteTimeseriesTaskManagerHolder.INSTANCE;
  }

  /** singleton pattern. */
  private static class RewriteTimeseriesTaskManagerHolder {

    private static final RewriteTimeseriesTaskManager INSTANCE = new RewriteTimeseriesTaskManager();
  }
}
