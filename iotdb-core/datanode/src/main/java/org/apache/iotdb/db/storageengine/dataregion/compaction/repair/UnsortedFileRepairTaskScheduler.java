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

package org.apache.iotdb.db.storageengine.dataregion.compaction.repair;

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionScheduler;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileRepairStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;

public class UnsortedFileRepairTaskScheduler implements Runnable {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(UnsortedFileRepairTaskScheduler.class);
  private final Set<RepairTimePartition> allTimePartitionFiles = new HashSet<>();
  private RepairLogger repairLogger;
  private final boolean isRecover;
  private boolean initSuccess = false;
  private long repairTaskTime;
  private RepairProgress repairProgress;

  /** Used for create a new repair schedule task */
  public UnsortedFileRepairTaskScheduler(
      List<DataRegion> dataRegions, boolean isRecoverStorageEngine) {
    this.isRecover = false;
    try {
      repairLogger = new RepairLogger(isRecoverStorageEngine);
    } catch (Exception e) {
      try {
        LOGGER.error("[RepairScheduler] Failed to create repair logger", e);
        repairLogger.close();
      } catch (IOException closeException) {
        LOGGER.error("[RepairScheduler] Failed to close repair logger", closeException);
      }
      return;
    }
    File logFile = repairLogger.getLogFile();
    this.repairTaskTime = repairLogger.getRepairTaskStartTime();
    collectTimePartitions(dataRegions);
    if (repairLogger.isNeedRecoverFromLogFile()) {
      try {
        recover(logFile);
      } catch (Exception e) {
        LOGGER.error(
            "[RepairScheduler] Failed to parse repair log file {}", logFile.getAbsolutePath(), e);
        return;
      }
    }
    repairProgress = new RepairProgress(allTimePartitionFiles.size());
    initSuccess = true;
  }

  @TestOnly
  public UnsortedFileRepairTaskScheduler(
      List<DataRegion> dataRegions, boolean isRecoverIoTDB, File logFileDir) {
    this.isRecover = false;
    try {
      repairLogger = new RepairLogger(logFileDir, isRecoverIoTDB);
    } catch (Exception e) {
      try {
        LOGGER.error("[RepairScheduler] Failed to create repair logger", e);
        repairLogger.close();
      } catch (IOException closeException) {
        LOGGER.error("[RepairScheduler] Failed to close repair logger", closeException);
      }
      return;
    }
    File logFile = repairLogger.getLogFile();
    this.repairTaskTime = repairLogger.getRepairTaskStartTime();
    collectTimePartitions(dataRegions);
    if (repairLogger.isNeedRecoverFromLogFile()) {
      try {
        recover(logFile);
      } catch (Exception e) {
        LOGGER.error(
            "[RepairScheduler] Failed to parse repair log file {}", logFile.getAbsolutePath(), e);
        return;
      }
    }
    repairProgress = new RepairProgress(allTimePartitionFiles.size());
    initSuccess = true;
  }

  private void recover(File logFile) throws IOException {
    RepairTaskRecoverLogParser recoverLogParser = new RepairTaskRecoverLogParser(logFile);
    LOGGER.info(
        "[RepairScheduler] recover unfinished repair schedule task from log file: {}",
        recoverLogParser.getRepairLogFilePath());
    recoverLogParser.parse();
    Map<RepairTimePartition, Set<String>> repairedTimePartitionWithCannotRepairFiles =
        recoverLogParser.getRepairedTimePartitionsWithCannotRepairFiles();
    for (RepairTimePartition timePartition : allTimePartitionFiles) {
      Set<String> cannotRepairFiles =
          repairedTimePartitionWithCannotRepairFiles.remove(timePartition);
      if (cannotRepairFiles == null) {
        continue;
      }
      // mark time partition as repaired
      timePartition.setRepaired(true);
      if (cannotRepairFiles.isEmpty()) {
        continue;
      }
      // mark cannot repair file in TsFileResource
      List<TsFileResource> resources = timePartition.getAllFileSnapshot();
      for (TsFileResource resource : resources) {
        if (resource.getStatus() != TsFileResourceStatus.NORMAL) {
          continue;
        }
        if (cannotRepairFiles.contains(resource.getTsFile().getName())) {
          resource.setTsFileRepairStatus(TsFileRepairStatus.NEED_TO_REPAIR);
        }
      }
    }
  }

  private void collectTimePartitions(List<DataRegion> dataRegions) {
    for (DataRegion dataRegion : dataRegions) {
      if (dataRegion == null) {
        continue;
      }
      List<Long> timePartitions = dataRegion.getTimePartitions();
      for (long timePartition : timePartitions) {
        allTimePartitionFiles.add(
            new RepairTimePartition(dataRegion, timePartition, repairTaskTime));
      }
    }
  }

  @Override
  public void run() {
    try {
      RepairTaskManager.getInstance().waitReady();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return;
    }
    if (!initSuccess) {
      LOGGER.info("[RepairScheduler] Failed to init repair schedule task");
      RepairTaskManager.getInstance().markRepairTaskFinish();
      return;
    }
    CompactionScheduler.exclusiveLockCompactionSelection();
    try {
      CompactionTaskManager.getInstance().waitAllCompactionFinish();
      runTimePartitionScanTasks();
    } catch (InterruptedException interruptedException) {
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      LOGGER.error("[RepairScheduler] Meet error when execute repair schedule task", e);
    } finally {
      RepairTaskManager.getInstance().markRepairTaskFinish();
      try {
        repairLogger.close();
      } catch (Exception e) {
        LOGGER.error(
            "[RepairScheduler] Failed to close repair logger {}",
            repairLogger.getRepairLogFilePath(),
            e);
      }
      LOGGER.info("[RepairScheduler] Finished repair task");
      CompactionScheduler.exclusiveUnlockCompactionSelection();
    }
  }

  private void runTimePartitionScanTasks() throws InterruptedException {
    List<Future<Void>> results = new ArrayList<>();
    for (RepairTimePartition timePartition : allTimePartitionFiles) {
      results.add(
          RepairTaskManager.getInstance()
              .submitScanTask(
                  new RepairTimePartitionScanTask(
                      Collections.singletonList(timePartition), repairLogger, repairProgress)));
    }
    for (Future<Void> result : results) {
      try {
        result.get();
      } catch (CancellationException cancellationException) {
        LOGGER.error("[RepairScheduler] scan task is cancelled");
      } catch (Exception e) {
        LOGGER.error("[RepairScheduler] Meet errors when scan time partition files", e);
      }
    }
  }
}
