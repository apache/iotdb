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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class UnsortedFileRepairTaskScheduler implements Runnable {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(UnsortedFileRepairTaskScheduler.class);
  private final Set<RepairTimePartition> allTimePartitionFiles = new HashSet<>();
  private RepairLogger repairLogger;
  private boolean initSuccess = false;
  private boolean isRecoverStoppedTask = false;
  private long repairTaskTime;
  private RepairProgress repairProgress;

  /** Used for create a new repair schedule task */
  public UnsortedFileRepairTaskScheduler(
      List<DataRegion> dataRegions, boolean isRecoverStorageEngine) {
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
    isRecoverStoppedTask = repairLogger.isPreviousTaskStopped();
  }

  @TestOnly
  public UnsortedFileRepairTaskScheduler(
      List<DataRegion> dataRegions, boolean isRecoverStorageEngine, File logFileDir) {
    try {
      repairLogger = new RepairLogger(logFileDir, isRecoverStorageEngine);
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
    isRecoverStoppedTask = repairLogger.isPreviousTaskStopped();
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
      if (!checkConditionsToStartRepairTask()) {
        return;
      }
      LOGGER.info("[RepairScheduler] Wait compaction schedule task finish");
      CompactionScheduler.exclusiveLockCompactionSelection();
      try {
        LOGGER.info("[RepairScheduler] Wait all running compaction task finish");
        CompactionTaskManager.getInstance().waitAllCompactionFinish();
        startTimePartitionScanTasks();
        LOGGER.info("[RepairScheduler] Repair task finished");
      } finally {
        CompactionScheduler.exclusiveUnlockCompactionSelection();
      }
    } catch (InterruptedException ignored) {
      // ignored the InterruptedException and let the task exit
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
    }
  }

  private boolean checkConditionsToStartRepairTask() throws InterruptedException {
    RepairTaskManager.getInstance().checkReady();
    if (isRecoverStoppedTask) {
      return false;
    }
    if (!initSuccess) {
      LOGGER.info("[RepairScheduler] Failed to init repair schedule task");
      return false;
    }
    return true;
  }

  private void startTimePartitionScanTasks() throws InterruptedException {
    for (RepairTimePartition timePartition : allTimePartitionFiles) {
      RepairTaskManager.getInstance()
          .submitScanTask(
              new RepairTimePartitionScanTask(timePartition, repairLogger, repairProgress));
    }
    RepairTaskManager.getInstance().waitRepairTaskFinish();
  }
}
