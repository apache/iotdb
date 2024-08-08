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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionScheduleTaskManager;
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
    initRepairDataTask(dataRegions, isRecoverStorageEngine, getOrCreateRepairLogDir());
  }

  public UnsortedFileRepairTaskScheduler(
      List<DataRegion> dataRegions, boolean isRecoverStorageEngine, File logFileDir) {
    initRepairDataTask(dataRegions, isRecoverStorageEngine, logFileDir);
  }

  private void initRepairDataTask(
      List<DataRegion> dataRegions, boolean isRecoverStorageEngine, File logFileDir) {
    // 1. init repair logger
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
    // 2. parse log file and get the repair task start time
    File logFile = repairLogger.getLogFile();
    RepairTaskRecoverLogParser recoverLogParser = new RepairTaskRecoverLogParser(logFile);
    if (repairLogger.isNeedRecoverFromLogFile()) {
      try {
        recoverLogParser.parse();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    // if the start time is not recorded in log file, the return value is System.currentTimeMillis()
    this.repairTaskTime = recoverLogParser.getRepairDataTaskStartTime();
    // 3. collect the time partition info of data regions
    collectTimePartitions(dataRegions);
    // 4. recover the repair progress
    if (repairLogger.isNeedRecoverFromLogFile()) {
      try {
        recoverRepairProgress(recoverLogParser);
      } catch (Exception e) {
        LOGGER.error(
            "[RepairScheduler] Failed to parse repair log file {}", logFile.getAbsolutePath(), e);
        return;
      }
    }
    try {
      repairLogger.recordRepairTaskStartTimeIfLogFileEmpty(repairTaskTime);
    } catch (IOException e) {
      LOGGER.error(
          "[RepairScheduler] Failed to record repair task start time in log file {}",
          logFile.getAbsolutePath(),
          e);
      return;
    }
    repairProgress = new RepairProgress(allTimePartitionFiles.size());
    initSuccess = true;
    isRecoverStoppedTask = repairLogger.isPreviousTaskStopped();
  }

  private File getOrCreateRepairLogDir() {
    File logFileDir =
        new File(
            IoTDBDescriptor.getInstance().getConfig().getSystemDir()
                + File.separator
                + RepairLogger.repairLogDir);
    if (!logFileDir.exists()) {
      logFileDir.mkdirs();
    }
    return logFileDir;
  }

  private void recoverRepairProgress(RepairTaskRecoverLogParser recoverLogParser) {
    LOGGER.info(
        "[RepairScheduler] recover unfinished repair schedule task from log file: {}",
        recoverLogParser.getRepairLogFilePath());

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
    // skip the time partition which is created after the repair task start time
    for (RepairTimePartition timePartition : allTimePartitionFiles) {
      if (!timePartition.isRepaired() && !timePartition.needRepair()) {
        timePartition.setRepaired(true);
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
  @SuppressWarnings("java:S2142")
  public void run() {
    try {
      if (!checkConditionsToStartRepairTask()) {
        return;
      }
      LOGGER.info("[RepairScheduler] Wait compaction schedule task finish");
      CompactionScheduleTaskManager.getInstance().stopCompactionScheduleTasks();
      try {
        LOGGER.info("[RepairScheduler] Wait all running compaction task finish");
        CompactionTaskManager.getInstance().waitAllCompactionFinish();
        startTimePartitionScanTasks();
        LOGGER.info("[RepairScheduler] Repair task finished");
      } finally {
        CompactionScheduleTaskManager.getInstance().startScheduleTasks();
      }
    } catch (InterruptedException ignored) {
      // ignored the InterruptedException and let the task exit
    } catch (Exception e) {
      LOGGER.error("[RepairScheduler] Meet error when execute repair schedule task", e);
    } finally {
      try {
        repairLogger.close();
      } catch (Exception e) {
        LOGGER.error(
            "[RepairScheduler] Failed to close repair logger {}",
            repairLogger.getRepairLogFilePath(),
            e);
      }
      CompactionScheduleTaskManager.getRepairTaskManagerInstance().markRepairTaskFinish();
    }
  }

  private boolean checkConditionsToStartRepairTask() throws InterruptedException {
    if (isRecoverStoppedTask) {
      return false;
    }
    if (!initSuccess) {
      LOGGER.info("[RepairScheduler] Failed to init repair schedule task");
      return false;
    }
    for (RepairTimePartition timePartition : allTimePartitionFiles) {
      if (!timePartition.isRepaired()) {
        return true;
      }
    }
    LOGGER.info("[RepairScheduler] All time partitions have been repaired, skip repair task");
    return false;
  }

  private void startTimePartitionScanTasks() throws InterruptedException {
    CompactionScheduleTaskManager.RepairDataTaskManager repairDataTaskManager =
        CompactionScheduleTaskManager.getRepairTaskManagerInstance();
    try {
      for (RepairTimePartition timePartition : allTimePartitionFiles) {
        if (timePartition.isRepaired()) {
          LOGGER.info(
              "[RepairScheduler][{}][{}] skip repair time partition {} because it is repaired",
              timePartition.getDatabaseName(),
              timePartition.getDataRegionId(),
              timePartition.getTimePartitionId());
          repairProgress.incrementRepairedTimePartitionNum();
          continue;
        }
        LOGGER.info(
            "[RepairScheduler] submit a repair time partition scan task {}-{}-{}",
            timePartition.getDatabaseName(),
            timePartition.getDataRegionId(),
            timePartition.getTimePartitionId());
        repairDataTaskManager.submitRepairScanTask(
            new RepairTimePartitionScanTask(timePartition, repairLogger, repairProgress));
      }
    } finally {
      repairDataTaskManager.waitRepairTaskFinish();
    }
  }
}
