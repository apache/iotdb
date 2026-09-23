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
import org.apache.iotdb.db.i18n.StorageEngineMessages;
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
        LOGGER.error(StorageEngineMessages.REPAIR_FAILED_CREATE_LOGGER, e);
        repairLogger.close();
      } catch (IOException closeException) {
        LOGGER.error(StorageEngineMessages.REPAIR_FAILED_CLOSE_LOGGER, closeException);
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
            StorageEngineMessages
                .STORAGE_LOG_REPAIRSCHEDULER_FAILED_TO_PARSE_REPAIR_LOG_FILE_142D2568,
            logFile.getAbsolutePath(),
            e);
        return;
      }
    }
    try {
      repairLogger.recordRepairTaskStartTimeIfLogFileEmpty(repairTaskTime);
    } catch (IOException e) {
      LOGGER.error(
          StorageEngineMessages
              .STORAGE_LOG_REPAIRSCHEDULER_FAILED_TO_RECORD_REPAIR_TASK_START_TIME_95552D7E,
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
        StorageEngineMessages
            .STORAGE_LOG_REPAIRSCHEDULER_RECOVER_UNFINISHED_REPAIR_SCHEDULE_TASK_7C5B6D5F,
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
          resource.setTsFileRepairStatus(TsFileRepairStatus.NEED_TO_REPAIR_BY_REWRITE);
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
      LOGGER.info(StorageEngineMessages.REPAIR_WAIT_COMPACTION_FINISH);
      CompactionScheduleTaskManager.getInstance().stopCompactionScheduleTasks();
      try {
        LOGGER.info(StorageEngineMessages.REPAIR_WAIT_ALL_RUNNING_TASK_FINISH);
        CompactionTaskManager.getInstance().waitAllCompactionFinish();
        startTimePartitionScanTasks();
        LOGGER.info(StorageEngineMessages.REPAIR_TASK_FINISHED);
      } finally {
        CompactionScheduleTaskManager.getInstance().startScheduleTasks();
      }
    } catch (InterruptedException ignored) {
      // ignored the InterruptedException and let the task exit
    } catch (Exception e) {
      LOGGER.error(StorageEngineMessages.REPAIR_SCHEDULE_TASK_ERROR, e);
    } finally {
      try {
        repairLogger.close();
      } catch (Exception e) {
        LOGGER.error(
            StorageEngineMessages
                .STORAGE_LOG_REPAIRSCHEDULER_FAILED_TO_CLOSE_REPAIR_LOGGER_EC191F6B,
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
      LOGGER.info(StorageEngineMessages.REPAIR_FAILED_INIT_SCHEDULE_TASK);
      return false;
    }
    for (RepairTimePartition timePartition : allTimePartitionFiles) {
      if (!timePartition.isRepaired()) {
        return true;
      }
    }
    LOGGER.info(StorageEngineMessages.REPAIR_ALL_PARTITIONS_DONE_SKIP);
    return false;
  }

  private void startTimePartitionScanTasks() throws InterruptedException {
    CompactionScheduleTaskManager.RepairDataTaskManager repairDataTaskManager =
        CompactionScheduleTaskManager.getRepairTaskManagerInstance();
    try {
      for (RepairTimePartition timePartition : allTimePartitionFiles) {
        if (timePartition.isRepaired()) {
          LOGGER.info(
              StorageEngineMessages
                  .STORAGE_LOG_REPAIRSCHEDULER_SKIP_REPAIR_TIME_PARTITION_BECAUSE_IT_IS_BDD35739,
              timePartition.getDatabaseName(),
              timePartition.getDataRegionId(),
              timePartition.getTimePartitionId());
          repairProgress.incrementRepairedTimePartitionNum();
          continue;
        }
        LOGGER.info(
            StorageEngineMessages
                .STORAGE_LOG_REPAIRSCHEDULER_SUBMIT_A_REPAIR_TIME_PARTITION_SCAN_TASK_0E98F12C,
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
