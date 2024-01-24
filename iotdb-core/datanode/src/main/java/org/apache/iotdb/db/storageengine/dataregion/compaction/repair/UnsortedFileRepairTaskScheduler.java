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

import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.RepairUnsortedFileCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionScheduler;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileRepairStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.DeviceTimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ITimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.utils.TsFileResourceUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class UnsortedFileRepairTaskScheduler implements Runnable {

  /** a repair task is running */
  private static final AtomicBoolean isRepairingData = new AtomicBoolean(false);

  private static final Logger LOGGER =
      LoggerFactory.getLogger(UnsortedFileRepairTaskScheduler.class);
  private final Set<RepairTimePartition> allTimePartitionFiles = new HashSet<>();
  private RepairLogger repairLogger;
  private final boolean isRecover;
  private boolean initSuccess = false;
  private long repairTaskTime;
  private int repairProgress = 0;

  public static boolean markRepairTaskStart() {
    return isRepairingData.compareAndSet(false, true);
  }

  public static void markRepairTaskFinish() {
    isRepairingData.set(false);
  }

  /** Used for create a new repair schedule task */
  public UnsortedFileRepairTaskScheduler(List<DataRegion> dataRegions) {
    this.isRecover = false;
    try {
      repairLogger = new RepairLogger();
    } catch (Exception e) {
      try {
        LOGGER.error("[RepairScheduler] Failed to create repair logger", e);
        repairLogger.close();
      } catch (IOException closeException) {
        LOGGER.error("[RepairScheduler] Failed to close repair logger", closeException);
      }
      return;
    }
    this.repairTaskTime = repairLogger.getRepairTaskStartTime();
    collectTimePartitions(dataRegions);
    initSuccess = true;
  }

  /** Used for recover from log file */
  public UnsortedFileRepairTaskScheduler(List<DataRegion> dataRegions, File logFile) {
    this.isRecover = true;
    LOGGER.info("[RepairScheduler] start recover repair log {}", logFile.getAbsolutePath());
    try {
      repairLogger = new RepairLogger(logFile);
    } catch (Exception e) {
      try {
        LOGGER.error(
            "[RepairScheduler] Failed to get repair logger from log file {}",
            logFile.getAbsolutePath(),
            e);
        repairLogger.close();
      } catch (IOException closeException) {
        LOGGER.error(
            "[RepairScheduler] Failed to close log file {}",
            logFile.getAbsolutePath(),
            closeException);
      }
      return;
    }
    this.repairTaskTime = repairLogger.getRepairTaskStartTime();
    collectTimePartitions(dataRegions);
    try {
      recover(logFile);
    } catch (Exception e) {
      LOGGER.error(
          "[RepairScheduler] Failed to parse repair log file {}", logFile.getAbsolutePath(), e);
      return;
    }
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
    if (!initSuccess) {
      LOGGER.info("[RepairScheduler] Failed to init repair schedule task");
      markRepairTaskFinish();
      return;
    }
    CompactionScheduler.exclusiveLockCompactionSelection();
    CompactionTaskManager.getInstance().waitAllCompactionFinish();
    try {
      executeRepair();
    } catch (Exception e) {
      LOGGER.error("[RepairScheduler] Meet error when execute repair schedule task", e);
    } finally {
      markRepairTaskFinish();
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

  private void executeRepair() throws InterruptedException {
    for (RepairTimePartition timePartition : allTimePartitionFiles) {
      if (timePartition.isRepaired()) {
        LOGGER.info(
            "[RepairScheduler][{}][{}] skip repair time partition {} because it is repaired",
            timePartition.getDatabaseName(),
            timePartition.getDataRegionId(),
            timePartition.getTimePartitionId());
        repairProgress++;
        continue;
      }
      // repair unsorted data in single file
      checkInternalUnsortedFileAndRepair(timePartition);
      // repair unsorted data between sequence files
      checkOverlapInSequenceSpaceAndRepair(timePartition);
      finishRepairTimePartition(timePartition);
    }
  }

  private void checkInternalUnsortedFileAndRepair(RepairTimePartition timePartition)
      throws InterruptedException {
    List<TsFileResource> sourceFiles =
        Stream.concat(
                timePartition.getSeqFileSnapshot().stream(),
                timePartition.getUnSeqFileSnapshot().stream())
            .collect(Collectors.toList());
    for (TsFileResource sourceFile : sourceFiles) {
      sourceFile.readLock();
      try {
        if (sourceFile.getStatus() != TsFileResourceStatus.NORMAL) {
          continue;
        }
        if (TsFileResourceUtils.validateTsFileDataCorrectness(sourceFile)) {
          continue;
        }
      } finally {
        sourceFile.readUnlock();
      }
      LOGGER.info(
          "[RepairScheduler] file {} need to repair because it has internal unsorted data",
          sourceFile);
      TsFileManager tsFileManager = timePartition.getTsFileManager();
      CountDownLatch latch = new CountDownLatch(1);
      RepairUnsortedFileCompactionTask task =
          new RepairUnsortedFileCompactionTask(
              timePartition.getTimePartitionId(),
              timePartition.getTsFileManager(),
              sourceFile,
              latch,
              sourceFile.isSeq(),
              tsFileManager.getNextCompactionTaskId());
      if (CompactionTaskManager.getInstance().addTaskToWaitingQueue(task)) {
        latch.await();
      }
    }
  }

  private void checkOverlapInSequenceSpaceAndRepair(RepairTimePartition timePartition)
      throws InterruptedException {
    TsFileManager tsFileManager = timePartition.getTsFileManager();
    List<TsFileResource> seqList =
        tsFileManager.getTsFileListSnapshot(timePartition.getTimePartitionId(), true);
    List<TsFileResource> overlapFiles = checkTimePartitionHasOverlap(seqList);
    for (TsFileResource overlapFile : overlapFiles) {
      CountDownLatch latch = new CountDownLatch(1);
      RepairUnsortedFileCompactionTask task =
          new RepairUnsortedFileCompactionTask(
              timePartition.getTimePartitionId(),
              timePartition.getTsFileManager(),
              overlapFile,
              latch,
              true,
              false,
              tsFileManager.getNextCompactionTaskId());
      LOGGER.info(
          "[RepairScheduler] file {} need to repair because it is overlapped with other files",
          overlapFile);
      if (CompactionTaskManager.getInstance().addTaskToWaitingQueue(task)) {
        latch.await();
      }
    }
  }

  private List<TsFileResource> checkTimePartitionHasOverlap(List<TsFileResource> resources) {
    List<TsFileResource> overlapResources = new ArrayList<>();
    Map<String, Long> deviceEndTimeMap = new HashMap<>();
    for (TsFileResource resource : resources) {
      if (resource.getStatus() == TsFileResourceStatus.UNCLOSED
          || resource.getStatus() == TsFileResourceStatus.DELETED) {
        continue;
      }
      DeviceTimeIndex deviceTimeIndex;
      try {
        deviceTimeIndex = getDeviceTimeIndex(resource);
      } catch (Exception ignored) {
        continue;
      }

      Set<String> devices = deviceTimeIndex.getDevices();
      boolean fileHasOverlap = false;
      // check overlap
      for (String device : devices) {
        long deviceStartTimeInCurrentFile = deviceTimeIndex.getStartTime(device);
        if (deviceStartTimeInCurrentFile > deviceTimeIndex.getEndTime(device)) {
          continue;
        }
        if (!deviceEndTimeMap.containsKey(device)) {
          continue;
        }
        long deviceEndTimeInPreviousFile = deviceEndTimeMap.get(device);
        if (deviceStartTimeInCurrentFile <= deviceEndTimeInPreviousFile) {
          fileHasOverlap = true;
          overlapResources.add(resource);
          break;
        }
      }
      // update end time map
      if (!fileHasOverlap) {
        for (String device : devices) {
          deviceEndTimeMap.put(device, deviceTimeIndex.getEndTime(device));
        }
      }
    }
    return overlapResources;
  }

  private DeviceTimeIndex getDeviceTimeIndex(TsFileResource resource) throws IOException {
    ITimeIndex timeIndex = resource.getTimeIndex();
    if (timeIndex instanceof DeviceTimeIndex) {
      return (DeviceTimeIndex) timeIndex;
    }
    return resource.buildDeviceTimeIndex();
  }

  private void finishRepairTimePartition(RepairTimePartition timePartition) {
    try {
      repairLogger.recordRepairedTimePartition(timePartition);
    } catch (Exception e) {
      LOGGER.error(
          "[RepairScheduler][{}][{}] failed to record repair log for time partition {}",
          timePartition.getDatabaseName(),
          timePartition.getDataRegionId(),
          timePartition.getTimePartitionId());
    }
    repairProgress++;
    LOGGER.info(
        "[RepairScheduler][{}][{}] time partition {} has been repaired, progress: {}/{}",
        timePartition.getDatabaseName(),
        timePartition.getDataRegionId(),
        timePartition.getTimePartitionId(),
        repairProgress,
        allTimePartitionFiles.size());
  }
}
