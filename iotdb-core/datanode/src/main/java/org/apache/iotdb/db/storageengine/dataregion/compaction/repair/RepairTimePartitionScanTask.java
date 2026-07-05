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

import org.apache.iotdb.db.i18n.StorageEngineMessages;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.RepairUnsortedFileCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileRepairStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RepairTimePartitionScanTask implements Callable<Void> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(UnsortedFileRepairTaskScheduler.class);

  private final RepairTimePartition repairTimePartition;
  private final RepairLogger repairLogger;
  private final RepairProgress progress;
  private static final Lock submitRepairFileTaskLock = new ReentrantLock();

  public RepairTimePartitionScanTask(
      RepairTimePartition repairTimePartition, RepairLogger repairLogger, RepairProgress progress) {
    this.repairTimePartition = repairTimePartition;
    this.repairLogger = repairLogger;
    this.progress = progress;
  }

  @Override
  @SuppressWarnings("java:S2142")
  public Void call() {
    try {
      scanTimePartitionFiles();
    } catch (InterruptedException ignored) {
      // Allow task stop
    }
    return null;
  }

  private void scanTimePartitionFiles() throws InterruptedException {
    LOGGER.info(
        StorageEngineMessages.STORAGE_LOG_REPAIRSCHEDULER_START_SCAN_REPAIR_TIME_PARTITION_1D6789DB,
        repairTimePartition.getDatabaseName(),
        repairTimePartition.getDataRegionId(),
        repairTimePartition.getTimePartitionId());
    // repair unsorted data in single file
    checkInternalUnsortedFileAndRepair(repairTimePartition);
    // repair unsorted data between sequence files
    checkOverlapInSequenceSpaceAndRepair(repairTimePartition);
    finishRepairTimePartition(repairTimePartition);
  }

  private void checkInternalUnsortedFileAndRepair(RepairTimePartition timePartition)
      throws InterruptedException {
    List<TsFileResource> sourceFiles =
        Stream.concat(
                timePartition.getSeqFileSnapshot().stream(),
                timePartition.getUnSeqFileSnapshot().stream())
            .collect(Collectors.toList());
    CountDownLatch latch = new CountDownLatch(sourceFiles.size());
    for (TsFileResource sourceFile : sourceFiles) {
      if (!timePartition.getTsFileManager().isAllowCompaction()) {
        LOGGER.info(
            StorageEngineMessages
                .STORAGE_LOG_REPAIRSCHEDULER_CANNOT_SCAN_SOURCE_FILES_IN_BECAUSE_ALLOWCOMPACTION_5E644A6D,
            repairTimePartition.getDataRegionId());
        return;
      }
      checkTaskStatusAndMayStop();
      sourceFile.readLock();
      try {
        if (sourceFile.getStatus() != TsFileResourceStatus.NORMAL) {
          latch.countDown();
          continue;
        }
        LOGGER.info(StorageEngineMessages.REPAIR_START_CHECK_TSFILE, sourceFile);
        RepairDataFileScanUtil scanUtil = new RepairDataFileScanUtil(sourceFile);
        scanUtil.scanTsFile(true);
        checkTaskStatusAndMayStop();
        if (scanUtil.isBrokenFile()) {
          LOGGER.warn(StorageEngineMessages.REPAIR_SKIPPED_BROKEN_FILE, sourceFile);
          sourceFile.setTsFileRepairStatus(TsFileRepairStatus.CAN_NOT_REPAIR);
          latch.countDown();
          continue;
        }
        if (!scanUtil.hasUnsortedDataOrWrongStatistics()) {
          latch.countDown();
          continue;
        }
      } finally {
        sourceFile.readUnlock();
      }
      sourceFile.setTsFileRepairStatus(TsFileRepairStatus.NEED_TO_REPAIR_BY_REWRITE);
      LOGGER.info(
          StorageEngineMessages
              .STORAGE_LOG_REPAIRSCHEDULER_NEED_TO_REPAIR_BECAUSE_IT_HAS_INTERNAL_UNSORTED_C1596DC3,
          sourceFile);
      TsFileManager tsFileManager = timePartition.getTsFileManager();
      RepairUnsortedFileCompactionTask task =
          new RepairUnsortedFileCompactionTask(
              timePartition.getTimePartitionId(),
              timePartition.getTsFileManager(),
              sourceFile,
              latch,
              sourceFile.isSeq(),
              tsFileManager.getNextCompactionTaskId());
      if (!submitRepairFileTaskSafely(task)) {
        latch.countDown();
      }
    }
    latch.await();
  }

  private void checkOverlapInSequenceSpaceAndRepair(RepairTimePartition timePartition)
      throws InterruptedException {
    TsFileManager tsFileManager = timePartition.getTsFileManager();
    List<TsFileResource> seqList =
        tsFileManager.getTsFileListSnapshot(timePartition.getTimePartitionId(), true);
    List<TsFileResource> overlapFiles =
        RepairDataFileScanUtil.checkTimePartitionHasOverlap(seqList, false);
    for (TsFileResource overlapFile : overlapFiles) {
      if (!timePartition.getTsFileManager().isAllowCompaction()) {
        LOGGER.info(
            StorageEngineMessages
                .STORAGE_LOG_REPAIRSCHEDULER_CANNOT_SCAN_SOURCE_FILES_IN_BECAUSE_ALLOWCOMPACTION_5E644A6D,
            repairTimePartition.getDataRegionId());
        return;
      }
      checkTaskStatusAndMayStop();
      CountDownLatch latch = new CountDownLatch(1);
      overlapFile.setTsFileRepairStatus(TsFileRepairStatus.NEED_TO_REPAIR_BY_MOVE);
      RepairUnsortedFileCompactionTask task =
          new RepairUnsortedFileCompactionTask(
              timePartition.getTimePartitionId(),
              timePartition.getTsFileManager(),
              overlapFile,
              latch,
              true,
              tsFileManager.getNextCompactionTaskId());
      LOGGER.info(
          StorageEngineMessages
              .STORAGE_LOG_REPAIRSCHEDULER_NEED_TO_REPAIR_BECAUSE_IT_IS_OVERLAPPED_F1AC0C78,
          overlapFile);
      if (submitRepairFileTaskSafely(task)) {
        latch.await();
      }
    }
  }

  private boolean submitRepairFileTaskSafely(RepairUnsortedFileCompactionTask task)
      throws InterruptedException {
    // check waiting queue size to avoid any repair task been kicked out
    submitRepairFileTaskLock.lock();
    try {
      while (CompactionTaskManager.getInstance().isWaitingQueueFull()) {
        Thread.sleep(TimeUnit.SECONDS.toMillis(1));
      }
      return CompactionTaskManager.getInstance().addTaskToWaitingQueue(task);
    } finally {
      submitRepairFileTaskLock.unlock();
    }
  }

  private void finishRepairTimePartition(RepairTimePartition timePartition) {
    try {
      synchronized (repairLogger) {
        repairLogger.recordRepairedTimePartition(timePartition);
      }
    } catch (Exception e) {
      LOGGER.error(
          StorageEngineMessages
              .STORAGE_LOG_REPAIRSCHEDULER_FAILED_TO_RECORD_REPAIR_LOG_FOR_TIME_PARTITION_11251247,
          timePartition.getDatabaseName(),
          timePartition.getDataRegionId(),
          timePartition.getTimePartitionId(),
          e);
    }
    LOGGER.info(
        StorageEngineMessages
            .STORAGE_LOG_REPAIRSCHEDULER_TIME_PARTITION_HAS_BEEN_REPAIRED_PROGRESS_697FEA22,
        timePartition.getDatabaseName(),
        timePartition.getDataRegionId(),
        timePartition.getTimePartitionId(),
        progress.incrementRepairedTimePartitionNum(),
        progress.getTotalTimePartitionNum());
  }

  private void checkTaskStatusAndMayStop() throws InterruptedException {
    if (Thread.interrupted()) {
      throw new InterruptedException();
    }
  }
}
