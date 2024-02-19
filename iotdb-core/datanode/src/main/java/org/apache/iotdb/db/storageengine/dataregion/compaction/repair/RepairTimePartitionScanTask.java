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

  private final List<RepairTimePartition> repairTimePartitions;
  private final RepairLogger repairLogger;
  private final RepairProgress progress;
  private static final Lock submitRepairFileTaskLock = new ReentrantLock();

  public RepairTimePartitionScanTask(
      List<RepairTimePartition> repairTimePartitions,
      RepairLogger repairLogger,
      RepairProgress progress) {
    this.repairTimePartitions = repairTimePartitions;
    this.repairLogger = repairLogger;
    this.progress = progress;
  }

  @Override
  public Void call() {
    try {
      scanTimePartitionFiles();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    return null;
  }

  private void scanTimePartitionFiles() throws InterruptedException {
    for (RepairTimePartition timePartition : repairTimePartitions) {
      if (timePartition.isRepaired()) {
        LOGGER.info(
            "[RepairScheduler][{}][{}] skip repair time partition {} because it is repaired",
            timePartition.getDatabaseName(),
            timePartition.getDataRegionId(),
            timePartition.getTimePartitionId());
        progress.incrementRepairedTimePartitionNum();
        continue;
      }
      LOGGER.info(
          "[RepairScheduler][{}][{}] start scan repair time partition {}",
          timePartition.getDatabaseName(),
          timePartition.getDataRegionId(),
          timePartition.getTimePartitionId());
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
    CountDownLatch latch = new CountDownLatch(sourceFiles.size());
    for (TsFileResource sourceFile : sourceFiles) {
      if (Thread.interrupted()) {
        Thread.currentThread().interrupt();
        throw new InterruptedException();
      }
      sourceFile.readLock();
      try {
        if (sourceFile.getStatus() != TsFileResourceStatus.NORMAL) {
          latch.countDown();
          continue;
        }
        RepairDataFileScanUtil scanUtil = new RepairDataFileScanUtil(sourceFile);
        scanUtil.scanTsFile();
        if (scanUtil.isBrokenFile()) {
          LOGGER.warn("[RepairScheduler] file {} is skipped because it is broken", sourceFile);
          sourceFile.setTsFileRepairStatus(TsFileRepairStatus.CAN_NOT_REPAIR);
          latch.countDown();
          continue;
        }
        if (!scanUtil.hasUnsortedData()) {
          latch.countDown();
          continue;
        }
      } finally {
        sourceFile.readUnlock();
      }
      LOGGER.info(
          "[RepairScheduler] file {} need to repair because it has internal unsorted data",
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
        RepairDataFileScanUtil.checkTimePartitionHasOverlap(seqList);
    for (TsFileResource overlapFile : overlapFiles) {
      if (Thread.interrupted()) {
        throw new InterruptedException();
      }
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
          "[RepairScheduler][{}][{}] failed to record repair log for time partition {}",
          timePartition.getDatabaseName(),
          timePartition.getDataRegionId(),
          timePartition.getTimePartitionId(),
          e);
    }
    LOGGER.info(
        "[RepairScheduler][{}][{}] time partition {} has been repaired, progress: {}/{}",
        timePartition.getDatabaseName(),
        timePartition.getDataRegionId(),
        timePartition.getTimePartitionId(),
        progress.incrementRepairedTimePartitionNum(),
        progress.getTotalTimePartitionNum());
  }
}
