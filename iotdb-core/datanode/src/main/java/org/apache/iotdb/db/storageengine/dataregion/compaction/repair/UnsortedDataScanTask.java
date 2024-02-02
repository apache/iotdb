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
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.DeviceTimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ITimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.utils.TsFileResourceUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class UnsortedDataScanTask implements Callable<Void> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(UnsortedFileRepairTaskScheduler.class);

  private final List<RepairTimePartition> repairTimePartitions;
  private final RepairLogger repairLogger;
  private final RepairProgress progress;
  private final static Lock submitRepairFileTaskLock = new ReentrantLock();

  public UnsortedDataScanTask(
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
      sourceFile.readLock();
      try {
        if (sourceFile.getStatus() != TsFileResourceStatus.NORMAL) {
          continue;
        }
        if (TsFileResourceUtils.validateTsFileDataCorrectness(sourceFile)) {
          continue;
        }
      } finally {
        latch.countDown();
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
      synchronized (repairLogger) {
        repairLogger.recordRepairedTimePartition(timePartition);
      }
    } catch (Exception e) {
      LOGGER.error(
          "[RepairScheduler][{}][{}] failed to record repair log for time partition {}",
          timePartition.getDatabaseName(),
          timePartition.getDataRegionId(),
          timePartition.getTimePartitionId());
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
