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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class UnsortedFileRepairTaskScheduler implements Runnable {

  /** a repair task is running */
  private static final AtomicBoolean isRepairingData = new AtomicBoolean(false);

  private static final Logger LOGGER =
      LoggerFactory.getLogger(UnsortedFileRepairTaskScheduler.class);
  private final Set<TimePartitionFiles> allTimePartitionFiles = new HashSet<>();
  private RepairLogger repairLogger;
  private final boolean isRecover;

  public static boolean markRepairTaskStart() {
    return isRepairingData.compareAndSet(false, true);
  }

  /** Used for create a new repair schedule task */
  public UnsortedFileRepairTaskScheduler(List<DataRegion> dataRegions) {
    this(dataRegions, false);
  }

  /** Used for recover from log file */
  public UnsortedFileRepairTaskScheduler(List<DataRegion> dataRegions, boolean isRecover) {
    this.isRecover = isRecover;
    collectTimePartitions(dataRegions);
    if (isRecover) {
      recover(dataRegions);
    }
  }

  private void recover(List<DataRegion> dataRegions) {
    RepairTaskRecoveryPerformer recoveryPerformer = new RepairTaskRecoveryPerformer(dataRegions);
    LOGGER.info(
        "recover unfinished repair schedule task from log file: {}",
        recoveryPerformer.getRepairLogFilePath());
    try {
      recoveryPerformer.perform();
      Map<TimePartitionFiles, Set<String>> repairedTimePartitionWithCannotRepairFiles =
          recoveryPerformer.getRepairedTimePartitionsWithCannotRepairFiles();
      for (TimePartitionFiles timePartition : allTimePartitionFiles) {
        Set<String> cannotRepairFiles = repairedTimePartitionWithCannotRepairFiles.remove(timePartition);
        if (cannotRepairFiles == null || cannotRepairFiles.isEmpty()) {
          continue;
        }
        // mark cannot repair file in TsFileResource
        List<TsFileResource> resources = timePartition.getAllFiles();
        for (TsFileResource resource : resources) {
          if (resource.getStatus() != TsFileResourceStatus.DELETED) {
            resource.setTsFileRepairStatus(TsFileRepairStatus.NEED_TO_REPAIR);
          }
        }
      }
    } catch (Exception e) {
      LOGGER.error(
          "Failed to parse repair log file {}", recoveryPerformer.getRepairLogFilePath(), e);
    }
  }

  private void collectTimePartitions(List<DataRegion> dataRegions) {
    for (DataRegion dataRegion : dataRegions) {
      if (dataRegion == null) {
        continue;
      }
      List<Long> timePartitions = dataRegion.getTimePartitions();
      for (long timePartition : timePartitions) {
        allTimePartitionFiles.add(new TimePartitionFiles(dataRegion, timePartition));
      }
    }
  }

  @Override
  public void run() {
    CompactionScheduler.exclusiveLockCompactionSelection();
    CompactionTaskManager.getInstance().waitAllCompactionFinish();
    try {
      repairLogger = new RepairLogger(isRecover);
      executeRepair();
    } catch (Exception e) {
      LOGGER.error("Meet error when execute repair schedule task", e);
    } finally {
      isRepairingData.set(false);
      try {
        repairLogger.close();
      } catch (Exception e) {
        LOGGER.error("Failed to close repair logger {}", repairLogger.getRepairLogFilePath(), e);
      }
      CompactionScheduler.exclusiveUnlockCompactionSelection();
    }
  }

  private void executeRepair() throws InterruptedException {
    for (TimePartitionFiles timePartition : allTimePartitionFiles) {
      // repair unsorted data in single file
      checkInternalUnsortedFileAndRepair(timePartition);
      // repair unsorted data between sequence files
      checkOverlapInSequenceSpaceAndRepair(timePartition);
      finishRepairTimePartition(timePartition);
    }
  }

  private void checkInternalUnsortedFileAndRepair(TimePartitionFiles timePartition)
      throws InterruptedException {
    List<TsFileResource> sourceFiles =
        Stream.concat(timePartition.getSeqFiles().stream(), timePartition.getUnseqFiles().stream())
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
      TsFileManager tsFileManager = timePartition.getTsFileManager();
      RepairUnsortedFileCompactionTask task =
          new RepairUnsortedFileCompactionTask(
              timePartition.getTimePartition(),
              timePartition.getTsFileManager(),
              sourceFile,
              sourceFile.isSeq(),
              tsFileManager.getNextCompactionTaskId());
      if (CompactionTaskManager.getInstance().addTaskToWaitingQueue(task)) {
        // TODO: wait the repair compaction task finished
      }
    }
  }

  private void checkOverlapInSequenceSpaceAndRepair(TimePartitionFiles timePartition)
      throws InterruptedException {
    TsFileManager tsFileManager = timePartition.getTsFileManager();
    List<TsFileResource> seqList =
        tsFileManager.getTsFileListSnapshot(timePartition.getTimePartition(), true);
    List<TsFileResource> overlapFiles = checkTimePartitionHasOverlap(seqList);
    for (TsFileResource overlapFile : overlapFiles) {
      RepairUnsortedFileCompactionTask task =
          new RepairUnsortedFileCompactionTask(
              timePartition.getTimePartition(),
              timePartition.getTsFileManager(),
              overlapFile,
              true,
              false,
              tsFileManager.getNextCompactionTaskId());
      if (CompactionTaskManager.getInstance().addTaskToWaitingQueue(task)) {
        // TODO: wait the repair compaction task finished

      }
    }
  }

  private List<TsFileResource> checkTimePartitionHasOverlap(List<TsFileResource> resources) {
    List<TsFileResource> overlapResources = new ArrayList<>();
    Map<String, Long> deviceEndTimeMap = new HashMap<>();
    Map<String, TsFileResource> deviceLastExistTsFileMap = new HashMap<>();
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
          long deviceEndTimeInCurrentFile = deviceTimeIndex.getEndTime(device);
          if (!deviceLastExistTsFileMap.containsKey(device)) {
            deviceEndTimeMap.put(device, deviceEndTimeInCurrentFile);
            deviceLastExistTsFileMap.put(device, resource);
            continue;
          }
          deviceEndTimeMap.put(device, resource.getEndTime(device));
          deviceLastExistTsFileMap.put(device, resource);
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

  private void finishRepairTimePartition(TimePartitionFiles timePartition) {
    allTimePartitionFiles.remove(timePartition);
    try {
      repairLogger.recordRepairedTimePartition(timePartition);
    } catch (Exception e) {
      LOGGER.error(
          "[RepairScheduler][{}][{}] failed to record repair log for time partition {}",
          timePartition.getDatabaseName(),
          timePartition.getDataRegionId(),
          timePartition.getTimePartition());
    }
    LOGGER.info(
        "[RepairScheduler][{}][{}] time partition {} has been repaired",
        timePartition.getDatabaseName(),
        timePartition.getDataRegionId(),
        timePartition.getTimePartition());
  }
}
