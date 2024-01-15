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
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.DeviceTimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ITimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.utils.TsFileResourceUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
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

  public static boolean markRepairTaskStart() {
    return isRepairingData.compareAndSet(false, true);
  }

  public UnsortedFileRepairTaskScheduler(List<DataRegion> dataRegions) {
    collectFiles(dataRegions);
  }

  private void collectFiles(List<DataRegion> dataRegions) {
    for (DataRegion dataRegion : dataRegions) {
      if (dataRegion == null) {
        continue;
      }
      List<Long> timePartitions = dataRegion.getTimePartitions();
      timePartitions.sort(Comparator.reverseOrder());
      TsFileManager tsFileManager = dataRegion.getTsFileManager();
      for (long timePartition : timePartitions) {
        allTimePartitionFiles.add(new TimePartitionFiles(dataRegion, timePartition));
      }
    }
  }

  @Override
  public void run() {
    CompactionScheduler.lockCompactionSelection();
    CompactionTaskManager.getInstance().waitAllCompactionFinish();
    try {
      executeRepair();
    } catch (Exception e) {

    } finally {
      isRepairingData.set(false);
      CompactionScheduler.unlockCompactionSelection();
    }
  }

  private void executeRepair() throws InterruptedException {
    for (TimePartitionFiles timePartition : allTimePartitionFiles) {
      checkInternalUnsortedFileAndRepair(timePartition);
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
              0);
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
      } catch (Exception e) {
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
  }

  private static class TimePartitionFiles {
    private final String databaseName;
    private final String dataRegionId;
    private final TsFileManager tsFileManager;
    private final long timePartition;
    private final long maxVersion;

    private TimePartitionFiles(DataRegion dataRegion, long timePartition) {
      this.databaseName = dataRegion.getDatabaseName();
      this.dataRegionId = dataRegion.getDataRegionId();
      this.tsFileManager = dataRegion.getTsFileManager();
      this.timePartition = timePartition;
      this.maxVersion = calculateMaxVersion();
    }

    private long calculateMaxVersion() {
      long maxVersion = 0;
      List<TsFileResource> resources = tsFileManager.getTsFileListSnapshot(timePartition, true);
      if (!resources.isEmpty()) {
        maxVersion = getFileVersion(resources.get(resources.size() - 1));
      }
      resources = tsFileManager.getTsFileListSnapshot(timePartition, false);
      if (!resources.isEmpty()) {
        maxVersion = Math.max(maxVersion, getFileVersion(resources.get(resources.size() - 1)));
      }
      return maxVersion;
    }

    public long getTimePartition() {
      return timePartition;
    }

    public String getDatabaseName() {
      return databaseName;
    }

    public String getDataRegionId() {
      return dataRegionId;
    }

    public TsFileManager getTsFileManager() {
      return tsFileManager;
    }

    public List<TsFileResource> getSeqFiles() {
      return tsFileManager.getTsFileListSnapshot(timePartition, true).stream()
          .filter(this::resourceVersionFilter)
          .collect(Collectors.toList());
    }

    public List<TsFileResource> getUnseqFiles() {
      return tsFileManager.getTsFileListSnapshot(timePartition, false).stream()
          .filter(this::resourceVersionFilter)
          .collect(Collectors.toList());
    }

    private boolean resourceVersionFilter(TsFileResource resource) {
      if (resource.getStatus() == TsFileResourceStatus.DELETED
          || resource.getStatus() == TsFileResourceStatus.UNCLOSED) {
        return false;
      }
      long fileVersion = getFileVersion(resource);
      return fileVersion >= 0 && fileVersion <= maxVersion;
    }

    private long getFileVersion(TsFileResource resource) {
      long version = -1;
      try {
        TsFileNameGenerator.TsFileName tsFileName =
            TsFileNameGenerator.getTsFileName(resource.getTsFile().getName());
        version = tsFileName.getVersion();
      } catch (IOException ignored) {
      }
      return version;
    }
  }
}
