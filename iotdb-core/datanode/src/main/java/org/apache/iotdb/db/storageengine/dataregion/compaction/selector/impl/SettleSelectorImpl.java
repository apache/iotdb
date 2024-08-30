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
package org.apache.iotdb.db.storageengine.dataregion.compaction.selector.impl;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeTTLCache;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.ICompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.SettleCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.ISettleSelector;
import org.apache.iotdb.db.storageengine.dataregion.modification.Deletion;
import org.apache.iotdb.db.storageengine.dataregion.modification.Modification;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ArrayDeviceTimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.FileTimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ITimeIndex;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.iotdb.db.storageengine.dataregion.compaction.selector.impl.SettleSelectorImpl.DirtyStatus.NOT_SATISFIED;
import static org.apache.iotdb.db.storageengine.dataregion.compaction.selector.impl.SettleSelectorImpl.DirtyStatus.PARTIALLY_DIRTY;

public class SettleSelectorImpl implements ISettleSelector {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  // this parameter indicates whether to use the costly method for settle file selection. The
  // high-cost selection has a lower triggering frequency, while the low-cost selection has a higher
  // triggering frequency.
  private final boolean heavySelect;
  private final String storageGroupName;
  private final String dataRegionId;
  private final long timePartition;
  private final TsFileManager tsFileManager;
  private boolean isSeq;

  public SettleSelectorImpl(
      boolean heavySelect,
      String storageGroupName,
      String dataRegionId,
      long timePartition,
      TsFileManager tsFileManager) {
    this.heavySelect = heavySelect;
    this.storageGroupName = storageGroupName;
    this.dataRegionId = dataRegionId;
    this.timePartition = timePartition;
    this.tsFileManager = tsFileManager;
  }

  static class FileDirtyInfo {
    DirtyStatus status;
    long dirtyDataSize = 0;

    public FileDirtyInfo(DirtyStatus status) {
      this.status = status;
    }

    public FileDirtyInfo(DirtyStatus status, long dirtyDataSize) {
      this.status = status;
      this.dirtyDataSize = dirtyDataSize;
    }
  }

  static class PartiallyDirtyResource {
    List<TsFileResource> resources = new ArrayList<>();
    long totalFileSize = 0;

    public boolean add(TsFileResource resource, long dirtyDataSize) {
      resources.add(resource);
      totalFileSize += resource.getTsFileSize();
      totalFileSize -= dirtyDataSize;
      return checkHasReachedThreshold();
    }

    public List<TsFileResource> getResources() {
      return resources;
    }

    public boolean checkHasReachedThreshold() {
      return resources.size() >= config.getInnerCompactionCandidateFileNum()
          || totalFileSize >= config.getTargetCompactionFileSize();
    }
  }

  @Override
  public List<SettleCompactionTask> selectSettleTask(List<TsFileResource> tsFileResources) {
    if (tsFileResources.isEmpty()) {
      return Collections.emptyList();
    }
    this.isSeq = tsFileResources.get(0).isSeq();
    return selectTasks(tsFileResources);
  }

  private List<SettleCompactionTask> selectTasks(List<TsFileResource> resources) {
    List<TsFileResource> fullyDirtyResource = new ArrayList<>();
    List<PartiallyDirtyResource> partiallyDirtyResourceList = new ArrayList<>();
    PartiallyDirtyResource partiallyDirtyResource = new PartiallyDirtyResource();
    try {
      for (TsFileResource resource : resources) {
        boolean shouldStop = false;
        FileDirtyInfo fileDirtyInfo;
        if (resource.getStatus() != TsFileResourceStatus.NORMAL) {
          fileDirtyInfo = new FileDirtyInfo(NOT_SATISFIED);
        } else {
          if (!heavySelect) {
            fileDirtyInfo = selectFileBaseOnModSize(resource);
          } else {
            fileDirtyInfo = selectFileBaseOnDirtyData(resource);
          }
        }

        switch (fileDirtyInfo.status) {
          case FULLY_DIRTY:
            fullyDirtyResource.add(resource);
            break;
          case PARTIALLY_DIRTY:
            shouldStop = partiallyDirtyResource.add(resource, fileDirtyInfo.dirtyDataSize);
            break;
          case NOT_SATISFIED:
            shouldStop = !partiallyDirtyResource.getResources().isEmpty();
            break;
          default:
            // do nothing
        }

        if (shouldStop) {
          partiallyDirtyResourceList.add(partiallyDirtyResource);
          partiallyDirtyResource = new PartiallyDirtyResource();
          if (!heavySelect) {
            // Non-heavy selection is triggered more frequently. In order to avoid selecting too
            // many files containing mods for compaction when the disk is insufficient, the number
            // and size of files are limited here.
            break;
          }
        }
      }
      partiallyDirtyResourceList.add(partiallyDirtyResource);
      return createTask(fullyDirtyResource, partiallyDirtyResourceList);
    } catch (Exception e) {
      LOGGER.error(
          "{}-{} cannot select file for settle compaction", storageGroupName, dataRegionId, e);
    }
    return Collections.emptyList();
  }

  private FileDirtyInfo selectFileBaseOnModSize(TsFileResource resource) {
    ModificationFile modFile = resource.getModFile();
    if (modFile == null || !modFile.exists()) {
      return new FileDirtyInfo(DirtyStatus.NOT_SATISFIED);
    }
    return modFile.getSize() > config.getInnerCompactionTaskSelectionModsFileThreshold()
            || !CompactionUtils.isDiskHasSpace(
                config.getInnerCompactionTaskSelectionDiskRedundancy())
        ? new FileDirtyInfo(PARTIALLY_DIRTY)
        : new FileDirtyInfo(DirtyStatus.NOT_SATISFIED);
  }

  /**
   * Only when all devices with ttl are deleted may they be selected. On the basic of the previous,
   * only when the number of deleted devices exceeds the threshold or has expired for too long will
   * they be selected.
   *
   * @return dirty status means the status of current resource.
   */
  private FileDirtyInfo selectFileBaseOnDirtyData(TsFileResource resource)
      throws IOException, IllegalPathException {
    ModificationFile modFile = resource.getModFile();
    ITimeIndex timeIndex = resource.getTimeIndex();
    if (timeIndex instanceof FileTimeIndex) {
      timeIndex = CompactionUtils.buildDeviceTimeIndex(resource);
    }
    Set<IDeviceID> deletedDevices = new HashSet<>();
    boolean hasExpiredTooLong = false;
    long currentTime = CommonDateTimeUtils.currentTime();

    Collection<Modification> modifications = modFile.getModifications();
    for (IDeviceID device : ((ArrayDeviceTimeIndex) timeIndex).getDevices()) {
      // check expired device by ttl
      // TODO: remove deviceId conversion
      long deviceTTL = DataNodeTTLCache.getInstance().getTTL(device);
      boolean hasSetTTL = deviceTTL != Long.MAX_VALUE;
      boolean isDeleted =
          !timeIndex.isDeviceAlive(device, deviceTTL)
              || isDeviceDeletedByMods(
                  modifications,
                  device,
                  timeIndex.getStartTime(device),
                  timeIndex.getEndTime(device));

      if (hasSetTTL) {
        if (!isDeleted) {
          // For devices with TTL set, all data must expire in order to meet the conditions for
          // being selected.
          return new FileDirtyInfo(DirtyStatus.NOT_SATISFIED);
        }
        long outdatedTimeDiff = currentTime - timeIndex.getEndTime(device);
        hasExpiredTooLong =
            hasExpiredTooLong
                || outdatedTimeDiff > Math.min(config.getMaxExpiredTime(), 3 * deviceTTL);
      }

      if (isDeleted) {
        deletedDevices.add(device);
      }
    }

    double deletedDeviceRatio =
        ((double) deletedDevices.size()) / ((ArrayDeviceTimeIndex) timeIndex).getDevices().size();
    if (deletedDeviceRatio == 1d) {
      // the whole file is completely dirty
      return new FileDirtyInfo(DirtyStatus.FULLY_DIRTY);
    }
    hasExpiredTooLong = config.getMaxExpiredTime() != Long.MAX_VALUE && hasExpiredTooLong;
    if (hasExpiredTooLong || deletedDeviceRatio >= config.getExpiredDataRatio()) {
      // evaluate dirty data size in the tsfile
      return new FileDirtyInfo(
          PARTIALLY_DIRTY, (long) (deletedDeviceRatio * resource.getTsFileSize()));
    }
    return new FileDirtyInfo(DirtyStatus.NOT_SATISFIED);
  }

  /** Check whether the device is completely deleted by mods or not. */
  private boolean isDeviceDeletedByMods(
      Collection<Modification> modifications, IDeviceID device, long startTime, long endTime)
      throws IllegalPathException {
    for (Modification modification : modifications) {
      PartialPath path = modification.getPath();
      if (path.endWithMultiLevelWildcard()
          && path.getDevicePath().matchFullPath(new PartialPath(device))
          && ((Deletion) modification).getTimeRange().contains(startTime, endTime)) {
        return true;
      }
    }
    return false;
  }

  private List<SettleCompactionTask> createTask(
      List<TsFileResource> fullyDirtyResources,
      List<PartiallyDirtyResource> partiallyDirtyResourceList) {
    List<SettleCompactionTask> tasks = new ArrayList<>();
    for (int i = 0; i < partiallyDirtyResourceList.size(); i++) {
      if (i == 0) {
        if (fullyDirtyResources.isEmpty()
            && partiallyDirtyResourceList.get(i).getResources().isEmpty()) {
          continue;
        }
        tasks.add(
            new SettleCompactionTask(
                timePartition,
                tsFileManager,
                fullyDirtyResources,
                partiallyDirtyResourceList.get(i).getResources(),
                isSeq,
                createCompactionPerformer(),
                tsFileManager.getNextCompactionTaskId()));
      } else {
        if (partiallyDirtyResourceList.get(i).getResources().isEmpty()) {
          continue;
        }
        tasks.add(
            new SettleCompactionTask(
                timePartition,
                tsFileManager,
                Collections.emptyList(),
                partiallyDirtyResourceList.get(i).getResources(),
                isSeq,
                createCompactionPerformer(),
                tsFileManager.getNextCompactionTaskId()));
      }
    }
    return tasks;
  }

  private ICompactionPerformer createCompactionPerformer() {
    return isSeq
        ? IoTDBDescriptor.getInstance()
            .getConfig()
            .getInnerSeqCompactionPerformer()
            .createInstance()
        : IoTDBDescriptor.getInstance()
            .getConfig()
            .getInnerUnseqCompactionPerformer()
            .createInstance();
  }

  enum DirtyStatus {
    FULLY_DIRTY, // the whole file is dirty
    PARTIALLY_DIRTY, // the file is partial dirty
    NOT_SATISFIED; // do not satisfy settle condition, which does not mean there is no dirty data
  }
}
