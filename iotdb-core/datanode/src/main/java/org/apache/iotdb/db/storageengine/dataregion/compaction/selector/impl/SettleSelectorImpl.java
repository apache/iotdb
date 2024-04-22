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
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.DeviceTimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ITimeIndex;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.PlainDeviceID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.iotdb.db.storageengine.dataregion.compaction.selector.impl.SettleSelectorImpl.DirtyStatus.PARTIAL_DELETED;

public class SettleSelectorImpl implements ISettleSelector {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

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

  static class AllDirtyResource {
    List<TsFileResource> resources = new ArrayList<>();

    public void add(TsFileResource resource) {
      resources.add(resource);
    }

    public List<TsFileResource> getResources() {
      return resources;
    }
  }

  static class PartialDirtyResource {
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
      return resources.size() >= config.getFileLimitPerInnerTask()
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
    AllDirtyResource allDirtyResource = new AllDirtyResource();
    List<PartialDirtyResource> partialDirtyResourceList = new ArrayList<>();
    PartialDirtyResource partialDirtyResource = new PartialDirtyResource();
    try {
      for (TsFileResource resource : resources) {
        if (resource.getStatus() != TsFileResourceStatus.NORMAL) {
          continue;
        }
        boolean shouldStop = false;
        DirtyStatus dirtyStatus;
        if (!heavySelect) {
          dirtyStatus = selectFileBaseOnModSize(resource);
        } else {
          dirtyStatus = selectFileBaseOnDirtyData(resource);
        }

        switch (dirtyStatus) {
          case ALL_DELETED:
            allDirtyResource.add(resource);
            break;
          case PARTIAL_DELETED:
            shouldStop = partialDirtyResource.add(resource, dirtyStatus.getDirtyDataSize());
            break;
          case NOT_SATISFIED:
            shouldStop = !partialDirtyResource.getResources().isEmpty();
            break;
          default:
            // do nothing
        }

        if (shouldStop) {
          partialDirtyResourceList.add(partialDirtyResource);
          partialDirtyResource = new PartialDirtyResource();
          if (!heavySelect) {
            // Non-heavy selection is triggered more frequently. In order to avoid selecting too
            // many files containing mods for compaction when the disk is insufficient, the number
            // and size of files are limited here.
            break;
          }
        }
      }
      partialDirtyResourceList.add(partialDirtyResource);
      return creatTask(allDirtyResource, partialDirtyResourceList);
    } catch (Exception e) {
      LOGGER.error(
          "{}-{} cannot select file for settle compaction", storageGroupName, dataRegionId, e);
    }
    return Collections.emptyList();
  }

  private DirtyStatus selectFileBaseOnModSize(TsFileResource resource) {
    ModificationFile modFile = resource.getModFile();
    if (modFile == null || !modFile.exists()) {
      return DirtyStatus.NOT_SATISFIED;
    }
    return modFile.getSize() > config.getInnerCompactionTaskSelectionModsFileThreshold()
            || (!heavySelect
                && !CompactionUtils.isDiskHasSpace(
                    config.getInnerCompactionTaskSelectionDiskRedundancy()))
        ? PARTIAL_DELETED
        : DirtyStatus.NOT_SATISFIED;
  }

  /**
   * Only when all devices with ttl are deleted may they be selected. On the basic of the previous,
   * only when the number of deleted devices exceeds the threshold or has expired for too long will
   * they be selected.
   *
   * @return dirty status means the status of current resource.
   */
  private DirtyStatus selectFileBaseOnDirtyData(TsFileResource resource)
      throws IOException, IllegalPathException {
    ModificationFile modFile = resource.getModFile();
    DeviceTimeIndex deviceTimeIndex =
        resource.getTimeIndexType() == ITimeIndex.FILE_TIME_INDEX_TYPE
            ? resource.buildDeviceTimeIndex()
            : (DeviceTimeIndex) resource.getTimeIndex();
    Set<IDeviceID> deletedDevices = new HashSet<>();
    boolean hasExpiredTooLong = false;
    long currentTime = CommonDateTimeUtils.currentTime();

    Collection<Modification> modifications = modFile.getModifications();
    for (IDeviceID device : deviceTimeIndex.getDevices()) {
      // check expired device by ttl
      long deviceTTL = DataNodeTTLCache.getInstance().getTTL(((PlainDeviceID) device).toStringID());
      boolean hasSetTTL = deviceTTL != Long.MAX_VALUE;
      boolean isDeleted =
          !deviceTimeIndex.isDeviceAlive(device, deviceTTL)
              || isDeviceDeletedByMods(
                  modifications,
                  device,
                  deviceTimeIndex.getStartTime(device),
                  deviceTimeIndex.getEndTime(device));

      if (hasSetTTL) {
        if (!isDeleted) {
          // For devices with TTL set, all data must expire in order to meet the conditions for
          // being selected.
          return DirtyStatus.NOT_SATISFIED;
        }
        long outdatedTimeDiff = currentTime - deviceTimeIndex.getEndTime(device);
        hasExpiredTooLong =
            hasExpiredTooLong
                || outdatedTimeDiff > Math.min(config.getMaxExpiredTime(), 3 * deviceTTL);
      }

      if (isDeleted) {
        deletedDevices.add(device);
      }
    }

    float deletedDeviceRate = (float) (deletedDevices.size()) / deviceTimeIndex.getDevices().size();
    if (deletedDeviceRate == 1f) {
      // the whole file is completely dirty
      return DirtyStatus.ALL_DELETED;
    }
    hasExpiredTooLong = config.getMaxExpiredTime() != Long.MAX_VALUE && hasExpiredTooLong;
    if (hasExpiredTooLong || deletedDeviceRate >= config.getExpiredDataRate()) {
      // evaluate dirty data size in the tsfile
      DirtyStatus partialDeleted = DirtyStatus.PARTIAL_DELETED;
      partialDeleted.setDirtyDataSize((long) (deletedDeviceRate * resource.getTsFileSize()));
      return partialDeleted;
    }
    return DirtyStatus.NOT_SATISFIED;
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

  private List<SettleCompactionTask> creatTask(
      AllDirtyResource allDirtyResource, List<PartialDirtyResource> partialDirtyResourceList) {
    List<SettleCompactionTask> tasks = new ArrayList<>();
    for (int i = 0; i < partialDirtyResourceList.size(); i++) {
      if (i == 0) {
        if (allDirtyResource.getResources().isEmpty()
            && partialDirtyResourceList.get(i).getResources().isEmpty()) {
          continue;
        }
        tasks.add(
            new SettleCompactionTask(
                timePartition,
                tsFileManager,
                allDirtyResource.getResources(),
                partialDirtyResourceList.get(i).getResources(),
                isSeq,
                createCompactionPerformer(),
                tsFileManager.getNextCompactionTaskId()));
      } else {
        if (partialDirtyResourceList.get(i).getResources().isEmpty()) {
          continue;
        }
        tasks.add(
            new SettleCompactionTask(
                timePartition,
                tsFileManager,
                Collections.emptyList(),
                partialDirtyResourceList.get(i).getResources(),
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
    ALL_DELETED, // the whole file is deleted
    PARTIAL_DELETED, // the file is partial deleted
    NOT_SATISFIED; // do not satisfy settle condition, which does not mean there is no dirty data

    private long dirtyDataSize = 0;

    public void setDirtyDataSize(long dirtyDataSize) {
      this.dirtyDataSize = dirtyDataSize;
    }

    public long getDirtyDataSize() {
      return dirtyDataSize;
    }
  }
}
