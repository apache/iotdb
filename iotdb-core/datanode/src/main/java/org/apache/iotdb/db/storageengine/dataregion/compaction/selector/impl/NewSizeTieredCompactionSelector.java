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

import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.InnerSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionScheduleContext;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.utils.TsFileResourceCandidate;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.SystemMetric;

import org.apache.tsfile.file.metadata.IDeviceID;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class NewSizeTieredCompactionSelector extends SizeTieredCompactionSelector {

  private List<TsFileResourceCandidate> tsFileResourceCandidateList = new ArrayList<>();
  private final long totalFileSizeThreshold;
  // the total file num in one task can not exceed this value
  private final long totalFileNumUpperBound;
  // When the number of selected files exceeds this value, the conditions for constructing a
  // compaction task are met.
  private final int totalFileNumLowerBound;
  private final long singleFileSizeThreshold;
  private final int maxLevelGap;
  private boolean isActiveTimePartition;

  public NewSizeTieredCompactionSelector(
      String storageGroupName,
      String dataRegionId,
      long timePartition,
      boolean sequence,
      TsFileManager tsFileManager,
      CompactionScheduleContext context) {
    super(storageGroupName, dataRegionId, timePartition, sequence, tsFileManager, context);
    double availableDiskSpaceInByte =
        MetricService.getInstance()
            .getAutoGauge(
                SystemMetric.SYS_DISK_AVAILABLE_SPACE.toString(),
                MetricLevel.CORE,
                Tag.NAME.toString(),
                "system")
            .getValue();
    long maxDiskSizeForTempFiles =
        (long) availableDiskSpaceInByte / config.getCompactionThreadCount();
    maxDiskSizeForTempFiles =
        maxDiskSizeForTempFiles == 0 ? Long.MAX_VALUE : maxDiskSizeForTempFiles;
    this.maxLevelGap = config.getMaxLevelGapInInnerCompaction();
    this.totalFileNumUpperBound = config.getInnerCompactionTotalFileNumThreshold();
    this.totalFileNumLowerBound = config.getInnerCompactionCandidateFileNum();
    this.totalFileSizeThreshold =
        Math.min(config.getInnerCompactionTotalFileSizeThresholdInByte(), maxDiskSizeForTempFiles);
    this.singleFileSizeThreshold =
        Math.min(config.getTargetCompactionFileSize(), maxDiskSizeForTempFiles);
  }

  @Override
  public List<InnerSpaceCompactionTask> selectInnerSpaceTask(List<TsFileResource> tsFileResources) {
    if (tsFileResources.isEmpty()) {
      return Collections.emptyList();
    }
    this.isActiveTimePartition = checkIsActiveTimePartition(tsFileResources);
    this.tsFileResourceCandidateList =
        tsFileResources.stream()
            .map(resource -> new TsFileResourceCandidate(resource, context))
            .collect(Collectors.toList());
    return super.selectInnerSpaceTask(tsFileResources);
  }

  private boolean checkIsActiveTimePartition(List<TsFileResource> resources) {
    TsFileResource lastResource = resources.get(resources.size() - 1);
    return (System.currentTimeMillis() - lastResource.getTsFileID().getTimestamp())
        < 2 * config.getCompactionScheduleIntervalInMs();
  }

  @Override
  protected List<InnerSpaceCompactionTask> selectTaskBaseOnLevel() throws IOException {
    int maxLevel = searchMaxFileLevel();
    for (int currentLevel = 0; currentLevel <= maxLevel; currentLevel++) {
      List<InnerSpaceCompactionTask> selectedResourceList = selectTasksByLevel(currentLevel);
      if (!selectedResourceList.isEmpty()) {
        return selectedResourceList;
      }
    }
    return Collections.emptyList();
  }

  @SuppressWarnings("java:S135")
  private List<InnerSpaceCompactionTask> selectTasksByLevel(int level) throws IOException {
    InnerSpaceCompactionTaskSelection levelTaskSelection =
        new InnerSpaceCompactionTaskSelection(level);
    int startSelectIndex = 0;
    while (startSelectIndex < tsFileResourceCandidateList.size()) {
      int idx = 0;
      for (idx = startSelectIndex; idx < tsFileResourceCandidateList.size(); idx++) {
        TsFileResourceCandidate currentFile = tsFileResourceCandidateList.get(idx);
        long innerCompactionCount = currentFile.resource.getTsFileID().getInnerCompactionCount();

        if (levelTaskSelection.isCurrentTaskEmpty() && innerCompactionCount != level) {
          continue;
        }

        if (!currentFile.isValidCandidate) {
          levelTaskSelection.endCurrentTaskSelection();
          break;
        }

        boolean skipCurrentFile = !levelTaskSelection.haveOverlappedDevices(currentFile);
        if (skipCurrentFile) {
          levelTaskSelection.addSkippedResource(currentFile, idx);
          continue;
        }

        if (!levelTaskSelection.currentFileSizeSatisfied(currentFile)
            || !levelTaskSelection.isFileLevelSatisfied(innerCompactionCount)) {
          levelTaskSelection.endCurrentTaskSelection();
          break;
        }

        if (levelTaskSelection.isTaskTooLarge(currentFile)) {
          levelTaskSelection.endCurrentTaskSelection();
          break;
        }
        levelTaskSelection.addSelectedResource(currentFile, idx);
      }
      levelTaskSelection.endCurrentTaskSelection();
      startSelectIndex = Math.min(idx + 1, levelTaskSelection.getNextTaskStartIndex());
    }
    return levelTaskSelection.getSelectedTaskList();
  }

  private class InnerSpaceCompactionTaskSelection {
    List<InnerSpaceCompactionTask> selectedTaskList = new ArrayList<>();

    long level;
    List<TsFileResource> currentSelectedResources = new ArrayList<>();
    List<TsFileResource> currentSkippedResources = new ArrayList<>();
    List<TsFileResource> lastContinuousSkippedResources = new ArrayList<>();
    HashSet<IDeviceID> currentSelectedDevices = new HashSet<>();
    long currentSelectedFileTotalSize = 0;
    long currentSkippedFileTotalSize = 0;

    int lastSelectedFileIndex = -1;
    int nextTaskStartIndex = -1;

    private InnerSpaceCompactionTaskSelection(long level) {
      this.level = level;
    }

    private boolean haveOverlappedDevices(TsFileResourceCandidate resourceCandidate)
        throws IOException {
      return currentSelectedDevices.isEmpty()
          || resourceCandidate.getDevices().stream().anyMatch(currentSelectedDevices::contains);
    }

    private void addSelectedResource(TsFileResourceCandidate currentFile, int idx)
        throws IOException {
      currentSelectedResources.add(currentFile.resource);
      currentSelectedDevices.addAll(currentFile.getDevices());
      currentSelectedFileTotalSize += currentFile.resource.getTsFileSize();
      lastSelectedFileIndex = idx;
      // At this point we can be sure that these skipped files will be selected for the task
      if (!lastContinuousSkippedResources.isEmpty()) {
        currentSkippedResources.addAll(lastContinuousSkippedResources);
        for (TsFileResource resource : lastContinuousSkippedResources) {
          currentSkippedFileTotalSize += resource.getTsFileSize();
        }
        lastContinuousSkippedResources.clear();
      }
    }

    private void addSkippedResource(TsFileResourceCandidate currentFile, int idx) {
      lastContinuousSkippedResources.add(currentFile.resource);
    }

    private boolean currentFileSizeSatisfied(TsFileResourceCandidate currentFile) {
      return currentFile.resource.getTsFileSize() < totalFileSizeThreshold;
    }

    private boolean isFileLevelSatisfied(long innerCompactionCount) {
      return Math.abs(innerCompactionCount - level) <= maxLevelGap;
    }

    private boolean isCurrentTaskEmpty() {
      return currentSelectedResources.isEmpty();
    }

    private void reset() {
      currentSelectedResources = new ArrayList<>();
      currentSkippedResources = new ArrayList<>();
      currentSelectedDevices = new HashSet<>();
      lastContinuousSkippedResources = new ArrayList<>();
      currentSelectedFileTotalSize = 0;
      currentSkippedFileTotalSize = 0;
    }

    private boolean isTaskTooLarge(TsFileResourceCandidate currentFile) {
      return (currentFile.resource.getTsFileSize() + currentSelectedFileTotalSize
              > totalFileSizeThreshold)
          || currentSelectedResources.size() + 1 > totalFileNumUpperBound;
    }

    private void endCurrentTaskSelection() {
      if (isCurrentTaskEmpty()) {
        return;
      }
      try {
        // When the total files size does not exceed the limit of the
        // size of a single file, merge all files together and try to include
        // as many files as possible within the limit.
        long totalFileSize = currentSelectedFileTotalSize + currentSkippedFileTotalSize;
        int totalFileNum = currentSelectedResources.size() + currentSkippedResources.size();
        nextTaskStartIndex = lastSelectedFileIndex + 1;
        for (TsFileResource resource : lastContinuousSkippedResources) {
          long currentFileSize = resource.getTsFileSize();
          if (totalFileSize + currentFileSize > singleFileSizeThreshold
              || totalFileNum > totalFileNumUpperBound
              || !isFileLevelSatisfied(resource.getTsFileID().getInnerCompactionCount())) {
            break;
          }
          currentSkippedResources.add(resource);
          totalFileSize += currentFileSize;
          currentSkippedFileTotalSize += currentFileSize;
          totalFileNum++;
          nextTaskStartIndex++;
        }

        if (totalFileNum < 2) {
          return;
        }

        boolean canCompactAllFiles =
            totalFileSize <= singleFileSizeThreshold && totalFileNum <= totalFileNumUpperBound;
        if (canCompactAllFiles) {
          currentSelectedResources =
              Stream.concat(currentSelectedResources.stream(), currentSkippedResources.stream())
                  .sorted(TsFileResource::compareFileName)
                  .collect(Collectors.toList());
          currentSkippedResources.clear();
          currentSelectedFileTotalSize += currentSkippedFileTotalSize;
          currentSkippedFileTotalSize = 0;
        }

        boolean isSatisfied =
            (currentSelectedResources.size() >= totalFileNumLowerBound
                    || !isActiveTimePartition
                    || currentSelectedFileTotalSize >= singleFileSizeThreshold)
                && currentSelectedResources.size() > 1;
        if (isSatisfied) {
          InnerSpaceCompactionTask task = createInnerSpaceCompactionTask();
          selectedTaskList.add(task);
        }
      } finally {
        reset();
      }
    }

    private int getNextTaskStartIndex() {
      try {
        if (lastSelectedFileIndex == -1) {
          return Integer.MAX_VALUE;
        }
        return nextTaskStartIndex;
      } finally {
        nextTaskStartIndex = -1;
        lastSelectedFileIndex = -1;
      }
    }

    private InnerSpaceCompactionTask createInnerSpaceCompactionTask() {
      return new InnerSpaceCompactionTask(
          timePartition,
          tsFileManager,
          currentSelectedResources,
          currentSkippedResources,
          sequence,
          createCompactionPerformer(),
          tsFileManager.getNextCompactionTaskId());
    }

    private List<InnerSpaceCompactionTask> getSelectedTaskList() {
      return selectedTaskList;
    }
  }
}
