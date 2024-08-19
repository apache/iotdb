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
  private final long totalFileNumThreshold;
  private final int totalFileNumLowerBound;
  private final long singleFileSizeThreshold;
  private final int maxLevelGap;
  private final CompactionScheduleContext context;
  private boolean isActiveTimePartition;

  public NewSizeTieredCompactionSelector(
      String storageGroupName,
      String dataRegionId,
      long timePartition,
      boolean sequence,
      TsFileManager tsFileManager,
      CompactionScheduleContext context) {
    super(storageGroupName, dataRegionId, timePartition, sequence, tsFileManager);
    double availableDisk =
        MetricService.getInstance()
            .getAutoGauge(
                SystemMetric.SYS_DISK_AVAILABLE_SPACE.toString(),
                MetricLevel.CORE,
                Tag.NAME.toString(),
                "system")
            .getValue();
    long maxDiskSizeForTempFiles = (long) availableDisk / config.getCompactionThreadCount();
    maxDiskSizeForTempFiles =
        maxDiskSizeForTempFiles == 0 ? Long.MAX_VALUE : maxDiskSizeForTempFiles;
    this.maxLevelGap = config.getMaxLevelGapInInnerCompaction();
    this.totalFileNumThreshold = config.getInnerCompactionTotalFileNumThreshold();
    this.totalFileNumLowerBound = config.getInnerCompactionCandidateFileNum();
    this.totalFileSizeThreshold =
        Math.min(config.getInnerCompactionTotalFileSizeThreshold(), maxDiskSizeForTempFiles);
    this.singleFileSizeThreshold =
        Math.min(config.getTargetCompactionFileSize(), maxDiskSizeForTempFiles);
    this.context = context;
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
    InnerSpaceCompactionTaskSelection levelTaskSelection = new InnerSpaceCompactionTaskSelection();
    int startSelectIndex = 0;
    while (startSelectIndex < tsFileResourceCandidateList.size()) {
      for (int i = startSelectIndex; i < tsFileResourceCandidateList.size(); i++) {
        TsFileResourceCandidate currentFile = tsFileResourceCandidateList.get(i);
        long innerCompactionCount = currentFile.resource.getTsFileID().getInnerCompactionCount();

        if (levelTaskSelection.isCurrentTaskEmpty() && innerCompactionCount != level) {
          continue;
        }

        if (!currentFile.isValidCandidate || Math.abs(innerCompactionCount - level) > maxLevelGap) {
          levelTaskSelection.endCurrentTaskSelection();
          break;
        }

        boolean skipCurrentFile = !levelTaskSelection.haveOverlappedDevices(currentFile);
        if (skipCurrentFile) {
          levelTaskSelection.addSkippedResource(currentFile, i);
          continue;
        }

        if (!levelTaskSelection.currentFileSatisfied(currentFile)) {
          levelTaskSelection.endCurrentTaskSelection();
          break;
        }

        if (levelTaskSelection.isTaskTooLarge(currentFile)) {
          levelTaskSelection.endCurrentTaskSelection();
          break;
        }
        levelTaskSelection.addSelectedResource(currentFile, i);
      }
      levelTaskSelection.endCurrentTaskSelection();
      startSelectIndex = levelTaskSelection.getNextTaskStartIndex();
    }
    return levelTaskSelection.getSelectedTaskList();
  }

  private class InnerSpaceCompactionTaskSelection {
    List<InnerSpaceCompactionTask> selectedTaskList = new ArrayList<>();

    List<TsFileResource> currentSelectedResources = new ArrayList<>();
    List<TsFileResource> currentSkippedResources = new ArrayList<>();
    List<TsFileResource> lastContinuousSkippedResources = new ArrayList<>();
    HashSet<IDeviceID> currentSelectedDevices = new HashSet<>();
    long currentSelectedFileTotalSize = 0;
    long currentSkippedFileTotalSize = 0;

    int lastSelectedFileIndex = -1;
    int nextTaskStartIndex = -1;

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

    private boolean currentFileSatisfied(TsFileResourceCandidate currentFile) {
      return currentFile.resource.getTsFileSize() < totalFileSizeThreshold;
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
          || currentSelectedResources.size() + 1 > totalFileNumThreshold;
    }

    private void endCurrentTaskSelection() {
      try {
        long totalFileSize = currentSelectedFileTotalSize + currentSkippedFileTotalSize;
        nextTaskStartIndex = lastSelectedFileIndex + 1;
        for (TsFileResource resource : lastContinuousSkippedResources) {
          long currentFileSize = resource.getTsFileSize();
          if (totalFileSize + currentFileSize > singleFileSizeThreshold) {
            break;
          }
          currentSkippedResources.add(resource);
          totalFileSize += currentFileSize;
          currentSkippedFileTotalSize += currentFileSize;
          nextTaskStartIndex++;
        }

        int totalFileNum = currentSelectedResources.size() + currentSkippedResources.size();
        if (totalFileNum < 2) {
          return;
        }

        boolean canCompactAllFiles = totalFileSize <= singleFileSizeThreshold;
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
