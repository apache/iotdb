/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.ICompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CompactionTaskPriorityType;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.InnerSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.comparator.ICompactionTaskComparator;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.IInnerSeqSpaceSelector;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.IInnerUnseqSpaceSelector;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * SizeTieredCompactionSelector selects files to be compacted based on the size of files. The
 * selector traverses the file list from old to new. If the size of selected files or the number of
 * select files exceed given threshold, a compaction task will be submitted to task queue in
 * CompactionTaskManager. In CompactionTaskManager, tasks are ordered by {@link
 * ICompactionTaskComparator}. To maximize compaction efficiency, selector searches compaction task
 * from 0 compaction files(that is, file that never been compacted, named level 0 file) to higher
 * level files. If a compaction task is found in some level, selector will not search higher level
 * anymore.
 */
public class SizeTieredCompactionSelector
    implements IInnerSeqSpaceSelector, IInnerUnseqSpaceSelector {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  protected String storageGroupName;
  protected String dataRegionId;
  protected long timePartition;
  protected List<TsFileResource> tsFileResources;
  protected boolean sequence;
  protected TsFileManager tsFileManager;
  protected boolean hasNextTimePartition;

  public SizeTieredCompactionSelector(
      String storageGroupName,
      String dataRegionId,
      long timePartition,
      boolean sequence,
      TsFileManager tsFileManager) {
    this.storageGroupName = storageGroupName;
    this.dataRegionId = dataRegionId;
    this.timePartition = timePartition;
    this.sequence = sequence;
    this.tsFileManager = tsFileManager;
    hasNextTimePartition = tsFileManager.hasNextTimePartition(timePartition, sequence);
  }

  /**
   * This method searches for all files on the given level. If there are consecutive files on the
   * level that meet the system preset conditions (the number exceeds 10 or the total file size
   * exceeds 2G), a compaction task is created for the batch of files and placed in the
   * taskPriorityQueue queue , and continue to search for the next batch. If at least one batch of
   * files to be compacted is found on this layer, it will return false (indicating that it will no
   * longer search for higher layers), otherwise it will return true.
   *
   * @param level the level to be searched
   * @return return whether to continue the search to higher levels
   * @throws IOException if the name of tsfile is incorrect
   */
  @SuppressWarnings({"squid:S3776", "squid:S135"})
  private List<List<TsFileResource>> selectTsFileResourcesByLevel(int level) throws IOException {
    List<TsFileResource> selectedFileList = new ArrayList<>();
    long selectedFileSize = 0L;
    long targetCompactionFileSize = config.getTargetCompactionFileSize();

    List<List<TsFileResource>> taskList = new ArrayList<>();
    for (TsFileResource currentFile : tsFileResources) {
      TsFileNameGenerator.TsFileName currentName =
          TsFileNameGenerator.getTsFileName(currentFile.getTsFile().getName());
      if (currentName.getInnerCompactionCnt() != level) {
        // meet files of another level
        if (selectedFileList.size() > 1) {
          taskList.add(new ArrayList<>(selectedFileList));
        }
        selectedFileList = new ArrayList<>();
        selectedFileSize = 0L;
        continue;
      }
      if (currentFile.getStatus() != TsFileResourceStatus.NORMAL) {
        selectedFileList.clear();
        selectedFileSize = 0L;
        continue;
      }
      LOGGER.debug("Current File is {}, size is {}", currentFile, currentFile.getTsFileSize());
      selectedFileList.add(currentFile);
      selectedFileSize += currentFile.getTsFileSize();
      LOGGER.debug(
          "Add tsfile {}, current select file num is {}, size is {}",
          currentFile,
          selectedFileList.size(),
          selectedFileSize);
      // if the file size or file num reach threshold
      if (selectedFileSize >= targetCompactionFileSize
          || selectedFileList.size() >= config.getFileLimitPerInnerTask()) {
        // submit the task
        if (selectedFileList.size() > 1) {
          taskList.add(new ArrayList<>(selectedFileList));
        }
        selectedFileList = new ArrayList<>();
        selectedFileSize = 0L;
      }
    }

    // if next time partition exists
    // submit a merge task even it does not meet the requirement for file num or file size
    if (hasNextTimePartition && selectedFileList.size() > 1) {
      taskList.add(new ArrayList<>(selectedFileList));
    }
    return taskList;
  }

  /**
   * This method is used to select a batch of files to be merged. There are two ways to select
   * files.If the first method selects the appropriate file, the second method is not executed. The
   * first one is based on the mods file corresponding to the file. We will preferentially select
   * file with mods file larger than 50M. The second way is based on the file layer from layer 0 to
   * the highest layer. If there are more than a batch of files to be merged on a certain layer, it
   * does not search to higher layers. It creates a compaction thread for each batch of files and
   * put it into the candidateCompactionTaskQueue of the {@link CompactionTaskManager}.
   *
   * @return Returns whether the file was found and submits the merge task
   */
  @Override
  public List<InnerSpaceCompactionTask> selectInnerSpaceTask(List<TsFileResource> tsFileResources) {
    this.tsFileResources = tsFileResources;
    try {
      // 1. preferentially select compaction task based on mod file
      List<InnerSpaceCompactionTask> taskList = selectTaskBaseOnModFile();
      if (!taskList.isEmpty()) {
        return taskList;
      }
      // 2. if a suitable compaction task is not selected in the first step, select the compaction
      // task at the tsFile level
      return selectTaskBaseOnLevel();
    } catch (Exception e) {
      LOGGER.error("Exception occurs while selecting files", e);
    }
    return Collections.emptyList();
  }

  private List<InnerSpaceCompactionTask> selectTaskBaseOnLevel() throws IOException {
    int maxLevel = searchMaxFileLevel();
    for (int currentLevel = 0; currentLevel <= maxLevel; currentLevel++) {
      List<List<TsFileResource>> selectedResourceList = selectTsFileResourcesByLevel(currentLevel);
      if (!selectedResourceList.isEmpty()) {
        return createCompactionTasks(selectedResourceList, CompactionTaskPriorityType.NORMAL);
      }
    }
    return Collections.emptyList();
  }

  private List<InnerSpaceCompactionTask> selectTaskBaseOnModFile() {
    List<InnerSpaceCompactionTask> taskList = new ArrayList<>();
    for (TsFileResource tsFileResource : tsFileResources) {
      ModificationFile modFile = tsFileResource.getModFile();
      if (Objects.isNull(modFile) || !modFile.exists()) {
        continue;
      }
      if (tsFileResource.getStatus() != TsFileResourceStatus.NORMAL) {
        continue;
      }
      if (modFile.getSize() > config.getInnerCompactionTaskSelectionModsFileThreshold()
          || !CompactionUtils.isDiskHasSpace(
              config.getInnerCompactionTaskSelectionDiskRedundancy())) {
        taskList.add(
            createCompactionTask(
                Collections.singletonList(tsFileResource), CompactionTaskPriorityType.MOD_SETTLE));
        LOGGER.debug("select tsfile {},the mod file size is {}", tsFileResource, modFile.getSize());
      }
    }
    return taskList;
  }

  private ICompactionPerformer createCompactionPerformer() {
    return sequence
        ? IoTDBDescriptor.getInstance()
            .getConfig()
            .getInnerSeqCompactionPerformer()
            .createInstance()
        : IoTDBDescriptor.getInstance()
            .getConfig()
            .getInnerUnseqCompactionPerformer()
            .createInstance();
  }

  private int searchMaxFileLevel() throws IOException {
    int maxLevel = -1;
    for (TsFileResource currentFile : tsFileResources) {
      TsFileNameGenerator.TsFileName currentName =
          TsFileNameGenerator.getTsFileName(currentFile.getTsFile().getName());
      if (currentName.getInnerCompactionCnt() > maxLevel) {
        maxLevel = currentName.getInnerCompactionCnt();
      }
    }
    return maxLevel;
  }

  private List<InnerSpaceCompactionTask> createCompactionTasks(
      List<List<TsFileResource>> selectedTsFileResourceList,
      CompactionTaskPriorityType compactionTaskType) {
    List<InnerSpaceCompactionTask> tasks = new ArrayList<>();
    for (List<TsFileResource> tsFileResourceList : selectedTsFileResourceList) {
      tasks.add(createCompactionTask(tsFileResourceList, compactionTaskType));
    }
    return tasks;
  }

  private InnerSpaceCompactionTask createCompactionTask(
      List<TsFileResource> fileResources, CompactionTaskPriorityType compactionTaskType) {
    return new InnerSpaceCompactionTask(
        timePartition,
        tsFileManager,
        fileResources,
        sequence,
        createCompactionPerformer(),
        tsFileManager.getNextCompactionTaskId(),
        compactionTaskType);
  }
}
