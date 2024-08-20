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
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.ICompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.InnerSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.RepairUnsortedFileCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.comparator.ICompactionTaskComparator;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.IInnerSeqSpaceSelector;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.IInnerUnseqSpaceSelector;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileRepairStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
  protected static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
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
    int fileLimit = config.getInnerCompactionCandidateFileNum();

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
      if (cannotSelectCurrentFileToNormalCompaction(currentFile)) {
        selectedFileList.clear();
        selectedFileSize = 0L;
        continue;
      }

      long totalSizeIfSelectCurrentFile = selectedFileSize + currentFile.getTsFileSize();
      boolean canNotAddCurrentFileIntoCurrentTask =
          totalSizeIfSelectCurrentFile > targetCompactionFileSize
              || selectedFileList.size() >= fileLimit;
      if (canNotAddCurrentFileIntoCurrentTask) {
        // total file size or num will beyond the threshold if select current file, stop the
        // selection of current task
        if (selectedFileList.size() > 1) {
          // submit the task
          taskList.add(new ArrayList<>(selectedFileList));
        }
        // add current file in a new selected file list
        selectedFileList = new ArrayList<>();
        selectedFileList.add(currentFile);
        selectedFileSize = currentFile.getTsFileSize();
      } else {
        LOGGER.debug("Current File is {}, size is {}", currentFile, currentFile.getTsFileSize());
        selectedFileList.add(currentFile);
        selectedFileSize += currentFile.getTsFileSize();
        LOGGER.debug(
            "Add tsfile {}, current select file num is {}, size is {}",
            currentFile,
            selectedFileList.size(),
            selectedFileSize);
      }
    }

    // if the selected file size reach the condition to submit
    if (selectedFileList.size() == fileLimit) {
      taskList.add(new ArrayList<>(selectedFileList));
      selectedFileList.clear();
      selectedFileSize = 0;
    }

    // if next time partition exists
    // submit a merge task even it does not meet the requirement for file num or file size
    if (hasNextTimePartition && selectedFileList.size() > 1) {
      taskList.add(new ArrayList<>(selectedFileList));
    }
    return taskList;
  }

  private boolean cannotSelectCurrentFileToNormalCompaction(TsFileResource resource) {
    return resource.getStatus() != TsFileResourceStatus.NORMAL
        || resource.getTsFileRepairStatus() == TsFileRepairStatus.NEED_TO_REPAIR
        || resource.getTsFileRepairStatus() == TsFileRepairStatus.CAN_NOT_REPAIR;
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
      // 1. select compaction task based on file which need to repair
      List<InnerSpaceCompactionTask> taskList = selectFileNeedToRepair();
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

  protected List<InnerSpaceCompactionTask> selectTaskBaseOnLevel()
      throws IOException, DiskSpaceInsufficientException {
    int maxLevel = searchMaxFileLevel();
    for (int currentLevel = 0; currentLevel <= maxLevel; currentLevel++) {
      List<List<TsFileResource>> selectedResourceList = selectTsFileResourcesByLevel(currentLevel);
      if (!selectedResourceList.isEmpty()) {
        return createCompactionTasks(selectedResourceList);
      }
    }
    return Collections.emptyList();
  }

  private List<InnerSpaceCompactionTask> selectFileNeedToRepair() {
    List<InnerSpaceCompactionTask> taskList = new ArrayList<>();
    for (TsFileResource resource : tsFileResources) {
      if (resource.getStatus() == TsFileResourceStatus.NORMAL
          && resource.getTsFileRepairStatus() == TsFileRepairStatus.NEED_TO_REPAIR) {
        taskList.add(
            new RepairUnsortedFileCompactionTask(
                timePartition,
                tsFileManager,
                resource,
                sequence,
                tsFileManager.getNextCompactionTaskId()));
      }
    }
    return taskList;
  }

  protected ICompactionPerformer createCompactionPerformer() {
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

  protected int searchMaxFileLevel() throws IOException {
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
      List<List<TsFileResource>> selectedTsFileResourceList) {
    List<InnerSpaceCompactionTask> tasks = new ArrayList<>();
    for (List<TsFileResource> tsFileResourceList : selectedTsFileResourceList) {
      tasks.add(createCompactionTask(tsFileResourceList));
    }
    return tasks;
  }

  private InnerSpaceCompactionTask createCompactionTask(List<TsFileResource> fileResources) {
    return new InnerSpaceCompactionTask(
        timePartition,
        tsFileManager,
        fileResources,
        sequence,
        createCompactionPerformer(),
        tsFileManager.getNextCompactionTaskId());
  }
}
