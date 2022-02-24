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
package org.apache.iotdb.db.engine.compaction.inner.sizetiered;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.CompactionTaskManager;
import org.apache.iotdb.db.engine.compaction.inner.AbstractInnerSpaceCompactionSelector;
import org.apache.iotdb.db.engine.compaction.inner.InnerSpaceCompactionTaskFactory;
import org.apache.iotdb.db.engine.compaction.task.AbstractCompactionTask;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;

/**
 * SizeTierCompactionSelector selects files to be compacted based on the size of files. The selector
 * traverses the file list from old to new. If the size of selected files or the number of select
 * files exceed given threshold, a compaction task will be submitted to task queue in
 * CompactionTaskManager. In CompactionTaskManager, tasks are ordered by {@link
 * org.apache.iotdb.db.engine.compaction.CompactionTaskComparator}. To maximize compaction
 * efficiency, selector searches compaction task from 0 compaction files(that is, file that never
 * been compacted, named level 0 file) to higher level files. If a compaction task is found in some
 * level, selector will not search higher level anymore.
 */
public class SizeTieredCompactionSelector extends AbstractInnerSpaceCompactionSelector {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);
  private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  public SizeTieredCompactionSelector(
      String logicalStorageGroupName,
      String virtualStorageGroupName,
      long timePartition,
      TsFileManager tsFileManager,
      boolean sequence,
      InnerSpaceCompactionTaskFactory taskFactory) {
    super(
        logicalStorageGroupName,
        virtualStorageGroupName,
        timePartition,
        tsFileManager,
        sequence,
        taskFactory);
  }

  /**
   * This method searches for a batch of files to be compacted from layer 0 to the highest layer. If
   * there are more than a batch of files to be merged on a certain layer, it does not search to
   * higher layers. It creates a compaction thread for each batch of files and put it into the
   * candidateCompactionTaskQueue of the {@link CompactionTaskManager}.
   *
   * @return Returns whether the file was found and submits the merge task
   */
  @Override
  public void selectAndSubmit() {
    PriorityQueue<Pair<List<TsFileResource>, Long>> taskPriorityQueue =
        new PriorityQueue<>(new SizeTierCompactionTaskComparator());
    try {
      selectSandwichTask(taskPriorityQueue);
      if (taskPriorityQueue.size() == 0) {
        selectTierTask(taskPriorityQueue);
      }
      while (taskPriorityQueue.size() > 0) {
        createAndSubmitTask(taskPriorityQueue.poll().left);
      }
    } catch (Exception e) {
      LOGGER.error("Exception occurs while selecting files", e);
    }
  }

  private void selectTierTask(PriorityQueue<Pair<List<TsFileResource>, Long>> taskPriorityQueue)
      throws IOException {
    int maxTier = searchMaxFileLevel();
    for (int currentTier = 0; currentTier <= maxTier; currentTier++) {
      if (!selectTierTask(currentTier, taskPriorityQueue)) {
        break;
      }
    }
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
   * @param taskPriorityQueue it stores the batches of files to be compacted and the total size of
   *     each batch
   * @return return whether to continue the search to higher levels
   * @throws IOException
   */
  private boolean selectTierTask(
      int level, PriorityQueue<Pair<List<TsFileResource>, Long>> taskPriorityQueue)
      throws IOException {
    boolean shouldContinueToSearch = true;
    List<TsFileResource> selectedFileList = new ArrayList<>();
    long selectedFileSize = 0L;
    long targetCompactionFileSize = config.getTargetCompactionFileSize();

    for (TsFileResource currentFile : tsFileResources) {
      if (getTierOfResource(currentFile) != level || currentFile.isCompactionCandidate()) {
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
          || selectedFileList.size() >= config.getMaxCompactionCandidateFileNum()) {
        // submit the task
        if (selectedFileList.size() > 1) {
          taskPriorityQueue.add(new Pair<>(new ArrayList<>(selectedFileList), selectedFileSize));
        }
        selectedFileList = new ArrayList<>();
        selectedFileSize = 0L;
        shouldContinueToSearch = false;
      }
    }
    return shouldContinueToSearch;
  }

  private int searchMaxFileLevel() throws IOException {
    int maxLevel = -1;
    Iterator<TsFileResource> iterator = tsFileResources.iterator();
    while (iterator.hasNext()) {
      TsFileResource currentFile = iterator.next();
      TsFileNameGenerator.TsFileName currentName =
          TsFileNameGenerator.getTsFileName(currentFile.getTsFile().getName());
      if (currentName.getInnerCompactionCnt() > maxLevel) {
        maxLevel = currentName.getInnerCompactionCnt();
      }
    }
    return maxLevel;
  }

  /**
   * After selecting tier task, there could be some files of low tier which are surrounded by some
   * higher, and those files in low tier cannot meet the terms of choice in tier selection, and we
   * call those files a sandwich structure.
   *
   * <p>|--1--| |--0--| |--1--| |--0--| |--1--| |--0--| |--1--|
   *
   * <p>Like above, the files in 0-tier cannot be selected by tier selector. So this function is
   * used to select the files with sandwich structure.
   *
   * @param taskPriorityQueue
   */
  private void selectSandwichTask(PriorityQueue<Pair<List<TsFileResource>, Long>> taskPriorityQueue)
      throws IOException {
    int currentTier = -1;
    List<TsFileResource> selectedTsFileResources = new ArrayList<>();
    long selectedTsFileSize = 0L;
    LinkedList<List<TsFileResource>> tempTaskList = new LinkedList<>();
    List<List<TsFileResource>> sandwichTaskList = new ArrayList<>();
    boolean isSandwich = false;
    long minTier = searchMinFileTier();
    for (int i = 0; i < tsFileResources.size(); ++i) {
      TsFileResource tsFileResource = tsFileResources.get(i);
      int tierOfCurrentFile = getTierOfResource(tsFileResource);
      if (currentTier == -1) {
        selectedTsFileResources.add(tsFileResource);
        selectedTsFileSize += tsFileResource.getTsFileSize();
      } else if (tierOfCurrentFile > currentTier && selectedTsFileResources.size() > 0) {
        if (isSandwich) {
          sandwichTaskList.add(new ArrayList<>(selectedTsFileResources));
          isSandwich = false;
          selectedTsFileResources.clear();
          selectedTsFileSize = 0;
          selectedTsFileResources.add(tsFileResource);
        } else if (currentTier != minTier) {
          selectedTsFileResources.clear();
          selectedTsFileSize = 0L;
          selectedTsFileResources.add(tsFileResource);
          selectedTsFileSize += tsFileResource.getTsFileSize();
        } else {
          if (tempTaskList.size() == 0) {
            if (selectedTsFileResources.size() == 1) {
              // if only one file is selected, selected the next high tier
              // and compact them together
              selectedTsFileResources.add(tsFileResource);
              selectedTsFileSize += tsFileResource.getTsFileSize();
              isSandwich = true;
            } else {
              // if there are more than one files, compact these files
              sandwichTaskList.add(new ArrayList<>(selectedTsFileResources));
              selectedTsFileSize = 0L;
              selectedTsFileResources.clear();
            }
          } else {
            List<TsFileResource> lastTask = tempTaskList.getLast();
            TsFileResource lastResourceInLastTask = lastTask.get(lastTask.size() - 1);
            if (getTierOfResource(lastResourceInLastTask) == tierOfCurrentFile
                && tsFileResources.indexOf(lastResourceInLastTask)
                    == tsFileResources.indexOf(selectedTsFileResources.get(0))) {
              lastTask.addAll(selectedTsFileResources);
              sandwichTaskList.add(lastTask);
              selectedTsFileResources.clear();
              selectedTsFileSize = 0;
            } else {
              selectedTsFileResources.add(tsFileResource);
              selectedTsFileSize += tsFileResource.getTsFileSize();
              isSandwich = true;
            }
          }
        }
      } else if (tierOfCurrentFile == currentTier) {
        selectedTsFileResources.add(tsFileResource);
        selectedTsFileSize += tsFileResource.getTsFileSize();
      } else {
        // meet a file in lower tier
        selectedTsFileSize = 0L;
        selectedTsFileResources.clear();
        selectedTsFileResources.add(tsFileResource);
        selectedTsFileSize += tsFileResource.getTsFileSize();
      }
      currentTier = tierOfCurrentFile;
      if ((selectedTsFileResources.size() >= config.getMaxCompactionCandidateFileNum()
          || selectedTsFileSize >= config.getTargetCompactionFileSize())) {
        if (isSandwich) {
          sandwichTaskList.add(new ArrayList<>(selectedTsFileResources));
        } else {
          tempTaskList.add(new ArrayList<>(selectedTsFileResources));
        }
        isSandwich = false;
        selectedTsFileResources.clear();
        selectedTsFileSize = 0;
      }
    }

    for (List<TsFileResource> sandwichTask : sandwichTaskList) {
      long totalSize = 0L;
      for (TsFileResource resource : sandwichTask) {
        totalSize += resource.getTsFileSize();
      }
      taskPriorityQueue.add(new Pair<>(sandwichTask, totalSize));
    }
  }

  private int searchMinFileTier() throws IOException {
    int minTier = Integer.MAX_VALUE;
    Iterator<TsFileResource> iterator = tsFileResources.iterator();
    while (iterator.hasNext()) {
      TsFileResource currentFile = iterator.next();
      TsFileNameGenerator.TsFileName currentName =
          TsFileNameGenerator.getTsFileName(currentFile.getTsFile().getName());
      if (currentName.getInnerCompactionCnt() < minTier) {
        minTier = currentName.getInnerCompactionCnt();
      }
    }
    return minTier;
  }

  private int getTierOfResource(TsFileResource resource) throws IOException {
    TsFileNameGenerator.TsFileName currentName =
        TsFileNameGenerator.getTsFileName(resource.getTsFile().getName());
    return currentName.getInnerCompactionCnt();
  }

  private boolean createAndSubmitTask(List<TsFileResource> selectedFileList)
      throws InterruptedException {
    selectedFileList.forEach(x -> x.setCompactionCandidate(true));
    AbstractCompactionTask compactionTask =
        taskFactory.createTask(
            logicalStorageGroupName,
            virtualStorageGroupName,
            timePartition,
            tsFileManager,
            selectedFileList,
            sequence);
    return CompactionTaskManager.getInstance().addTaskToWaitingQueue(compactionTask);
  }

  private class SizeTierCompactionTaskComparator
      implements Comparator<Pair<List<TsFileResource>, Long>> {

    @Override
    public int compare(Pair<List<TsFileResource>, Long> o1, Pair<List<TsFileResource>, Long> o2) {
      TsFileResource resourceOfO1 = o1.left.get(0);
      TsFileResource resourceOfO2 = o2.left.get(0);
      try {
        TsFileNameGenerator.TsFileName fileNameOfO1 =
            TsFileNameGenerator.getTsFileName(resourceOfO1.getTsFile().getName());
        TsFileNameGenerator.TsFileName fileNameOfO2 =
            TsFileNameGenerator.getTsFileName(resourceOfO2.getTsFile().getName());
        if (fileNameOfO1.getInnerCompactionCnt() != fileNameOfO2.getInnerCompactionCnt()) {
          return fileNameOfO2.getInnerCompactionCnt() - fileNameOfO1.getInnerCompactionCnt();
        }
        return (int) (fileNameOfO2.getVersion() - fileNameOfO1.getVersion());
      } catch (IOException e) {
        return 0;
      }
    }
  }
}
