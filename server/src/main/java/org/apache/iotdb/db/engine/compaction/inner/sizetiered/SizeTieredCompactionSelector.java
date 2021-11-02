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
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.CompactionTaskManager;
import org.apache.iotdb.db.engine.compaction.inner.AbstractInnerSpaceCompactionSelector;
import org.apache.iotdb.db.engine.compaction.inner.InnerSpaceCompactionTaskFactory;
import org.apache.iotdb.db.engine.compaction.task.AbstractCompactionTask;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * SizeTieredCompactionSelector selects files to be compacted based on the size of files. The
 * selector traverses the file list from old to new. If the size of selected files or the number of
 * select files exceed given threshold, a compaction task will be submitted to task queue in
 * CompactionTaskManager. In CompactionTaskManager, tasks are ordered by {@link
 * org.apache.iotdb.db.engine.compaction.CompactionTaskComparator}. To maximize compaction
 * efficiency, selector searches compaction task from 0 compaction files(that is, file that never
 * been compacted, named level 0 file) to higher level files. If a compaction task is found in some
 * level, selector will not search higher level anymore.
 */
public class SizeTieredCompactionSelector extends AbstractInnerSpaceCompactionSelector {
  private static final Logger LOGGER = LoggerFactory.getLogger("COMPACTION");
  private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  public SizeTieredCompactionSelector(
      String logicalStorageGroupName,
      String virtualStorageGroupName,
      long timePartition,
      TsFileManager tsFileManager,
      TsFileResourceList tsFileResources,
      boolean sequence,
      InnerSpaceCompactionTaskFactory taskFactory) {
    super(
        logicalStorageGroupName,
        virtualStorageGroupName,
        timePartition,
        tsFileManager,
        tsFileResources,
        sequence,
        taskFactory);
  }

  @Override
  public boolean selectAndSubmit() {
    LOGGER.debug(
        "{} [Compaction] SizeTiredCompactionSelector start to select, target file size is {}, "
            + "target file num is {}, current task num is {}, total task num is {}, "
            + "max task num is {}",
        logicalStorageGroupName + "-" + virtualStorageGroupName,
        IoTDBDescriptor.getInstance().getConfig().getTargetCompactionFileSize(),
        IoTDBDescriptor.getInstance().getConfig().getMaxCompactionCandidateFileNum(),
        CompactionTaskManager.currentTaskNum.get(),
        CompactionTaskManager.getInstance().getTaskCount(),
        IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread());
    tsFileResources.readLock();
    PriorityQueue<Pair<List<TsFileResource>, Long>> taskPriorityQueue =
        new PriorityQueue<>(new SizeTieredCompactionTaskComparator());
    try {
      int maxLevel = searchMaxFileLevel();
      for (int currentLevel = 0; currentLevel <= maxLevel; currentLevel++) {
        if (!selectLevelTask(currentLevel, taskPriorityQueue)) {
          break;
        }
      }
      while (taskPriorityQueue.size() > 0) {
        createAndSubmitTask(taskPriorityQueue.poll().left);
      }
    } catch (Exception e) {
      LOGGER.error("Exception occurs while selecting files", e);
    } finally {
      tsFileResources.readUnlock();
    }
    return true;
  }

  private boolean selectLevelTask(
      int level, PriorityQueue<Pair<List<TsFileResource>, Long>> taskPriorityQueue)
      throws IOException {
    boolean shouldContinueToSearch = true;
    List<TsFileResource> selectedFileList = new ArrayList<>();
    long selectedFileSize = 0L;
    long targetCompactionFileSize = config.getTargetCompactionFileSize();

    // this iterator traverses the list in reverse order
    for (TsFileResource currentFile : tsFileResources) {
      TsFileNameGenerator.TsFileName currentName =
          TsFileNameGenerator.getTsFileName(currentFile.getTsFile().getName());
      if (currentName.getInnerCompactionCnt() != level) {
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

  private boolean createAndSubmitTask(List<TsFileResource> selectedFileList) {
    AbstractCompactionTask compactionTask =
        taskFactory.createTask(
            logicalStorageGroupName,
            virtualStorageGroupName,
            timePartition,
            tsFileManager,
            tsFileResources,
            selectedFileList,
            sequence);
    return CompactionTaskManager.getInstance().addTaskToWaitingQueue(compactionTask);
  }

  private class SizeTieredCompactionTaskComparator
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
      } catch (IOException e) {
        return 0;
      }
      if (o1.left.size() != o2.left.size()) {
        return o1.left.size() - o2.left.size();
      } else {
        return ((int) (o2.right - o1.right));
      }
    }
  }
}
