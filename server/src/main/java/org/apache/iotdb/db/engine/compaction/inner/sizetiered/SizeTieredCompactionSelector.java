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
import org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceManager;
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
 * SizeTiredSelector selects files based on the total size or total number of files, and the
 * selected files are always continuous. When the total size of consecutively selected files exceeds
 * the threshold or the number exceeds the threshold, these files are submitted to the inner space
 * compaction task. If a file that is being compacted or not closed is encountered when the
 * threshold is not reached, the selected files will be put into a queue. If the system is idle, the
 * candidate task in queue will be submitted.
 */
public class SizeTieredCompactionSelector extends AbstractInnerSpaceCompactionSelector {
  private static final Logger LOGGER = LoggerFactory.getLogger("COMPACTION");
  private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  public SizeTieredCompactionSelector(
      String logicalStorageGroupName,
      String virtualStorageGroupName,
      long timePartition,
      TsFileResourceManager tsFileResourceManager,
      TsFileResourceList tsFileResources,
      boolean sequence,
      InnerSpaceCompactionTaskFactory taskFactory) {
    super(
        logicalStorageGroupName,
        virtualStorageGroupName,
        timePartition,
        tsFileResourceManager,
        tsFileResources,
        sequence,
        taskFactory);
  }

  @Override
  public boolean selectAndSubmit() {
    boolean taskSubmitted = false;
    List<TsFileResource> selectedFileList = new ArrayList<>();
    long selectedFileSize = 0L;
    long targetCompactionFileSize = config.getTargetCompactionFileSize();
    boolean enableSeqSpaceCompaction = config.isEnableSeqSpaceCompaction();
    boolean enableUnseqSpaceCompaction = config.isEnableUnseqSpaceCompaction();
    int concurrentCompactionThread = config.getConcurrentCompactionThread();
    PriorityQueue<Pair<List<TsFileResource>, Long>> taskPriorityQueue =
        new PriorityQueue<>(10, new SizeTieredCompactionTaskComparator());

    // this iterator traverses the list in reverse order
    tsFileResources.readLock();
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
    int submitTaskNum = 0;
    int currentCompactionCount = 0;
    try {
      // traverse the tsfile from new to old
      Iterator<TsFileResource> iterator = tsFileResources.iterator();
      LOGGER.debug("Current file list is {}", tsFileResources.getArrayList());
      while (iterator.hasNext()) {
        TsFileResource currentFile = iterator.next();
        TsFileNameGenerator.TsFileName currentName =
            TsFileNameGenerator.getTsFileName(currentFile.getTsFile().getName());
        if (currentName.getInnerCompactionCnt() != currentCompactionCount) {
          selectedFileList.clear();
          selectedFileSize = 0L;
          currentCompactionCount = currentName.getInnerCompactionCnt();
        }
        LOGGER.debug("Current File is {}, size is {}", currentFile, currentFile.getTsFileSize());
        // if no available thread for new compaction task
        // or compaction of current type is disable
        // just return
        if ((!enableSeqSpaceCompaction && sequence) || (!enableUnseqSpaceCompaction && !sequence)) {
          LOGGER.debug(
              "{} [Compaction] Return selection because compaction is not enable",
              logicalStorageGroupName + "-" + virtualStorageGroupName);
          return taskSubmitted;
        }
        // the file size reach threshold
        // or meet an unelectable file
        if (currentFile.getTsFileSize() >= targetCompactionFileSize
            || currentFile.isMerging()
            || !currentFile.isClosed()) {
          selectedFileList.clear();
          selectedFileSize = 0L;
          continue;
        }
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
          LOGGER.debug(
              "Submit a {} inner space compaction task because {}",
              sequence ? "sequence" : "unsequence",
              selectedFileSize > targetCompactionFileSize ? "file size enough" : "file num enough");
          // submit the task
          taskPriorityQueue.add(new Pair<>(new ArrayList<>(selectedFileList), selectedFileSize));
          selectedFileList = new ArrayList<>();
          selectedFileSize = 0L;
        }
      }
      while (taskPriorityQueue.size() > 0) {
        if (createAndSubmitTask(taskPriorityQueue.poll().left)) {
          taskSubmitted = true;
          submitTaskNum += 1;
        }
      }
      if (taskSubmitted) {
        LOGGER.info(
            "{} [Compaction] SizeTiredCompactionSelector submit {} tasks",
            logicalStorageGroupName + "-" + virtualStorageGroupName,
            submitTaskNum);
      }
      return taskSubmitted;
    } catch (IOException e) {
      e.printStackTrace();
      return taskSubmitted;
    } finally {
      tsFileResources.readUnlock();
    }
  }

  private boolean createAndSubmitTask(List<TsFileResource> selectedFileList) {
    AbstractCompactionTask compactionTask =
        taskFactory.createTask(
            logicalStorageGroupName,
            virtualStorageGroupName,
            timePartition,
            tsFileResourceManager,
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
