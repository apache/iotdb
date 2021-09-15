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
package org.apache.iotdb.db.engine.compaction.inner.sizetired;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.CompactionTaskManager;
import org.apache.iotdb.db.engine.compaction.inner.AbstractInnerSpaceCompactionSelector;
import org.apache.iotdb.db.engine.compaction.inner.InnerSpaceCompactionTaskFactory;
import org.apache.iotdb.db.engine.compaction.task.AbstractCompactionTask;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceManager;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * threshold is not reached, a compaction task will also be submitted.
 */
public class SizeTiredCompactionSelector extends AbstractInnerSpaceCompactionSelector {
  private static final Logger LOGGER = LoggerFactory.getLogger(SizeTiredCompactionSelector.class);
  private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  public SizeTiredCompactionSelector(
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
    PriorityQueue<Pair<List<TsFileResource>, Long>> notFullCompactionQueue =
        new PriorityQueue<>(10, new SizeTiredCompactionTaskComparator());

    // this iterator traverses the list in reverse order
    tsFileResources.readLock();
    LOGGER.info(
        "{} [Compaction] SizeTiredCompactionSelector start to select, target file size is {}, "
            + "target file num is {}, current task num is {}, total task num is {}",
        logicalStorageGroupName + "-" + virtualStorageGroupName,
        IoTDBDescriptor.getInstance().getConfig().getTargetCompactionFileSize(),
        IoTDBDescriptor.getInstance().getConfig().getMaxCompactionCandidateFileNum(),
        CompactionTaskManager.currentTaskNum.get(),
        IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread());
    int submitTaskNum = 0;
    try {
      // traverse the tsfile from new to old
      Iterator<TsFileResource> iterator = tsFileResources.reverseIterator();
      LOGGER.debug("Current file list is {}", tsFileResources.getArrayList());
      while (iterator.hasNext()) {
        TsFileResource currentFile = iterator.next();
        LOGGER.debug("Current File is {}, size is {}", currentFile, currentFile.getTsFileSize());
        // if no available thread for new compaction task
        // or compaction of current type is disable
        // just return
        if ((CompactionTaskManager.currentTaskNum.get() >= concurrentCompactionThread)
            || (!enableSeqSpaceCompaction && sequence)
            || (!enableUnseqSpaceCompaction && !sequence)) {
          if (CompactionTaskManager.currentTaskNum.get() >= concurrentCompactionThread) {
            LOGGER.info(
                "{} [Compaction] Return selection because too many compaction thread, "
                    + "current thread num is {}, total task num is {}",
                logicalStorageGroupName + "-" + virtualStorageGroupName,
                CompactionTaskManager.currentTaskNum,
                CompactionTaskManager.getInstance().getTaskCount());
          } else {
            LOGGER.info(
                "{} [Compaction] Return selection because compaction is not enable",
                logicalStorageGroupName + "-" + virtualStorageGroupName);
          }
          LOGGER.info(
              "{} [Compaction] SizeTiredCompactionSelector submit {} tasks",
              logicalStorageGroupName + "-" + virtualStorageGroupName,
              submitTaskNum);
          return taskSubmitted;
        }
        // the file size reach threshold
        // or meet an unelectable file
        if (currentFile.getTsFileSize() >= targetCompactionFileSize
            || currentFile.isMerging()
            || !currentFile.isClosed()) {
          if (currentFile.getTsFileSize() >= targetCompactionFileSize) {
            LOGGER.debug("Selected file list is clear because current file is too large");
          } else {
            LOGGER.debug(
                "Selected file list is clear because current file is {}",
                currentFile.isMerging() ? "merging" : "not closed");
          }
          if (selectedFileList.size() > 1) {
            notFullCompactionQueue.add(
                new Pair<>(new ArrayList<>(selectedFileList), selectedFileSize));
          }
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
          LOGGER.info(
              "Submit a {} inner space compaction task because {}",
              sequence ? "sequence" : "unsequence",
              selectedFileSize > targetCompactionFileSize ? "file size enough" : "file num enough");
          // submit the task
          createAndSubmitTask(selectedFileList);
          taskSubmitted = true;
          submitTaskNum += 1;
          selectedFileList = new ArrayList<>();
          selectedFileSize = 0L;
        }
      }
      if (selectedFileList.size() > 1) {
        notFullCompactionQueue.add(new Pair<>(new ArrayList<>(selectedFileList), selectedFileSize));
      }
      if (config.isEnableNotFullCompaction()) {
        while (CompactionTaskManager.currentTaskNum.get()
                < (int) (config.getConcurrentCompactionThread() * 0.3)
            && notFullCompactionQueue.size() > 0) {
          createAndSubmitTask(notFullCompactionQueue.poll().left);
          submitTaskNum++;
        }
      }
      LOGGER.info(
          "{} [Compaction] SizeTiredCompactionSelector submit {} tasks",
          logicalStorageGroupName + "-" + virtualStorageGroupName,
          submitTaskNum);
      return taskSubmitted;
    } finally {
      tsFileResources.readUnlock();
    }
  }

  private void createAndSubmitTask(List<TsFileResource> selectedFileList) {
    AbstractCompactionTask compactionTask =
        taskFactory.createTask(
            logicalStorageGroupName,
            virtualStorageGroupName,
            timePartition,
            tsFileResourceManager,
            tsFileResources,
            selectedFileList,
            sequence);
    for (TsFileResource resource : selectedFileList) {
      resource.setMerging(true);
      LOGGER.info(
          "{}-{} [Compaction] start to compact TsFile {}",
          logicalStorageGroupName,
          virtualStorageGroupName,
          resource);
    }
    CompactionTaskManager.getInstance()
        .submitTask(
            logicalStorageGroupName + "-" + virtualStorageGroupName, timePartition, compactionTask);
    LOGGER.info(
        "{}-{} [Compaction] submit a inner compaction task of {} files",
        logicalStorageGroupName,
        virtualStorageGroupName,
        selectedFileList.size());
  }

  private class SizeTiredCompactionTaskComparator
      implements Comparator<Pair<List<TsFileResource>, Long>> {

    @Override
    public int compare(Pair<List<TsFileResource>, Long> o1, Pair<List<TsFileResource>, Long> o2) {
      if (o1.left.size() != o2.left.size()) {
        return o1.left.size() - o2.left.size();
      } else {
        return ((int) (o1.right - o2.right));
      }
    }
  }
}
