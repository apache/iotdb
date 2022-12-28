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
package org.apache.iotdb.db.engine.compaction.cross.rewrite;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.CompactionTaskManager;
import org.apache.iotdb.db.engine.compaction.cross.AbstractCrossSpaceCompactionSelector;
import org.apache.iotdb.db.engine.compaction.cross.CrossSpaceCompactionTaskFactory;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.manage.CrossSpaceCompactionResource;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.selector.ICrossSpaceMergeFileSelector;
import org.apache.iotdb.db.engine.compaction.inner.utils.InnerSpaceCompactionUtils;
import org.apache.iotdb.db.engine.compaction.task.AbstractCompactionTask;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.rescon.SystemInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class RewriteCrossSpaceCompactionSelector extends AbstractCrossSpaceCompactionSelector {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  public RewriteCrossSpaceCompactionSelector(
      String logicalStorageGroupName,
      String virtualStorageGroupId,
      String storageGroupDir,
      long timePartition,
      TsFileManager tsFileManager,
      CrossSpaceCompactionTaskFactory taskFactory) {
    super(
        logicalStorageGroupName,
        virtualStorageGroupId,
        storageGroupDir,
        timePartition,
        tsFileManager,
        taskFactory);
  }

  /**
   * This method creates a specific file selector according to the file selection strategy of
   * crossSpace compaction, uses the file selector to select all unseqFiles and seqFiles to be
   * compacted under the time partition of the virtual storage group, and creates a compaction task
   * for them. The task is put into the compactionTaskQueue of the {@link CompactionTaskManager}.
   *
   * @return Returns whether the file was found and submits the merge task
   */
  @Override
  public void selectAndSubmit() {
    if ((CompactionTaskManager.currentTaskNum.get() >= config.getConcurrentCompactionThread())
        || (!config.isEnableCrossSpaceCompaction())) {
      return;
    }
    Iterator<TsFileResource> seqIterator = sequenceFileList.iterator();
    Iterator<TsFileResource> unSeqIterator = unsequenceFileList.iterator();
    List<TsFileResource> seqFileList = new ArrayList<>();
    List<TsFileResource> unSeqFileList = new ArrayList<>();
    while (seqIterator.hasNext()) {
      seqFileList.add(seqIterator.next());
    }
    while (unSeqIterator.hasNext()) {
      unSeqFileList.add(unSeqIterator.next());
    }
    if (seqFileList.isEmpty() || unSeqFileList.isEmpty()) {
      return;
    }
    long budget =
        SystemInfo.getInstance().getMemorySizeForCompaction()
            / config.getConcurrentCompactionThread();
    long timeLowerBound = System.currentTimeMillis() - Long.MAX_VALUE;
    CrossSpaceCompactionResource mergeResource =
        new CrossSpaceCompactionResource(seqFileList, unSeqFileList, timeLowerBound);

    ICrossSpaceMergeFileSelector fileSelector =
        InnerSpaceCompactionUtils.getCrossSpaceFileSelector(budget, mergeResource);
    try {
      List[] mergeFiles = fileSelector.select();
      List<Long> memoryCost = fileSelector.getMemoryCost();
      // avoid pending tasks holds the metadata and streams
      mergeResource.clear();
      if (mergeFiles.length == 0) {
        if (mergeResource.getUnseqFiles().size() > 0) {
          // still have unseq files but cannot be selected
          LOGGER.warn(
              "{} cannot select merge candidates under the budget {}",
              logicalStorageGroupName,
              budget);
        }
        return;
      }
      LOGGER.info(
          "select files for cross compaction, sequence files: {}, unsequence files {}, memory cost is {}",
          mergeFiles[0],
          mergeFiles[1],
          memoryCost.get(0));

      if (mergeFiles[0].size() > 0 && mergeFiles[1].size() > 0) {
        AbstractCompactionTask compactionTask =
            taskFactory.createTask(
                logicalStorageGroupName,
                virtualGroupId,
                timePartition,
                tsFileManager,
                mergeFiles[0],
                mergeFiles[1],
                memoryCost.get(0));
        CompactionTaskManager.getInstance().addTaskToWaitingQueue(compactionTask);
        LOGGER.info(
            "{} [Compaction] submit a task with {} sequence file and {} unseq files",
            logicalStorageGroupName + "-" + virtualGroupId,
            mergeResource.getSeqFiles().size(),
            mergeResource.getUnseqFiles().size());
      }

    } catch (Exception e) {
      LOGGER.error("{} cannot select file for cross space compaction", logicalStorageGroupName, e);
    }
  }
}
