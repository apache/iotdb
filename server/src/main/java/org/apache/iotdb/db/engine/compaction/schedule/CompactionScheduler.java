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

package org.apache.iotdb.db.engine.compaction.schedule;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.execute.performer.ICompactionPerformer;
import org.apache.iotdb.db.engine.compaction.execute.task.CrossSpaceCompactionTask;
import org.apache.iotdb.db.engine.compaction.execute.task.InnerSpaceCompactionTask;
import org.apache.iotdb.db.engine.compaction.selector.ICompactionSelector;
import org.apache.iotdb.db.engine.compaction.selector.ICrossSpaceSelector;
import org.apache.iotdb.db.engine.compaction.selector.utils.CrossCompactionTaskResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * CompactionScheduler schedules and submits the compaction task periodically, and it counts the
 * total number of running compaction task. There are three compaction strategy: BALANCE,
 * INNER_CROSS, CROSS_INNER. Difference strategies will lead to different compaction preferences.
 * For different types of compaction task(e.g. InnerSpaceCompaction), CompactionScheduler will call
 * the corresponding {@link ICompactionSelector selector} according to the compaction machanism of
 * the task(e.g. LevelCompaction, SizeTiredCompaction), and the selection and submission process is
 * carried out in the {@link ICompactionSelector#selectInnerSpaceTask(List)} () and {@link
 * ICompactionSelector#selectCrossSpaceTask(List, List)}} in selector.
 */
public class CompactionScheduler {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);
  private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  public static void scheduleCompaction(TsFileManager tsFileManager, long timePartition) {
    if (!tsFileManager.isAllowCompaction()) {
      return;
    }
    try {
      tryToSubmitCrossSpaceCompactionTask(tsFileManager, timePartition);
      tryToSubmitInnerSpaceCompactionTask(tsFileManager, timePartition);
    } catch (InterruptedException e) {
      LOGGER.error("Exception occurs when selecting compaction tasks", e);
      Thread.currentThread().interrupt();
    }
  }

  private static List<List<TsFileResource>> selectInnerSpaceCompactionTask(
      long timePartition, TsFileManager tsFileManager, boolean sequence) {
    if ((!config.isEnableSeqSpaceCompaction() && sequence)
        || (!config.isEnableUnseqSpaceCompaction() && !sequence)) {
      return Collections.emptyList();
    }
    String storageGroupName = tsFileManager.getStorageGroupName();
    String dataRegionId = tsFileManager.getDataRegionId();

    ICompactionSelector innerSpaceCompactionSelector = null;
    if (sequence) {
      innerSpaceCompactionSelector =
          config
              .getInnerSequenceCompactionSelector()
              .createInstance(storageGroupName, dataRegionId, timePartition, tsFileManager);
    } else {
      innerSpaceCompactionSelector =
          config
              .getInnerUnsequenceCompactionSelector()
              .createInstance(storageGroupName, dataRegionId, timePartition, tsFileManager);
    }

    return innerSpaceCompactionSelector.selectInnerSpaceTask(
        sequence
            ? tsFileManager.getOrCreateSequenceListByTimePartition(timePartition)
            : tsFileManager.getOrCreateUnsequenceListByTimePartition(timePartition));
  }

  public static void tryToSubmitInnerSpaceCompactionTask(
      TsFileManager tsFileManager, long timePartition) throws InterruptedException {
    List<List<TsFileResource>> seqTaskList =
        selectInnerSpaceCompactionTask(timePartition, tsFileManager, true);
    List<List<TsFileResource>> unseqTaskList =
        selectInnerSpaceCompactionTask(timePartition, tsFileManager, false);
    int taskFreeSize =
        config.getCandidateCompactionTaskQueueSize()
            - CompactionTaskManager.getInstance().getCompactionCandidateTaskCount();
    int taskSize = Math.max(seqTaskList.size(), unseqTaskList.size());
    for (int i = 0; i < taskSize; i++) {
      if (taskFreeSize <= 0) {
        break;
      }
      // submit one seq inner space task
      if (i < seqTaskList.size()) {
        submitInnerTask(seqTaskList.get(i), tsFileManager, timePartition, true);
        taskFreeSize--;
      }

      // submit one unseq inner space task
      if (i < unseqTaskList.size()) {
        submitInnerTask(unseqTaskList.get(i), tsFileManager, timePartition, false);
        taskFreeSize--;
      }
    }
  }

  private static void submitInnerTask(
      List<TsFileResource> taskList,
      TsFileManager tsFileManager,
      long timePartition,
      boolean sequence)
      throws InterruptedException {
    ICompactionPerformer performer =
        sequence
            ? IoTDBDescriptor.getInstance()
                .getConfig()
                .getInnerSeqCompactionPerformer()
                .createInstance()
            : IoTDBDescriptor.getInstance()
                .getConfig()
                .getInnerUnseqCompactionPerformer()
                .createInstance();
    CompactionTaskManager.getInstance()
        .addTaskToWaitingQueue(
            new InnerSpaceCompactionTask(
                timePartition,
                tsFileManager,
                taskList,
                sequence,
                performer,
                CompactionTaskManager.currentTaskNum,
                tsFileManager.getNextCompactionTaskId()));
  }

  private static void tryToSubmitCrossSpaceCompactionTask(
      TsFileManager tsFileManager, long timePartition) throws InterruptedException {
    if (!config.isEnableCrossSpaceCompaction()) {
      return;
    }
    String logicalStorageGroupName = tsFileManager.getStorageGroupName();
    String dataRegionId = tsFileManager.getDataRegionId();
    ICrossSpaceSelector crossSpaceCompactionSelector =
        config
            .getCrossCompactionSelector()
            .createInstance(logicalStorageGroupName, dataRegionId, timePartition, tsFileManager);
    List<CrossCompactionTaskResource> taskList =
        crossSpaceCompactionSelector.selectCrossSpaceTask(
            tsFileManager.getOrCreateSequenceListByTimePartition(timePartition),
            tsFileManager.getOrCreateUnsequenceListByTimePartition(timePartition));
    List<Long> memoryCost =
        taskList.stream()
            .map(CrossCompactionTaskResource::getTotalMemoryCost)
            .collect(Collectors.toList());
    for (int i = 0, size = taskList.size(); i < size; ++i) {
      CompactionTaskManager.getInstance()
          .addTaskToWaitingQueue(
              new CrossSpaceCompactionTask(
                  timePartition,
                  tsFileManager,
                  taskList.get(i).getSeqFiles(),
                  taskList.get(i).getUnseqFiles(),
                  IoTDBDescriptor.getInstance()
                      .getConfig()
                      .getCrossCompactionPerformer()
                      .createInstance(),
                  CompactionTaskManager.currentTaskNum,
                  memoryCost.get(i),
                  tsFileManager.getNextCompactionTaskId()));
    }
  }
}
