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

package org.apache.iotdb.db.storageengine.dataregion.compaction.schedule;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.ICompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CrossSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.InnerSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.ICompactionSelector;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.ICrossSpaceSelector;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.utils.CrossCompactionTaskResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private CompactionScheduler() {}

  /**
   * Select compaction task and submit them to CompactionTaskManager.
   *
   * @param tsFileManager tsfileManager that contains source files
   * @param timePartition the time partition to execute the selection
   * @return the count of submitted task
   */
  public static int scheduleCompaction(TsFileManager tsFileManager, long timePartition) {
    if (!tsFileManager.isAllowCompaction()) {
      return 0;
    }
    // the name of this variable is trySubmitCount, because the task submitted to the queue could be
    // evicted due to the low priority of the task
    int trySubmitCount = 0;
    try {
      trySubmitCount += tryToSubmitCrossSpaceCompactionTask(tsFileManager, timePartition);
      trySubmitCount += tryToSubmitInnerSpaceCompactionTask(tsFileManager, timePartition, true);
      trySubmitCount += tryToSubmitInnerSpaceCompactionTask(tsFileManager, timePartition, false);
    } catch (InterruptedException e) {
      LOGGER.error("Exception occurs when selecting compaction tasks", e);
      Thread.currentThread().interrupt();
    }
    return trySubmitCount;
  }

  public static int tryToSubmitInnerSpaceCompactionTask(
      TsFileManager tsFileManager, long timePartition, boolean sequence)
      throws InterruptedException {
    if ((!config.isEnableSeqSpaceCompaction() && sequence)
        || (!config.isEnableUnseqSpaceCompaction() && !sequence)) {
      return 0;
    }

    String storageGroupName = tsFileManager.getStorageGroupName();
    String dataRegionId = tsFileManager.getDataRegionId();

    ICompactionSelector innerSpaceCompactionSelector;
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
    List<List<TsFileResource>> taskList =
        innerSpaceCompactionSelector.selectInnerSpaceTask(
            sequence
                ? tsFileManager.getOrCreateSequenceListByTimePartition(timePartition)
                : tsFileManager.getOrCreateUnsequenceListByTimePartition(timePartition));
    // the name of this variable is trySubmitCount, because the task submitted to the queue could be
    // evicted due to the low priority of the task
    int trySubmitCount = 0;
    for (List<TsFileResource> task : taskList) {
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
      if (CompactionTaskManager.getInstance()
          .addTaskToWaitingQueue(
              new InnerSpaceCompactionTask(
                  timePartition,
                  tsFileManager,
                  task,
                  sequence,
                  performer,
                  CompactionTaskManager.currentTaskNum,
                  tsFileManager.getNextCompactionTaskId()))) {
        trySubmitCount++;
      }
    }
    return trySubmitCount;
  }

  private static int tryToSubmitCrossSpaceCompactionTask(
      TsFileManager tsFileManager, long timePartition) throws InterruptedException {
    if (!config.isEnableCrossSpaceCompaction()) {
      return 0;
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
    // the name of this variable is trySubmitCount, because the task submitted to the queue could be
    // evicted due to the low priority of the task
    int trySubmitCount = 0;
    for (int i = 0, size = taskList.size(); i < size; ++i) {
      if (CompactionTaskManager.getInstance()
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
                  tsFileManager.getNextCompactionTaskId()))) {
        trySubmitCount++;
      }
    }
    return trySubmitCount;
  }
}
