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

package org.apache.iotdb.db.engine.compaction;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.cross.CrossSpaceCompactionTask;
import org.apache.iotdb.db.engine.compaction.cross.ICrossSpaceSelector;
import org.apache.iotdb.db.engine.compaction.inner.InnerSpaceCompactionTask;
import org.apache.iotdb.db.engine.compaction.performer.ICompactionPerformer;
import org.apache.iotdb.db.engine.compaction.task.ICompactionSelector;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

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
      tryToSubmitCrossSpaceCompactionTask(
          tsFileManager.getStorageGroupName(),
          tsFileManager.getDataRegionId(),
          timePartition,
          tsFileManager);
      tryToSubmitInnerSpaceCompactionTask(
          tsFileManager.getStorageGroupName(),
          tsFileManager.getDataRegionId(),
          timePartition,
          tsFileManager,
          true);
      tryToSubmitInnerSpaceCompactionTask(
          tsFileManager.getStorageGroupName(),
          tsFileManager.getDataRegionId(),
          timePartition,
          tsFileManager,
          false);
    } catch (InterruptedException e) {
      LOGGER.error("Exception occurs when selecting compaction tasks", e);
      Thread.currentThread().interrupt();
    }
  }

  public static void tryToSubmitInnerSpaceCompactionTask(
      String storageGroupName,
      String dataRegionId,
      long timePartition,
      TsFileManager tsFileManager,
      boolean sequence)
      throws InterruptedException {
    if ((!config.isEnableSeqSpaceCompaction() && sequence)
        || (!config.isEnableUnseqSpaceCompaction() && !sequence)) {
      return;
    }

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
    List<List<TsFileResource>> taskList =
        innerSpaceCompactionSelector.selectInnerSpaceTask(
            sequence
                ? tsFileManager.getSequenceListByTimePartition(timePartition)
                : tsFileManager.getUnsequenceListByTimePartition(timePartition));
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
      CompactionTaskManager.getInstance()
          .addTaskToWaitingQueue(
              new InnerSpaceCompactionTask(
                  timePartition,
                  tsFileManager,
                  task,
                  sequence,
                  performer,
                  CompactionTaskManager.currentTaskNum,
                  tsFileManager.getNextCompactionTaskId()));
    }
  }

  private static void tryToSubmitCrossSpaceCompactionTask(
      String logicalStorageGroupName,
      String dataRegionId,
      long timePartition,
      TsFileManager tsFileManager)
      throws InterruptedException {
    if (!config.isEnableCrossSpaceCompaction()) {
      return;
    }
    ICrossSpaceSelector crossSpaceCompactionSelector =
        config
            .getCrossCompactionSelector()
            .createInstance(logicalStorageGroupName, dataRegionId, timePartition, tsFileManager);
    List<Pair<List<TsFileResource>, List<TsFileResource>>> taskList =
        crossSpaceCompactionSelector.selectCrossSpaceTask(
            tsFileManager.getSequenceListByTimePartition(timePartition),
            tsFileManager.getUnsequenceListByTimePartition(timePartition));
    for (Pair<List<TsFileResource>, List<TsFileResource>> selectedFilesPair : taskList) {
      CompactionTaskManager.getInstance()
          .addTaskToWaitingQueue(
              new CrossSpaceCompactionTask(
                  timePartition,
                  tsFileManager,
                  selectedFilesPair.left,
                  selectedFilesPair.right,
                  IoTDBDescriptor.getInstance()
                      .getConfig()
                      .getCrossCompactionPerformer()
                      .createInstance(),
                  CompactionTaskManager.currentTaskNum,
                  tsFileManager.getNextCompactionTaskId()));
    }
  }
}
