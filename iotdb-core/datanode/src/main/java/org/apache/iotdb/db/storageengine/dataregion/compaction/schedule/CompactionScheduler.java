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
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.service.metrics.CompactionMetrics;
import org.apache.iotdb.db.storageengine.dataregion.compaction.constant.CompactionTaskType;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.AbstractCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CrossSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.InnerSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.InsertionCrossSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.ICompactionSelector;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.ICrossSpaceSelector;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.impl.RewriteCrossSpaceCompactionSelector;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.impl.SettleSelectorImpl;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.utils.CrossCompactionTaskResource;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.utils.InsertionCrossCompactionTaskResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.rescon.memory.SystemInfo;
import org.apache.iotdb.db.utils.EncryptDBUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Phaser;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
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
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private CompactionScheduler() {}

  /** avoid timed compaction schedule conflict with manually triggered schedule */
  private static final ReadWriteLock compactionTaskSelectionLock = new ReentrantReadWriteLock();

  public static void sharedLockCompactionSelection() {
    compactionTaskSelectionLock.readLock().lock();
  }

  public static void sharedUnlockCompactionSelection() {
    compactionTaskSelectionLock.readLock().unlock();
  }

  public static void exclusiveLockCompactionSelection() {
    compactionTaskSelectionLock.writeLock().lock();
  }

  public static void exclusiveUnlockCompactionSelection() {
    compactionTaskSelectionLock.writeLock().unlock();
  }

  /**
   * Select compaction task and submit them to CompactionTaskManager.
   *
   * @param tsFileManager tsfileManager that contains source files
   * @param timePartition the time partition to execute the selection
   * @param context the context of compaction schedule
   * @return the count of submitted task
   */
  public static void scheduleCompaction(
      TsFileManager tsFileManager, long timePartition, CompactionScheduleContext context)
      throws InterruptedException {
    if (!tsFileManager.isAllowCompaction()) {
      return;
    }
    // the name of this variable is trySubmitCount, because the task submitted to the queue could be
    // evicted due to the low priority of the task
    try {
      int submitInnerTaskNum = 0;
      submitInnerTaskNum +=
          tryToSubmitInnerSpaceCompactionTask(tsFileManager, timePartition, true, context);
      submitInnerTaskNum +=
          tryToSubmitInnerSpaceCompactionTask(tsFileManager, timePartition, false, context);
      boolean executeDelayedInsertionSelection =
          submitInnerTaskNum == 0 && context.isInsertionSelectionDelayed(timePartition);
      if (executeDelayedInsertionSelection) {
        scheduleInsertionCompaction(tsFileManager, timePartition, context);
      }
      tryToSubmitCrossSpaceCompactionTask(tsFileManager, timePartition, context);
      tryToSubmitSettleCompactionTask(tsFileManager, timePartition, context, false);
    } catch (InterruptedException e) {
      throw e;
    } catch (Throwable e) {
      LOGGER.error("Meet error in compaction schedule.", e);
    }
  }

  @TestOnly
  public static void scheduleCompaction(TsFileManager tsFileManager, long timePartition)
      throws InterruptedException {
    scheduleCompaction(
        tsFileManager,
        timePartition,
        new CompactionScheduleContext(
            EncryptDBUtils.getFirstEncryptParamFromDatabase(tsFileManager.getStorageGroupName())));
  }

  public static int tryToSubmitInnerSpaceCompactionTask(
      TsFileManager tsFileManager,
      long timePartition,
      boolean sequence,
      CompactionScheduleContext context)
      throws InterruptedException {
    if ((!config.isEnableSeqSpaceCompaction() && sequence)
        || (!config.isEnableUnseqSpaceCompaction() && !sequence)) {
      return 0;
    }

    String storageGroupName = tsFileManager.getStorageGroupName();
    String dataRegionId = tsFileManager.getDataRegionId();

    long compactionConfigVersionWhenSelectTask =
        CompactionTaskManager.getInstance().getCurrentCompactionConfigVersion();
    ICompactionSelector innerSpaceCompactionSelector;
    if (sequence) {
      innerSpaceCompactionSelector =
          config
              .getInnerSequenceCompactionSelector()
              .createInstance(
                  storageGroupName, dataRegionId, timePartition, tsFileManager, context);
    } else {
      innerSpaceCompactionSelector =
          config
              .getInnerUnsequenceCompactionSelector()
              .createInstance(
                  storageGroupName, dataRegionId, timePartition, tsFileManager, context);
    }
    long startTime = System.currentTimeMillis();
    List<InnerSpaceCompactionTask> innerSpaceTaskList =
        innerSpaceCompactionSelector.selectInnerSpaceTask(
            tsFileManager.getTsFileListSnapshot(timePartition, sequence));
    CompactionMetrics.getInstance()
        .updateCompactionTaskSelectionTimeCost(
            sequence ? CompactionTaskType.INNER_SEQ : CompactionTaskType.INNER_UNSEQ,
            System.currentTimeMillis() - startTime);
    innerSpaceTaskList.forEach(
        task -> task.setCompactionConfigVersion(compactionConfigVersionWhenSelectTask));
    // the name of this variable is trySubmitCount, because the task submitted to the queue could be
    // evicted due to the low priority of the task
    int trySubmitCount = addTaskToWaitingQueue(innerSpaceTaskList);
    context.incrementSubmitTaskNum(
        sequence ? CompactionTaskType.INNER_SEQ : CompactionTaskType.INNER_UNSEQ, trySubmitCount);
    return trySubmitCount;
  }

  private static int addTaskToWaitingQueue(List<? extends AbstractCompactionTask> tasks)
      throws InterruptedException {
    int trySubmitCount = 0;
    for (AbstractCompactionTask task : tasks) {
      if (!canAddTaskToWaitingQueue(task)) {
        continue;
      }
      if (CompactionTaskManager.getInstance().addTaskToWaitingQueue(task)) {
        trySubmitCount++;
      }
    }
    return trySubmitCount;
  }

  private static boolean canAddTaskToWaitingQueue(AbstractCompactionTask task)
      throws InterruptedException {
    if (Thread.interrupted()) {
      throw new InterruptedException();
    }
    // check file num
    long fileNumLimitForCompaction = SystemInfo.getInstance().getTotalFileLimitForCompaction();
    if (task.getProcessedFileNum() > fileNumLimitForCompaction) {
      return false;
    }
    // check disk space
    if (!task.isDiskSpaceCheckPassed()) {
      LOGGER.info(
          "Compaction task start check failed because disk free ratio is less than disk_space_warning_threshold");
      return false;
    }
    return true;
  }

  public static int scheduleInsertionCompaction(
      TsFileManager tsFileManager, long timePartition, CompactionScheduleContext context)
      throws InterruptedException {
    int count = 0;
    while (true) {
      Phaser insertionTaskPhaser = new Phaser(1);
      int selectedTaskNum =
          tryToSubmitInsertionCompactionTask(
              tsFileManager, timePartition, insertionTaskPhaser, context);
      insertionTaskPhaser.awaitAdvanceInterruptibly(insertionTaskPhaser.arrive());
      if (selectedTaskNum <= 0) {
        break;
      }
      count += selectedTaskNum;
    }
    return count;
  }

  public static int tryToSubmitInsertionCompactionTask(
      TsFileManager tsFileManager,
      long timePartition,
      Phaser insertionTaskPhaser,
      CompactionScheduleContext context)
      throws InterruptedException {
    if (!tsFileManager.isAllowCompaction() || !config.isEnableCrossSpaceCompaction()) {
      return 0;
    }
    String logicalStorageGroupName = tsFileManager.getStorageGroupName();
    String dataRegionId = tsFileManager.getDataRegionId();
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector(
            logicalStorageGroupName, dataRegionId, timePartition, tsFileManager, context);

    List<CrossCompactionTaskResource> selectedTasks =
        selector.selectInsertionCrossSpaceTask(
            tsFileManager.getTsFileListSnapshot(timePartition, true),
            tsFileManager.getTsFileListSnapshot(timePartition, false));
    if (selectedTasks.isEmpty()) {
      return 0;
    }

    InsertionCrossSpaceCompactionTask task =
        new InsertionCrossSpaceCompactionTask(
            insertionTaskPhaser,
            timePartition,
            tsFileManager,
            (InsertionCrossCompactionTaskResource) selectedTasks.get(0),
            tsFileManager.getNextCompactionTaskId());
    insertionTaskPhaser.register();
    if (!CompactionTaskManager.getInstance().addTaskToWaitingQueue(task)) {
      insertionTaskPhaser.arrive();
      return 0;
    }
    return 1;
  }

  private static int tryToSubmitCrossSpaceCompactionTask(
      TsFileManager tsFileManager, long timePartition, CompactionScheduleContext context)
      throws InterruptedException {
    if (!config.isEnableCrossSpaceCompaction()) {
      return 0;
    }
    if (!CompactionTaskManager.getInstance().shouldSelectCrossSpaceCompactionTask()) {
      return 0;
    }
    String logicalStorageGroupName = tsFileManager.getStorageGroupName();
    String dataRegionId = tsFileManager.getDataRegionId();
    long compactionConfigVersionWhenSelectTask =
        CompactionTaskManager.getInstance().getCurrentCompactionConfigVersion();
    ICrossSpaceSelector crossSpaceCompactionSelector =
        config
            .getCrossCompactionSelector()
            .createInstance(
                logicalStorageGroupName, dataRegionId, timePartition, tsFileManager, context);

    List<CrossCompactionTaskResource> taskList =
        crossSpaceCompactionSelector.selectCrossSpaceTask(
            tsFileManager.getTsFileListSnapshot(timePartition, true),
            tsFileManager.getTsFileListSnapshot(timePartition, false));
    List<Long> memoryCost =
        taskList.stream()
            .map(CrossCompactionTaskResource::getTotalMemoryCost)
            .collect(Collectors.toList());
    // the name of this variable is trySubmitCount, because the task submitted to the queue could be
    // evicted due to the low priority of the task
    int trySubmitCount = 0;
    for (int i = 0, size = taskList.size(); i < size; ++i) {
      CrossSpaceCompactionTask task =
          new CrossSpaceCompactionTask(
              timePartition,
              tsFileManager,
              taskList.get(i).getSeqFiles(),
              taskList.get(i).getUnseqFiles(),
              context.getCrossCompactionPerformer(),
              memoryCost.get(i),
              tsFileManager.getNextCompactionTaskId());
      task.setCompactionConfigVersion(compactionConfigVersionWhenSelectTask);
      trySubmitCount = addTaskToWaitingQueue(Collections.singletonList(task));
    }
    context.incrementSubmitTaskNum(CompactionTaskType.CROSS, trySubmitCount);
    return trySubmitCount;
  }

  public static int tryToSubmitSettleCompactionTask(
      TsFileManager tsFileManager,
      long timePartition,
      CompactionScheduleContext context,
      boolean heavySelect)
      throws InterruptedException {
    if (!config.isEnableSeqSpaceCompaction() && !config.isEnableUnseqSpaceCompaction()) {
      return 0;
    }
    String logicalStorageGroupName = tsFileManager.getStorageGroupName();
    String dataRegionId = tsFileManager.getDataRegionId();
    SettleSelectorImpl settleSelector =
        new SettleSelectorImpl(
            heavySelect,
            logicalStorageGroupName,
            dataRegionId,
            timePartition,
            tsFileManager,
            context);
    long startTime = System.currentTimeMillis();
    List<AbstractCompactionTask> taskList = new ArrayList<>();
    if (config.isEnableSeqSpaceCompaction()) {
      taskList.addAll(
          settleSelector.selectSettleTask(
              tsFileManager.getTsFileListSnapshot(timePartition, true)));
    }
    if (config.isEnableUnseqSpaceCompaction()) {
      taskList.addAll(
          settleSelector.selectSettleTask(
              tsFileManager.getTsFileListSnapshot(timePartition, false)));
    }
    CompactionMetrics.getInstance()
        .updateCompactionTaskSelectionTimeCost(
            CompactionTaskType.SETTLE, System.currentTimeMillis() - startTime);
    // the name of this variable is trySubmitCount, because the task submitted to the queue could be
    // evicted due to the low priority of the task
    int trySubmitCount = 0;
    for (AbstractCompactionTask task : taskList) {
      if (CompactionTaskManager.getInstance().addTaskToWaitingQueue(task)) {
        context.updateTTLInfo(task);
        trySubmitCount++;
      }
    }
    context.incrementSubmitTaskNum(CompactionTaskType.SETTLE, trySubmitCount);
    return trySubmitCount;
  }
}
