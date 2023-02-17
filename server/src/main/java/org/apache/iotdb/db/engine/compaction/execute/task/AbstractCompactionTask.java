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

package org.apache.iotdb.db.engine.compaction.execute.task;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.engine.compaction.execute.performer.ICompactionPerformer;
import org.apache.iotdb.db.engine.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.service.metrics.recorder.CompactionMetricsManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * AbstractCompactionTask is the base class for all compaction task, it carries out the execution of
 * compaction. AbstractCompactionTask uses a template method, it executes the abstract function
 * {@link AbstractCompactionTask#doCompaction()} implemented by subclass, and decrease the
 * currentTaskNum in CompactionScheduler when the {@link AbstractCompactionTask#doCompaction()} is
 * finished. The future returns the {@link CompactionTaskSummary} of this task execution.
 */
public abstract class AbstractCompactionTask {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);
  protected String dataRegionId;
  protected String storageGroupName;
  protected long timePartition;
  protected final AtomicInteger currentTaskNum;
  protected final TsFileManager tsFileManager;
  protected ICompactionPerformer performer;
  protected int hashCode = -1;
  protected CompactionTaskSummary summary;
  protected long serialId;
  protected boolean crossTask;
  protected boolean innerSeqTask;

  public AbstractCompactionTask(
      String storageGroupName,
      String dataRegionId,
      long timePartition,
      TsFileManager tsFileManager,
      AtomicInteger currentTaskNum,
      long serialId) {
    this.storageGroupName = storageGroupName;
    this.dataRegionId = dataRegionId;
    this.timePartition = timePartition;
    this.tsFileManager = tsFileManager;
    this.currentTaskNum = currentTaskNum;
    this.serialId = serialId;
  }

  public abstract void setSourceFilesToCompactionCandidate();

  protected abstract void doCompaction();

  public void start() {
    currentTaskNum.incrementAndGet();
    boolean isSuccess = false;
    CompactionMetricsManager.getInstance().reportTaskStartRunning(crossTask, innerSeqTask);
    try {
      summary.start();
      doCompaction();
      isSuccess = true;
    } finally {
      this.currentTaskNum.decrementAndGet();
      summary.finish(isSuccess);
      CompactionTaskManager.getInstance().removeRunningTaskFuture(this);
      CompactionMetricsManager.getInstance()
          .reportTaskFinishOrAbort(crossTask, innerSeqTask, summary.getTimeCost());
    }
  }

  public String getStorageGroupName() {
    return this.storageGroupName;
  }

  public String getDataRegionId() {
    return this.dataRegionId;
  }

  public long getTimePartition() {
    return timePartition;
  }

  public abstract boolean equalsOtherTask(AbstractCompactionTask otherTask);

  /**
   * Check if the compaction task is valid (selected files are not merging, closed and exist). If
   * the task is valid, then set the merging status of selected files to true.
   *
   * @return true if the task is valid else false
   */
  public abstract boolean checkValidAndSetMerging();

  @Override
  public boolean equals(Object other) {
    if (other instanceof AbstractCompactionTask) {
      return equalsOtherTask((AbstractCompactionTask) other);
    }
    return false;
  }

  public abstract void resetCompactionCandidateStatusForAllSourceFiles();

  public long getTimeCost() {
    return summary.getTimeCost();
  }

  protected void checkInterrupted() throws InterruptedException {
    if (Thread.currentThread().isInterrupted()) {
      throw new InterruptedException(
          String.format("%s-%s [Compaction] abort", storageGroupName, dataRegionId));
    }
  }

  public boolean isTaskRan() {
    return summary.isRan();
  }

  public void cancel() {
    summary.cancel();
  }

  public boolean isSuccess() {
    return summary.isSuccess();
  }

  public CompactionTaskSummary getSummary() {
    return summary;
  }

  public boolean isTaskFinished() {
    return summary.isFinished();
  }

  public long getSerialId() {
    return serialId;
  }

  protected abstract void createSummary();

  public boolean isCrossTask() {
    return crossTask;
  }

  public boolean isInnerSeqTask() {
    return innerSeqTask;
  }
}
