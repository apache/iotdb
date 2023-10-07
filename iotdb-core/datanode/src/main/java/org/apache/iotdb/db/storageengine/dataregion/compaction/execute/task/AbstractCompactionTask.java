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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task;

import org.apache.iotdb.db.service.metrics.CompactionMetrics;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.FileCannotTransitToCompactingException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.ICompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * AbstractCompactionTask is the base class for all compaction task, it carries out the execution of
 * compaction. AbstractCompactionTask uses a template method, it executes the abstract function
 * {@link AbstractCompactionTask#doCompaction()} implemented by subclass, and decrease the
 * currentTaskNum in CompactionScheduler when the {@link AbstractCompactionTask#doCompaction()} is
 * finished. The future returns the {@link CompactionTaskSummary} of this task execution.
 */
public abstract class AbstractCompactionTask {
  protected String dataRegionId;
  protected String storageGroupName;
  protected long timePartition;
  protected final TsFileManager tsFileManager;
  protected ICompactionPerformer performer;
  protected int hashCode = -1;
  protected CompactionTaskSummary summary;
  protected long serialId;
  protected boolean crossTask;
  protected boolean innerSeqTask;

  protected long memoryCost = 0L;

  protected CompactionTaskType compactionTaskType;

  protected AbstractCompactionTask(
      String storageGroupName,
      String dataRegionId,
      long timePartition,
      TsFileManager tsFileManager,
      long serialId) {
    this(
        storageGroupName,
        dataRegionId,
        timePartition,
        tsFileManager,
        serialId,
        CompactionTaskType.NORMAL);
  }

  protected AbstractCompactionTask(
      String storageGroupName,
      String dataRegionId,
      long timePartition,
      TsFileManager tsFileManager,
      long serialId,
      CompactionTaskType compactionTaskType) {
    this.storageGroupName = storageGroupName;
    this.dataRegionId = dataRegionId;
    this.timePartition = timePartition;
    this.tsFileManager = tsFileManager;
    this.serialId = serialId;
    this.compactionTaskType = compactionTaskType;
  }

  protected abstract List<TsFileResource> getAllSourceTsFiles();

  /**
   * This method will try to set the files to COMPACTION_CANDIDATE. If failed, it should roll back
   * all status to original value
   *
   * @return set status successfully or not
   */
  public boolean setSourceFilesToCompactionCandidate() {
    List<TsFileResource> files = getAllSourceTsFiles();
    for (int i = 0; i < files.size(); i++) {
      if (!files.get(i).setStatus(TsFileResourceStatus.COMPACTION_CANDIDATE)) {
        // rollback status to NORMAL
        for (int j = 0; j < i; j++) {
          files.get(j).setStatus(TsFileResourceStatus.NORMAL);
        }
        return false;
      }
    }
    return true;
  }

  protected abstract boolean doCompaction();

  public void start() {
    boolean isSuccess = false;
    summary.start();
    try {
      isSuccess = doCompaction();
    } finally {
      summary.finish(isSuccess);
      CompactionTaskManager.getInstance().removeRunningTaskFuture(this);
      CompactionMetrics.getInstance()
          .recordTaskFinishOrAbort(crossTask, innerSeqTask, summary.getTimeCost());
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

  public void transitSourceFilesToMerging() throws FileCannotTransitToCompactingException {
    for (TsFileResource f : getAllSourceTsFiles()) {
      if (!f.setStatus(TsFileResourceStatus.COMPACTING)) {
        throw new FileCannotTransitToCompactingException(f);
      }
    }
  }

  public abstract long getEstimatedMemoryCost() throws IOException;

  public abstract int getProcessedFileNum();

  public boolean isCompactionAllowed() {
    return tsFileManager.isAllowCompaction();
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof AbstractCompactionTask) {
      return equalsOtherTask((AbstractCompactionTask) other);
    }
    return false;
  }

  public void resetCompactionCandidateStatusForAllSourceFiles() {
    List<TsFileResource> resources = getAllSourceTsFiles();
    // only reset status of the resources whose status is COMPACTING and COMPACTING_CANDIDATE
    resources.forEach(x -> x.setStatus(TsFileResourceStatus.NORMAL));
  }

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

  public long getTemporalFileSize() {
    return summary.getTemporalFileSize();
  }

  public boolean isInnerSeqTask() {
    return innerSeqTask;
  }

  public CompactionTaskType getCompactionTaskType() {
    return compactionTaskType;
  }

  public boolean isDiskSpaceCheckPassed() {
    if (compactionTaskType == CompactionTaskType.MOD_SETTLE) {
      return true;
    }
    return CompactionUtils.isDiskHasSpace();
  }
}
