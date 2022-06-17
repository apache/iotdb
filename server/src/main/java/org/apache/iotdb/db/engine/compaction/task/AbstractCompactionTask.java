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

package org.apache.iotdb.db.engine.compaction.task;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.engine.compaction.CompactionTaskManager;
import org.apache.iotdb.db.engine.compaction.performer.ICompactionPerformer;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * AbstractCompactionTask is the base class for all compaction task, it carries out the execution of
 * compaction. AbstractCompactionTask uses a template method, it executes the abstract function
 * {@link AbstractCompactionTask#doCompaction()} implemented by subclass, and decrease the
 * currentTaskNum in CompactionScheduler when the {@link AbstractCompactionTask#doCompaction()} is
 * finished. The future returns the {@link CompactionTaskSummary} of this task execution.
 */
public abstract class AbstractCompactionTask implements Callable<CompactionTaskSummary> {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);
  protected String fullStorageGroupName;
  protected long timePartition;
  protected final AtomicInteger currentTaskNum;
  protected final TsFileManager tsFileManager;
  protected long timeCost = 0L;
  protected volatile boolean ran = false;
  protected volatile boolean finished = false;
  protected volatile boolean cancel = false;
  protected volatile boolean success = false;
  protected ICompactionPerformer performer;
  protected int hashCode = -1;
  protected CompactionTaskSummary summary = new CompactionTaskSummary();
  private Future<CompactionTaskSummary> future;

  public AbstractCompactionTask(
      String fullStorageGroupName,
      long timePartition,
      TsFileManager tsFileManager,
      AtomicInteger currentTaskNum) {
    this.fullStorageGroupName = fullStorageGroupName;
    this.timePartition = timePartition;
    this.tsFileManager = tsFileManager;
    this.currentTaskNum = currentTaskNum;
  }

  public abstract void setSourceFilesToCompactionCandidate();

  protected abstract void doCompaction() throws Exception;

  @Override
  public CompactionTaskSummary call() throws Exception {
    ran = true;
    long startTime = System.currentTimeMillis();
    currentTaskNum.incrementAndGet();
    boolean isSuccess = false;
    try {
      summary.start();
      doCompaction();
      isSuccess = true;
    } catch (InterruptedException e) {
      LOGGER.warn("{} [Compaction] Current task is interrupted", fullStorageGroupName);
    } catch (Throwable e) {
      // Use throwable to catch OOM exception.
      LOGGER.error("{} [Compaction] Running compaction task failed", fullStorageGroupName, e);
    } finally {
      this.currentTaskNum.decrementAndGet();
      timeCost = System.currentTimeMillis() - startTime;
      summary.finish(success, timeCost);
      CompactionTaskManager.getInstance().removeRunningTaskFuture(this);
    }
    return summary;
  }

  public String getFullStorageGroupName() {
    return fullStorageGroupName;
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
    return timeCost;
  }

  protected void checkInterrupted() throws InterruptedException {
    if (Thread.currentThread().isInterrupted()) {
      throw new InterruptedException(String.format("%s [Compaction] abort", fullStorageGroupName));
    }
  }

  public boolean isTaskRan() {
    return summary.isRan();
  }

  public void setCancel(boolean cancel) {
    this.cancel = cancel;
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

  public void setFuture(Future<CompactionTaskSummary> future) {
    this.future = future;
  }

  public Future<CompactionTaskSummary> getFuture() {
    return future;
  }
}
