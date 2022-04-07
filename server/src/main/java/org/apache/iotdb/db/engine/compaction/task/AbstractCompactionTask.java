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
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * AbstractCompactionTask is the base class for all compaction task, it carries out the execution of
 * compaction. AbstractCompactionTask uses a template method, it execute the abstract function
 * <i>doCompaction</i> implemented by subclass, and decrease the currentTaskNum in
 * CompactionScheduler when the <i>doCompaction</i> finish.
 */
public abstract class AbstractCompactionTask implements Callable<Void> {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);
  protected String fullStorageGroupName;
  protected long timePartition;
  protected final AtomicInteger currentTaskNum;
  protected final TsFileManager tsFileManager;
  protected long timeCost = 0L;
  protected volatile boolean ran = false;
  protected volatile boolean finished = false;

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

  protected abstract void performCompaction() throws Exception;

  @Override
  public Void call() throws Exception {
    ran = true;
    long startTime = System.currentTimeMillis();
    currentTaskNum.incrementAndGet();
    try {
      doCompaction();
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
    } finally {
      this.currentTaskNum.decrementAndGet();
      CompactionTaskManager.getInstance().removeRunningTaskFromList(this);
      timeCost = System.currentTimeMillis() - startTime;
      finished = true;
    }

    return null;
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
    return ran;
  }

  public boolean isTaskFinished() {
    return finished;
  }
}
