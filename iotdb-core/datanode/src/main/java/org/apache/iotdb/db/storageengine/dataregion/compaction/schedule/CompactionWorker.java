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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.service.metrics.CompactionMetrics;
import org.apache.iotdb.db.storageengine.dataregion.compaction.constant.CompactionTaskType;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.CompactionFileCountExceededException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.CompactionMemoryNotEnoughException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.FileCannotTransitToCompactingException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.AbstractCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.estimator.AbstractCompactionEstimator;
import org.apache.iotdb.db.storageengine.rescon.memory.SystemInfo;
import org.apache.iotdb.db.utils.datastructure.FixedPriorityBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class CompactionWorker implements Runnable {
  private static final Logger LOGGER = LoggerFactory.getLogger("COMPACTION");
  private final int threadId;
  private final FixedPriorityBlockingQueue<AbstractCompactionTask> compactionTaskQueue;

  public CompactionWorker(
      int threadId, FixedPriorityBlockingQueue<AbstractCompactionTask> compactionTaskQueue) {
    this.threadId = threadId;
    this.compactionTaskQueue = compactionTaskQueue;
  }

  @SuppressWarnings("squid:S2142")
  @Override
  public void run() {
    while (!Thread.currentThread().isInterrupted()) {
      AbstractCompactionTask task;
      try {
        task = compactionTaskQueue.take();
      } catch (InterruptedException e) {
        LOGGER.warn("CompactionThread-{} terminates because interruption", threadId);
        Thread.currentThread().interrupt();
        return;
      }
      processOneCompactionTask(task);
    }
  }

  public boolean processOneCompactionTask(AbstractCompactionTask task) {
    long estimatedMemoryCost = 0L;
    boolean memoryAcquired = false;
    boolean fileHandleAcquired = false;
    boolean taskSuccess = false;
    try {
      if (task == null || !task.isCompactionAllowed()) {
        LOGGER.info(
            "Compaction task is not allowed to be executed by TsFileManager. Task {}", task);
        return false;
      }
      if (!task.isDiskSpaceCheckPassed()) {
        LOGGER.debug(
            "Compaction task start check failed because disk free ratio is less than disk_space_warning_threshold");
        return false;
      }
      task.transitSourceFilesToMerging();
      if (IoTDBDescriptor.getInstance().getConfig().isEnableCompactionMemControl()) {
        estimatedMemoryCost = task.getEstimatedMemoryCost();
        if (estimatedMemoryCost < 0) {
          return false;
        }
        CompactionTaskType taskType = task.getCompactionTaskType();
        memoryAcquired =
            SystemInfo.getInstance().addCompactionMemoryCost(taskType, estimatedMemoryCost, 60);
        CompactionMetrics.getInstance()
            .updateCompactionMemoryMetrics(taskType, estimatedMemoryCost);
        CompactionMetrics.getInstance()
            .updateCompactionTaskSelectedFileNum(taskType, task.getAllSourceTsFiles().size());
      }
      fileHandleAcquired =
          SystemInfo.getInstance().addCompactionFileNum(task.getProcessedFileNum(), 60);
      CompactionTaskSummary summary = task.getSummary();
      CompactionTaskFuture future = new CompactionTaskFuture(summary);
      CompactionTaskManager.getInstance().recordTask(task, future);
      task.start();
      taskSuccess = true;
      return true;
    } catch (FileCannotTransitToCompactingException
        | CompactionMemoryNotEnoughException
        | CompactionFileCountExceededException e) {
      LOGGER.info("CompactionTask {} cannot be executed. Reason: {}", task, e.getMessage());
    } catch (InterruptedException e) {
      LOGGER.warn("InterruptedException occurred when preparing compaction task. {}", task, e);
      Thread.currentThread().interrupt();
    } finally {
      if (task != null) {
        task.resetCompactionCandidateStatusForAllSourceFiles();
        task.handleTaskCleanup();
      }
      if (memoryAcquired) {
        SystemInfo.getInstance()
            .resetCompactionMemoryCost(task.getCompactionTaskType(), estimatedMemoryCost);
      }
      if (fileHandleAcquired) {
        SystemInfo.getInstance().decreaseCompactionFileNumCost(task.getProcessedFileNum());
      }
      if (taskSuccess) {
        task.getAllSourceTsFiles()
            .forEach(AbstractCompactionEstimator::removeFileInfoFromGlobalFileInfoCache);
      }
    }
    return false;
  }

  static class CompactionTaskFuture implements Future<CompactionTaskSummary> {
    CompactionTaskSummary summary;

    public CompactionTaskFuture(CompactionTaskSummary summary) {
      this.summary = summary;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      summary.cancel();
      return true;
    }

    @Override
    public boolean isCancelled() {
      return summary.isCancel();
    }

    @Override
    public boolean isDone() {
      return summary.isFinished();
    }

    @Override
    public CompactionTaskSummary get() throws InterruptedException, ExecutionException {
      while (!summary.isFinished()) {
        TimeUnit.MILLISECONDS.sleep(100);
      }
      return summary;
    }

    @Override
    public CompactionTaskSummary get(long timeout, @NotNull TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      long perSleepTime = timeout < 100 ? timeout : 100;
      long totalSleepTime = 0L;
      while (!summary.isFinished()) {
        if (totalSleepTime >= timeout) {
          throw new TimeoutException("Timeout when trying to get compaction task summary");
        }
        unit.sleep(perSleepTime);
        totalSleepTime += perSleepTime;
      }
      return summary;
    }
  }
}
