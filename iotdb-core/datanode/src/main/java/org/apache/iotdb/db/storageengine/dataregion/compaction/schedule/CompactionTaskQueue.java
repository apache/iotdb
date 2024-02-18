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

import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.CompactionFileCountExceededException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.CompactionMemoryNotEnoughException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.AbstractCompactionTask;
import org.apache.iotdb.db.storageengine.rescon.memory.SystemInfo;
import org.apache.iotdb.db.utils.datastructure.FixedPriorityBlockingQueue;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class CompactionTaskQueue extends FixedPriorityBlockingQueue<AbstractCompactionTask> {
  public CompactionTaskQueue(int maxSize, Comparator<AbstractCompactionTask> comparator) {
    super(maxSize, comparator);
  }

  @Override
  public AbstractCompactionTask take() throws InterruptedException {
    final ReentrantLock lock = this.lock;
    while (true) {
      lock.lockInterruptibly();
      try {
        while (queue.isEmpty()) {
          notEmpty.await();
        }
        AbstractCompactionTask task = pollExecutableTask();
        // task == null indicates that there is no runnable task now
        if (task != null) {
          return task;
        }
      } finally {
        lock.unlock();
      }
      Thread.sleep(TimeUnit.SECONDS.toMillis(1));
    }
  }

  private AbstractCompactionTask pollExecutableTask() {
    List<AbstractCompactionTask> retryTasks = new ArrayList<>();
    try {
      while (true) {
        if (queue.isEmpty()) {
          queue.addAll(retryTasks);
          retryTasks.clear();
          return null;
        }
        AbstractCompactionTask task = queue.pollFirst();
        if (task == null) {
          continue;
        }
        if (!tryOccupyResourcesForRunningTask(task)) {
          incrementTaskPriority(task);
          retryTasks.add(task);
          continue;
        }
        if (!checkTaskValid(task)) {
          dropCompactionTask(task);
          continue;
        }
        return task;
      }
    } finally {
      queue.addAll(retryTasks);
    }
  }

  private void dropCompactionTask(AbstractCompactionTask task) {
    task.resetCompactionCandidateStatusForAllSourceFiles();
    task.handleTaskCleanup();
    SystemInfo.getInstance()
        .resetCompactionMemoryCost(task.getCompactionTaskType(), task.getEstimatedMemoryCost());
    SystemInfo.getInstance().decreaseCompactionFileNumCost(task.getProcessedFileNum());
  }

  private boolean checkTaskValid(AbstractCompactionTask task) {
    if (!task.isCompactionAllowed()) {
      return false;
    }
    try {
      task.transitSourceFilesToMerging();
    } catch (Exception e) {
      return false;
    }
    return true;
  }

  private boolean tryOccupyResourcesForRunningTask(AbstractCompactionTask task) {
    if (!task.isDiskSpaceCheckPassed()) {
      return false;
    }
    // check task retry times
    int maxRetryTimes = 5;
    boolean blockUntilCanExecute = task.getRetryAllocateResourcesTimes() >= maxRetryTimes;
    long estimatedMemoryCost = task.getEstimatedMemoryCost();
    boolean memoryAcquired = false;
    boolean fileHandleAcquired = false;
    try {
      SystemInfo.getInstance()
          .addCompactionMemoryCost(
              task.getCompactionTaskType(), estimatedMemoryCost, blockUntilCanExecute);
      memoryAcquired = true;
      SystemInfo.getInstance()
          .addCompactionFileNum(task.getProcessedFileNum(), blockUntilCanExecute);
      fileHandleAcquired = true;
    } catch (CompactionMemoryNotEnoughException | CompactionFileCountExceededException ignored) {
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      // if file num acquired successfully, the allocation will success
      if (memoryAcquired && !fileHandleAcquired) {
        SystemInfo.getInstance()
            .resetCompactionMemoryCost(task.getCompactionTaskType(), estimatedMemoryCost);
      }
    }
    return memoryAcquired && fileHandleAcquired;
  }

  private void incrementTaskPriority(AbstractCompactionTask task) {
    task.updateRetryAllocateResourcesTimes();
  }
}
