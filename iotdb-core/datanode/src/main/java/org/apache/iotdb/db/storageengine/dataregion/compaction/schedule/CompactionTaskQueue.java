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

import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.AbstractCompactionTask;
import org.apache.iotdb.db.storageengine.rescon.memory.SystemInfo;
import org.apache.iotdb.db.utils.datastructure.FixedPriorityBlockingQueue;

import java.util.Comparator;
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
      AbstractCompactionTask task = null;
      lock.lockInterruptibly();
      try {
        while (queue.isEmpty()) {
          notEmpty.await();
        }
        task = queue.pollFirst();
      } finally {
        lock.unlock();
      }
      boolean prepareTaskSuccess = prepareTask(task);
      if (!prepareTaskSuccess) {
        Thread.sleep(TimeUnit.SECONDS.toMillis(1));
        continue;
      }
      return task;
    }
  }

  private boolean prepareTask(AbstractCompactionTask task) throws InterruptedException {
    if (task == null) {
      return false;
    }
    if (!checkTaskValid(task)) {
      dropCompactionTask(task);
      return false;
    }
    if (!task.tryOccupyResourcesForRunning()) {
      put(task);
      return false;
    }
    if (!transitTaskFileStatus(task)) {
      dropCompactionTask(task);
      return false;
    }
    return true;
  }

  private void dropCompactionTask(AbstractCompactionTask task) {
    task.resetCompactionCandidateStatusForAllSourceFiles();
    task.handleTaskCleanup();
    task.releaseOccupiedResources();
  }

  private boolean checkTaskValid(AbstractCompactionTask task) {
    return task.isCompactionAllowed()
        && task.getCompactionConfigVersion()
            >= CompactionTaskManager.getInstance().getCurrentCompactionConfigVersion()
        // The estimated memory cost being less than 0 indicates that an exception occurred during
        // the process of estimating the memory for the compaction task.
        && task.getEstimatedMemoryCost() >= 0
        && task.getEstimatedMemoryCost() <= SystemInfo.getInstance().getMemorySizeForCompaction()
        && task.getProcessedFileNum() <= SystemInfo.getInstance().getTotalFileLimitForCompaction();
  }

  private boolean transitTaskFileStatus(AbstractCompactionTask task) {
    try {
      task.transitSourceFilesToMerging();
    } catch (Exception e) {
      return false;
    }
    return true;
  }
}
