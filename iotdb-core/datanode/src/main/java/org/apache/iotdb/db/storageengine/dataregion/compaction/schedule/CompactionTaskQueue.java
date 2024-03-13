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
      lock.lockInterruptibly();
      try {
        while (queue.isEmpty()) {
          notEmpty.await();
        }
        AbstractCompactionTask task = tryPollExecutableTask();
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

  private AbstractCompactionTask tryPollExecutableTask() {
    while (true) {
      if (queue.isEmpty()) {
        return null;
      }
      AbstractCompactionTask task = queue.pollFirst();
      if (task == null) {
        continue;
      }
      if (!checkTaskValid(task)) {
        dropCompactionTask(task);
        continue;
      }
      if (!task.tryOccupyResourcesForRunning()) {
        queue.add(task);
        return null;
      }
      if (!transitTaskFileStatus(task)) {
        dropCompactionTask(task);
        continue;
      }
      return task;
    }
  }

  private void dropCompactionTask(AbstractCompactionTask task) {
    task.resetCompactionCandidateStatusForAllSourceFiles();
    task.handleTaskCleanup();
    task.releaseOccupiedResources();
  }

  private boolean checkTaskValid(AbstractCompactionTask task) {
    return task.isCompactionAllowed()
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
