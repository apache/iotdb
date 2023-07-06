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
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CompactionTaskSummary;
import org.apache.iotdb.db.utils.datastructure.FixedPriorityBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class CompactionWorker implements Runnable {
  private static final Logger log = LoggerFactory.getLogger("COMPACTION");
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
        log.warn("CompactionThread-{} terminates because interruption", threadId);
        return;
      }
      try {
        if (task != null && task.checkValidAndSetMerging()) {
          CompactionTaskSummary summary = task.getSummary();
          CompactionTaskFuture future = new CompactionTaskFuture(summary);
          CompactionTaskManager.getInstance().recordTask(task, future);
          task.start();
        }
      } catch (Exception e) {
        log.error("CompactionWorker.run(), Exception.", e);
      }
    }
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
