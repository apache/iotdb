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
package org.apache.iotdb.db.mpp.schedule;

import org.apache.iotdb.db.mpp.execution.ExecFragmentInstance;
import org.apache.iotdb.db.mpp.schedule.queue.IndexedBlockingQueue;
import org.apache.iotdb.db.mpp.schedule.task.FragmentInstanceTask;
import org.apache.iotdb.db.utils.stats.CpuTimer;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.airlift.units.Duration;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/** the worker thread of {@link FragmentInstanceTask} */
public class FragmentInstanceTaskExecutor extends AbstractExecutor {

  private static final Duration EXECUTION_TIME_SLICE = new Duration(100, TimeUnit.MILLISECONDS);

  // As the callback is lightweight enough, there's no need to use another one thread to execute.
  private static final Executor listeningExecutor = MoreExecutors.directExecutor();

  public FragmentInstanceTaskExecutor(
      String workerId,
      ThreadGroup tg,
      IndexedBlockingQueue<FragmentInstanceTask> queue,
      ITaskScheduler scheduler) {
    super(workerId, tg, queue, scheduler);
  }

  @Override
  public void execute(FragmentInstanceTask task) throws InterruptedException {
    // try to switch it to RUNNING
    if (!getScheduler().readyToRunning(task)) {
      return;
    }
    ExecFragmentInstance instance = task.getFragmentInstance();
    CpuTimer timer = new CpuTimer();
    ListenableFuture<Void> future = instance.processFor(EXECUTION_TIME_SLICE);
    CpuTimer.CpuDuration duration = timer.elapsedTime();
    // long cost = System.nanoTime() - startTime;
    // If the future is cancelled, the task is in an error and should be thrown.
    if (future.isCancelled()) {
      getScheduler().toAborted(task);
      return;
    }
    ExecutionContext context = new ExecutionContext();
    context.setCpuDuration(duration);
    context.setTimeSlice(EXECUTION_TIME_SLICE);
    if (instance.isFinished()) {
      getScheduler().runningToFinished(task, context);
      return;
    }

    if (future.isDone()) {
      getScheduler().runningToReady(task, context);
    } else {
      getScheduler().runningToBlocked(task, context);
      future.addListener(
          () -> {
            getScheduler().blockedToReady(task);
          },
          listeningExecutor);
    }
  }
}
