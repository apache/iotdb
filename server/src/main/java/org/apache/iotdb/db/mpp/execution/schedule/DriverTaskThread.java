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
package org.apache.iotdb.db.mpp.execution.schedule;

import org.apache.iotdb.db.mpp.execution.driver.IDriver;
import org.apache.iotdb.db.mpp.execution.schedule.queue.IndexedBlockingQueue;
import org.apache.iotdb.db.mpp.execution.schedule.task.DriverTask;
import org.apache.iotdb.db.utils.stats.CpuTimer;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.airlift.concurrent.SetThreadName;
import io.airlift.units.Duration;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/** the worker thread of {@link DriverTask} */
public class DriverTaskThread extends AbstractDriverThread {

  public static final Duration EXECUTION_TIME_SLICE = new Duration(100, TimeUnit.MILLISECONDS);

  // As the callback is lightweight enough, there's no need to use another one thread to execute.
  private static final Executor listeningExecutor = MoreExecutors.directExecutor();

  public DriverTaskThread(
      String workerId,
      ThreadGroup tg,
      IndexedBlockingQueue<DriverTask> queue,
      ITaskScheduler scheduler) {
    super(workerId, tg, queue, scheduler);
  }

  @Override
  public void execute(DriverTask task) throws InterruptedException {
    // try to switch it to RUNNING
    if (!scheduler.readyToRunning(task)) {
      return;
    }
    IDriver instance = task.getFragmentInstance();
    CpuTimer timer = new CpuTimer();
    ListenableFuture<?> future = instance.processFor(EXECUTION_TIME_SLICE);
    CpuTimer.CpuDuration duration = timer.elapsedTime();
    // long cost = System.nanoTime() - startTime;
    // If the future is cancelled, the task is in an error and should be thrown.
    if (future.isCancelled()) {
      task.setAbortCause(FragmentInstanceAbortedException.BY_ALREADY_BEING_CANCELLED);
      scheduler.toAborted(task);
      return;
    }
    ExecutionContext context = new ExecutionContext();
    context.setCpuDuration(duration);
    context.setTimeSlice(EXECUTION_TIME_SLICE);
    if (instance.isFinished()) {
      scheduler.runningToFinished(task, context);
      return;
    }

    if (future.isDone()) {
      scheduler.runningToReady(task, context);
    } else {
      scheduler.runningToBlocked(task, context);
      future.addListener(
          () -> {
            try (SetThreadName fragmentInstanceName2 =
                new SetThreadName(task.getFragmentInstance().getInfo().getFullId())) {
              scheduler.blockedToReady(task);
            }
          },
          listeningExecutor);
    }
  }
}
