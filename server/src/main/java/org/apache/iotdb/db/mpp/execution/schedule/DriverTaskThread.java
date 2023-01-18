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

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.mpp.execution.driver.IDriver;
import org.apache.iotdb.db.mpp.execution.schedule.queue.IndexedBlockingQueue;
import org.apache.iotdb.db.mpp.execution.schedule.task.DriverTask;
import org.apache.iotdb.db.utils.SetThreadName;
import org.apache.iotdb.db.utils.stats.CpuTimer;

import com.google.common.base.Ticker;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/** the worker thread of {@link DriverTask} */
public class DriverTaskThread extends AbstractDriverThread {

  public static final Duration EXECUTION_TIME_SLICE =
      new Duration(
          IoTDBDescriptor.getInstance().getConfig().getDriverTaskExecutionTimeSliceInMs(),
          TimeUnit.MILLISECONDS);

  // we manage thread pool size directly, so create an unlimited pool
  private static final Executor listeningExecutor =
      IoTDBThreadPoolFactory.newCachedThreadPool("scheduler-notification");

  private final Ticker ticker;

  public DriverTaskThread(
      String workerId,
      ThreadGroup tg,
      IndexedBlockingQueue<DriverTask> queue,
      ITaskScheduler scheduler,
      ThreadProducer producer) {
    super(workerId, tg, queue, scheduler, producer);
    this.ticker = Ticker.systemTicker();
  }

  @Override
  public void execute(DriverTask task) throws InterruptedException {
    long startNanos = ticker.read();
    // try to switch it to RUNNING
    if (!scheduler.readyToRunning(task)) {
      return;
    }
    IDriver driver = task.getDriver();
    CpuTimer timer = new CpuTimer();
    ListenableFuture<?> future = driver.processFor(EXECUTION_TIME_SLICE);
    CpuTimer.CpuDuration duration = timer.elapsedTime();
    // If the future is cancelled, the task is in an error and should be thrown.
    if (future.isCancelled()) {
      task.setAbortCause(DriverTaskAbortedException.BY_ALREADY_BEING_CANCELLED);
      scheduler.toAborted(task);
      return;
    }
    long quantaScheduledNanos = ticker.read() - startNanos;
    ExecutionContext context = new ExecutionContext();
    context.setCpuDuration(duration);
    context.setScheduledTimeInNanos(quantaScheduledNanos);
    context.setTimeSlice(EXECUTION_TIME_SLICE);
    if (driver.isFinished()) {
      scheduler.runningToFinished(task, context);
      return;
    }

    if (future.isDone()) {
      scheduler.runningToReady(task, context);
    } else {
      scheduler.runningToBlocked(task, context);
      future.addListener(
          () -> {
            try (SetThreadName driverTaskName2 =
                new SetThreadName(task.getDriver().getDriverTaskId().getFullId())) {
              scheduler.blockedToReady(task);
            }
          },
          listeningExecutor);
    }
  }
}
