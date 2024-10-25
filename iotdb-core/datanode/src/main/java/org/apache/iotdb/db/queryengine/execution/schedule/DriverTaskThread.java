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

package org.apache.iotdb.db.queryengine.execution.schedule;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.execution.driver.IDriver;
import org.apache.iotdb.db.queryengine.execution.schedule.queue.IndexedBlockingQueue;
import org.apache.iotdb.db.queryengine.execution.schedule.queue.multilevelqueue.MultilevelPriorityQueue;
import org.apache.iotdb.db.queryengine.execution.schedule.task.DriverTask;
import org.apache.iotdb.db.utils.SetThreadName;

import com.google.common.base.Ticker;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

/** The worker thread of {@link DriverTask}. */
public class DriverTaskThread extends AbstractDriverThread {

  private static final double DRIVER_TASK_EXECUTION_TIME_SLICE_IN_MS =
      IoTDBDescriptor.getInstance().getConfig().getDriverTaskExecutionTimeSliceInMs();

  /**
   * In multi-level feedback queue, levels with lower priority have longer time slices. Currently,
   * we assign (level + 1) * TimeSliceUnit to each level.
   */
  private static final Duration[] TIME_SLICE_FOR_EACH_LEVEL =
      IntStream.range(0, MultilevelPriorityQueue.getNumOfPriorityLevels())
          .mapToObj(
              level ->
                  new Duration(
                      (level + 1) * DRIVER_TASK_EXECUTION_TIME_SLICE_IN_MS, TimeUnit.MILLISECONDS))
          .toArray(Duration[]::new);

  // We manage thread pool size directly, so create an unlimited pool
  private static final Executor listeningExecutor =
      IoTDBThreadPoolFactory.newCachedThreadPool(
          ThreadName.DRIVER_TASK_SCHEDULER_NOTIFICATION.getName());

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
    // Try to switch it to RUNNING
    if (!scheduler.readyToRunning(task)) {
      return;
    }
    IDriver driver = task.getDriver();
    Duration timeSlice = getExecutionTimeSliceForDriverTask(task);
    ListenableFuture<?> future = driver.processFor(timeSlice);
    // If the future is cancelled, the task is in an error and should be thrown.
    if (future.isCancelled()) {
      task.setAbortCause(
          new DriverTaskAbortedException(
              task.getDriverTaskId().getFullId(),
              DriverTaskAbortedException.BY_ALREADY_BEING_CANCELLED));
      scheduler.toAborted(task);
      return;
    }
    long quantaScheduledNanos = ticker.read() - startNanos;
    ExecutionContext context = new ExecutionContext();
    context.setScheduledTimeInNanos(quantaScheduledNanos);
    context.setTimeSlice(timeSlice);
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

  private Duration getExecutionTimeSliceForDriverTask(DriverTask driverTask) {
    if (driverTask.isHighestPriority()) {
      // highestPriorityTask has the same time slice as level0 task
      return TIME_SLICE_FOR_EACH_LEVEL[0];
    }
    // no need to check whether the level of DriverTask is out of bound since it has been checked
    // when DriverTask is polled out from MultiLevelPriorityQueue
    return TIME_SLICE_FOR_EACH_LEVEL[driverTask.getPriority().getLevel()];
  }
}
