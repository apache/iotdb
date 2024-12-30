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

package org.apache.iotdb.db.pipe.agent.task.subtask.processor;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.pipe.config.PipeConfig;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

public class PipeProcessorSubtaskWorkerManager {

  private static final int MAX_THREAD_NUM =
      PipeConfig.getInstance().getPipeSubtaskExecutorMaxThreadNum();
  private static final long EXECUTOR_PRE_SUBTASK_TIMEOUT_MS =
      CommonDescriptor.getInstance().getConfig().getExecutorPreSubtaskTimeoutMs();

  private final PipeProcessorSubtaskWorker[] workers;

  private final ListenableFuture[] listenableFuture;

  private final AtomicLong scheduledTaskNumber;

  public PipeProcessorSubtaskWorkerManager(
      ListeningExecutorService workerThreadPoolExecutor, ScheduledExecutorService timeoutExecutor) {
    workers = new PipeProcessorSubtaskWorker[MAX_THREAD_NUM];
    listenableFuture = new ListenableFuture[MAX_THREAD_NUM];
    for (int i = 0; i < MAX_THREAD_NUM; i++) {
      workers[i] = new PipeProcessorSubtaskWorker();
      listenableFuture[i] = workerThreadPoolExecutor.submit(workers[i]);
    }

    PipeProcessorTimedTimeOutChecker checker =
        new PipeProcessorTimedTimeOutChecker(workers, listenableFuture, workerThreadPoolExecutor);

    scheduledTaskNumber = new AtomicLong(0);
  }

  public void schedule(PipeProcessorSubtask pipeProcessorSubtask) {
    workers[(int) (scheduledTaskNumber.getAndIncrement() % MAX_THREAD_NUM)].schedule(
        pipeProcessorSubtask);
  }

  public static class PipeProcessorTimedTimeOutChecker implements Runnable {

    private static final Logger LOGGER =
        Logger.getLogger(PipeProcessorTimedTimeOutChecker.class.getName());

    PipeProcessorSubtaskWorker[] workers;
    ListenableFuture[] listenableFuture;
    ListeningExecutorService workerThreadPoolExecutor;

    PipeProcessorTimedTimeOutChecker(
        PipeProcessorSubtaskWorker[] workers,
        ListenableFuture[] listenableFuture,
        ListeningExecutorService workerThreadPoolExecutor) {
      this.workers = workers;
      this.listenableFuture = listenableFuture;
      this.workerThreadPoolExecutor = workerThreadPoolExecutor;
    }

    @Override
    public void run() {
      while (true) {
        int minTimeIndex = 0;
        long minTime =
            workers[0] != null ? workers[0].getLastRunningTime() : System.currentTimeMillis();

        for (int i = 1; i < workers.length; i++) {
          final PipeProcessorSubtaskWorker w = workers[i];
          if (w != null && w.getLastRunningTime() < minTime) {
            minTime = w.getLastRunningTime();
            minTimeIndex = i;
          }
        }

        try {
          Thread.sleep(EXECUTOR_PRE_SUBTASK_TIMEOUT_MS - (System.currentTimeMillis() - minTime));

          // Check if the worker has exceeded the timeout and hasn't processed the next task
          if (workers[minTimeIndex] == null
              || workers[minTimeIndex].getLastRunningTime() != minTime) {
            continue;
          }

          // If cancel is successful, it means the thread was interrupted and needs to be
          // resubmitted to the thread pool
          if (listenableFuture[minTimeIndex] == null
              || !listenableFuture[minTimeIndex].cancel(true)) {
            continue;
          }

          if (listenableFuture[minTimeIndex].isDone()) {
            listenableFuture[minTimeIndex] = workerThreadPoolExecutor.submit(workers[minTimeIndex]);
          }

        } catch (InterruptedException e) {
          // The thread was interrupted, log the exception
          LOGGER.warning("Thread was interrupted: " + e.getMessage());
          Thread.currentThread().interrupt(); // Reset the interrupt status
        } catch (Exception e) {
          // Log any other exceptions that occur
          LOGGER.warning("An exception occurred  " + e.getMessage());
        }
      }
    }
  }
}
