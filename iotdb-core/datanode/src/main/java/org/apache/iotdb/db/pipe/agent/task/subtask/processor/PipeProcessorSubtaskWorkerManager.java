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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

public class PipeProcessorSubtaskWorkerManager {

  private static final int MAX_THREAD_NUM =
      PipeConfig.getInstance().getPipeSubtaskExecutorMaxThreadNum();
  private static final long PIPE_SUBTASK_EXECUTION_TIMEOUT_MS =
      CommonDescriptor.getInstance().getConfig().getPipeSubtaskExecutionTimeoutMs();

  private final PipeProcessorSubtaskWorker[] workers;

  private final ListenableFuture[] listenableFuture;

  private final AtomicLong scheduledTaskNumber;

  public PipeProcessorSubtaskWorkerManager(
      ListeningExecutorService workerThreadPoolExecutor,
      ScheduledExecutorService timeoutExecutor,
      boolean isCheckTimeOut) {
    workers = new PipeProcessorSubtaskWorker[MAX_THREAD_NUM];
    listenableFuture = new ListenableFuture[MAX_THREAD_NUM];
    for (int i = 0; i < MAX_THREAD_NUM; i++) {
      workers[i] = new PipeProcessorSubtaskWorker();
      listenableFuture[i] = workerThreadPoolExecutor.submit(workers[i]);
    }

    if (isCheckTimeOut) {
      PipeProcessorTimedTimeOutChecker checker =
          new PipeProcessorTimedTimeOutChecker(workers, listenableFuture, workerThreadPoolExecutor);
      timeoutExecutor.submit(checker);
    }

    scheduledTaskNumber = new AtomicLong(0);
  }

  public void schedule(PipeProcessorSubtask pipeProcessorSubtask) {
    workers[(int) (scheduledTaskNumber.getAndIncrement() % MAX_THREAD_NUM)].schedule(
        pipeProcessorSubtask);
  }

  public static class PipeProcessorTimedTimeOutChecker implements Runnable {

    private static final Logger LOGGER =
        LoggerFactory.getLogger(PipeProcessorTimedTimeOutChecker.class);

    final PipeProcessorSubtaskWorker[] workers;
    final ListenableFuture[] listenableFuture;
    final ListeningExecutorService workerThreadPoolExecutor;

    PipeProcessorTimedTimeOutChecker(
        final PipeProcessorSubtaskWorker[] workers,
        final ListenableFuture[] listenableFuture,
        final ListeningExecutorService workerThreadPoolExecutor) {
      this.workers = workers;
      this.listenableFuture = listenableFuture;
      this.workerThreadPoolExecutor = workerThreadPoolExecutor;
    }

    @Override
    public void run() {
      while (true) {
        if (Objects.isNull(workers)) {
          LOGGER.info("Worker thread pool is empty. No workers available for processing.");
        }

        for (int i = 0; i < MAX_THREAD_NUM; i++) {
          PipeProcessorSubtaskWorker worker = workers[i];
          if (Objects.isNull(worker)) {
            LOGGER.info("Worker at index {} is null. Skipping.", i);
            continue;
          }
          for (PipeProcessorSubtask subtask : worker.getAllProcessorSubtasks()) {
            if (Objects.isNull(subtask)) {
              LOGGER.info("Subtask for worker {} is null Skipping.", workers[i]);
              continue;
            }
            synchronized (subtask) {
              if (!subtask.isScheduled()) {
                continue;
              }
              final long currTime = System.currentTimeMillis();
              if (currTime - subtask.getStartRunningTime() <= PIPE_SUBTASK_EXECUTION_TIMEOUT_MS) {
                continue;
              }
              ListenableFuture futures = listenableFuture[i];
              if (Objects.isNull(futures) || futures.isDone()) {
                subtask.markTimeoutStatus(true);
                LOGGER.info(
                    "Future for subtask {}@{} is null or already done. Resubmitting.",
                    subtask.getPipeName(),
                    subtask.getRegionId());
                listenableFuture[i] = workerThreadPoolExecutor.submit(worker);
              }

              subtask.markTimeoutStatus(true);
              // Interrupt a running thread
              futures.cancel(true);
              listenableFuture[i] = workerThreadPoolExecutor.submit(worker);
              LOGGER.info(
                  "Resubmitted worker {} for subtask {}@{} due to timeout.",
                  worker,
                  subtask.getPipeName(),
                  subtask.getRegionId());
            }
          }
        }
        try {
          Thread.sleep(PIPE_SUBTASK_EXECUTION_TIMEOUT_MS / 2);
        } catch (InterruptedException ignored) {
          LOGGER.info("time out check waiting to be interrupted");
        }
      }
    }
  }
}
