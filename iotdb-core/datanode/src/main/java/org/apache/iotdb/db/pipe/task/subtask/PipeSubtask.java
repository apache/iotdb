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

package org.apache.iotdb.db.pipe.task.subtask;

import org.apache.iotdb.commons.exception.pipe.PipeRuntimeCriticalException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeException;
import org.apache.iotdb.db.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.execution.scheduler.PipeSubtaskScheduler;
import org.apache.iotdb.pipe.api.event.Event;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class PipeSubtask
    implements FutureCallback<Boolean>, Callable<Boolean>, AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeSubtask.class);

  // Used for identifying the subtask
  protected final String taskID;

  // For thread pool to execute subtasks
  protected ListeningExecutorService subtaskWorkerThreadPoolExecutor;

  // For controlling the subtask execution
  protected final AtomicBoolean shouldStopSubmittingSelf = new AtomicBoolean(true);
  protected PipeSubtaskScheduler subtaskScheduler;

  // For fail-over
  public static final int MAX_RETRY_TIMES = 5;
  protected final AtomicInteger retryCount = new AtomicInteger(0);
  protected Event lastEvent;

  protected PipeSubtask(String taskID) {
    super();
    this.taskID = taskID;
  }

  public abstract void bindExecutors(
      ListeningExecutorService subtaskWorkerThreadPoolExecutor,
      ExecutorService subtaskCallbackListeningExecutor,
      PipeSubtaskScheduler subtaskScheduler);

  @Override
  public Boolean call() throws Exception {
    boolean hasAtLeastOneEventProcessed = false;

    // If the scheduler allows to schedule, then try to consume an event
    while (subtaskScheduler.schedule()) {
      // If the event is consumed successfully, then continue to consume the next event
      // otherwise, stop consuming
      if (!executeOnce()) {
        break;
      }
      hasAtLeastOneEventProcessed = true;
    }
    // Reset the scheduler to make sure that the scheduler can schedule again
    subtaskScheduler.reset();

    return hasAtLeastOneEventProcessed;
  }

  /**
   * Try to consume an event by the pipe plugin.
   *
   * @return true if the event is consumed successfully, false if no more event can be consumed
   * @throws Exception if any error occurs when consuming the event
   */
  @SuppressWarnings("squid:S112") // Allow to throw Exception
  protected abstract boolean executeOnce() throws Exception;

  @Override
  public void onSuccess(Boolean hasAtLeastOneEventProcessed) {
    retryCount.set(0);
    submitSelf();
  }

  @Override
  public void onFailure(@NotNull Throwable throwable) {
    if (retryCount.get() == 0) {
      LOGGER.warn(
          "Failed to execute subtask {}({}), because of {}. Will retry for {} times.",
          taskID,
          this.getClass().getSimpleName(),
          throwable.getMessage(),
          MAX_RETRY_TIMES,
          throwable);
    }

    if (retryCount.get() < MAX_RETRY_TIMES) {
      retryCount.incrementAndGet();
      LOGGER.warn(
          "Retry executing subtask {}({}), retry count [{}/{}]",
          taskID,
          this.getClass().getSimpleName(),
          retryCount.get(),
          MAX_RETRY_TIMES);
      submitSelf();
    } else {
      final String errorMessage =
          String.format(
              "Failed to execute subtask %s(%s), "
                  + "retry count exceeds the max retry times %d, last exception: %s",
              taskID, this.getClass().getSimpleName(), retryCount.get(), throwable.getMessage());
      LOGGER.warn(errorMessage, throwable);

      if (lastEvent instanceof EnrichedEvent) {
        ((EnrichedEvent) lastEvent)
            .reportException(
                throwable instanceof PipeRuntimeException
                    ? (PipeRuntimeException) throwable
                    : new PipeRuntimeCriticalException(errorMessage));
        LOGGER.warn(
            "The last event is an instance of EnrichedEvent, so the exception is reported. "
                + "Stopping current pipe task {}({}) locally... "
                + "Status shown when query the pipe will be 'STOPPED'. "
                + "Please restart the task by executing 'START PIPE' manually if needed.",
            taskID,
            this.getClass().getSimpleName(),
            throwable);
      } else {
        LOGGER.error(
            "The last event is not an instance of EnrichedEvent, "
                + "so the exception cannot be reported. "
                + "Stopping current pipe task {}({}) locally... "
                + "Status shown when query the pipe will be 'RUNNING' "
                + "instead of 'STOPPED', but the task is actually stopped. "
                + "Please restart the task by executing 'START PIPE' manually if needed.",
            taskID,
            this.getClass().getSimpleName(),
            throwable);
      }

      // Although the pipe task will be stopped, we still don't release the last event here
      // Because we need to keep it for the next retry. If user wants to restart the task,
      // the last event will be processed again. The last event will be released when the task
      // is dropped or the process is running normally.
    }
  }

  public abstract void submitSelf();

  public void allowSubmittingSelf() {
    retryCount.set(0);
    shouldStopSubmittingSelf.set(false);
  }

  /**
   * .
   *
   * @return true if the shouldStopSubmittingSelf state is changed from false to true, false
   *     otherwise
   */
  public boolean disallowSubmittingSelf() {
    return !shouldStopSubmittingSelf.getAndSet(true);
  }

  public boolean isSubmittingSelf() {
    return !shouldStopSubmittingSelf.get();
  }

  @Override
  public synchronized void close() {
    releaseLastEvent();
  }

  protected void releaseLastEvent() {
    if (lastEvent != null) {
      if (lastEvent instanceof EnrichedEvent) {
        ((EnrichedEvent) lastEvent).decreaseReferenceCount(this.getClass().getName());
      }
      lastEvent = null;
    }
  }

  public String getTaskID() {
    return taskID;
  }

  public int getRetryCount() {
    return retryCount.get();
  }
}
