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

package org.apache.iotdb.commons.pipe.task.subtask;

import org.apache.iotdb.commons.exception.pipe.PipeRuntimeConnectorRetryTimesConfigurableException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeCriticalException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeException;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public abstract class PipeReportableSubtask extends PipeSubtask {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeReportableSubtask.class);

  protected PipeReportableSubtask(String taskID, long creationTime) {
    super(taskID, creationTime);
  }

  @Override
  public synchronized void onFailure(Throwable throwable) {
    if (isClosed.get()) {
      LOGGER.info("onFailure in pipe subtask, ignored because pipe is dropped.", throwable);
      releaseLastEvent(false);
      return;
    }

<<<<<<< HEAD:iotdb-core/node-commons/src/main/java/org/apache/iotdb/commons/pipe/task/subtask/PipeReportableSubtask.java
    int maxRetryTimes =
        throwable instanceof PipeRuntimeConnectorRetryTimesConfigurableException
            ? ((PipeRuntimeConnectorRetryTimesConfigurableException) throwable).getRetryTimes()
            : MAX_RETRY_TIMES;

=======
    if (lastEvent instanceof EnrichedEvent) {
      onEnrichedEventFailure(throwable);
    } else {
      onNonEnrichedEventFailure(throwable);
    }

    // Although the pipe task will be stopped, we still don't release the last event here
    // Because we need to keep it for the next retry. If user wants to restart the task,
    // the last event will be processed again. The last event will be released when the task
    // is dropped or the process is running normally.
  }

  private void onEnrichedEventFailure(@NotNull Throwable throwable) {
>>>>>>> 6943524b000217bf6d4678b51097f93cfedad8f3:iotdb-core/datanode/src/main/java/org/apache/iotdb/db/pipe/task/subtask/PipeDataNodeSubtask.java
    if (retryCount.get() == 0) {
      LOGGER.warn(
          "Failed to execute subtask {} (creation time: {}, simple class: {}), because of {}. Will retry for {} times.",
          taskID,
          creationTime,
          this.getClass().getSimpleName(),
          throwable.getMessage(),
          maxRetryTimes,
          throwable);
    }

    if (retryCount.get() < maxRetryTimes) {
      retryCount.incrementAndGet();
      LOGGER.warn(
          "Retry executing subtask {} (creation time: {}, simple class: {}), retry count [{}/{}]",
          taskID,
          creationTime,
          this.getClass().getSimpleName(),
          retryCount.get(),
          maxRetryTimes);
      try {
        Thread.sleep(1000L * retryCount.get());
      } catch (InterruptedException e) {
        LOGGER.warn(
            "Interrupted when retrying to execute subtask {} (creation time: {}, simple class: {})",
            taskID,
            creationTime,
            this.getClass().getSimpleName(),
            e);
        Thread.currentThread().interrupt();
      }

      submitSelf();
    } else {
      String errorMessage =
          String.format(
<<<<<<< HEAD:iotdb-core/node-commons/src/main/java/org/apache/iotdb/commons/pipe/task/subtask/PipeReportableSubtask.java
              "Failed to execute subtask %s(%s), "
                  + "retry count exceeds the max retry times %d, last exception: %s",
              taskID, this.getClass().getSimpleName(), retryCount.get(), throwable.getMessage());
      String rootCause = getRootCause(throwable);
      if (Objects.nonNull(rootCause)) {
        errorMessage += String.format(", root cause: %s", rootCause);
      }

      LOGGER.warn(errorMessage, throwable);

      if (lastEvent instanceof EnrichedEvent) {
        report(
            ((EnrichedEvent) lastEvent),
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

  protected abstract String getRootCause(Throwable throwable);

  protected abstract void report(EnrichedEvent event, PipeRuntimeException exception);
=======
              "Failed to execute subtask %s (creation time: %s, simple class: %s), "
                  + "retry count exceeds the max retry times %d, last exception: %s, root cause: %s",
              taskID,
              creationTime,
              this.getClass().getSimpleName(),
              retryCount.get(),
              throwable.getMessage(),
              ErrorHandlingUtils.getRootCause(throwable).getMessage());
      LOGGER.warn(errorMessage, throwable);
      ((EnrichedEvent) lastEvent)
          .reportException(
              throwable instanceof PipeRuntimeException
                  ? (PipeRuntimeException) throwable
                  : new PipeRuntimeCriticalException(errorMessage));
      LOGGER.warn(
          "The last event is an instance of EnrichedEvent, so the exception is reported. "
              + "Stopping current pipe subtask {} (creation time: {}, simple class: {}) locally... "
              + "Status shown when query the pipe will be 'STOPPED'. "
              + "Please restart the task by executing 'START PIPE' manually if needed.",
          taskID,
          creationTime,
          this.getClass().getSimpleName(),
          throwable);
    }
  }

  private void onNonEnrichedEventFailure(@NotNull Throwable throwable) {
    if (retryCount.get() == 0) {
      LOGGER.warn(
          "Failed to execute subtask {} (creation time: {}, simple class: {}), "
              + "because of {}. Will retry forever.",
          taskID,
          creationTime,
          this.getClass().getSimpleName(),
          throwable.getMessage(),
          throwable);
    }

    retryCount.incrementAndGet();
    LOGGER.warn(
        "Retry executing subtask {} (creation time: {}, simple class: {}), retry count {}",
        taskID,
        creationTime,
        this.getClass().getSimpleName(),
        retryCount.get());
    try {
      Thread.sleep(Math.min(1000L * retryCount.get(), 10000));
    } catch (InterruptedException e) {
      LOGGER.warn(
          "Interrupted when retrying to execute subtask {} (creation time: {}, simple class: {})",
          taskID,
          creationTime,
          this.getClass().getSimpleName());
      Thread.currentThread().interrupt();
    }

    submitSelf();
  }

  @Override
  protected synchronized void releaseLastEvent(boolean shouldReport) {
    if (lastEvent != null) {
      if (lastEvent instanceof EnrichedEvent) {
        ((EnrichedEvent) lastEvent).decreaseReferenceCount(this.getClass().getName(), shouldReport);
      }
      lastEvent = null;
    }
  }
>>>>>>> 6943524b000217bf6d4678b51097f93cfedad8f3:iotdb-core/datanode/src/main/java/org/apache/iotdb/db/pipe/task/subtask/PipeDataNodeSubtask.java
}
