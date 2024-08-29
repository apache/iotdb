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

public abstract class PipeReportableSubtask extends PipeSubtask {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeReportableSubtask.class);

  protected PipeReportableSubtask(final String taskID, final long creationTime) {
    super(taskID, creationTime);
  }

  @Override
  public synchronized void onFailure(final Throwable throwable) {
    if (isClosed.get()) {
      LOGGER.info("onFailure in pipe subtask, ignored because pipe is dropped.", throwable);
      clearReferenceCountAndReleaseLastEvent(null);
      return;
    }

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

  private void onEnrichedEventFailure(final Throwable throwable) {
    final int maxRetryTimes =
        throwable instanceof PipeRuntimeConnectorRetryTimesConfigurableException
            ? ((PipeRuntimeConnectorRetryTimesConfigurableException) throwable).getRetryTimes()
            : MAX_RETRY_TIMES;

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

    retryCount.incrementAndGet();
    if (retryCount.get() <= maxRetryTimes) {
      LOGGER.warn(
          "Retry executing subtask {} (creation time: {}, simple class: {}), retry count [{}/{}], last exception: {}",
          taskID,
          creationTime,
          this.getClass().getSimpleName(),
          retryCount.get(),
          maxRetryTimes,
          throwable.getMessage(),
          throwable);
      try {
        Thread.sleep(Math.min(1000L * retryCount.get(), 10000));
      } catch (final InterruptedException e) {
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
      final String errorMessage =
          String.format(
              "Failed to execute subtask %s (creation time: %s, simple class: %s), "
                  + "retry count exceeds the max retry times %d, last exception: %s, root cause: %s",
              taskID,
              creationTime,
              this.getClass().getSimpleName(),
              retryCount.get() - 1,
              throwable.getMessage(),
              getRootCause(throwable));
      LOGGER.warn(errorMessage, throwable);
      report(
          (EnrichedEvent) lastEvent,
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

  protected abstract String getRootCause(final Throwable throwable);

  protected abstract void report(final EnrichedEvent event, final PipeRuntimeException exception);

  private void onNonEnrichedEventFailure(final Throwable throwable) {
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
        "Retry executing subtask {} (creation time: {}, simple class: {}), retry count {}, last exception: {}",
        taskID,
        creationTime,
        this.getClass().getSimpleName(),
        retryCount.get(),
        throwable.getMessage(),
        throwable);
    try {
      Thread.sleep(Math.min(1000L * retryCount.get(), 10000));
    } catch (final InterruptedException e) {
      LOGGER.warn(
          "Interrupted when retrying to execute subtask {} (creation time: {}, simple class: {})",
          taskID,
          creationTime,
          this.getClass().getSimpleName());
      Thread.currentThread().interrupt();
    }

    submitSelf();
  }
}
