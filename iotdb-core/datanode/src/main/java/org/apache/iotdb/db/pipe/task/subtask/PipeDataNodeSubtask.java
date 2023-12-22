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
import org.apache.iotdb.commons.pipe.task.subtask.PipeSubtask;
import org.apache.iotdb.db.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.utils.ErrorHandlingUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;

public abstract class PipeDataNodeSubtask extends PipeSubtask {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeDataNodeSubtask.class);

  protected PipeDataNodeSubtask(String taskID, long creationTime) {
    super(taskID, creationTime);
  }

  @Override
  public synchronized void onFailure(@NotNull Throwable throwable) {
    if (isClosed.get()) {
      LOGGER.info("onFailure in pipe subtask, ignored because pipe is dropped.");
      releaseLastEvent(false);
      return;
    }

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
      try {
        Thread.sleep(1000L * retryCount.get());
      } catch (InterruptedException e) {
        LOGGER.warn(
            "Interrupted when retrying to execute subtask {}({})",
            taskID,
            this.getClass().getSimpleName());
        Thread.currentThread().interrupt();
      }

      submitSelf();
    } else {
      final String errorMessage =
          String.format(
              "Failed to execute subtask %s(%s), "
                  + "retry count exceeds the max retry times %d, last exception: %s, root cause: %s",
              taskID,
              this.getClass().getSimpleName(),
              retryCount.get(),
              throwable.getMessage(),
              ErrorHandlingUtils.getRootCause(throwable).getMessage());
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

  @Override
  protected synchronized void releaseLastEvent(boolean shouldReport) {
    if (lastEvent != null) {
      if (lastEvent instanceof EnrichedEvent) {
        ((EnrichedEvent) lastEvent).decreaseReferenceCount(this.getClass().getName(), shouldReport);
      }
      lastEvent = null;
    }
  }
}
