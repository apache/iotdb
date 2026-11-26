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

package org.apache.iotdb.commons.pipe.agent.task.subtask;

import org.apache.iotdb.commons.exception.pipe.IoTConsensusV2RetryWithIncreasingIntervalException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeCriticalException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeSinkNonReportTimeConfigurableException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeSinkRetryTimesConfigurableException;
import org.apache.iotdb.commons.i18n.PipeMessages;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.resource.log.PipeLogger;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

public abstract class PipeReportableSubtask extends PipeSubtask {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeReportableSubtask.class);
  private static final long DEFAULT_LOGIN_LOCK_WINDOW_MS = TimeUnit.MINUTES.toMillis(10);
  private static final int DEFAULT_LOGIN_LOCK_FAILED_ATTEMPTS = 5;
  private static final int AUTHENTICATION_FAILURE_IMMEDIATE_ATTEMPTS = 2;
  protected static final long AUTHENTICATION_FAILURE_RETRY_INTERVAL_MS =
      DEFAULT_LOGIN_LOCK_WINDOW_MS
              / (DEFAULT_LOGIN_LOCK_FAILED_ATTEMPTS - AUTHENTICATION_FAILURE_IMMEDIATE_ATTEMPTS)
          + TimeUnit.SECONDS.toMillis(1);
  private static final Pattern AUTHENTICATION_FAILURE_STATUS_CODE_PATTERN =
      Pattern.compile(
          String.format(
              "(?i)(?:\\b(?:code|status code)\\s*[:=]\\s*(?:%d|%d)\\b|\\b(?:%d|%d):|\\b(?:WRONG_LOGIN_PASSWORD|USER_LOGIN_LOCKED)\\b)",
              TSStatusCode.WRONG_LOGIN_PASSWORD.getStatusCode(),
              TSStatusCode.USER_LOGIN_LOCKED.getStatusCode(),
              TSStatusCode.WRONG_LOGIN_PASSWORD.getStatusCode(),
              TSStatusCode.USER_LOGIN_LOCKED.getStatusCode()));
  // To ensure that high-priority tasks can obtain object locks first, a counter is now used to save
  // the number of high-priority tasks.
  protected final AtomicLong highPriorityLockTaskCount = new AtomicLong(0);

  protected PipeReportableSubtask(final String taskID, final long creationTime) {
    super(taskID, creationTime);
  }

  @Override
  public synchronized void onFailure(final Throwable throwable) {
    if (isClosed.get()) {
      LOGGER.info(PipeMessages.ON_FAILURE_IGNORED_PIPE_DROPPED, throwable);
      clearReferenceCountAndReleaseLastEvent(null);
      return;
    }

    if (lastEvent instanceof EnrichedEvent
        && !(throwable instanceof PipeRuntimeSinkNonReportTimeConfigurableException)) {
      onReportEventFailure(throwable);
    } else {
      onNonReportEventFailure(throwable);
    }

    // Although the pipe task will be stopped, we still don't release the last event here
    // Because we need to keep it for the next retry. If user wants to restart the task,
    // the last event will be processed again. The last event will be released when the task
    // is dropped or the process is running normally.
  }

  protected long getSleepIntervalBasedOnThrowable(final Throwable throwable) {
    long sleepInterval = Math.min(1000L * retryCount.get(), 10000);
    // if receiver is read-only/internal-error/write-reject, connector will retry with
    // power-increasing interval
    if (throwable instanceof IoTConsensusV2RetryWithIncreasingIntervalException) {
      if (retryCount.get() >= 5) {
        sleepInterval = 1000L * 20;
      } else {
        sleepInterval = 1000L * retryCount.get() * retryCount.get();
      }
    }
    if (isAuthenticationFailure(throwable)) {
      sleepInterval = Math.max(sleepInterval, AUTHENTICATION_FAILURE_RETRY_INTERVAL_MS);
    }
    return sleepInterval;
  }

  protected static boolean isAuthenticationFailure(final Throwable throwable) {
    Throwable current = throwable;
    while (current != null) {
      final String message = current.getMessage();
      if (message != null && AUTHENTICATION_FAILURE_STATUS_CODE_PATTERN.matcher(message).find()) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  private void onReportEventFailure(final Throwable throwable) {
    final int maxRetryTimes =
        throwable instanceof PipeRuntimeSinkRetryTimesConfigurableException
            ? ((PipeRuntimeSinkRetryTimesConfigurableException) throwable).getRetryTimes()
            : MAX_RETRY_TIMES;

    if (retryCount.get() == 0) {
      LOGGER.warn(
          PipeMessages.FAILED_TO_EXECUTE_SUBTASK,
          getDisplayTaskID(),
          creationTime,
          this.getClass().getSimpleName(),
          throwable.getMessage(),
          maxRetryTimes,
          throwable);
    }

    retryCount.incrementAndGet();
    if (retryCount.get() <= maxRetryTimes) {
      PipeLogger.log(
          LOGGER::warn,
          throwable,
          PipeMessages.RETRY_EXECUTING_SUBTASK,
          getDisplayTaskID(),
          creationTime,
          this.getClass().getSimpleName(),
          retryCount.get(),
          maxRetryTimes,
          throwable.getMessage());
      try {
        sleepIfNoHighPriorityTask(getSleepIntervalBasedOnThrowable(throwable));
      } catch (final InterruptedException e) {
        LOGGER.warn(
            PipeMessages.INTERRUPTED_RETRYING_SUBTASK,
            getDisplayTaskID(),
            creationTime,
            this.getClass().getSimpleName(),
            e);
        Thread.currentThread().interrupt();
      }

      submitSelf();
    } else {
      final String errorMessage =
          String.format(
              PipeMessages.SUBTASK_RETRY_EXCEEDED_FORMAT,
              getDisplayTaskID(),
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
          PipeMessages.SUBTASK_EXCEPTION_REPORTED,
          getDisplayTaskID(),
          creationTime,
          this.getClass().getSimpleName(),
          throwable);
    }
  }

  protected abstract String getRootCause(final Throwable throwable);

  protected abstract void report(final EnrichedEvent event, final PipeRuntimeException exception);

  private void onNonReportEventFailure(final Throwable throwable) {
    if (retryCount.get() == 0) {
      LOGGER.warn(
          PipeMessages.FAILED_TO_EXECUTE_SUBTASK_RETRY_FOREVER,
          getDisplayTaskID(),
          creationTime,
          this.getClass().getSimpleName(),
          throwable.getMessage(),
          throwable);
    }

    retryCount.incrementAndGet();
    PipeLogger.log(
        LOGGER::warn,
        PipeMessages.RETRY_EXECUTING_SUBTASK_FOREVER,
        getDisplayTaskID(),
        creationTime,
        this.getClass().getSimpleName(),
        retryCount.get(),
        throwable.getMessage(),
        throwable);
    try {
      sleepIfNoHighPriorityTask(getSleepIntervalBasedOnThrowable(throwable));
    } catch (final InterruptedException e) {
      LOGGER.warn(
          PipeMessages.INTERRUPTED_RETRYING_SUBTASK,
          getDisplayTaskID(),
          creationTime,
          this.getClass().getSimpleName());
      Thread.currentThread().interrupt();
    }

    submitSelf();
  }

  protected void preScheduleLowPriorityTask(int maxRetries) {
    while (highPriorityLockTaskCount.get() != 0L && maxRetries-- > 0) {
      try {
        // Introduce a short delay to avoid CPU spinning
        Thread.sleep(10);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.warn(PipeMessages.INTERRUPTED_WAITING_HIGH_PRIORITY, e);
        break;
      }
    }
  }

  protected void sleepIfNoHighPriorityTask(long sleepMillis) throws InterruptedException {
    if (sleepMillis <= 0) {
      return;
    }
    synchronized (highPriorityLockTaskCount) {
      // The wait operation will release the highPriorityLockTaskCount lock, so there will be
      // no deadlock.
      if (highPriorityLockTaskCount.get() == 0) {
        highPriorityLockTaskCount.wait(sleepMillis);
      }
    }
  }

  public void increaseHighPriorityTaskCount() {
    highPriorityLockTaskCount.incrementAndGet();
    synchronized (highPriorityLockTaskCount) {
      highPriorityLockTaskCount.notifyAll();
    }
  }

  public void decreaseHighPriorityTaskCount() {
    highPriorityLockTaskCount.decrementAndGet();
  }
}
