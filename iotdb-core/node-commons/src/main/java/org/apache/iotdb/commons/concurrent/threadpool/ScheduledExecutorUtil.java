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
package org.apache.iotdb.commons.concurrent.threadpool;

import org.apache.iotdb.commons.i18n.CommonMessages;

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class ScheduledExecutorUtil {

  private static final Logger logger = LoggerFactory.getLogger(ScheduledExecutorUtil.class);
  static final long SCHEDULE_TASK_FAILED_LOG_INTERVAL_MS = TimeUnit.MINUTES.toMillis(5);

  /**
   * A safe wrapper method to make sure the exception thrown by the previous running will not affect
   * the next one. Please reference the javadoc of {@link
   * ScheduledExecutorService#scheduleAtFixedRate(Runnable, long, long, TimeUnit)} for more details.
   *
   * @param executor the ScheduledExecutorService instance.
   * @param command same parameter in {@link ScheduledExecutorService#scheduleAtFixedRate(Runnable,
   *     long, long, TimeUnit)}.
   * @param initialDelay same parameter in {@link
   *     ScheduledExecutorService#scheduleAtFixedRate(Runnable, long, long, TimeUnit)}.
   * @param period same parameter in {@link ScheduledExecutorService#scheduleAtFixedRate(Runnable,
   *     long, long, TimeUnit)}.
   * @param unit same parameter in {@link ScheduledExecutorService#scheduleAtFixedRate(Runnable,
   *     long, long, TimeUnit)}.
   * @return the same return value of {@link ScheduledExecutorService#scheduleAtFixedRate(Runnable,
   *     long, long, TimeUnit)}.
   */
  @SuppressWarnings("unsafeThreadSchedule")
  public static ScheduledFuture<?> safelyScheduleAtFixedRate(
      ScheduledExecutorService executor,
      Runnable command,
      long initialDelay,
      long period,
      TimeUnit unit) {
    return scheduleAtFixedRate(executor, command, initialDelay, period, unit, false);
  }

  /**
   * A safe wrapper method to make sure the exception thrown by the previous running will not affect
   * the next one. Please reference the javadoc of {@link
   * ScheduledExecutorService#scheduleWithFixedDelay(Runnable, long, long, TimeUnit)} for more
   * details.
   *
   * @param executor the ScheduledExecutorService instance.
   * @param command same parameter in {@link
   *     ScheduledExecutorService#scheduleWithFixedDelay(Runnable, long, long, TimeUnit)}.
   * @param initialDelay same parameter in {@link
   *     ScheduledExecutorService#scheduleWithFixedDelay(Runnable, long, long, TimeUnit)}.
   * @param delay same parameter in {@link ScheduledExecutorService#scheduleWithFixedDelay(Runnable,
   *     long, long, TimeUnit)}.
   * @param unit same parameter in {@link ScheduledExecutorService#scheduleWithFixedDelay(Runnable,
   *     long, long, TimeUnit)}.
   * @return the same return value of {@link
   *     ScheduledExecutorService#scheduleWithFixedDelay(Runnable, long, long, TimeUnit)}.
   */
  @SuppressWarnings("unsafeThreadSchedule")
  public static ScheduledFuture<?> safelyScheduleWithFixedDelay(
      ScheduledExecutorService executor,
      Runnable command,
      long initialDelay,
      long delay,
      TimeUnit unit) {
    return scheduleWithFixedDelay(executor, command, initialDelay, delay, unit, false);
  }

  /**
   * A wrapper method to have the same semantic with {@link
   * ScheduledExecutorService#scheduleAtFixedRate(Runnable, long, long, TimeUnit)}, except for
   * logging an error log when any uncaught exception happens.
   *
   * @param executor the ScheduledExecutorService instance.
   * @param command same parameter in {@link ScheduledExecutorService#scheduleAtFixedRate(Runnable,
   *     long, long, TimeUnit)}.
   * @param initialDelay same parameter in {@link
   *     ScheduledExecutorService#scheduleAtFixedRate(Runnable, long, long, TimeUnit)}.
   * @param period same parameter in {@link ScheduledExecutorService#scheduleAtFixedRate(Runnable,
   *     long, long, TimeUnit)}.
   * @param unit same parameter in {@link ScheduledExecutorService#scheduleAtFixedRate(Runnable,
   *     long, long, TimeUnit)}.
   * @return the same return value of {@link ScheduledExecutorService#scheduleAtFixedRate(Runnable,
   *     long, long, TimeUnit)}.
   */
  @SuppressWarnings("unsafeThreadSchedule")
  public static ScheduledFuture<?> unsafelyScheduleAtFixedRate(
      ScheduledExecutorService executor,
      Runnable command,
      long initialDelay,
      long period,
      TimeUnit unit) {
    return scheduleAtFixedRate(executor, command, initialDelay, period, unit, true);
  }

  /**
   * A wrapper method to have the same semantic with {@link
   * ScheduledExecutorService#scheduleWithFixedDelay(Runnable, long, long, TimeUnit)}, except for
   * logging an error log when any uncaught exception happens.
   *
   * @param executor the ScheduledExecutorService instance.
   * @param command same parameter in {@link
   *     ScheduledExecutorService#scheduleWithFixedDelay(Runnable, long, long, TimeUnit)}.
   * @param initialDelay same parameter in {@link
   *     ScheduledExecutorService#scheduleWithFixedDelay(Runnable, long, long, TimeUnit)}.
   * @param delay same parameter in {@link ScheduledExecutorService#scheduleWithFixedDelay(Runnable,
   *     long, long, TimeUnit)}.
   * @param unit same parameter in {@link ScheduledExecutorService#scheduleWithFixedDelay(Runnable,
   *     long, long, TimeUnit)}.
   * @return the same return value of {@link
   *     ScheduledExecutorService#scheduleWithFixedDelay(Runnable, long, long, TimeUnit)}.
   */
  @SuppressWarnings("unsafeThreadSchedule")
  public static ScheduledFuture<?> unsafelyScheduleWithFixedDelay(
      ScheduledExecutorService executor,
      Runnable command,
      long initialDelay,
      long delay,
      TimeUnit unit) {
    return scheduleWithFixedDelay(executor, command, initialDelay, delay, unit, true);
  }

  @SuppressWarnings("unsafeThreadSchedule")
  private static ScheduledFuture<?> scheduleAtFixedRate(
      ScheduledExecutorService executor,
      Runnable command,
      long initialDelay,
      long period,
      TimeUnit unit,
      boolean unsafe) {
    return executor.scheduleAtFixedRate(
        wrapScheduleCommand(command, unsafe), initialDelay, period, unit);
  }

  @SuppressWarnings("unsafeThreadSchedule")
  private static ScheduledFuture<?> scheduleWithFixedDelay(
      ScheduledExecutorService executor,
      Runnable command,
      long initialDelay,
      long delay,
      TimeUnit unit,
      boolean unsafe) {
    return executor.scheduleWithFixedDelay(
        wrapScheduleCommand(command, unsafe), initialDelay, delay, unit);
  }

  private static Runnable wrapScheduleCommand(Runnable command, boolean unsafe) {
    FailureLogState failureLogState = new FailureLogState();
    return () -> {
      try {
        command.run();
        failureLogState.clear();
      } catch (Throwable t) {
        if (shouldLogFailure(failureLogState, t, System.currentTimeMillis())) {
          logger.error(CommonMessages.SCHEDULE_TASK_FAILED, t);
        }
        if (unsafe) {
          throw t;
        }
      }
    };
  }

  static boolean shouldLogFailure(
      FailureLogState failureLogState, Throwable failure, long currentTime) {
    return failureLogState.shouldLog(getFailureSignature(failure), currentTime);
  }

  private static String getFailureSignature(Throwable failure) {
    StringBuilder builder = new StringBuilder(failure.getClass().getName());
    if (failure.getMessage() != null) {
      builder.append(':').append(failure.getMessage());
    }
    Throwable cause = failure.getCause();
    if (cause != null) {
      builder.append(";cause=").append(cause.getClass().getName());
      if (cause.getMessage() != null) {
        builder.append(':').append(cause.getMessage());
      }
    }
    return builder.toString();
  }

  static class FailureLogState {

    private String lastFailureSignature;
    private long nextLogTime;

    private boolean shouldLog(String failureSignature, long currentTime) {
      if (!failureSignature.equals(lastFailureSignature) || currentTime >= nextLogTime) {
        lastFailureSignature = failureSignature;
        nextLogTime = currentTime + SCHEDULE_TASK_FAILED_LOG_INTERVAL_MS;
        return true;
      }
      return false;
    }

    void clear() {
      lastFailureSignature = null;
      nextLogTime = 0;
    }
  }

  public static RuntimeException propagate(Throwable throwable) {
    logger.error(CommonMessages.RUN_THREAD_FAILED, throwable);
    Throwables.throwIfUnchecked(throwable);
    throw new RuntimeException(throwable);
  }
}
