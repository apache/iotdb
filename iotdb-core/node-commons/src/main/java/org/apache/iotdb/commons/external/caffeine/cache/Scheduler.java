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

package org.apache.iotdb.commons.external.caffeine.cache;

import org.apache.iotdb.commons.i18n.CommonMessages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

/**
 * A scheduler that submits a task to an executor after a given delay.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
@FunctionalInterface
@SuppressWarnings("all")
public interface Scheduler {

  /**
   * Returns a future that will submit the task to the executor after the given delay.
   *
   * @param executor the executor to run the task
   * @param command the runnable task to schedule
   * @param delay how long to delay, in units of {@code unit}
   * @param unit a {@code TimeUnit} determining how to interpret the {@code delay} parameter
   * @return a scheduled future representing the pending submission of the task
   */
  Future<?> schedule(Executor executor, Runnable command, long delay, TimeUnit unit);

  /**
   * Returns a scheduler that always returns a successfully completed future.
   *
   * @return a scheduler that always returns a successfully completed future
   */
  static Scheduler disabledScheduler() {
    return DisabledScheduler.INSTANCE;
  }

  /**
   * Returns a scheduler that uses the system-wide scheduling thread by using {@link
   * CompletableFuture#delayedExecutor}.
   *
   * @return a scheduler that uses the system-wide scheduling thread
   */
  static Scheduler systemScheduler() {
    return SystemScheduler.INSTANCE;
  }

  /**
   * Returns a scheduler that delegates to a {@link ScheduledExecutorService}.
   *
   * <p>Note that this implementation will ignore scheduling the task if the executor was shutdown
   * or the submission was rejected. Consider implementing your own adapter if different behavior is
   * required.
   *
   * @param scheduledExecutorService the executor to schedule on
   * @return a scheduler that delegates to a {@link ScheduledExecutorService}
   */
  static Scheduler forScheduledExecutorService(ScheduledExecutorService scheduledExecutorService) {
    return new ExecutorServiceScheduler(scheduledExecutorService);
  }

  /**
   * Returns a scheduler that suppresses and logs any exception thrown by the delegate {@code
   * scheduler}.
   *
   * @param scheduler the scheduler to delegate to
   * @return a scheduler that suppresses and logs any exception thrown by the delegate
   */
  static Scheduler guardedScheduler(Scheduler scheduler) {
    return (scheduler instanceof GuardedScheduler) ? scheduler : new GuardedScheduler(scheduler);
  }
}

@SuppressWarnings("all")
enum SystemScheduler implements Scheduler {
  INSTANCE;

  @Override
  public Future<?> schedule(Executor executor, Runnable command, long delay, TimeUnit unit) {
    Executor delayedExecutor = CompletableFuture.delayedExecutor(delay, unit, executor);
    return CompletableFuture.runAsync(command, delayedExecutor);
  }
}

@SuppressWarnings("all")
final class ExecutorServiceScheduler implements Scheduler, Serializable {
  private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorServiceScheduler.class);
  private static final long serialVersionUID = 1;

  @SuppressWarnings("serial")
  final ScheduledExecutorService scheduledExecutorService;

  ExecutorServiceScheduler(ScheduledExecutorService scheduledExecutorService) {
    this.scheduledExecutorService = requireNonNull(scheduledExecutorService);
  }

  @Override
  public Future<?> schedule(Executor executor, Runnable command, long delay, TimeUnit unit) {
    requireNonNull(executor);
    requireNonNull(command);
    requireNonNull(unit);

    if (scheduledExecutorService.isShutdown()) {
      return DisabledFuture.instance();
    }
    return scheduledExecutorService.schedule(
        () -> {
          try {
            executor.execute(command);
          } catch (Throwable t) {
            LOGGER.warn(
                CommonMessages.LOG_EXCEPTION_THROWN_WHEN_SUBMITTING_SCHEDULED_TASK_78E45BDD, t);
            throw t;
          }
        },
        delay,
        unit);
  }
}

@SuppressWarnings("all")
final class GuardedScheduler implements Scheduler, Serializable {
  private static final Logger LOGGER = LoggerFactory.getLogger(GuardedScheduler.class);
  private static final long serialVersionUID = 1;

  @SuppressWarnings("serial")
  final Scheduler delegate;

  GuardedScheduler(Scheduler delegate) {
    this.delegate = requireNonNull(delegate);
  }

  @Override
  @SuppressWarnings("ConstantValue")
  public Future<?> schedule(Executor executor, Runnable command, long delay, TimeUnit unit) {
    try {
      var future = delegate.schedule(executor, command, delay, unit);
      return (future == null) ? DisabledFuture.instance() : future;
    } catch (Throwable t) {
      LOGGER.warn(CommonMessages.LOG_EXCEPTION_THROWN_BY_SCHEDULER_DISCARDED_TASK_A219ACAC, t);
      return DisabledFuture.instance();
    }
  }
}

@SuppressWarnings("all")
enum DisabledScheduler implements Scheduler {
  INSTANCE;

  @Override
  public Future<?> schedule(Executor executor, Runnable command, long delay, TimeUnit unit) {
    requireNonNull(executor);
    requireNonNull(command);
    requireNonNull(unit);
    return DisabledFuture.instance();
  }
}

@SuppressWarnings("all")
enum DisabledFuture implements Future<Void> {
  INSTANCE;

  static Future<?> instance() {
    return INSTANCE;
  }

  @Override
  public boolean isDone() {
    return true;
  }

  @Override
  public boolean isCancelled() {
    return false;
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    return false;
  }

  @Override
  public Void get(long timeout, TimeUnit unit) {
    requireNonNull(unit);
    return null;
  }

  @Override
  public Void get() {
    return null;
  }
}
