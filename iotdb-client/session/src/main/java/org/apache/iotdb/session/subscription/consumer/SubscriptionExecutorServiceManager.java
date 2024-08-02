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

package org.apache.iotdb.session.subscription.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public final class SubscriptionExecutorServiceManager {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SubscriptionExecutorServiceManager.class);

  private static final long AWAIT_TERMINATION_TIMEOUT_MS = 15_000L;

  private static final String CONTROL_FLOW_EXECUTOR_NAME = "SubscriptionControlFlowExecutor";
  private static final String UPSTREAM_DATA_FLOW_EXECUTOR_NAME =
      "SubscriptionUpstreamDataFlowExecutor";
  private static final String DOWNSTREAM_DATA_FLOW_EXECUTOR_NAME =
      "SubscriptionDownstreamDataFlowExecutor";

  /** Control Flow Executor: execute heartbeat worker, endpoints syncer and auto poll worker */
  private static final SubscriptionScheduledExecutorService CONTROL_FLOW_EXECUTOR =
      new SubscriptionScheduledExecutorService(
          CONTROL_FLOW_EXECUTOR_NAME, Math.max(Runtime.getRuntime().availableProcessors() / 2, 1));

  /** Upstream Data Flow Executor: execute auto commit worker and async commit worker */
  private static final SubscriptionScheduledExecutorService UPSTREAM_DATA_FLOW_EXECUTOR =
      new SubscriptionScheduledExecutorService(
          UPSTREAM_DATA_FLOW_EXECUTOR_NAME,
          Math.max(Runtime.getRuntime().availableProcessors() / 2, 1));

  /** Downstream Data Flow Executor: execute poll task */
  private static final SubscriptionExecutorService DOWNSTREAM_DATA_FLOW_EXECUTOR =
      new SubscriptionExecutorService(
          DOWNSTREAM_DATA_FLOW_EXECUTOR_NAME,
          Math.max(Runtime.getRuntime().availableProcessors(), 1));

  /////////////////////////////// set core pool size ///////////////////////////////

  public static void setControlFlowExecutorCorePoolSize(final int corePoolSize) {
    CONTROL_FLOW_EXECUTOR.setCorePoolSize(corePoolSize);
  }

  public static void setUpstreamDataFlowExecutorCorePoolSize(final int corePoolSize) {
    UPSTREAM_DATA_FLOW_EXECUTOR.setCorePoolSize(corePoolSize);
  }

  public static void setDownstreamDataFlowExecutorCorePoolSize(final int corePoolSize) {
    DOWNSTREAM_DATA_FLOW_EXECUTOR.setCorePoolSize(corePoolSize);
  }

  /////////////////////////////// shutdown hook ///////////////////////////////

  static {
    // register shutdown hook
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                new SubscriptionExecutorServiceShutdownHook(),
                "SubscriptionExecutorServiceShutdownHook"));
  }

  private static class SubscriptionExecutorServiceShutdownHook implements Runnable {

    @Override
    public void run() {
      // shutdown executors
      CONTROL_FLOW_EXECUTOR.shutdown();
      UPSTREAM_DATA_FLOW_EXECUTOR.shutdown();
      DOWNSTREAM_DATA_FLOW_EXECUTOR.shutdown();
    }
  }

  /////////////////////////////// submitter ///////////////////////////////

  @SuppressWarnings("unsafeThreadSchedule")
  public static ScheduledFuture<?> submitHeartbeatWorker(
      final Runnable task, final long heartbeatIntervalMs) {
    CONTROL_FLOW_EXECUTOR.launchIfNeeded();
    return CONTROL_FLOW_EXECUTOR.scheduleWithFixedDelay(
        task,
        generateRandomInitialDelayMs(heartbeatIntervalMs),
        heartbeatIntervalMs,
        TimeUnit.MILLISECONDS);
  }

  @SuppressWarnings("unsafeThreadSchedule")
  public static ScheduledFuture<?> submitEndpointsSyncer(
      final Runnable task, final long endpointsSyncIntervalMs) {
    CONTROL_FLOW_EXECUTOR.launchIfNeeded();
    return CONTROL_FLOW_EXECUTOR.scheduleWithFixedDelay(
        task,
        generateRandomInitialDelayMs(endpointsSyncIntervalMs),
        endpointsSyncIntervalMs,
        TimeUnit.MILLISECONDS);
  }

  @SuppressWarnings("unsafeThreadSchedule")
  public static ScheduledFuture<?> submitAutoPollWorker(
      final Runnable task, final long autoPollIntervalMs) {
    CONTROL_FLOW_EXECUTOR.launchIfNeeded();
    return CONTROL_FLOW_EXECUTOR.scheduleWithFixedDelay(
        task,
        generateRandomInitialDelayMs(autoPollIntervalMs),
        autoPollIntervalMs,
        TimeUnit.MILLISECONDS);
  }

  @SuppressWarnings("unsafeThreadSchedule")
  public static ScheduledFuture<?> submitAutoCommitWorker(
      final Runnable task, final long autoCommitIntervalMs) {
    UPSTREAM_DATA_FLOW_EXECUTOR.launchIfNeeded();
    return UPSTREAM_DATA_FLOW_EXECUTOR.scheduleWithFixedDelay(
        task,
        generateRandomInitialDelayMs(autoCommitIntervalMs),
        autoCommitIntervalMs,
        TimeUnit.MILLISECONDS);
  }

  public static void submitAsyncCommitWorker(final Runnable task) {
    UPSTREAM_DATA_FLOW_EXECUTOR.launchIfNeeded();
    UPSTREAM_DATA_FLOW_EXECUTOR.submit(task);
  }

  public static <T> List<Future<T>> submitMultiplePollTasks(
      final Collection<? extends Callable<T>> tasks, final long timeoutMs)
      throws InterruptedException {
    DOWNSTREAM_DATA_FLOW_EXECUTOR.launchIfNeeded();
    return DOWNSTREAM_DATA_FLOW_EXECUTOR.invokeAll(tasks, timeoutMs);
  }

  public static int getAvailableThreadCountForPollTasks() {
    DOWNSTREAM_DATA_FLOW_EXECUTOR.launchIfNeeded();
    return DOWNSTREAM_DATA_FLOW_EXECUTOR.getAvailableCount();
  }

  /////////////////////////////// subscription executor service ///////////////////////////////

  private static class SubscriptionExecutorService {

    String name;
    volatile int corePoolSize;
    volatile ExecutorService executor;

    SubscriptionExecutorService(final String name, final int corePoolSize) {
      this.name = name;
      this.corePoolSize = corePoolSize;
    }

    boolean isShutdown() {
      return Objects.isNull(this.executor);
    }

    void setCorePoolSize(final int corePoolSize) {
      if (isShutdown()) {
        synchronized (this) {
          if (isShutdown()) {
            this.corePoolSize = corePoolSize;
            return;
          }
        }
      }

      LOGGER.warn(
          "{} has been launched, set core pool size to {} will be ignored",
          this.name,
          corePoolSize);
    }

    void launchIfNeeded() {
      if (isShutdown()) {
        synchronized (this) {
          if (isShutdown()) {
            LOGGER.info("Launching {} with core pool size {}...", this.name, this.corePoolSize);

            this.executor =
                Executors.newFixedThreadPool(
                    this.corePoolSize,
                    r -> {
                      final Thread t =
                          new Thread(Thread.currentThread().getThreadGroup(), r, this.name, 0);
                      if (!t.isDaemon()) {
                        t.setDaemon(true);
                      }
                      if (t.getPriority() != Thread.NORM_PRIORITY) {
                        t.setPriority(Thread.NORM_PRIORITY);
                      }
                      return t;
                    });
          }
        }
      }
    }

    void shutdown() {
      if (!isShutdown()) {
        synchronized (this) {
          if (!isShutdown()) {
            LOGGER.info("Shutting down {}...", this.name);

            this.executor.shutdown();
            try {
              if (!this.executor.awaitTermination(
                  AWAIT_TERMINATION_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                this.executor.shutdownNow();
                LOGGER.warn(
                    "Interrupt the worker, which may cause some task inconsistent. Please check the biz logs.");
                if (!this.executor.awaitTermination(
                    AWAIT_TERMINATION_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                  LOGGER.error(
                      "Thread pool can't be shutdown even with interrupting worker threads, which may cause some task inconsistent. Please check the biz logs.");
                }
              }
            } catch (final InterruptedException e) {
              this.executor.shutdownNow();
              LOGGER.error(
                  "The current thread is interrupted when it is trying to stop the worker threads. This may leave an inconsistent state. Please check the biz logs.");
              Thread.currentThread().interrupt();
            }

            this.executor = null;
          }
        }
      }
    }

    Future<?> submit(final Runnable task) {
      if (!isShutdown()) {
        synchronized (this) {
          if (!isShutdown()) {
            return this.executor.submit(task);
          }
        }
      }

      LOGGER.warn("{} has not been launched, ignore submit task", this.name);
      return null;
    }

    <T> List<Future<T>> invokeAll(
        final Collection<? extends Callable<T>> tasks, final long timeoutMs)
        throws InterruptedException {
      if (!isShutdown()) {
        synchronized (this) {
          if (!isShutdown()) {
            return this.executor.invokeAll(tasks, timeoutMs, TimeUnit.MILLISECONDS);
          }
        }
      }

      LOGGER.warn("{} has not been launched, ignore invoke all tasks", this.name);
      return null;
    }

    int getAvailableCount() {
      if (!isShutdown()) {
        synchronized (this) {
          if (!isShutdown()) {
            return Math.max(
                ((ThreadPoolExecutor) this.executor).getPoolSize()
                    - ((ThreadPoolExecutor) this.executor).getActiveCount(),
                0);
          }
        }
      }

      LOGGER.warn("{} has not been launched, return zero", this.name);
      return 0;
    }
  }

  private static class SubscriptionScheduledExecutorService extends SubscriptionExecutorService {

    SubscriptionScheduledExecutorService(final String name, final int corePoolSize) {
      super(name, corePoolSize);
    }

    @Override
    void launchIfNeeded() {
      if (isShutdown()) {
        synchronized (this) {
          if (isShutdown()) {
            LOGGER.info("Launching {} with core pool size {}...", this.name, this.corePoolSize);

            this.executor =
                Executors.newScheduledThreadPool(
                    this.corePoolSize,
                    r -> {
                      final Thread t =
                          new Thread(Thread.currentThread().getThreadGroup(), r, this.name, 0);
                      if (!t.isDaemon()) {
                        t.setDaemon(true);
                      }
                      if (t.getPriority() != Thread.NORM_PRIORITY) {
                        t.setPriority(Thread.NORM_PRIORITY);
                      }
                      return t;
                    });
          }
        }
      }
    }

    @SuppressWarnings("unsafeThreadSchedule")
    ScheduledFuture<?> scheduleWithFixedDelay(
        final Runnable task, final long initialDelay, final long delay, final TimeUnit unit) {
      if (!isShutdown()) {
        synchronized (this) {
          if (!isShutdown()) {
            return ((ScheduledExecutorService) this.executor)
                .scheduleWithFixedDelay(task, initialDelay, delay, unit);
          }
        }
      }

      LOGGER.warn("{} has not been launched, ignore scheduleWithFixedDelay for task", this.name);
      return null;
    }
  }

  /////////////////////////////// utility ///////////////////////////////

  private static long generateRandomInitialDelayMs(final long maxMs) {
    return (long) (Math.random() * maxMs);
  }
}
