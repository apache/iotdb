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

import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

final class SubscriptionExecutorServiceManager {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SubscriptionExecutorServiceManager.class);

  private static final long AWAIT_TERMINATION_TIMEOUT_MS = 10_000L;

  /**
   * Control Flow Executor: execute heartbeat worker and endpoints syncer for {@link
   * SubscriptionConsumer}
   */
  private static final SubscriptionExecutorService CONTROL_FLOW_EXECUTOR =
      new SubscriptionExecutorService(
          "SubscriptionControlFlowExecutor",
          Math.max(Runtime.getRuntime().availableProcessors() / 2, 1));

  /**
   * Upstream Data Flow Executor: execute auto commit worker and async commit worker for {@link
   * SubscriptionPullConsumer}
   */
  private static final SubscriptionExecutorService UPSTREAM_DATA_FLOW_EXECUTOR =
      new SubscriptionExecutorService(
          "SubscriptionUpstreamDataFlowExecutor",
          Math.max(Runtime.getRuntime().availableProcessors() / 2, 1));

  /**
   * Downstream Data Flow Executor: execute auto poll worker for {@link SubscriptionPushConsumer}
   */
  private static final SubscriptionExecutorService DOWNSTREAM_DATA_FLOW_EXECUTOR =
      new SubscriptionExecutorService(
          "SubscriptionDownstreamDataFlowExecutor",
          Math.max(Runtime.getRuntime().availableProcessors(), 1));

  /////////////////////////////// setter ///////////////////////////////

  private static void setExecutorCorePoolSize(
      final SubscriptionExecutorService executor, final int corePoolSize) {
    if (executor.isShutdown()) {
      synchronized (SubscriptionExecutorServiceManager.class) {
        if (executor.isShutdown()) {
          executor.corePoolSize = corePoolSize;
          return;
        }
      }
    }
    LOGGER.warn(
        "{} has been launched, set core pool size to {} will be ignored",
        executor.name,
        corePoolSize);
  }

  public static void setControlFlowExecutorCorePoolSize(final int corePoolSize) {
    setExecutorCorePoolSize(CONTROL_FLOW_EXECUTOR, corePoolSize);
  }

  public static void setUpstreamDataFlowExecutorCorePoolSize(final int corePoolSize) {
    setExecutorCorePoolSize(UPSTREAM_DATA_FLOW_EXECUTOR, corePoolSize);
  }

  public static void setDownstreamDataFlowExecutorCorePoolSize(final int corePoolSize) {
    setExecutorCorePoolSize(DOWNSTREAM_DATA_FLOW_EXECUTOR, corePoolSize);
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
      shutdownExecutor(CONTROL_FLOW_EXECUTOR);
      shutdownExecutor(UPSTREAM_DATA_FLOW_EXECUTOR);
      shutdownExecutor(DOWNSTREAM_DATA_FLOW_EXECUTOR);
    }
  }

  private static void shutdownExecutor(final SubscriptionExecutorService executor) {
    if (!executor.isShutdown()) {
      synchronized (SubscriptionExecutorServiceManager.class) {
        if (!executor.isShutdown()) {
          LOGGER.info("Shutting down {}...", executor.name);
          executor.shutdown();
        }
      }
    }
  }

  /////////////////////////////// launcher ///////////////////////////////

  private static void launchExecutorIfNeeded(final SubscriptionExecutorService executor) {
    if (executor.isShutdown()) {
      synchronized (SubscriptionExecutorServiceManager.class) {
        if (executor.isShutdown()) {
          LOGGER.info(
              "Launching {} with core pool size {}...", executor.name, executor.corePoolSize);
          executor.launch();
        }
      }
    }
  }

  private static void launchControlFlowExecutorIfNeeded() {
    launchExecutorIfNeeded(CONTROL_FLOW_EXECUTOR);
  }

  private static void launchUpstreamDataFlowExecutorIfNeeded() {
    launchExecutorIfNeeded(UPSTREAM_DATA_FLOW_EXECUTOR);
  }

  private static void launchDownstreamDataFlowExecutorIfNeeded() {
    launchExecutorIfNeeded(DOWNSTREAM_DATA_FLOW_EXECUTOR);
  }

  /////////////////////////////// submitter ///////////////////////////////

  @SuppressWarnings("unsafeThreadSchedule")
  public static ScheduledFuture<?> submitHeartbeatWorker(
      final Runnable task, final long heartbeatIntervalMs) {
    launchControlFlowExecutorIfNeeded();
    return CONTROL_FLOW_EXECUTOR.scheduleWithFixedDelay(
        task,
        generateRandomInitialDelayMs(heartbeatIntervalMs),
        heartbeatIntervalMs,
        TimeUnit.MILLISECONDS);
  }

  @SuppressWarnings("unsafeThreadSchedule")
  public static ScheduledFuture<?> submitEndpointsSyncer(
      final Runnable task, final long endpointsSyncIntervalMs) {
    launchControlFlowExecutorIfNeeded();
    return CONTROL_FLOW_EXECUTOR.scheduleWithFixedDelay(
        task,
        generateRandomInitialDelayMs(endpointsSyncIntervalMs),
        endpointsSyncIntervalMs,
        TimeUnit.MILLISECONDS);
  }

  @SuppressWarnings("unsafeThreadSchedule")
  public static ScheduledFuture<?> submitAutoCommitWorker(
      final Runnable task, final long autoCommitIntervalMs) {
    launchUpstreamDataFlowExecutorIfNeeded();
    return UPSTREAM_DATA_FLOW_EXECUTOR.scheduleWithFixedDelay(
        task,
        generateRandomInitialDelayMs(autoCommitIntervalMs),
        autoCommitIntervalMs,
        TimeUnit.MILLISECONDS);
  }

  public static void submitAsyncCommitWorker(final Runnable task) {
    launchUpstreamDataFlowExecutorIfNeeded();
    UPSTREAM_DATA_FLOW_EXECUTOR.submit(task);
  }

  @SuppressWarnings("unsafeThreadSchedule")
  public static ScheduledFuture<?> submitAutoPollWorker(
      final Runnable task, final long autoPollIntervalMs) {
    launchDownstreamDataFlowExecutorIfNeeded();
    return DOWNSTREAM_DATA_FLOW_EXECUTOR.scheduleWithFixedDelay(
        task,
        generateRandomInitialDelayMs(autoPollIntervalMs),
        autoPollIntervalMs,
        TimeUnit.MILLISECONDS);
  }

  /////////////////////////////// subscription executor service ///////////////////////////////

  private static class SubscriptionExecutorService {

    String name;
    volatile int corePoolSize;
    volatile ScheduledExecutorService executor;

    SubscriptionExecutorService(final String name, final int corePoolSize) {
      this.name = name;
      this.corePoolSize = corePoolSize;
    }

    boolean isShutdown() {
      return Objects.isNull(executor);
    }

    void launch() {
      if (!isShutdown()) {
        return;
      }

      this.executor =
          Executors.newScheduledThreadPool(
              corePoolSize,
              r -> {
                final Thread t = new Thread(Thread.currentThread().getThreadGroup(), r, name, 0);
                if (!t.isDaemon()) {
                  t.setDaemon(true);
                }
                if (t.getPriority() != Thread.NORM_PRIORITY) {
                  t.setPriority(Thread.NORM_PRIORITY);
                }
                return t;
              });
      ;
    }

    void shutdown() {
      if (isShutdown()) {
        return;
      }

      executor.shutdown();
      try {
        if (!executor.awaitTermination(AWAIT_TERMINATION_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
          executor.shutdownNow();
          LOGGER.warn(
              "Interrupt the worker, which may cause some task inconsistent. Please check the biz logs.");
          if (!executor.awaitTermination(AWAIT_TERMINATION_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
            LOGGER.error(
                "Thread pool can't be shutdown even with interrupting worker threads, which may cause some task inconsistent. Please check the biz logs.");
          }
        }
      } catch (final InterruptedException e) {
        executor.shutdownNow();
        LOGGER.error(
            "The current thread is interrupted when it is trying to stop the worker threads. This may leave an inconsistent state. Please check the biz logs.");
        Thread.currentThread().interrupt();
      }

      executor = null;
    }

    ScheduledFuture<?> scheduleWithFixedDelay(
        final Runnable task, final long initialDelay, final long delay, final TimeUnit unit) {
      if (isShutdown()) {
        LOGGER.warn("{} has not been launched, ignore scheduleWithFixedDelay for task", name);
        return null;
      }

      return executor.scheduleWithFixedDelay(task, initialDelay, delay, unit);
    }

    Future<?> submit(final Runnable task) {
      if (isShutdown()) {
        LOGGER.warn("{} has not been launched, ignore submit task", name);
        return null;
      }

      return executor.submit(task);
    }
  }

  /////////////////////////////// utility ///////////////////////////////

  private static long generateRandomInitialDelayMs(final long maxMs) {
    return (long) (Math.random() * maxMs);
  }
}
