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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

final class SubscriptionExecutorService {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionExecutorService.class);

  private static final long AWAIT_TERMINATION_TIMEOUT_MS = 10_000L;

  private static int controlFlowExecutorCorePoolSize =
      Math.max(Runtime.getRuntime().availableProcessors() / 2, 1);
  private static int upstreamDataFlowExecutorCorePoolSize =
      Math.max(Runtime.getRuntime().availableProcessors() / 2, 1);
  private static int downstreamDataFlowExecutorCorePoolSize =
      Math.max(Runtime.getRuntime().availableProcessors(), 1);

  /**
   * Control Flow Executor: execute heartbeat worker and endpoints syncer for {@link
   * SubscriptionConsumer}
   */
  private static volatile ScheduledExecutorService controlFlowExecutor;

  /**
   * Upstream Data Flow Executor: execute auto commit worker and async commit worker for {@link
   * SubscriptionPullConsumer}
   */
  private static volatile ScheduledExecutorService upstreamDataFlowExecutor;

  /**
   * Downstream Data Flow Executor: execute auto poll worker for {@link SubscriptionPushConsumer}
   */
  private static volatile ScheduledExecutorService downstreamDataFlowExecutor;

  /////////////////////////////// setter ///////////////////////////////

  public static synchronized void setControlFlowExecutorCorePoolSize(
      final int controlFlowExecutorCorePoolSize) {
    if (Objects.nonNull(controlFlowExecutor)) {
      LOGGER.warn(
          "control flow executor has been initialized, set core pool size to {} will be ignored",
          controlFlowExecutorCorePoolSize);
      return;
    }
    SubscriptionExecutorService.controlFlowExecutorCorePoolSize = controlFlowExecutorCorePoolSize;
  }

  public static synchronized void setUpstreamDataFlowExecutorCorePoolSize(
      final int upstreamDataFlowExecutorCorePoolSize) {
    if (Objects.nonNull(upstreamDataFlowExecutor)) {
      LOGGER.warn(
          "upstream data flow executor has been initialized, set core pool size to {} will be ignored",
          upstreamDataFlowExecutorCorePoolSize);
      return;
    }
    SubscriptionExecutorService.upstreamDataFlowExecutorCorePoolSize =
        upstreamDataFlowExecutorCorePoolSize;
  }

  public static synchronized void setDownstreamDataFlowExecutorCorePoolSize(
      final int downstreamDataFlowExecutorCorePoolSize) {
    if (Objects.nonNull(downstreamDataFlowExecutor)) {
      LOGGER.warn(
          "downstream data flow executor has been initialized, set core pool size to {} will be ignored",
          downstreamDataFlowExecutorCorePoolSize);
      return;
    }
    SubscriptionExecutorService.downstreamDataFlowExecutorCorePoolSize =
        downstreamDataFlowExecutorCorePoolSize;
  }

  /////////////////////////////// shutdown hook ///////////////////////////////

  static {
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                new SubscriptionExecutorServiceShutdownHook(),
                "SubscriptionExecutorServiceShutdownHook"));
  }

  private static class SubscriptionExecutorServiceShutdownHook implements Runnable {

    @Override
    public synchronized void run() {
      if (Objects.nonNull(controlFlowExecutor)) {
        LOGGER.info("Shutting down control flow executor...");
        shutdownExecutor(controlFlowExecutor);
      }
      if (Objects.nonNull(upstreamDataFlowExecutor)) {
        LOGGER.info("Shutting down upstream data flow executor...");
        shutdownExecutor(upstreamDataFlowExecutor);
      }
      if (Objects.nonNull(downstreamDataFlowExecutor)) {
        LOGGER.info("Shutting down downstream data flow executor...");
        shutdownExecutor(downstreamDataFlowExecutor);
      }
    }
  }

  private static void shutdownExecutor(final ExecutorService executor) {
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
  }

  /////////////////////////////// launcher ///////////////////////////////

  private static void launchControlFlowExecutorIfNeeded() {
    if (Objects.isNull(controlFlowExecutor)) {
      synchronized (SubscriptionExecutorService.class) {
        if (Objects.nonNull(controlFlowExecutor)) {
          return;
        }

        LOGGER.info(
            "Launching control flow executor with core pool size {}...",
            controlFlowExecutorCorePoolSize);
        controlFlowExecutor =
            Executors.newScheduledThreadPool(
                controlFlowExecutorCorePoolSize,
                r -> {
                  final Thread t =
                      new Thread(
                          Thread.currentThread().getThreadGroup(),
                          r,
                          "SubscriptionControlFlowExecutor",
                          0);
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

  private static void launchUpstreamDataFlowExecutorIfNeeded() {
    if (Objects.isNull(upstreamDataFlowExecutor)) {
      synchronized (SubscriptionExecutorService.class) {
        if (Objects.nonNull(upstreamDataFlowExecutor)) {
          return;
        }

        LOGGER.info(
            "Launching upstream data flow executor with core pool size {}...",
            upstreamDataFlowExecutorCorePoolSize);
        upstreamDataFlowExecutor =
            Executors.newScheduledThreadPool(
                upstreamDataFlowExecutorCorePoolSize,
                r -> {
                  final Thread t =
                      new Thread(
                          Thread.currentThread().getThreadGroup(),
                          r,
                          "SubscriptionUpstreamDataFlowExecutor",
                          0);
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

  private static void launchDownstreamDataFlowExecutorIfNeeded() {
    if (Objects.isNull(downstreamDataFlowExecutor)) {
      synchronized (SubscriptionExecutorService.class) {
        if (Objects.nonNull(downstreamDataFlowExecutor)) {
          return;
        }

        LOGGER.info(
            "Launching downstream data flow executor with core pool size {}...",
            downstreamDataFlowExecutorCorePoolSize);
        downstreamDataFlowExecutor =
            Executors.newScheduledThreadPool(
                downstreamDataFlowExecutorCorePoolSize,
                r -> {
                  final Thread t =
                      new Thread(
                          Thread.currentThread().getThreadGroup(),
                          r,
                          "SubscriptionDownstreamDataFlowExecutor",
                          0);
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

  /////////////////////////////// submitter ///////////////////////////////

  @SuppressWarnings("unsafeThreadSchedule")
  public static ScheduledFuture<?> submitHeartbeatWorker(
      final Runnable task, final long heartbeatIntervalMs) {
    launchControlFlowExecutorIfNeeded();
    return controlFlowExecutor.scheduleWithFixedDelay(
        task,
        generateRandomInitialDelayMs(heartbeatIntervalMs),
        heartbeatIntervalMs,
        TimeUnit.MILLISECONDS);
  }

  @SuppressWarnings("unsafeThreadSchedule")
  public static ScheduledFuture<?> submitEndpointsSyncer(
      final Runnable task, final long endpointsSyncIntervalMs) {
    launchControlFlowExecutorIfNeeded();
    return controlFlowExecutor.scheduleWithFixedDelay(
        task,
        generateRandomInitialDelayMs(endpointsSyncIntervalMs),
        endpointsSyncIntervalMs,
        TimeUnit.MILLISECONDS);
  }

  @SuppressWarnings("unsafeThreadSchedule")
  public static ScheduledFuture<?> submitAutoCommitWorker(
      final Runnable task, final long autoCommitIntervalMs) {
    launchUpstreamDataFlowExecutorIfNeeded();
    return upstreamDataFlowExecutor.scheduleWithFixedDelay(
        task,
        generateRandomInitialDelayMs(autoCommitIntervalMs),
        autoCommitIntervalMs,
        TimeUnit.MILLISECONDS);
  }

  public static void submitAsyncCommitWorker(final Runnable task) {
    launchUpstreamDataFlowExecutorIfNeeded();
    upstreamDataFlowExecutor.submit(task);
  }

  @SuppressWarnings("unsafeThreadSchedule")
  public static ScheduledFuture<?> submitAutoPollWorker(
      final Runnable task, final long autoPollIntervalMs) {
    launchDownstreamDataFlowExecutorIfNeeded();
    return downstreamDataFlowExecutor.scheduleWithFixedDelay(
        task,
        generateRandomInitialDelayMs(autoPollIntervalMs),
        autoPollIntervalMs,
        TimeUnit.MILLISECONDS);
  }

  /////////////////////////////// utility ///////////////////////////////

  private static long generateRandomInitialDelayMs(final long maxMs) {
    return (long) (Math.random() * maxMs);
  }
}
