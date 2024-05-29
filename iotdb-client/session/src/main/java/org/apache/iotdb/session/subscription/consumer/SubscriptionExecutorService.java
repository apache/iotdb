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

  private static int clusterConnectionExecutorCorePoolSize =
      Math.max(Runtime.getRuntime().availableProcessors() / 2, 1);
  private static int toUpstreamCommunicationExecutorCorePoolSize =
      Math.max(Runtime.getRuntime().availableProcessors() / 2, 1);
  private static int toDownstreamCommunicationExecutorCorePoolSize =
      Math.max(Runtime.getRuntime().availableProcessors(), 1);

  /**
   * Cluster Connection Executor: execute heartbeat worker and endpoints syncer for {@link
   * SubscriptionConsumer}
   */
  private static volatile ScheduledExecutorService clusterConnectionExecutor;

  /**
   * Downstream to Upstream Communication Executor: execute auto commit worker and async commit
   * worker for {@link SubscriptionPullConsumer}
   */
  private static volatile ScheduledExecutorService toUpstreamCommunicationExecutor;

  /**
   * Upstream to Downstream Communication Executor: execute auto poll worker for {@link
   * SubscriptionPushConsumer}
   */
  private static volatile ScheduledExecutorService toDownstreamCommunicationExecutor;

  /////////////////////////////// setter ///////////////////////////////

  public static void setClusterConnectionExecutorCorePoolSize(
      final int clusterConnectionExecutorCorePoolSize) {
    if (Objects.nonNull(clusterConnectionExecutor)) {
      LOGGER.warn(
          "cluster connection executor has been initialized, set core pool size to {} will be ignored",
          clusterConnectionExecutorCorePoolSize);
      return;
    }
    SubscriptionExecutorService.clusterConnectionExecutorCorePoolSize =
        clusterConnectionExecutorCorePoolSize;
  }

  public static void setToUpstreamCommunicationExecutorCorePoolSize(
      final int toUpstreamCommunicationExecutorCorePoolSize) {
    if (Objects.nonNull(toUpstreamCommunicationExecutor)) {
      LOGGER.warn(
          "to upstream communication executor has been initialized, set core pool size to {} will be ignored",
          toUpstreamCommunicationExecutorCorePoolSize);
    }
    SubscriptionExecutorService.toUpstreamCommunicationExecutorCorePoolSize =
        toUpstreamCommunicationExecutorCorePoolSize;
  }

  public static void setToDownstreamCommunicationExecutorCorePoolSize(
      final int toDownstreamCommunicationExecutorCorePoolSize) {
    if (Objects.nonNull(toDownstreamCommunicationExecutor)) {
      LOGGER.warn(
          "to downstream communication executor has been initialized, set core pool size to {} will be ignored",
          toDownstreamCommunicationExecutorCorePoolSize);
    }
    SubscriptionExecutorService.toDownstreamCommunicationExecutorCorePoolSize =
        toDownstreamCommunicationExecutorCorePoolSize;
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
    public void run() {
      if (Objects.nonNull(clusterConnectionExecutor)) {
        LOGGER.info("Shutting down cluster connection executor...");
        shutdownExecutor(clusterConnectionExecutor);
      }
      if (Objects.nonNull(toUpstreamCommunicationExecutor)) {
        LOGGER.info("Shutting down to upstream communication executor...");
        shutdownExecutor(toUpstreamCommunicationExecutor);
      }
      if (Objects.nonNull(toDownstreamCommunicationExecutor)) {
        LOGGER.info("Shutting down to downstream communication executor...");
        shutdownExecutor(toDownstreamCommunicationExecutor);
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

  private static void launchClusterConnectionExecutorIfNeeded() {
    if (Objects.isNull(clusterConnectionExecutor)) {
      synchronized (SubscriptionExecutorService.class) {
        if (Objects.nonNull(clusterConnectionExecutor)) {
          return;
        }

        LOGGER.info(
            "Launching cluster connection executor with core pool size {}...",
            clusterConnectionExecutorCorePoolSize);
        clusterConnectionExecutor =
            Executors.newScheduledThreadPool(
                clusterConnectionExecutorCorePoolSize,
                r -> {
                  final Thread t =
                      new Thread(
                          Thread.currentThread().getThreadGroup(),
                          r,
                          "SubscriptionClusterConnectionExecutor",
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

  private static void launchToUpstreamCommunicationExecutorIfNeeded() {
    if (Objects.isNull(toUpstreamCommunicationExecutor)) {
      synchronized (SubscriptionExecutorService.class) {
        if (Objects.nonNull(toUpstreamCommunicationExecutor)) {
          return;
        }

        LOGGER.info(
            "Launching to upstream communication executor with core pool size {}...",
            toUpstreamCommunicationExecutorCorePoolSize);
        toUpstreamCommunicationExecutor =
            Executors.newScheduledThreadPool(
                toUpstreamCommunicationExecutorCorePoolSize,
                r -> {
                  final Thread t =
                      new Thread(
                          Thread.currentThread().getThreadGroup(),
                          r,
                          "SubscriptionToUpstreamCommunicationExecutor",
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

  private static void launchToDownstreamCommunicationExecutorIfNeeded() {
    if (Objects.isNull(toDownstreamCommunicationExecutor)) {
      synchronized (SubscriptionExecutorService.class) {
        if (Objects.nonNull(toDownstreamCommunicationExecutor)) {
          return;
        }

        LOGGER.info(
            "Launching to downstream communication executor with core pool size {}...",
            toDownstreamCommunicationExecutorCorePoolSize);
        toDownstreamCommunicationExecutor =
            Executors.newScheduledThreadPool(
                toDownstreamCommunicationExecutorCorePoolSize,
                r -> {
                  final Thread t =
                      new Thread(
                          Thread.currentThread().getThreadGroup(),
                          r,
                          "SubscriptionToDownstreamCommunicationExecutor",
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
    launchClusterConnectionExecutorIfNeeded();
    return clusterConnectionExecutor.scheduleWithFixedDelay(
        task,
        generateRandomInitialDelayMs(heartbeatIntervalMs),
        heartbeatIntervalMs,
        TimeUnit.MILLISECONDS);
  }

  @SuppressWarnings("unsafeThreadSchedule")
  public static ScheduledFuture<?> submitEndpointsSyncer(
      final Runnable task, final long endpointsSyncIntervalMs) {
    launchClusterConnectionExecutorIfNeeded();
    return clusterConnectionExecutor.scheduleWithFixedDelay(
        task,
        generateRandomInitialDelayMs(endpointsSyncIntervalMs),
        endpointsSyncIntervalMs,
        TimeUnit.MILLISECONDS);
  }

  @SuppressWarnings("unsafeThreadSchedule")
  public static ScheduledFuture<?> submitAutoCommitWorker(
      final Runnable task, final long autoCommitIntervalMs) {
    launchToUpstreamCommunicationExecutorIfNeeded();
    return toUpstreamCommunicationExecutor.scheduleWithFixedDelay(
        task,
        generateRandomInitialDelayMs(autoCommitIntervalMs),
        autoCommitIntervalMs,
        TimeUnit.MILLISECONDS);
  }

  public static void submitAsyncCommitWorker(final Runnable task) {
    launchToUpstreamCommunicationExecutorIfNeeded();
    toUpstreamCommunicationExecutor.submit(task);
  }

  @SuppressWarnings("unsafeThreadSchedule")
  public static ScheduledFuture<?> submitAutoPollWorker(
      final Runnable task, final long autoPollIntervalMs) {
    launchToDownstreamCommunicationExecutorIfNeeded();
    return toDownstreamCommunicationExecutor.scheduleWithFixedDelay(
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
