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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

final class SubscriptionExecutorService {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionExecutorService.class);

  private static final long AWAIT_TERMINATION_TIMEOUT_MS = 10_000L;
  private static final long KEEP_ALIVE_TIME_MS = 10_000L;
  private static final int WORK_QUEUE_CAPACITY = 1024;

  private static volatile ScheduledExecutorService heartbeatWorkerExecutor;
  private static volatile ScheduledExecutorService endpointsSyncerExecutor;
  private static volatile ScheduledExecutorService autoCommitWorkerExecutor;
  private static volatile ScheduledExecutorService autoPollWorkerExecutor;
  private static volatile ExecutorService asyncCommitWorkerExecutor;

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
      if (Objects.nonNull(heartbeatWorkerExecutor)) {
        LOGGER.info("Shutting down heartbeat worker executor...");
        shutdownExecutor(heartbeatWorkerExecutor);
      }
      if (Objects.nonNull(endpointsSyncerExecutor)) {
        LOGGER.info("Shutting down endpoints syncer executor...");
        shutdownExecutor(endpointsSyncerExecutor);
      }
      if (Objects.nonNull(autoCommitWorkerExecutor)) {
        LOGGER.info("Shutting down auto commit worker executor...");
        shutdownExecutor(autoCommitWorkerExecutor);
      }
      if (Objects.nonNull(autoPollWorkerExecutor)) {
        LOGGER.info("Shutting down auto poll worker executor...");
        shutdownExecutor(autoPollWorkerExecutor);
      }
      if (Objects.nonNull(asyncCommitWorkerExecutor)) {
        LOGGER.info("Shutting down async commit worker executor...");
        shutdownExecutor(asyncCommitWorkerExecutor);
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

  /////////////////////////////// heartbeat worker ///////////////////////////////

  private static void launchHeartbeatWorkerExecutorIfNeeded(final int heartbeatMaxTasksIfNotExist) {
    if (Objects.isNull(heartbeatWorkerExecutor)) {
      synchronized (SubscriptionExecutorService.class) {
        if (Objects.nonNull(heartbeatWorkerExecutor)) {
          return;
        }

        LOGGER.info("Launching heartbeat worker executor...");
        heartbeatWorkerExecutor =
            Executors.newScheduledThreadPool(
                heartbeatMaxTasksIfNotExist,
                r -> {
                  final Thread t =
                      new Thread(
                          Thread.currentThread().getThreadGroup(),
                          r,
                          "SubscriptionConsumerHeartbeatWorker",
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

  @SuppressWarnings("unsafeThreadSchedule")
  public static ScheduledFuture<?> submitHeartbeatWorker(
      final Runnable task, final int heartbeatMaxTasksIfNotExist, final long heartbeatIntervalMs) {
    launchHeartbeatWorkerExecutorIfNeeded(heartbeatMaxTasksIfNotExist);
    return heartbeatWorkerExecutor.scheduleWithFixedDelay(
        task,
        generateRandomInitialDelayMs(heartbeatIntervalMs),
        heartbeatIntervalMs,
        TimeUnit.MILLISECONDS);
  }

  /////////////////////////////// endpoints syncer ///////////////////////////////

  private static void launchEndpointsSyncerExecutorIfNeeded(
      final int endpointsSyncMaxTasksIfNotExist) {
    if (Objects.isNull(endpointsSyncerExecutor)) {
      synchronized (SubscriptionExecutorService.class) {
        if (Objects.nonNull(endpointsSyncerExecutor)) {
          return;
        }

        LOGGER.info("Launching endpoints syncer executor...");
        endpointsSyncerExecutor =
            Executors.newScheduledThreadPool(
                endpointsSyncMaxTasksIfNotExist,
                r -> {
                  final Thread t =
                      new Thread(
                          Thread.currentThread().getThreadGroup(),
                          r,
                          "SubscriptionConsumerEndpointsSyncer",
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

  @SuppressWarnings("unsafeThreadSchedule")
  public static ScheduledFuture<?> submitEndpointsSyncer(
      final Runnable task,
      final int endpointsSyncMaxTasksIfNotExist,
      final long endpointsSyncIntervalMs) {
    launchEndpointsSyncerExecutorIfNeeded(endpointsSyncMaxTasksIfNotExist);
    return endpointsSyncerExecutor.scheduleWithFixedDelay(
        task,
        generateRandomInitialDelayMs(endpointsSyncIntervalMs),
        endpointsSyncIntervalMs,
        TimeUnit.MILLISECONDS);
  }

  /////////////////////////////// auto commit worker ///////////////////////////////

  private static void launchAutoCommitWorkerExecutorIfNeeded() {
    if (Objects.isNull(autoCommitWorkerExecutor)) {
      synchronized (SubscriptionExecutorService.class) {
        if (Objects.nonNull(autoCommitWorkerExecutor)) {
          return;
        }

        LOGGER.info("Launching auto commit worker executor...");
        autoCommitWorkerExecutor =
            Executors.newScheduledThreadPool(
                getCorePoolSize(),
                r -> {
                  final Thread t =
                      new Thread(
                          Thread.currentThread().getThreadGroup(),
                          r,
                          "SubscriptionPullConsumerAutoCommitWorker",
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

  @SuppressWarnings("unsafeThreadSchedule")
  public static ScheduledFuture<?> submitAutoCommitWorker(
      final Runnable task, final long autoCommitIntervalMs) {
    launchAutoCommitWorkerExecutorIfNeeded();
    return autoCommitWorkerExecutor.scheduleWithFixedDelay(
        task,
        generateRandomInitialDelayMs(autoCommitIntervalMs),
        autoCommitIntervalMs,
        TimeUnit.MILLISECONDS);
  }

  /////////////////////////////// auto poll worker ///////////////////////////////

  private static void launchAutoPollWorkerExecutorIfNeeded() {
    if (Objects.isNull(autoPollWorkerExecutor)) {
      synchronized (SubscriptionExecutorService.class) {
        if (Objects.nonNull(autoPollWorkerExecutor)) {
          return;
        }

        LOGGER.info("Launching auto poll worker executor...");
        autoPollWorkerExecutor =
            Executors.newScheduledThreadPool(
                getCorePoolSize(),
                r -> {
                  final Thread t =
                      new Thread(
                          Thread.currentThread().getThreadGroup(),
                          r,
                          "SubscriptionPushConsumerAutoPollWorker",
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

  @SuppressWarnings("unsafeThreadSchedule")
  public static ScheduledFuture<?> submitAutoPollWorker(
      final Runnable task, final long autoPollIntervalMs) {
    launchAutoPollWorkerExecutorIfNeeded();
    return autoPollWorkerExecutor.scheduleWithFixedDelay(
        task,
        generateRandomInitialDelayMs(autoPollIntervalMs),
        autoPollIntervalMs,
        TimeUnit.MILLISECONDS);
  }

  /////////////////////////////// async commit worker ///////////////////////////////

  private static void launchAsyncCommitWorkerExecutorIfNeeded() {
    if (Objects.isNull(asyncCommitWorkerExecutor)) {
      synchronized (SubscriptionExecutorService.class) {
        if (Objects.nonNull(asyncCommitWorkerExecutor)) {
          return;
        }

        LOGGER.info("Launching async commit worker executor...");
        asyncCommitWorkerExecutor =
            new ThreadPoolExecutor(
                getCorePoolSize(),
                getMaximumPoolSize(),
                KEEP_ALIVE_TIME_MS,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(WORK_QUEUE_CAPACITY, true),
                r -> {
                  final Thread t =
                      new Thread(
                          Thread.currentThread().getThreadGroup(),
                          r,
                          "SubscriptionPullConsumerAsyncCommitWorker",
                          0);
                  if (!t.isDaemon()) {
                    t.setDaemon(true);
                  }
                  if (t.getPriority() != Thread.NORM_PRIORITY) {
                    t.setPriority(Thread.NORM_PRIORITY);
                  }
                  return t;
                },
                new ThreadPoolExecutor.CallerRunsPolicy());
      }
    }
  }

  public static void submitAsyncCommitWorker(final Runnable task) {
    launchAsyncCommitWorkerExecutorIfNeeded();
    asyncCommitWorkerExecutor.submit(task);
  }

  /////////////////////////////// utility ///////////////////////////////

  private static int getCorePoolSize() {
    return Runtime.getRuntime().availableProcessors() / 2;
  }

  public static int getMaximumPoolSize() {
    return Runtime.getRuntime().availableProcessors();
  }

  private static long generateRandomInitialDelayMs(final long maxMs) {
    return (long) (Math.random() * maxMs);
  }
}
