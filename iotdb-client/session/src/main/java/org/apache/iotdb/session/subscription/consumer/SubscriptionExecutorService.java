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

import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

final class SubscriptionExecutorService {

  private static volatile ScheduledExecutorService heartbeatWorkerExecutor;
  private static volatile ScheduledExecutorService endpointsSyncerExecutor;
  private static volatile ScheduledExecutorService asyncCommitWorkerExecutor;
  private static volatile ScheduledExecutorService autoCommitWorkerExecutor;
  private static volatile ScheduledExecutorService autoPollWorkerExecutor;

  static {
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  if (Objects.nonNull(heartbeatWorkerExecutor)) {
                    heartbeatWorkerExecutor.shutdown();
                    heartbeatWorkerExecutor = null;
                  }
                  if (Objects.nonNull(endpointsSyncerExecutor)) {
                    endpointsSyncerExecutor.shutdown();
                    endpointsSyncerExecutor = null;
                  }
                  if (Objects.nonNull(asyncCommitWorkerExecutor)) {
                    asyncCommitWorkerExecutor.shutdown();
                    asyncCommitWorkerExecutor = null;
                  }
                  if (Objects.nonNull(autoCommitWorkerExecutor)) {
                    autoCommitWorkerExecutor.shutdown();
                    autoCommitWorkerExecutor = null;
                  }
                  if (Objects.nonNull(autoPollWorkerExecutor)) {
                    autoPollWorkerExecutor.shutdown();
                    autoPollWorkerExecutor = null;
                  }
                }));
  }

  /////////////////////////////// heartbeat worker ///////////////////////////////

  @SuppressWarnings("unsafeThreadSchedule")
  private static void launchHeartbeatWorkerExecutorIfNeeded() {
    if (Objects.isNull(heartbeatWorkerExecutor)) {
      synchronized (SubscriptionExecutorService.class) {
        if (Objects.nonNull(heartbeatWorkerExecutor)) {
          return;
        }

        heartbeatWorkerExecutor =
            Executors.newScheduledThreadPool(
                getCorePoolSize(),
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

  public static ScheduledFuture<?> submitHeartbeatWorker(
      final Runnable task, final long heartbeatIntervalMs) {
    launchHeartbeatWorkerExecutorIfNeeded();
    return heartbeatWorkerExecutor.scheduleWithFixedDelay(
        task,
        generateRandomInitialDelayMs(heartbeatIntervalMs),
        heartbeatIntervalMs,
        TimeUnit.MILLISECONDS);
  }

  /////////////////////////////// endpoints syncer ///////////////////////////////

  @SuppressWarnings("unsafeThreadSchedule")
  private static void launchEndpointsSyncerExecutorIfNeeded() {
    if (Objects.isNull(endpointsSyncerExecutor)) {
      synchronized (SubscriptionExecutorService.class) {
        if (Objects.nonNull(endpointsSyncerExecutor)) {
          return;
        }

        endpointsSyncerExecutor =
            Executors.newScheduledThreadPool(
                getCorePoolSize(),
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

  public static ScheduledFuture<?> submitEndpointsSyncer(
      final Runnable task, final long endpointsSyncIntervalMs) {
    launchEndpointsSyncerExecutorIfNeeded();
    return endpointsSyncerExecutor.scheduleWithFixedDelay(
        task,
        generateRandomInitialDelayMs(endpointsSyncIntervalMs),
        endpointsSyncIntervalMs,
        TimeUnit.MILLISECONDS);
  }

  /////////////////////////////// async commit worker ///////////////////////////////

  @SuppressWarnings("unsafeThreadSchedule")
  private static void launchAsyncCommitWorkerExecutorIfNeeded() {
    if (Objects.isNull(asyncCommitWorkerExecutor)) {
      synchronized (SubscriptionExecutorService.class) {
        if (Objects.nonNull(asyncCommitWorkerExecutor)) {
          return;
        }

        asyncCommitWorkerExecutor =
            Executors.newScheduledThreadPool(
                getCorePoolSize(),
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
                });
      }
    }
  }

  public static void submitAsyncCommitWorker(final Runnable task) {
    launchAsyncCommitWorkerExecutorIfNeeded();
    asyncCommitWorkerExecutor.submit(task);
  }

  /////////////////////////////// auto commit worker ///////////////////////////////

  @SuppressWarnings("unsafeThreadSchedule")
  private static void launchAutoCommitWorkerExecutorIfNeeded() {
    if (Objects.isNull(autoCommitWorkerExecutor)) {
      synchronized (SubscriptionExecutorService.class) {
        if (Objects.nonNull(autoCommitWorkerExecutor)) {
          return;
        }

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

  @SuppressWarnings("unsafeThreadSchedule")
  private static void launchAutoPollWorkerExecutorIfNeeded() {
    if (Objects.isNull(autoPollWorkerExecutor)) {
      synchronized (SubscriptionExecutorService.class) {
        if (Objects.nonNull(autoPollWorkerExecutor)) {
          return;
        }

        autoPollWorkerExecutor =
            Executors.newScheduledThreadPool(
                getCorePoolSize(),
                r -> {
                  final Thread t =
                      new Thread(
                          Thread.currentThread().getThreadGroup(),
                          r,
                          "SubscriptionPullConsumerAutoPollWorker",
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

  public static ScheduledFuture<?> submitAutoPollWorker(
      final Runnable task, final long autoPollIntervalMs) {
    launchAutoPollWorkerExecutorIfNeeded();
    return autoPollWorkerExecutor.scheduleWithFixedDelay(
        task,
        generateRandomInitialDelayMs(autoPollIntervalMs),
        autoPollIntervalMs,
        TimeUnit.MILLISECONDS);
  }

  /////////////////////////////// utility ///////////////////////////////

  private static int getCorePoolSize() {
    return Math.min(5, Math.max(1, Runtime.getRuntime().availableProcessors() / 2));
  }

  private static long generateRandomInitialDelayMs(final long maxMs) {
    return (long) (Math.random() * maxMs);
  }
}
