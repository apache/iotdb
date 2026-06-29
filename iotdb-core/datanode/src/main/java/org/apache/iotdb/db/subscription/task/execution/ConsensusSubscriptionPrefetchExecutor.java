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

package org.apache.iotdb.db.subscription.task.execution;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.subscription.config.SubscriptionConfig;
import org.apache.iotdb.db.subscription.task.subtask.ConsensusPrefetchSubtask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ConsensusSubscriptionPrefetchExecutor {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ConsensusSubscriptionPrefetchExecutor.class);

  private static final AtomicInteger ID_GENERATOR = new AtomicInteger(0);

  private final String workerThreadName;
  private final String schedulerThreadName;
  private final int workerThreadNum;

  private final BlockingQueue<ConsensusPrefetchSubtask> readyQueue = new LinkedBlockingQueue<>();
  private final Map<String, ConsensusPrefetchSubtask> taskIdToSubtask = new ConcurrentHashMap<>();
  private final AtomicBoolean shutdown = new AtomicBoolean(false);

  private final ExecutorService workerPool;
  private final ScheduledExecutorService delayedScheduler;

  public ConsensusSubscriptionPrefetchExecutor() {
    final int executorId = ID_GENERATOR.getAndIncrement();
    this.workerThreadNum =
        Math.max(
            1,
            SubscriptionConfig.getInstance()
                .getSubscriptionConsensusPrefetchExecutorMaxThreadNum());
    this.workerThreadName =
        ThreadName.SUBSCRIPTION_CONSENSUS_PREFETCH_EXECUTOR_POOL.getName() + "-" + executorId;
    this.schedulerThreadName =
        ThreadName.SUBSCRIPTION_CONSENSUS_PREFETCH_SCHEDULER.getName() + "-" + executorId;
    this.workerPool = IoTDBThreadPoolFactory.newFixedThreadPool(workerThreadNum, workerThreadName);
    this.delayedScheduler =
        IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(schedulerThreadName);

    for (int i = 0; i < workerThreadNum; i++) {
      workerPool.submit(this::workerLoop);
    }
  }

  public synchronized boolean register(final ConsensusPrefetchSubtask subtask) {
    if (shutdown.get()) {
      LOGGER.warn(
          "Consensus prefetch executor is shutdown, skip registering {}", subtask.getTaskId());
      return false;
    }
    if (taskIdToSubtask.putIfAbsent(subtask.getTaskId(), subtask) != null) {
      LOGGER.warn("Consensus prefetch subtask {} is already registered", subtask.getTaskId());
      return false;
    }
    subtask.bindExecutor(this);
    return true;
  }

  public synchronized void deregister(final String taskId) {
    final ConsensusPrefetchSubtask subtask = taskIdToSubtask.remove(taskId);
    if (subtask == null) {
      return;
    }
    readyQueue.remove(subtask);
    subtask.cancelPendingExecution();
    subtask.close();
  }

  public void enqueue(final ConsensusPrefetchSubtask subtask) {
    if (shutdown.get() || subtask.isClosed()) {
      return;
    }
    readyQueue.offer(subtask);
  }

  public void schedule(
      final ConsensusPrefetchSubtask subtask, final long delayMs, final long delayedToken) {
    if (shutdown.get() || subtask.isClosed()) {
      return;
    }
    delayedScheduler.schedule(
        () -> {
          if (!shutdown.get()) {
            subtask.fireScheduledWakeup(delayedToken);
          }
        },
        delayMs,
        TimeUnit.MILLISECONDS);
  }

  public synchronized void shutdown() {
    if (!shutdown.compareAndSet(false, true)) {
      return;
    }

    for (final ConsensusPrefetchSubtask subtask : taskIdToSubtask.values()) {
      readyQueue.remove(subtask);
      subtask.cancelPendingExecution();
      subtask.close();
    }
    taskIdToSubtask.clear();
    readyQueue.clear();

    delayedScheduler.shutdownNow();
    workerPool.shutdownNow();
  }

  public boolean isShutdown() {
    return shutdown.get();
  }

  private void workerLoop() {
    try {
      while (!shutdown.get() && !Thread.currentThread().isInterrupted()) {
        final ConsensusPrefetchSubtask subtask = readyQueue.take();
        if (subtask.isClosed()) {
          continue;
        }
        subtask.runOneRound();
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (final Throwable t) {
      LOGGER.error("Consensus prefetch worker loop exits abnormally", t);
    }
  }
}
