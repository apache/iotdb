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

package org.apache.iotdb.commons.pipe.agent.task.execution;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.WrappedThreadPoolExecutor;
import org.apache.iotdb.commons.pipe.agent.task.subtask.PipeSubtask;
import org.apache.iotdb.commons.utils.TestOnly;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

public abstract class PipeSubtaskExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeSubtaskExecutor.class);

  private static final ExecutorService subtaskCallbackListeningExecutor =
      IoTDBThreadPoolFactory.newSingleThreadExecutor(
          ThreadName.PIPE_SUBTASK_CALLBACK_EXECUTOR_POOL.getName());

  protected final WrappedThreadPoolExecutor underlyingThreadPool;
  protected final ListeningExecutorService subtaskWorkerThreadPoolExecutor;

  private final Map<String, PipeSubtask> registeredIdSubtaskMapper;

  private final int corePoolSize;
  private int runningSubtaskNumber;

  protected PipeSubtaskExecutor(
      final int corePoolSize, final ThreadName threadName, final boolean disableLogInThreadPool) {
    underlyingThreadPool =
        (WrappedThreadPoolExecutor)
            IoTDBThreadPoolFactory.newFixedThreadPool(corePoolSize, threadName.getName());
    if (disableLogInThreadPool) {
      underlyingThreadPool.disableErrorLog();
    }
    subtaskWorkerThreadPoolExecutor = MoreExecutors.listeningDecorator(underlyingThreadPool);

    registeredIdSubtaskMapper = new ConcurrentHashMap<>();

    this.corePoolSize = corePoolSize;
    runningSubtaskNumber = 0;
  }

  /////////////////////// Subtask management ///////////////////////

  public final synchronized void register(final PipeSubtask subtask) {
    if (registeredIdSubtaskMapper.containsKey(subtask.getTaskID())) {
      LOGGER.warn("The subtask {} is already registered.", subtask.getTaskID());
      return;
    }

    registeredIdSubtaskMapper.put(subtask.getTaskID(), subtask);
    subtask.bindExecutors(
        subtaskWorkerThreadPoolExecutor, subtaskCallbackListeningExecutor, schedulerSupplier(this));
  }

  protected PipeSubtaskScheduler schedulerSupplier(final PipeSubtaskExecutor executor) {
    return new PipeSubtaskScheduler(executor);
  }

  public final synchronized void start(final String subTaskID) {
    if (!registeredIdSubtaskMapper.containsKey(subTaskID)) {
      LOGGER.warn("The subtask {} is not registered.", subTaskID);
      return;
    }

    final PipeSubtask subtask = registeredIdSubtaskMapper.get(subTaskID);
    if (subtask.isSubmittingSelf()) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("The subtask {} is already running.", subTaskID);
      }
    } else {
      subtask.allowSubmittingSelf();
      subtask.submitSelf();
      ++runningSubtaskNumber;
      LOGGER.info("The subtask {} is started to submit self.", subTaskID);
    }
  }

  public final synchronized void stop(final String subTaskID) {
    if (!registeredIdSubtaskMapper.containsKey(subTaskID)) {
      LOGGER.warn("The subtask {} is not registered.", subTaskID);
      return;
    }

    if (registeredIdSubtaskMapper.get(subTaskID).disallowSubmittingSelf()) {
      --runningSubtaskNumber;
    }
  }

  public final synchronized void deregister(final String subTaskID) {
    stop(subTaskID);

    final PipeSubtask subtask = registeredIdSubtaskMapper.remove(subTaskID);

    if (subtask != null) {
      try {
        subtask.close();
        LOGGER.info("The subtask {} is closed successfully.", subTaskID);
      } catch (final Exception e) {
        LOGGER.error("Failed to close the subtask {}.", subTaskID, e);
      }
    }
  }

  @TestOnly
  public final boolean isRegistered(final String subTaskID) {
    return registeredIdSubtaskMapper.containsKey(subTaskID);
  }

  @TestOnly
  public final int getRegisteredSubtaskNumber() {
    return registeredIdSubtaskMapper.size();
  }

  /////////////////////// executor management  ///////////////////////

  public final synchronized void shutdown() {
    if (isShutdown()) {
      return;
    }

    // stop all subtasks before shutting down the executor
    for (final PipeSubtask subtask : registeredIdSubtaskMapper.values()) {
      subtask.disallowSubmittingSelf();
    }

    subtaskWorkerThreadPoolExecutor.shutdown();
  }

  public final boolean isShutdown() {
    return subtaskWorkerThreadPoolExecutor.isShutdown();
  }

  public final int getCorePoolSize() {
    return corePoolSize;
  }

  public final int getRunningSubtaskNumber() {
    return runningSubtaskNumber;
  }

  protected final boolean hasAvailableThread() {
    // TODO: temporarily disable async receiver subtask execution
    return false;
    // return getAvailableThreadCount() > 0;
  }

  private int getAvailableThreadCount() {
    return underlyingThreadPool.getCorePoolSize() - underlyingThreadPool.getActiveCount();
  }
}
