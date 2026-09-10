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
import org.apache.iotdb.commons.i18n.PipeMessages;
import org.apache.iotdb.commons.pipe.agent.task.subtask.PipeSubtask;
import org.apache.iotdb.commons.utils.TestOnly;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public abstract class PipeSubtaskExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeSubtaskExecutor.class);

  private static final long WORKER_THREAD_KEEP_ALIVE_TIME_IN_SECONDS = 60L;

  private static final ExecutorService globalSubtaskCallbackListeningExecutor =
      IoTDBThreadPoolFactory.newSingleThreadExecutor(
          ThreadName.PIPE_SUBTASK_CALLBACK_EXECUTOR_POOL.getName());

  private final ExecutorService subtaskCallbackListeningExecutor;

  protected final WrappedThreadPoolExecutor underlyingThreadPool;
  protected final ListeningExecutorService subtaskWorkerThreadPoolExecutor;
  protected final ListeningScheduledExecutorService subtaskWorkerScheduledExecutor;

  private final Map<String, PipeSubtask> registeredIdSubtaskMapper;

  private final int corePoolSize;
  private int runningSubtaskNumber;
  private final String workingThreadName;
  private final String callbackThreadName;

  protected PipeSubtaskExecutor(
      final int corePoolSize,
      final String workingThreadName,
      final boolean disableLogInThreadPool) {
    this(corePoolSize, workingThreadName, null, disableLogInThreadPool);
  }

  protected PipeSubtaskExecutor(
      final int corePoolSize,
      final String workingThreadName,
      final @Nullable String callbackThreadName,
      final boolean disableLogInThreadPool) {
    this.workingThreadName = workingThreadName;
    this.callbackThreadName =
        Objects.nonNull(callbackThreadName)
            ? callbackThreadName
            : ThreadName.PIPE_SUBTASK_CALLBACK_EXECUTOR_POOL.getName();
    underlyingThreadPool =
        (WrappedThreadPoolExecutor)
            IoTDBThreadPoolFactory.newFixedThreadPoolWithIdleThreadTimeout(
                corePoolSize,
                WORKER_THREAD_KEEP_ALIVE_TIME_IN_SECONDS,
                TimeUnit.SECONDS,
                workingThreadName);
    if (disableLogInThreadPool) {
      underlyingThreadPool.disableErrorLog();
    }
    subtaskWorkerThreadPoolExecutor = MoreExecutors.listeningDecorator(underlyingThreadPool);
    final ScheduledExecutorService underlyingScheduledExecutor =
        IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(workingThreadName + "-Scheduler");
    subtaskWorkerScheduledExecutor = MoreExecutors.listeningDecorator(underlyingScheduledExecutor);
    subtaskCallbackListeningExecutor =
        Objects.nonNull(callbackThreadName)
            ? IoTDBThreadPoolFactory.newSingleThreadExecutor(
                callbackThreadName, new ThreadPoolExecutor.DiscardPolicy())
            : globalSubtaskCallbackListeningExecutor;

    registeredIdSubtaskMapper = new ConcurrentHashMap<>();

    this.corePoolSize = corePoolSize;
    runningSubtaskNumber = 0;
  }

  /////////////////////// Subtask management ///////////////////////

  public final synchronized void register(final PipeSubtask subtask) {
    if (registeredIdSubtaskMapper.containsKey(subtask.getTaskID())) {
      LOGGER.warn(PipeMessages.SUBTASK_ALREADY_REGISTERED, subtask.getDisplayTaskID());
      return;
    }

    registeredIdSubtaskMapper.put(subtask.getTaskID(), subtask);
    subtask.bindExecutors(
        subtaskWorkerThreadPoolExecutor,
        subtaskWorkerScheduledExecutor,
        subtaskCallbackListeningExecutor,
        schedulerSupplier(this));
  }

  private static String getSafeSubtaskStr(final String subtaskID) {
    return subtaskID.replaceAll("password=[^,}]*", "password=******");
  }

  protected PipeSubtaskScheduler schedulerSupplier(final PipeSubtaskExecutor executor) {
    return new PipeSubtaskScheduler(executor);
  }

  public final synchronized void start(final String subTaskID) {
    if (!registeredIdSubtaskMapper.containsKey(subTaskID)) {
      LOGGER.warn(PipeMessages.SUBTASK_NOT_REGISTERED, getSafeSubtaskStr(subTaskID));
      return;
    }

    final PipeSubtask subtask = registeredIdSubtaskMapper.get(subTaskID);
    if (subtask.isSubmittingSelf()) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(PipeMessages.SUBTASK_ALREADY_RUNNING, subtask.getDisplayTaskID());
      }
    } else {
      subtask.allowSubmittingSelf();
      subtask.submitSelf();
      ++runningSubtaskNumber;
      LOGGER.info(PipeMessages.SUBTASK_STARTED, subtask.getDisplayTaskID());
    }
  }

  public final synchronized void stop(final String subTaskID) {
    if (!registeredIdSubtaskMapper.containsKey(subTaskID)) {
      LOGGER.warn(PipeMessages.SUBTASK_NOT_REGISTERED, getSafeSubtaskStr(subTaskID));
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
        LOGGER.info(PipeMessages.SUBTASK_CLOSED, subtask.getDisplayTaskID());
      } catch (final Exception e) {
        LOGGER.error(PipeMessages.SUBTASK_CLOSE_FAILED, subtask.getDisplayTaskID(), e);
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
    subtaskWorkerScheduledExecutor.shutdown();
    if (subtaskCallbackListeningExecutor != globalSubtaskCallbackListeningExecutor) {
      subtaskCallbackListeningExecutor.shutdown();
    }
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

  public String getWorkingThreadName() {
    return workingThreadName;
  }

  public String getCallbackThreadName() {
    return callbackThreadName;
  }
}
