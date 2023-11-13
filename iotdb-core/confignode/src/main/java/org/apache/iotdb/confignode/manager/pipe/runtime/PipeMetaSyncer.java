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

package org.apache.iotdb.confignode.manager.pipe.runtime;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.ProcedureManager;
import org.apache.iotdb.confignode.persistence.pipe.PipeTaskInfo;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class PipeMetaSyncer {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeMetaSyncer.class);

  private static final ScheduledExecutorService SYNC_EXECUTOR =
      IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
          ThreadName.PIPE_RUNTIME_META_SYNCER.getName());
  private static final long INITIAL_SYNC_DELAY_MINUTES =
      PipeConfig.getInstance().getPipeMetaSyncerInitialSyncDelayMinutes();
  private static final long SYNC_INTERVAL_MINUTES =
      PipeConfig.getInstance().getPipeMetaSyncerSyncIntervalMinutes();

  private final ConfigManager configManager;

  private Future<?> metaSyncFuture;

  private final AtomicInteger pipeAutoRestartRoundCounter = new AtomicInteger(0);

  private final boolean pipeAutoRestartEnabled =
      PipeConfig.getInstance().getPipeAutoRestartEnabled();

  // This variable is used to record whether the sync operation was successful last time when there
  // was no pipe in CN. If successful, before the next sync operation, if there is still no pipe in
  // CN, it means that the pipe metadata in DN and CN have been synchronized, and this round of sync
  // can be safely skipped.
  private boolean isLastEmptyPipeSyncSuccessful = false;

  PipeMetaSyncer(ConfigManager configManager) {
    this.configManager = configManager;
  }

  public synchronized void start() {
    while (configManager.getConsensusManager() == null) {
      try {
        LOGGER.info("Consensus layer is not ready, sleep 1s...");
        TimeUnit.SECONDS.sleep(1);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.warn("Unexpected interruption during waiting for consensus layer ready.");
      }
    }

    if (metaSyncFuture == null) {
      metaSyncFuture =
          ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
              SYNC_EXECUTOR,
              this::sync,
              INITIAL_SYNC_DELAY_MINUTES,
              SYNC_INTERVAL_MINUTES,
              TimeUnit.MINUTES);
      LOGGER.info("PipeMetaSyncer is started successfully.");
    }
  }

  private synchronized void sync() {
    if (!configManager.getPipeManager().getPipeTaskCoordinator().hasAnyPipe()
        && isLastEmptyPipeSyncSuccessful) {
      return;
    }

    isLastEmptyPipeSyncSuccessful = false;

    if (configManager.getPipeManager().getPipeTaskCoordinator().isLocked()) {
      LOGGER.warn(
          "PipeTaskCoordinatorLock is held by another thread, skip this round of sync to avoid procedure and rpc accumulation as much as possible");
      return;
    }

    final ProcedureManager procedureManager = configManager.getProcedureManager();

    boolean somePipesNeedRestarting = false;

    if (pipeAutoRestartEnabled
        && pipeAutoRestartRoundCounter.incrementAndGet()
            == PipeConfig.getInstance().getPipeMetaSyncerAutoRestartPipeCheckIntervalRound()) {
      somePipesNeedRestarting = autoRestartWithLock();
      pipeAutoRestartRoundCounter.set(0);
    }

    final TSStatus metaSyncStatus = procedureManager.pipeMetaSync();

    if (somePipesNeedRestarting
        && metaSyncStatus.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      final boolean isRestartSuccessful = handleSuccessfulRestartWithLock();
      final TSStatus handleMetaChangeStatus =
          procedureManager.pipeHandleMetaChangeWithBlock(true, false);
      if (!configManager.getPipeManager().getPipeTaskCoordinator().hasAnyPipe()
          && isRestartSuccessful
          && handleMetaChangeStatus.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.info(
            "set isLastEmptyPipeSyncSuccessful to true, skip next round of sync if no pipe");
        isLastEmptyPipeSyncSuccessful = true;
      }
    }

    if (!somePipesNeedRestarting
        && metaSyncStatus.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      if (!configManager.getPipeManager().getPipeTaskCoordinator().hasAnyPipe()) {
        LOGGER.info(
            "set isLastEmptyPipeSyncSuccessful to true, skip next round of sync if no pipe");
        isLastEmptyPipeSyncSuccessful = true;
      }
    }

    if (metaSyncStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn("Failed to sync pipe meta. Result status: {}.", metaSyncStatus);
      final TSStatus handleMetaChangeStatus =
          procedureManager.pipeHandleMetaChangeWithBlock(true, true);
      if (handleMetaChangeStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.warn(
            "Failed to handle pipe meta change. Result status: {}.", handleMetaChangeStatus);
      }
    }
  }

  public synchronized void stop() {
    if (metaSyncFuture != null) {
      metaSyncFuture.cancel(false);
      metaSyncFuture = null;
      LOGGER.info("PipeMetaSyncer is stopped successfully.");
    }
  }

  private boolean autoRestartWithLock() {
    final AtomicReference<PipeTaskInfo> pipeTaskInfo =
        configManager.getPipeManager().getPipeTaskCoordinator().tryLock();
    if (pipeTaskInfo == null) {
      LOGGER.warn("Failed to acquire pipe lock for auto restart pipe task.");
      return false;
    }
    try {
      return pipeTaskInfo.get().autoRestart();
    } finally {
      configManager.getPipeManager().getPipeTaskCoordinator().unlock();
    }
  }

  private boolean handleSuccessfulRestartWithLock() {
    final AtomicReference<PipeTaskInfo> pipeTaskInfo =
        configManager.getPipeManager().getPipeTaskCoordinator().tryLock();
    if (pipeTaskInfo == null) {
      LOGGER.warn("Failed to acquire pipe lock for handling successful restart.");
      return false;
    }
    try {
      pipeTaskInfo.get().handleSuccessfulRestart();
      return true;
    } finally {
      configManager.getPipeManager().getPipeTaskCoordinator().unlock();
    }
  }
}
