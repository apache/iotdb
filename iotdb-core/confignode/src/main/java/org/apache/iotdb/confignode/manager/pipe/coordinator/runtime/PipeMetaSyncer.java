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

package org.apache.iotdb.confignode.manager.pipe.coordinator.runtime;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.resource.log.PipeLogger;
import org.apache.iotdb.confignode.i18n.ManagerMessages;
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
  private final AtomicInteger consensusPipeCheckRoundCounter = new AtomicInteger(0);

  private static final int CONSENSUS_PIPE_CHECK_INTERVAL_ROUND = 5;

  private final boolean pipeAutoRestartEnabled =
      PipeConfig.getInstance().getPipeAutoRestartEnabled();

  // This variable is used to record whether the last sync operation was successful. If successful,
  // before the next sync operation, it can be determined based on PipeTaskInfoVersion whether the
  // pipe metadata in DN and the latest pipe metadata in CN have been synchronized and whether there
  // is a pipe task in the current cluster, thereby skipping unnecessary sync operations.
  private boolean isLastPipeSyncSuccessful = false;

  PipeMetaSyncer(ConfigManager configManager) {
    this.configManager = configManager;
  }

  public synchronized void start() {
    if (metaSyncFuture == null) {
      metaSyncFuture =
          ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
              SYNC_EXECUTOR,
              this::sync,
              INITIAL_SYNC_DELAY_MINUTES,
              SYNC_INTERVAL_MINUTES,
              TimeUnit.MINUTES);
      LOGGER.info(ManagerMessages.PIPEMETASYNCER_IS_STARTED_SUCCESSFULLY);
    }
  }

  private synchronized void sync() {
    if (isLastPipeSyncSuccessful
        && configManager.getPipeManager().getPipeTaskCoordinator().canSkipNextSync()) {
      return;
    }

    isLastPipeSyncSuccessful = false;

    if (configManager.getPipeManager().getPipeTaskCoordinator().isLocked()) {
      PipeLogger.log(
          LOGGER::warn,
          ManagerMessages.PIPETASKCOORDINATORLOCK_IS_HELD_BY_ANOTHER_THREAD_SKIP_THIS_ROUND_OF_2);
      return;
    }

    final ProcedureManager procedureManager = configManager.getProcedureManager();

    boolean somePipesNeedRestarting = false;

    if (pipeAutoRestartEnabled
        && pipeAutoRestartRoundCounter.incrementAndGet()
            == PipeConfig.getInstance().getPipeMetaSyncerAutoRestartPipeCheckIntervalRound()) {
      somePipesNeedRestarting = autoRestartWithLock();
      if (somePipesNeedRestarting) {
        PipeLogger.log(
            LOGGER::info,
            ManagerMessages.SOME_PIPES_NEED_RESTARTING_WILL_RESTART_THEM_AFTER_THIS_SYNC);
      }
      pipeAutoRestartRoundCounter.set(0);
    }

    if (consensusPipeCheckRoundCounter.incrementAndGet() >= CONSENSUS_PIPE_CHECK_INTERVAL_ROUND) {
      consensusPipeCheckRoundCounter.set(0);
      checkAndRepairConsensusPipes();
    }

    final TSStatus metaSyncStatus = procedureManager.pipeMetaSync();

    if (metaSyncStatus.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      boolean successfulSync = !somePipesNeedRestarting;

      if (somePipesNeedRestarting && handleSuccessfulRestartWithLock()) {
        final TSStatus handleMetaChangeStatus =
            procedureManager.pipeHandleMetaChangeWithBlock(true, false);
        if (handleMetaChangeStatus.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          successfulSync = true;
        } else {
          PipeLogger.log(
              LOGGER::warn,
              ManagerMessages.FAILED_TO_HANDLE_PIPE_META_CHANGE_RESULT_STATUS,
              handleMetaChangeStatus);
        }
      }

      if (successfulSync) {
        PipeLogger.log(
            LOGGER::info,
            ManagerMessages.AFTER_THIS_SUCCESSFUL_SYNC_IF_PIPETASKINFO_IS_EMPTY_DURING_THIS);
        isLastPipeSyncSuccessful = true;
      }
    } else {
      PipeLogger.log(
          LOGGER::warn, ManagerMessages.FAILED_TO_SYNC_PIPE_META_RESULT_STATUS, metaSyncStatus);
    }
  }

  public synchronized void stop() {
    if (metaSyncFuture != null) {
      metaSyncFuture.cancel(false);
      metaSyncFuture = null;
      LOGGER.info(ManagerMessages.PIPEMETASYNCER_IS_STOPPED_SUCCESSFULLY);
    }
  }

  private boolean autoRestartWithLock() {
    final AtomicReference<PipeTaskInfo> pipeTaskInfo =
        configManager.getPipeManager().getPipeTaskCoordinator().tryLock();
    if (pipeTaskInfo == null) {
      PipeLogger.log(
          LOGGER::warn, ManagerMessages.FAILED_TO_ACQUIRE_PIPE_LOCK_FOR_AUTO_RESTART_PIPE_TASK);
      return false;
    }
    try {
      return pipeTaskInfo.get().autoRestart();
    } finally {
      configManager.getPipeManager().getPipeTaskCoordinator().unlock();
    }
  }

  private void checkAndRepairConsensusPipes() {
    try {
      configManager
          .getProcedureManager()
          .getEnv()
          .getRegionMaintainHandler()
          .checkAndRepairConsensusPipes();
    } catch (Exception e) {
      PipeLogger.log(LOGGER::warn, e, ManagerMessages.FAILED_TO_CHECK_AND_REPAIR_CONSENSUS_PIPES);
    }
  }

  private boolean handleSuccessfulRestartWithLock() {
    final AtomicReference<PipeTaskInfo> pipeTaskInfo =
        configManager.getPipeManager().getPipeTaskCoordinator().tryLock();
    if (pipeTaskInfo == null) {
      PipeLogger.log(
          LOGGER::warn,
          ManagerMessages.FAILED_TO_ACQUIRE_PIPE_LOCK_FOR_HANDLING_SUCCESSFUL_RESTART);
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
