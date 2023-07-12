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

  private final boolean autoRestartPipeEnabled =
      PipeConfig.getInstance().getPipeAutoRestartEnabled();

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
    ProcedureManager procedureManager = configManager.getProcedureManager();
    final AtomicReference<PipeTaskInfo> pipeTaskInfo =
        configManager.getPipeManager().getPipeTaskCoordinator().lock();

    try {
      boolean needBroadcastRestartSignal = false;

      if (autoRestartPipeEnabled
          && pipeAutoRestartRoundCounter.incrementAndGet()
              == PipeConfig.getInstance().getPipeMetaSyncerAutoRestartPipeCheckIntervalRound()) {
        needBroadcastRestartSignal = pipeTaskInfo.get().autoRestart();
        pipeAutoRestartRoundCounter.set(0);
      }

      final TSStatus status = procedureManager.pipeMetaSync();

      if (needBroadcastRestartSignal) {
        pipeTaskInfo.get().handleSuccessfulRestart();
      }

      if (needBroadcastRestartSignal
          || status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        procedureManager.pipeHandleMetaChange(true, true);
      }
    } finally {
      configManager.getPipeManager().getPipeTaskCoordinator().unlock();
    }
  }

  public synchronized void stop() {
    if (metaSyncFuture != null) {
      metaSyncFuture.cancel(false);
      metaSyncFuture = null;
      LOGGER.info("PipeMetaSyncer is stopped successfully.");
    }
  }
}
