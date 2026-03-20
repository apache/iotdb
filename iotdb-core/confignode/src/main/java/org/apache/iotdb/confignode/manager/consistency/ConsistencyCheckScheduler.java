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

package org.apache.iotdb.confignode.manager.consistency;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.consensus.iotv2.consistency.RepairProgressTable;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.ProcedureManager;
import org.apache.iotdb.confignode.procedure.impl.consistency.LiveDataRegionRepairExecutionContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Comparator;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConsistencyCheckScheduler {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConsistencyCheckScheduler.class);

  private final ConfigManager configManager;
  private final ProcedureManager procedureManager;
  private final ScheduledExecutorService executorService =
      IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor("ConfigNode-Consistency-Check");
  private final RegionCheckExecutor regionCheckExecutor;
  private final long initialDelayMs;
  private final long intervalMs;
  private final AtomicBoolean roundRunning = new AtomicBoolean(false);
  private final Object scheduleMonitor = new Object();

  private Future<?> scheduledFuture;

  public ConsistencyCheckScheduler(ConfigManager configManager, ProcedureManager procedureManager) {
    this(
        configManager,
        procedureManager,
        ConfigNodeDescriptor.getInstance().getConf().getConsistencyCheckSchedulerInitialDelayInMs(),
        ConfigNodeDescriptor.getInstance().getConf().getConsistencyCheckSchedulerIntervalInMs(),
        (manager, consensusGroupId, progressTable) -> {
          LiveDataRegionRepairExecutionContext executionContext =
              new LiveDataRegionRepairExecutionContext(
                  manager, consensusGroupId, Collections.emptySet(), null, false);
          executionContext.collectPendingPartitions(
              executionContext.computeSafeWatermark(), progressTable);
        });
  }

  @TestOnly
  ConsistencyCheckScheduler(
      ConfigManager configManager,
      ProcedureManager procedureManager,
      long initialDelayMs,
      long intervalMs,
      RegionCheckExecutor regionCheckExecutor) {
    this.configManager = configManager;
    this.procedureManager = procedureManager;
    this.initialDelayMs = initialDelayMs;
    this.intervalMs = intervalMs;
    this.regionCheckExecutor = regionCheckExecutor;
  }

  public void start() {
    synchronized (scheduleMonitor) {
      if (scheduledFuture != null) {
        return;
      }
      scheduledFuture =
          ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
              executorService,
              this::runOneRound,
              initialDelayMs,
              intervalMs,
              TimeUnit.MILLISECONDS);
      LOGGER.info(
          "Consistency check scheduler started with initialDelay={}ms interval={}ms",
          initialDelayMs,
          intervalMs);
    }
  }

  public void stop() {
    synchronized (scheduleMonitor) {
      if (scheduledFuture == null) {
        return;
      }
      scheduledFuture.cancel(false);
      scheduledFuture = null;
      LOGGER.info("Consistency check scheduler stopped");
    }
  }

  void runOneRound() {
    if (!roundRunning.compareAndSet(false, true)) {
      LOGGER.debug("Skip consistency check round because the previous round is still running");
      return;
    }

    try {
      configManager
          .getPartitionManager()
          .getAllReplicaSetsMap(TConsensusGroupType.DataRegion)
          .keySet()
          .stream()
          .sorted(Comparator.comparingInt(TConsensusGroupId::getId))
          .forEach(this::checkOneRegion);
    } finally {
      roundRunning.set(false);
    }
  }

  private void checkOneRegion(TConsensusGroupId consensusGroupId) {
    if (procedureManager.hasRunningRepairProcedure(consensusGroupId)) {
      LOGGER.debug(
          "Skip background consistency check for region {} because a repair is running",
          consensusGroupId);
      return;
    }

    try {
      ConsistencyProgressManager consistencyProgressManager =
          configManager.getConsistencyProgressManager();
      if (consistencyProgressManager == null) {
        LOGGER.debug(
            "Skip background consistency check for region {} because progress manager is not ready",
            consensusGroupId);
        return;
      }
      RepairProgressTable progressTable =
          consistencyProgressManager.loadRepairProgressTable(consensusGroupId);
      regionCheckExecutor.execute(configManager, consensusGroupId, progressTable);
      consistencyProgressManager.persistRepairProgressTable(progressTable);
    } catch (Exception e) {
      LOGGER.warn("Background consistency check failed for region {}", consensusGroupId, e);
    }
  }

  @FunctionalInterface
  interface RegionCheckExecutor {
    void execute(
        ConfigManager configManager,
        TConsensusGroupId consensusGroupId,
        RepairProgressTable progressTable)
        throws Exception;
  }
}
