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

package org.apache.iotdb.confignode.manager.pipe;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.load.subscriber.IClusterStatusSubscriber;
import org.apache.iotdb.confignode.manager.load.subscriber.RouteChangeEvent;
import org.apache.iotdb.confignode.manager.load.subscriber.StatisticsChangeEvent;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class PipeRuntimeCoordinator implements IClusterStatusSubscriber {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeRuntimeCoordinator.class);

  private final ConfigManager configManager;

  /** * Pipe Meta Sync *** */
  private static final long SYNC_INTERVAL = 3; // 3 minutes

  private final Object metaSyncMonitor = new Object();
  private Future<?> currentMetaSyncFuture;
  private final ScheduledExecutorService metaSyncExecutor =
      IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor("PipeMeta-Sync-Thread");

  public PipeRuntimeCoordinator(ConfigManager configManager) {
    this.configManager = configManager;
  }

  @Override
  public void onClusterStatisticsChanged(StatisticsChangeEvent event) {
    // do nothing, because pipe task is not related to statistics
  }

  @Override
  public void onRegionGroupLeaderChanged(RouteChangeEvent event) {
    // we only care about data region leader change
    final Map<TConsensusGroupId, Pair<Integer, Integer>> dataRegionGroupToOldAndNewLeaderPairMap =
        new HashMap<>();
    event
        .getLeaderMap()
        .forEach(
            (regionId, pair) -> {
              if (regionId.getType().equals(TConsensusGroupType.DataRegion)) {
                dataRegionGroupToOldAndNewLeaderPairMap.put(regionId, pair);
              }
            });

    // if no data region leader change, return
    if (dataRegionGroupToOldAndNewLeaderPairMap.isEmpty()) {
      return;
    }

    final TSStatus result =
        configManager
            .getProcedureManager()
            .pipeHandleLeaderChange(dataRegionGroupToOldAndNewLeaderPairMap);
    if (result.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn(
          "PipeRuntimeCoordinator meets error in handling data region leader change, status: ({})",
          result);
    }
  }

  public void startPipeMetaSyncThread() {
    synchronized (metaSyncMonitor) {
      if (currentMetaSyncFuture == null) {
        currentMetaSyncFuture =
            ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
                metaSyncExecutor, this::pipeMetaSyncLoopBody, 0, SYNC_INTERVAL, TimeUnit.MINUTES);
      }
    }
  }

  public void stopPipeMetaSyncThread() {
    synchronized (metaSyncMonitor) {
      if (currentMetaSyncFuture != null) {
        currentMetaSyncFuture.cancel(false);
        currentMetaSyncFuture = null;
        LOGGER.info("PipeMetaSync Thread is stopped successfully.");
      }
    }
  }

  private void pipeMetaSyncLoopBody() {
    final TSStatus status = configManager.getProcedureManager().pipeMetaSync();
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn("PipeRuntimeCoordinator meets error in syncing pipe meta, status: ({})", status);
    }
  }
}
