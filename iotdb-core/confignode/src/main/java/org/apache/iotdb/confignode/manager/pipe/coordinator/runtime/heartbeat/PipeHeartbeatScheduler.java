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

package org.apache.iotdb.confignode.manager.pipe.coordinator.runtime.heartbeat;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
import org.apache.iotdb.confignode.client.async.CnToDnInternalServiceAsyncRequestManager;
import org.apache.iotdb.confignode.client.async.handlers.DataNodeAsyncRequestContext;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.pipe.agent.PipeConfigNodeAgent;
import org.apache.iotdb.mpp.rpc.thrift.TPipeHeartbeatReq;
import org.apache.iotdb.mpp.rpc.thrift.TPipeHeartbeatResp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class PipeHeartbeatScheduler {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeHeartbeatScheduler.class);

  private static final boolean IS_SEPERATED_PIPE_HEARTBEAT_ENABLED =
      PipeConfig.getInstance().isSeperatedPipeHeartbeatEnabled();
  private static final long HEARTBEAT_INTERVAL_SECONDS =
      PipeConfig.getInstance().getPipeHeartbeatIntervalSecondsForCollectingPipeMeta();

  private static final ScheduledExecutorService HEARTBEAT_EXECUTOR =
      IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
          ThreadName.PIPE_RUNTIME_HEARTBEAT.getName());

  private final ConfigManager configManager;
  private final PipeHeartbeatParser pipeHeartbeatParser;

  private Future<?> heartbeatFuture;

  public PipeHeartbeatScheduler(final ConfigManager configManager) {
    this.configManager = configManager;
    this.pipeHeartbeatParser = new PipeHeartbeatParser(configManager);
  }

  public synchronized void start() {
    if (IS_SEPERATED_PIPE_HEARTBEAT_ENABLED && heartbeatFuture == null) {
      heartbeatFuture =
          ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
              HEARTBEAT_EXECUTOR,
              this::heartbeat,
              HEARTBEAT_INTERVAL_SECONDS,
              HEARTBEAT_INTERVAL_SECONDS,
              TimeUnit.SECONDS);
      LOGGER.info("PipeHeartbeat is started successfully.");
    }
  }

  private synchronized void heartbeat() {
    if (!configManager.getPipeManager().getPipeTaskCoordinator().hasAnyPipe()) {
      return;
    }

    if (configManager.getPipeManager().getPipeTaskCoordinator().isLocked()) {
      LOGGER.warn(
          "PipeTaskCoordinatorLock is held by another thread, skip this round of heartbeat to avoid procedure and rpc accumulation as much as possible");
      return;
    }

    // Data node heartbeat
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    final TPipeHeartbeatReq request = new TPipeHeartbeatReq(System.currentTimeMillis());
    LOGGER.info("Collecting pipe heartbeat {} from data nodes", request.heartbeatId);

    final DataNodeAsyncRequestContext<TPipeHeartbeatReq, TPipeHeartbeatResp> clientHandler =
        new DataNodeAsyncRequestContext<>(
            CnToDnAsyncRequestType.PIPE_HEARTBEAT, request, dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance()
        .sendAsyncRequestToNodeWithRetryAndTimeoutInMs(
            clientHandler,
            PipeConfig.getInstance().getPipeHeartbeatIntervalSecondsForCollectingPipeMeta()
                * 1000L
                * 2
                / 3);
    clientHandler
        .getResponseMap()
        .forEach(
            (dataNodeId, resp) ->
                pipeHeartbeatParser.parseHeartbeat(
                    dataNodeId,
                    new PipeHeartbeat(
                        resp.getPipeMetaList(),
                        resp.getPipeCompletedList(),
                        resp.getPipeRemainingEventCountList(),
                        resp.getPipeRemainingTimeList())));

    // config node heartbeat
    try {
      final TPipeHeartbeatResp configNodeResp = new TPipeHeartbeatResp();
      PipeConfigNodeAgent.task().collectPipeMetaList(request, configNodeResp);
      pipeHeartbeatParser.parseHeartbeat(
          ConfigNodeDescriptor.getInstance().getConf().getConfigNodeId(),
          new PipeHeartbeat(
              configNodeResp.getPipeMetaList(),
              null,
              configNodeResp.getPipeRemainingEventCountList(),
              configNodeResp.getPipeRemainingTimeList()));
    } catch (final Exception e) {
      LOGGER.warn("Failed to collect pipe meta list from config node task agent", e);
    }
  }

  public synchronized void stop() {
    if (IS_SEPERATED_PIPE_HEARTBEAT_ENABLED && heartbeatFuture != null) {
      heartbeatFuture.cancel(false);
      heartbeatFuture = null;
      LOGGER.info("PipeHeartbeat is stopped successfully.");
    }
  }

  public void parseHeartbeat(final int dataNodeId, final PipeHeartbeat pipeHeartbeat) {
    pipeHeartbeatParser.parseHeartbeat(dataNodeId, pipeHeartbeat);
  }
}
