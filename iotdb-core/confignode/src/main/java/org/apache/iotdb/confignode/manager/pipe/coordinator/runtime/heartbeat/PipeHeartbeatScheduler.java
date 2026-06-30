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
import org.apache.iotdb.common.rpc.thrift.TPipeHeartbeatResp;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.resource.log.PipeLogger;
import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
import org.apache.iotdb.confignode.client.async.CnToDnInternalServiceAsyncRequestManager;
import org.apache.iotdb.confignode.client.async.handlers.DataNodeAsyncRequestContext;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.i18n.ManagerMessages;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.pipe.agent.PipeConfigNodeAgent;
import org.apache.iotdb.mpp.rpc.thrift.TPipeHeartbeatReq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
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
  private static final int PIPE_HEARTBEAT_RETRY_NUM = 1;

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
      LOGGER.info(ManagerMessages.PIPEHEARTBEAT_IS_STARTED_SUCCESSFULLY);
    }
  }

  private synchronized void heartbeat() {
    if (!configManager.getPipeManager().getPipeTaskCoordinator().hasAnyPipe()) {
      return;
    }

    if (configManager.getPipeManager().getPipeTaskCoordinator().isLocked()) {
      PipeLogger.log(
          LOGGER::warn,
          ManagerMessages.PIPETASKCOORDINATORLOCK_IS_HELD_BY_ANOTHER_THREAD_SKIP_THIS_ROUND_OF);
      return;
    }

    // Data node heartbeat
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    final TPipeHeartbeatReq request = new TPipeHeartbeatReq(System.currentTimeMillis());
    LOGGER.debug(ManagerMessages.COLLECTING_PIPE_HEARTBEAT_FROM_DATA_NODES, request.heartbeatId);

    final DataNodeAsyncRequestContext<TPipeHeartbeatReq, TPipeHeartbeatResp> clientHandler =
        new DataNodeAsyncRequestContext<>(
            CnToDnAsyncRequestType.PIPE_HEARTBEAT, request, dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance()
        .sendAsyncRequest(
            clientHandler, PIPE_HEARTBEAT_RETRY_NUM, getPipeHeartbeatRequestTimeoutInMs(), true);
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
                        resp.getPipeRemainingTimeList(),
                        resp.getPipeDegradedStatusList())));

    // config node heartbeat
    try {
      final TPipeHeartbeatResp configNodeResp = new TPipeHeartbeatResp(new ArrayList<>());
      PipeConfigNodeAgent.task().collectPipeMetaList(request, configNodeResp);
      pipeHeartbeatParser.parseHeartbeat(
          ConfigNodeDescriptor.getInstance().getConf().getConfigNodeId(),
          new PipeHeartbeat(
              configNodeResp.getPipeMetaList(),
              null,
              configNodeResp.getPipeRemainingEventCountList(),
              configNodeResp.getPipeRemainingTimeList(),
              configNodeResp.getPipeDegradedStatusList()));
    } catch (final Exception e) {
      PipeLogger.log(
          LOGGER::warn, e, ManagerMessages.FAILED_TO_COLLECT_PIPE_META_LIST_FROM_CONFIG_NODE_TASK);
    }
  }

  private static long getPipeHeartbeatRequestTimeoutInMs() {
    return TimeUnit.SECONDS.toMillis(HEARTBEAT_INTERVAL_SECONDS) * 2 / 3;
  }

  public synchronized void stop() {
    if (IS_SEPERATED_PIPE_HEARTBEAT_ENABLED && heartbeatFuture != null) {
      heartbeatFuture.cancel(false);
      heartbeatFuture = null;
      LOGGER.info(ManagerMessages.PIPEHEARTBEAT_IS_STOPPED_SUCCESSFULLY);
    }
  }

  public void parseHeartbeat(final int dataNodeId, final PipeHeartbeat pipeHeartbeat) {
    pipeHeartbeatParser.parseHeartbeat(dataNodeId, pipeHeartbeat);
  }
}
