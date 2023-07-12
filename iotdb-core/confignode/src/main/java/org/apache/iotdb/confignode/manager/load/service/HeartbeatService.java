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

package org.apache.iotdb.confignode.manager.load.service;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.confignode.client.async.AsyncConfigNodeHeartbeatClientPool;
import org.apache.iotdb.confignode.client.async.AsyncDataNodeHeartbeatClientPool;
import org.apache.iotdb.confignode.client.async.handlers.heartbeat.ConfigNodeHeartbeatHandler;
import org.apache.iotdb.confignode.client.async.handlers.heartbeat.DataNodeHeartbeatHandler;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.manager.load.cache.LoadCache;
import org.apache.iotdb.confignode.manager.load.cache.node.ConfigNodeHeartbeatCache;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.mpp.rpc.thrift.THeartbeatReq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** Maintain the Cluster-Heartbeat-Service. */
public class HeartbeatService {

  private static final Logger LOGGER = LoggerFactory.getLogger(HeartbeatService.class);

  private static final long HEARTBEAT_INTERVAL =
      ConfigNodeDescriptor.getInstance().getConf().getHeartbeatIntervalInMs();

  private final IManager configManager;
  private final LoadCache loadCache;

  /** Heartbeat executor service. */
  // Monitor for leadership change
  private final Object heartbeatScheduleMonitor = new Object();

  private Future<?> currentHeartbeatFuture;
  private final ScheduledExecutorService heartBeatExecutor =
      IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
          ThreadName.CONFIG_NODE_HEART_BEAT_SERVICE.getName());
  private final AtomicLong heartbeatCounter = new AtomicLong(0);

  public HeartbeatService(IManager configManager, LoadCache loadCache) {
    this.configManager = configManager;
    this.loadCache = loadCache;
  }

  /** Start the heartbeat service. */
  public void startHeartbeatService() {
    synchronized (heartbeatScheduleMonitor) {
      if (currentHeartbeatFuture == null) {
        currentHeartbeatFuture =
            ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
                heartBeatExecutor,
                this::heartbeatLoopBody,
                0,
                HEARTBEAT_INTERVAL,
                TimeUnit.MILLISECONDS);
        LOGGER.info("Heartbeat service is started successfully.");
      }
    }
  }

  /** Stop the heartbeat service. */
  public void stopHeartbeatService() {
    synchronized (heartbeatScheduleMonitor) {
      if (currentHeartbeatFuture != null) {
        currentHeartbeatFuture.cancel(false);
        currentHeartbeatFuture = null;
        LOGGER.info("Heartbeat service is stopped successfully.");
      }
    }
  }

  /** loop body of the heartbeat thread. */
  private void heartbeatLoopBody() {
    // The consensusManager of configManager may not be fully initialized at this time
    Optional.ofNullable(getConsensusManager())
        .ifPresent(
            consensusManager -> {
              if (getConsensusManager().isLeader()) {
                // Generate HeartbeatReq
                THeartbeatReq heartbeatReq = genHeartbeatReq();
                // Send heartbeat requests to all the registered ConfigNodes
                pingRegisteredConfigNodes(
                    heartbeatReq, getNodeManager().getRegisteredConfigNodes());
                // Send heartbeat requests to all the registered DataNodes
                pingRegisteredDataNodes(heartbeatReq, getNodeManager().getRegisteredDataNodes());
              }
            });
  }

  private THeartbeatReq genHeartbeatReq() {
    /* Generate heartbeat request */
    THeartbeatReq heartbeatReq = new THeartbeatReq();
    heartbeatReq.setHeartbeatTimestamp(System.currentTimeMillis());
    // Always sample RegionGroups' leadership as the Region heartbeat
    heartbeatReq.setNeedJudgeLeader(true);
    // We sample DataNode's load in every 10 heartbeat loop
    heartbeatReq.setNeedSamplingLoad(heartbeatCounter.get() % 10 == 0);
    heartbeatReq.setSchemaQuotaCount(configManager.getClusterSchemaManager().getSchemaQuotaCount());
    // We collect pipe meta in every 100 heartbeat loop
    heartbeatReq.setNeedPipeMetaList(
        !PipeConfig.getInstance().isSeperatedPipeHeartbeatEnabled()
            && heartbeatCounter.get()
                    % PipeConfig.getInstance()
                        .getPipeHeartbeatIntervalSecondsForCollectingPipeMeta()
                == 0);
    if (!configManager.getClusterQuotaManager().hasSpaceQuotaLimit()) {
      heartbeatReq.setSchemaRegionIds(configManager.getClusterQuotaManager().getSchemaRegionIds());
      heartbeatReq.setDataRegionIds(configManager.getClusterQuotaManager().getDataRegionIds());
      heartbeatReq.setSpaceQuotaUsage(configManager.getClusterQuotaManager().getSpaceQuotaUsage());
    }

    /* Update heartbeat counter */
    heartbeatCounter.getAndIncrement();

    return heartbeatReq;
  }

  /**
   * Send heartbeat requests to all the Registered ConfigNodes.
   *
   * @param registeredConfigNodes ConfigNodes that registered in cluster
   */
  private void pingRegisteredConfigNodes(
      THeartbeatReq heartbeatReq, List<TConfigNodeLocation> registeredConfigNodes) {
    // Send heartbeat requests
    for (TConfigNodeLocation configNodeLocation : registeredConfigNodes) {
      if (configNodeLocation.getConfigNodeId() == ConfigNodeHeartbeatCache.CURRENT_NODE_ID) {
        // Skip itself
        continue;
      }

      ConfigNodeHeartbeatHandler handler =
          new ConfigNodeHeartbeatHandler(configNodeLocation.getConfigNodeId(), loadCache);
      AsyncConfigNodeHeartbeatClientPool.getInstance()
          .getConfigNodeHeartBeat(
              configNodeLocation.getInternalEndPoint(),
              heartbeatReq.getHeartbeatTimestamp(),
              handler);
    }
  }

  /**
   * Send heartbeat requests to all the Registered DataNodes.
   *
   * @param registeredDataNodes DataNodes that registered in cluster
   */
  private void pingRegisteredDataNodes(
      THeartbeatReq heartbeatReq, List<TDataNodeConfiguration> registeredDataNodes) {
    // Send heartbeat requests
    for (TDataNodeConfiguration dataNodeInfo : registeredDataNodes) {
      DataNodeHeartbeatHandler handler =
          new DataNodeHeartbeatHandler(
              dataNodeInfo.getLocation().getDataNodeId(),
              loadCache,
              configManager.getClusterQuotaManager().getDeviceNum(),
              configManager.getClusterQuotaManager().getTimeSeriesNum(),
              configManager.getClusterQuotaManager().getRegionDisk(),
              configManager.getClusterSchemaManager()::updateSchemaQuota,
              configManager.getPipeManager().getPipeRuntimeCoordinator());
      configManager.getClusterQuotaManager().updateSpaceQuotaUsage();
      AsyncDataNodeHeartbeatClientPool.getInstance()
          .getDataNodeHeartBeat(
              dataNodeInfo.getLocation().getInternalEndPoint(), heartbeatReq, handler);
    }
  }

  private ConsensusManager getConsensusManager() {
    return configManager.getConsensusManager();
  }

  private NodeManager getNodeManager() {
    return configManager.getNodeManager();
  }
}
