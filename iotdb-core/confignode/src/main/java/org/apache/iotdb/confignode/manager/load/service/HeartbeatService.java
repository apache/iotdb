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

import org.apache.iotdb.ainode.rpc.thrift.TAIHeartbeatReq;
import org.apache.iotdb.common.rpc.thrift.TAINodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.confignode.client.async.AsyncAINodeHeartbeatClientPool;
import org.apache.iotdb.confignode.client.async.AsyncConfigNodeHeartbeatClientPool;
import org.apache.iotdb.confignode.client.async.AsyncDataNodeHeartbeatClientPool;
import org.apache.iotdb.confignode.client.async.handlers.heartbeat.AINodeHeartbeatHandler;
import org.apache.iotdb.confignode.client.async.handlers.heartbeat.ConfigNodeHeartbeatHandler;
import org.apache.iotdb.confignode.client.async.handlers.heartbeat.DataNodeHeartbeatHandler;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.manager.load.cache.LoadCache;
import org.apache.iotdb.confignode.manager.load.cache.node.ConfigNodeHeartbeatCache;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeHeartbeatReq;
import org.apache.iotdb.mpp.rpc.thrift.TDataNodeHeartbeatReq;

import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * HeartbeatService periodically sending heartbeat requests from ConfigNode-leader to all other
 * cluster Nodes.
 */
public class HeartbeatService {

  private static final Logger LOGGER = LoggerFactory.getLogger(HeartbeatService.class);

  private static final long HEARTBEAT_INTERVAL =
      ConfigNodeDescriptor.getInstance().getConf().getHeartbeatIntervalInMs();

  protected IManager configManager;
  private final LoadCache loadCache;

  /** Heartbeat executor service. */
  // Monitor for leadership change
  private final Object heartbeatScheduleMonitor = new Object();

  private Future<?> currentHeartbeatFuture;
  private final ScheduledExecutorService heartBeatExecutor =
      IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
          ThreadName.CONFIG_NODE_HEART_BEAT_SERVICE.getName());
  private final AtomicLong heartbeatCounter = new AtomicLong(0);
  private static final int configNodeListPeriodicallySyncInterval = 100;

  public HeartbeatService(IManager configManager, LoadCache loadCache) {
    setConfigManager(configManager);
    this.loadCache = loadCache;
  }

  protected void setConfigManager(IManager configManager) {
    this.configManager = configManager;
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
                // Send heartbeat requests to all the registered ConfigNodes
                pingRegisteredConfigNodes(
                    genConfigNodeHeartbeatReq(), getNodeManager().getRegisteredConfigNodes());
                // Send heartbeat requests to all the registered DataNodes
                pingRegisteredDataNodes(
                    genHeartbeatReq(), getNodeManager().getRegisteredDataNodes());
                // Send heartbeat requests to all the registered AINodes
                pingRegisteredAINodes(genMLHeartbeatReq(), getNodeManager().getRegisteredAINodes());
              }
            });
  }

  private TDataNodeHeartbeatReq genHeartbeatReq() {
    /* Generate heartbeat request */
    TDataNodeHeartbeatReq heartbeatReq = new TDataNodeHeartbeatReq();
    heartbeatReq.setHeartbeatTimestamp(System.nanoTime());
    // Always sample RegionGroups' leadership as the Region heartbeat
    heartbeatReq.setNeedJudgeLeader(true);
    // We sample DataNode's load in every 10 heartbeat loop
    heartbeatReq.setNeedSamplingLoad(heartbeatCounter.get() % 10 == 0);
    Pair<Long, Long> schemaQuotaRemain =
        configManager.getClusterSchemaManager().getSchemaQuotaRemain();
    heartbeatReq.setTimeSeriesQuotaRemain(schemaQuotaRemain.left);
    heartbeatReq.setDeviceQuotaRemain(schemaQuotaRemain.right);
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

  private void addConfigNodeLocationsToReq(int dataNodeId, TDataNodeHeartbeatReq req) {
    Set<TEndPoint> confirmedConfigNodes = loadCache.getConfirmedConfigNodeEndPoints(dataNodeId);
    Set<TEndPoint> actualConfigNodes =
        getNodeManager().getRegisteredConfigNodes().stream()
            .map(TConfigNodeLocation::getInternalEndPoint)
            .collect(Collectors.toSet());
    /*
      In most cases, comparing actualConfigNodes and confirmedConfigNodes is sufficient, but in some cases it's not, hence the need for periodic sending.
      Here's an example:
      1. There are ConfigNode A and B, and one DataNode in the cluster. DataNode persists "ConfigNode list = [A,B]".
      2. ConfigNode B is removed. DataNode persists "ConfigNode list = [A]" but fails to confirm it to ConfigNode.
      3. ConfigNode B is re-added to the cluster.
      4. At this point, because actualConfigNodes and confirmedConfigNodes are identical, the ConfigNode list is not re-sent to the DataNode.
    */
    if (!actualConfigNodes.equals(confirmedConfigNodes)
        || heartbeatCounter.get() % configNodeListPeriodicallySyncInterval == 0) {
      req.setConfigNodeEndPoints(actualConfigNodes);
    }
  }

  private TConfigNodeHeartbeatReq genConfigNodeHeartbeatReq() {
    TConfigNodeHeartbeatReq req = new TConfigNodeHeartbeatReq();
    req.setTimestamp(System.nanoTime());
    return req;
  }

  private TAIHeartbeatReq genMLHeartbeatReq() {
    /* Generate heartbeat request */
    TAIHeartbeatReq heartbeatReq = new TAIHeartbeatReq();
    heartbeatReq.setHeartbeatTimestamp(System.nanoTime());

    // We sample AINode's load in every 10 heartbeat loop
    heartbeatReq.setNeedSamplingLoad(heartbeatCounter.get() % 10 == 0);

    return heartbeatReq;
  }

  /**
   * Send heartbeat requests to all the Registered ConfigNodes.
   *
   * @param registeredConfigNodes ConfigNodes that registered in cluster
   */
  private void pingRegisteredConfigNodes(
      TConfigNodeHeartbeatReq heartbeatReq, List<TConfigNodeLocation> registeredConfigNodes) {
    // Send heartbeat requests
    for (TConfigNodeLocation configNodeLocation : registeredConfigNodes) {
      int configNodeId = configNodeLocation.getConfigNodeId();
      if (configNodeId == ConfigNodeHeartbeatCache.CURRENT_NODE_ID
          || loadCache.checkAndSetHeartbeatProcessing(configNodeId)) {
        // Skip itself and the ConfigNode that is processing heartbeat
        continue;
      }
      ConfigNodeHeartbeatHandler handler = getConfigNodeHeartbeatHandler(configNodeId);

      AsyncConfigNodeHeartbeatClientPool.getInstance()
          .getConfigNodeHeartBeat(configNodeLocation.getInternalEndPoint(), heartbeatReq, handler);
    }
  }

  protected ConfigNodeHeartbeatHandler getConfigNodeHeartbeatHandler(int configNodeId) {
    return new ConfigNodeHeartbeatHandler(configNodeId, configManager.getLoadManager());
  }

  /**
   * Send heartbeat requests to all the Registered DataNodes.
   *
   * @param registeredDataNodes DataNodes that registered in cluster
   */
  private void pingRegisteredDataNodes(
      TDataNodeHeartbeatReq heartbeatReq, List<TDataNodeConfiguration> registeredDataNodes) {
    // Send heartbeat requests
    for (TDataNodeConfiguration dataNodeInfo : registeredDataNodes) {
      int dataNodeId = dataNodeInfo.getLocation().getDataNodeId();
      if (loadCache.checkAndSetHeartbeatProcessing(dataNodeId)) {
        // Skip the DataNode that is processing heartbeat
        continue;
      }
      DataNodeHeartbeatHandler handler =
          new DataNodeHeartbeatHandler(
              dataNodeId,
              configManager.getLoadManager(),
              configManager.getClusterQuotaManager().getDeviceNum(),
              configManager.getClusterQuotaManager().getTimeSeriesNum(),
              configManager.getClusterQuotaManager().getRegionDisk(),
              configManager.getClusterSchemaManager()::updateTimeSeriesUsage,
              configManager.getClusterSchemaManager()::updateDeviceUsage,
              configManager.getPipeManager().getPipeRuntimeCoordinator());
      configManager.getClusterQuotaManager().updateSpaceQuotaUsage();
      addConfigNodeLocationsToReq(dataNodeId, heartbeatReq);
      AsyncDataNodeHeartbeatClientPool.getInstance()
          .getDataNodeHeartBeat(
              dataNodeInfo.getLocation().getInternalEndPoint(), heartbeatReq, handler);
    }
  }

  /**
   * Send heartbeat requests to all the Registered AINodes.
   *
   * @param registeredAINodes DataNodes that registered in cluster
   */
  private void pingRegisteredAINodes(
      TAIHeartbeatReq heartbeatReq, List<TAINodeConfiguration> registeredAINodes) {
    // Send heartbeat requests
    for (TAINodeConfiguration aiNodeInfo : registeredAINodes) {
      AINodeHeartbeatHandler handler =
          new AINodeHeartbeatHandler(
              aiNodeInfo.getLocation().getAiNodeId(), configManager.getLoadManager());
      AsyncAINodeHeartbeatClientPool.getInstance()
          .getAINodeHeartBeat(
              aiNodeInfo.getLocation().getInternalEndPoint(), heartbeatReq, handler);
    }
  }

  private ConsensusManager getConsensusManager() {
    return configManager.getConsensusManager();
  }

  private NodeManager getNodeManager() {
    return configManager.getNodeManager();
  }
}
