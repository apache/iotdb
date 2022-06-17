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
package org.apache.iotdb.confignode.manager.load;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeInfo;
import org.apache.iotdb.common.rpc.thrift.THeartbeatReq;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.partition.DataPartitionTable;
import org.apache.iotdb.commons.partition.SchemaPartitionTable;
import org.apache.iotdb.confignode.client.AsyncDataNodeClientPool;
import org.apache.iotdb.confignode.client.handlers.HeartbeatHandler;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.write.CreateRegionsReq;
import org.apache.iotdb.confignode.exception.NotEnoughDataNodeException;
import org.apache.iotdb.confignode.exception.StorageGroupNotExistsException;
import org.apache.iotdb.confignode.manager.ClusterSchemaManager;
import org.apache.iotdb.confignode.manager.ConsensusManager;
import org.apache.iotdb.confignode.manager.Manager;
import org.apache.iotdb.confignode.manager.NodeManager;
import org.apache.iotdb.confignode.manager.load.balancer.PartitionBalancer;
import org.apache.iotdb.confignode.manager.load.balancer.RegionBalancer;
import org.apache.iotdb.confignode.manager.load.heartbeat.HeartbeatCache;
import org.apache.iotdb.confignode.manager.load.heartbeat.IHeartbeatStatistic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * The LoadManager at ConfigNodeGroup-Leader is active. It proactively implements the cluster
 * dynamic load balancing policy and passively accepts the PartitionTable expansion request.
 */
public class LoadManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadManager.class);

  private final Manager configManager;

  private final long heartbeatInterval =
      ConfigNodeDescriptor.getInstance().getConf().getHeartbeatInterval();
  // Map<NodeId, HeartbeatCache>
  private final Map<Integer, HeartbeatCache> heartbeatCacheMap;

  // Balancers
  private final RegionBalancer regionBalancer;
  private final PartitionBalancer partitionBalancer;

  /** running state of heartbeat service */
  private boolean isRunning = false;

  private final Object isRunningMonitor = new Object();
  private int balanceCount = 0;

  public LoadManager(Manager configManager) {
    this.configManager = configManager;
    this.heartbeatCacheMap = new ConcurrentHashMap<>();

    this.regionBalancer = new RegionBalancer(configManager);
    this.partitionBalancer = new PartitionBalancer(configManager);

    LOGGER.info("Setup Heartbeat Service of LoadManager");
    // infinite task
    ScheduledExecutorUtil.unsafelyScheduleWithFixedDelay(
        IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
            LoadManager.class.getSimpleName(), this::loadExceptionHandler),
        this::heartbeatLoopBody,
        0,
        heartbeatInterval,
        TimeUnit.MILLISECONDS);
  }

  /**
   * Allocate and create one Region for each StorageGroup. TODO: Use procedure to protect create
   * Regions process
   *
   * @param storageGroups List<StorageGroupName>
   * @param consensusGroupType TConsensusGroupType of Region to be allocated
   * @param regionNum The number of Regions
   */
  public void initializeRegions(
      List<String> storageGroups, TConsensusGroupType consensusGroupType, int regionNum)
      throws NotEnoughDataNodeException, StorageGroupNotExistsException {
    CreateRegionsReq createRegionsReq =
        regionBalancer.genRegionsAllocationPlan(storageGroups, consensusGroupType, regionNum);
    createRegionsOnDataNodes(createRegionsReq);

    getConsensusManager().write(createRegionsReq);
  }

  private void createRegionsOnDataNodes(CreateRegionsReq createRegionsReq)
      throws StorageGroupNotExistsException {
    Map<String, Long> ttlMap = new HashMap<>();
    for (String storageGroup : createRegionsReq.getRegionMap().keySet()) {
      ttlMap.put(
          storageGroup,
          getClusterSchemaManager().getStorageGroupSchemaByName(storageGroup).getTTL());
    }
    AsyncDataNodeClientPool.getInstance().createRegions(createRegionsReq, ttlMap);
  }

  /**
   * Allocate SchemaPartitions
   *
   * @param unassignedSchemaPartitionSlotsMap SchemaPartitionSlots that should be assigned
   * @return Map<StorageGroupName, SchemaPartitionTable>, the allocating result
   */
  public Map<String, SchemaPartitionTable> allocateSchemaPartition(
      Map<String, List<TSeriesPartitionSlot>> unassignedSchemaPartitionSlotsMap) {
    return partitionBalancer.allocateSchemaPartition(unassignedSchemaPartitionSlotsMap);
  }

  /**
   * Allocate DataPartitions
   *
   * @param unassignedDataPartitionSlotsMap DataPartitionSlots that should be assigned
   * @return Map<StorageGroupName, DataPartitionTable>, the allocating result
   */
  public Map<String, DataPartitionTable> allocateDataPartition(
      Map<String, Map<TSeriesPartitionSlot, List<TTimePartitionSlot>>>
          unassignedDataPartitionSlotsMap) {
    return partitionBalancer.allocateDataPartition(unassignedDataPartitionSlotsMap);
  }

  /** start the heartbeat service */
  public void start() {
    LOGGER.debug("Start Heartbeat Service of LoadManager");
    synchronized (isRunningMonitor) {
      isRunning = true;
      // only 1 thread can start the heartbeat service
      isRunningMonitor.notifyAll();
    }
  }

  /** stop the heartbeat service */
  public void stop() {
    LOGGER.debug("Stop Heartbeat Service of LoadManager");
    synchronized (isRunningMonitor) {
      isRunning = false;
    }
  }

  /** loop body of the heartbeat thread */
  private void heartbeatLoopBody() {
    synchronized (isRunningMonitor) {
      // avoid spurious wakeup
      while (!isRunning) {
        // not longer the leader, discard all cache
        heartbeatCacheMap.clear();
        balanceCount = 0;
        try {
          isRunningMonitor.wait();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }
    if (getConsensusManager().isLeader()) {
      // Send heartbeat requests to all the online DataNodes
      pingOnlineDataNodes(getNodeManager().getOnlineDataNodes(-1));
      // TODO: Send heartbeat requests to all the online ConfigNodes

      // Do load balancing
      doLoadBalancing(balanceCount);
      balanceCount += 1;
    }
  }

  /**
   * Handler when the heartbeat thread is down
   *
   * @param t thread
   * @param e exception
   */
  private void loadExceptionHandler(Thread t, Throwable e) {
    LOGGER.error("Heartbeat thread has been interrupted, stopping ConfigNode...", e);
    System.exit(-1);
  }

  private THeartbeatReq genHeartbeatReq() {
    return new THeartbeatReq(System.currentTimeMillis());
  }

  private void doLoadBalancing(int balanceCount) {
    if (balanceCount % 5 == 0) {
      // We update nodes' load statistic in every 5s
      updateNodeLoadStatistic();
    }
  }

  private void updateNodeLoadStatistic() {
    heartbeatCacheMap.values().forEach(IHeartbeatStatistic::updateLoadStatistic);
  }

  /**
   * Send heartbeat requests to all the online DataNodes
   *
   * @param onlineDataNodes DataNodes that currently online
   */
  private void pingOnlineDataNodes(List<TDataNodeInfo> onlineDataNodes) {
    // Send heartbeat requests
    for (TDataNodeInfo dataNodeInfo : onlineDataNodes) {
      HeartbeatHandler handler =
          new HeartbeatHandler(
              dataNodeInfo.getLocation(),
              heartbeatCacheMap.computeIfAbsent(
                  dataNodeInfo.getLocation().getDataNodeId(), empty -> new HeartbeatCache()));
      AsyncDataNodeClientPool.getInstance()
          .getHeartBeat(
              dataNodeInfo.getLocation().getInternalEndPoint(), genHeartbeatReq(), handler);
    }
  }

  private ConsensusManager getConsensusManager() {
    return configManager.getConsensusManager();
  }

  private NodeManager getNodeManager() {
    return configManager.getNodeManager();
  }

  private ClusterSchemaManager getClusterSchemaManager() {
    return configManager.getClusterSchemaManager();
  }
}
