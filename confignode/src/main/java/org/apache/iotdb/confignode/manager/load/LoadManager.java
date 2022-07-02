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

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeInfo;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.partition.DataPartitionTable;
import org.apache.iotdb.commons.partition.SchemaPartitionTable;
import org.apache.iotdb.confignode.client.AsyncConfigNodeClientPool;
import org.apache.iotdb.confignode.client.AsyncDataNodeClientPool;
import org.apache.iotdb.confignode.client.handlers.ConfigNodeHeartbeatHandler;
import org.apache.iotdb.confignode.client.handlers.DataNodeHeartbeatHandler;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.write.CreateRegionGroupsPlan;
import org.apache.iotdb.confignode.exception.NotEnoughDataNodeException;
import org.apache.iotdb.confignode.exception.StorageGroupNotExistsException;
import org.apache.iotdb.confignode.manager.ClusterSchemaManager;
import org.apache.iotdb.confignode.manager.ConsensusManager;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.NodeManager;
import org.apache.iotdb.confignode.manager.PartitionManager;
import org.apache.iotdb.confignode.manager.load.balancer.PartitionBalancer;
import org.apache.iotdb.confignode.manager.load.balancer.RegionBalancer;
import org.apache.iotdb.confignode.manager.load.balancer.RouteBalancer;
import org.apache.iotdb.confignode.manager.load.heartbeat.ConfigNodeHeartbeatCache;
import org.apache.iotdb.confignode.manager.load.heartbeat.DataNodeHeartbeatCache;
import org.apache.iotdb.confignode.manager.load.heartbeat.IHeartbeatStatistic;
import org.apache.iotdb.confignode.manager.load.heartbeat.IRegionGroupCache;
import org.apache.iotdb.mpp.rpc.thrift.THeartbeatReq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * The LoadManager at ConfigNodeGroup-Leader is active. It proactively implements the cluster
 * dynamic load balancing policy and passively accepts the PartitionTable expansion request.
 */
public class LoadManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadManager.class);

  private final IManager configManager;

  private final long heartbeatInterval =
      ConfigNodeDescriptor.getInstance().getConf().getHeartbeatInterval();

  /** Heartbeat sample cache */
  // Map<NodeId, IHeartbeatStatistic>
  private final Map<Integer, IHeartbeatStatistic> heartbeatCacheMap;
  // Map<RegionId, RegionGroupCache>
  private final Map<TConsensusGroupId, IRegionGroupCache> regionGroupCacheMap;

  /** Balancers */
  private final RegionBalancer regionBalancer;

  private final PartitionBalancer partitionBalancer;
  private final RouteBalancer routeBalancer;

  /** heartbeat executor service */
  private final ScheduledExecutorService heartBeatExecutor =
      IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(LoadManager.class.getSimpleName());

  /** monitor for heartbeat state change */
  private final Object heartbeatMonitor = new Object();

  private Future<?> currentHeartbeatFuture;
  private final AtomicInteger balanceCount;

  public LoadManager(IManager configManager) {
    this.configManager = configManager;
    this.heartbeatCacheMap = new ConcurrentHashMap<>();
    this.regionGroupCacheMap = new ConcurrentHashMap<>();

    this.regionBalancer = new RegionBalancer(configManager);
    this.partitionBalancer = new PartitionBalancer(configManager);
    this.routeBalancer = new RouteBalancer(configManager);

    this.balanceCount = new AtomicInteger(0);
  }

  /**
   * Allocate and create Regions for each StorageGroup.
   *
   * @param allotmentMap Map<StorageGroupName, Region allotment>
   * @param consensusGroupType TConsensusGroupType of Region to be allocated
   */
  public void doRegionCreation(
      Map<String, Integer> allotmentMap, TConsensusGroupType consensusGroupType)
      throws NotEnoughDataNodeException, StorageGroupNotExistsException {
    CreateRegionGroupsPlan createRegionGroupsPlan =
        regionBalancer.genRegionsAllocationPlan(allotmentMap, consensusGroupType);

    // TODO: Use procedure to protect the following process
    // Create Regions on DataNodes
    Map<String, Long> ttlMap = new HashMap<>();
    for (String storageGroup : createRegionGroupsPlan.getRegionGroupMap().keySet()) {
      ttlMap.put(
          storageGroup,
          getClusterSchemaManager().getStorageGroupSchemaByName(storageGroup).getTTL());
    }
    AsyncDataNodeClientPool.getInstance().createRegions(createRegionGroupsPlan, ttlMap);
    // Persist the allocation result
    getConsensusManager().write(createRegionGroupsPlan);
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

  /**
   * Generate an optimal real-time read/write requests routing policy.
   *
   * @return Map<TConsensusGroupId, TRegionReplicaSet>, The routing policy of read/write requests
   *     for each Region is based on the order in the TRegionReplicaSet. The replica with higher
   *     sorting result have higher priority.
   */
  public Map<TConsensusGroupId, TRegionReplicaSet> genRealTimeRoutingPolicy() {
    return routeBalancer.genRealTimeRoutingPolicy(getPartitionManager().getAllReplicaSets());
  }

  /**
   * Get the loadScore of each DataNode
   *
   * @return Map<DataNodeId, loadScore>
   */
  public Map<Integer, Float> getAllLoadScores() {
    Map<Integer, Float> result = new ConcurrentHashMap<>();

    heartbeatCacheMap.forEach(
        (dataNodeId, heartbeatCache) -> result.put(dataNodeId, heartbeatCache.getLoadScore()));

    return result;
  }

  /**
   * Get the leadership of each RegionGroup
   *
   * @return Map<RegionGroupId, leader location>
   */
  public Map<TConsensusGroupId, Integer> getAllLeadership() {
    Map<TConsensusGroupId, Integer> result = new ConcurrentHashMap<>();

    regionGroupCacheMap.forEach(
        (consensusGroupId, regionGroupCache) ->
            result.put(consensusGroupId, regionGroupCache.getLeaderDataNodeId()));

    return result;
  }

  /** Start the heartbeat service */
  public void start() {
    LOGGER.debug("Start Heartbeat Service of LoadManager");
    synchronized (heartbeatMonitor) {
      balanceCount.set(0);
      if (currentHeartbeatFuture == null) {
        currentHeartbeatFuture =
            ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
                heartBeatExecutor,
                this::heartbeatLoopBody,
                0,
                heartbeatInterval,
                TimeUnit.MILLISECONDS);
      }
    }
  }

  /** Stop the heartbeat service */
  public void stop() {
    LOGGER.debug("Stop Heartbeat Service of LoadManager");
    synchronized (heartbeatMonitor) {
      if (currentHeartbeatFuture != null) {
        currentHeartbeatFuture.cancel(false);
        currentHeartbeatFuture = null;
      }
    }
  }

  /** loop body of the heartbeat thread */
  private void heartbeatLoopBody() {
    if (getConsensusManager().isLeader()) {
      // Send heartbeat requests to all the online DataNodes
      pingOnlineDataNodes(getNodeManager().getOnlineDataNodes(-1));
      // Send heartbeat requests to all the registered ConfigNodes
      pingRegisteredConfigNodes(getNodeManager().getRegisteredConfigNodes());
      // Do load balancing
      doLoadBalancing();
      balanceCount.getAndIncrement();
    }
  }

  private THeartbeatReq genHeartbeatReq() {
    THeartbeatReq heartbeatReq = new THeartbeatReq();
    heartbeatReq.setHeartbeatTimestamp(System.currentTimeMillis());
    // We update RegionGroups' leadership in every 5s
    heartbeatReq.setNeedJudgeLeader(balanceCount.get() % 5 == 0);
    // We sample DataNode load in every 10s
    heartbeatReq.setNeedSamplingLoad(balanceCount.get() % 10 == 0);
    return heartbeatReq;
  }

  private void doLoadBalancing() {
    if (balanceCount.get() % 10 == 0) {
      // We update nodes' load statistic in every 10s
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
      DataNodeHeartbeatHandler handler =
          new DataNodeHeartbeatHandler(
              dataNodeInfo.getLocation(),
              (DataNodeHeartbeatCache)
                  heartbeatCacheMap.computeIfAbsent(
                      dataNodeInfo.getLocation().getDataNodeId(),
                      empty -> new DataNodeHeartbeatCache()),
              regionGroupCacheMap);
      AsyncDataNodeClientPool.getInstance()
          .getDataNodeHeartBeat(
              dataNodeInfo.getLocation().getInternalEndPoint(), genHeartbeatReq(), handler);
    }
  }

  /**
   * Send heartbeat requests to all the online ConfigNodes
   *
   * @param registeredConfigNodes ConfigNodes that registered in cluster
   */
  private void pingRegisteredConfigNodes(List<TConfigNodeLocation> registeredConfigNodes) {
    // Send heartbeat requests
    for (TConfigNodeLocation configNodeLocation : registeredConfigNodes) {
      ConfigNodeHeartbeatHandler handler =
          new ConfigNodeHeartbeatHandler(
              configNodeLocation,
              (ConfigNodeHeartbeatCache)
                  heartbeatCacheMap.computeIfAbsent(
                      configNodeLocation.getConfigNodeId(),
                      empty -> new ConfigNodeHeartbeatCache()));
      AsyncConfigNodeClientPool.getInstance()
          .getConfigNodeHeartBeat(
              configNodeLocation.getInternalEndPoint(),
              genHeartbeatReq().getHeartbeatTimestamp(),
              handler);
    }
  }

  /**
   * When a node is removed, clear the node's cache
   *
   * @param nodeId removed node id
   */
  public void removeNodeHeartbeatHandCache(Integer nodeId) {
    heartbeatCacheMap.remove(nodeId);
  }

  public List<TConfigNodeLocation> getOnlineConfigNodes() {
    return getNodeManager().getRegisteredConfigNodes().stream()
        .filter(
            registeredConfigNode ->
                heartbeatCacheMap
                    .get(registeredConfigNode.getConfigNodeId())
                    .getNodeStatus()
                    .equals(NodeStatus.Running))
        .collect(Collectors.toList());
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

  private PartitionManager getPartitionManager() {
    return configManager.getPartitionManager();
  }

  public Map<Integer, IHeartbeatStatistic> getHeartbeatCacheMap() {
    return heartbeatCacheMap;
  }
}
