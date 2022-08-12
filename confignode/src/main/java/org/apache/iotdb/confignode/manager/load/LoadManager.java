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
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.partition.DataPartitionTable;
import org.apache.iotdb.commons.partition.SchemaPartitionTable;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.async.confignode.AsyncConfigNodeHeartbeatClientPool;
import org.apache.iotdb.confignode.client.async.datanode.AsyncDataNodeClientPool;
import org.apache.iotdb.confignode.client.async.datanode.AsyncDataNodeHeartbeatClientPool;
import org.apache.iotdb.confignode.client.async.handlers.ConfigNodeHeartbeatHandler;
import org.apache.iotdb.confignode.client.async.handlers.DataNodeHeartbeatHandler;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
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
import org.apache.iotdb.confignode.manager.load.heartbeat.INodeCache;
import org.apache.iotdb.confignode.manager.load.heartbeat.IRegionGroupCache;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.mpp.rpc.thrift.THeartbeatReq;
import org.apache.iotdb.mpp.rpc.thrift.TRegionRouteReq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * The LoadManager at ConfigNodeGroup-Leader is active. It proactively implements the cluster
 * dynamic load balancing policy and passively accepts the PartitionTable expansion request.
 */
public class LoadManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadManager.class);

  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();

  private static final long HEARTBEAT_INTERVAL = CONF.getHeartbeatInterval();

  public static final TEndPoint CURRENT_NODE =
      new TEndPoint(CONF.getInternalAddress(), CONF.getInternalPort());

  private final IManager configManager;

  /** Heartbeat sample cache */
  // Map<NodeId, IHeartbeatStatistic>
  private final Map<Integer, INodeCache> nodeCacheMap;
  // Map<RegionId, RegionGroupCache>
  private final Map<TConsensusGroupId, IRegionGroupCache> regionGroupCacheMap;

  /** Balancers */
  private final RegionBalancer regionBalancer;

  private final PartitionBalancer partitionBalancer;
  private final RouteBalancer routeBalancer;
  private final LoadManagerMetrics loadManagerMetrics;

  /** Heartbeat executor service */
  private final AtomicInteger heartbeatCounter = new AtomicInteger(0);

  private Future<?> currentHeartbeatFuture;
  private final ScheduledExecutorService heartBeatExecutor =
      IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(LoadManager.class.getSimpleName());

  /** Load balancing executor service */
  private Future<?> currentLoadBalancingFuture;

  private final ScheduledExecutorService loadBalancingExecutor =
      IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(LoadManager.class.getSimpleName());

  /** Monitor for leadership change */
  private final Object scheduleMonitor = new Object();

  public LoadManager(IManager configManager) {
    this.configManager = configManager;
    this.nodeCacheMap = new ConcurrentHashMap<>();
    this.regionGroupCacheMap = new ConcurrentHashMap<>();

    this.regionBalancer = new RegionBalancer(configManager);
    this.partitionBalancer = new PartitionBalancer(configManager);
    this.routeBalancer = new RouteBalancer(configManager);
    this.loadManagerMetrics = new LoadManagerMetrics(configManager);
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
    // Broadcast the latest RegionRouteMap
    broadcastLatestRegionRouteMap();
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
  public Map<TConsensusGroupId, TRegionReplicaSet> genLatestRegionRouteMap() {
    // Always take the latest locations of RegionGroups as the input parameter
    return routeBalancer.genLatestRegionRouteMap(getPartitionManager().getAllReplicaSets());
  }

  /**
   * Get the loadScore of each DataNode
   *
   * @return Map<DataNodeId, loadScore>
   */
  public Map<Integer, Long> getAllLoadScores() {
    Map<Integer, Long> result = new ConcurrentHashMap<>();

    nodeCacheMap.forEach(
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
    if (ConfigNodeDescriptor.getInstance()
        .getConf()
        .getDataRegionConsensusProtocolClass()
        .equals(ConsensusFactory.MultiLeaderConsensus)) {
      regionGroupCacheMap.forEach(
          (consensusGroupId, regionGroupCache) -> {
            if (consensusGroupId.getType().equals(TConsensusGroupType.SchemaRegion)) {
              result.put(consensusGroupId, regionGroupCache.getLeaderDataNodeId());
            }
          });
      routeBalancer
          .getRouteMap()
          .forEach(
              (consensusGroupId, regionReplicaSet) -> {
                result.put(
                    consensusGroupId,
                    regionReplicaSet.getDataNodeLocations().get(0).getDataNodeId());
              });
    } else {
      regionGroupCacheMap.forEach(
          (consensusGroupId, regionGroupCache) ->
              result.put(consensusGroupId, regionGroupCache.getLeaderDataNodeId()));
    }
    return result;
  }

  /** Start the heartbeat service and the load balancing service */
  public void start() {
    LOGGER.debug("Start Heartbeat Service of LoadManager");
    synchronized (scheduleMonitor) {
      /* Start the heartbeat service */
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

      /* Start the load balancing service */
      if (currentLoadBalancingFuture == null) {
        currentLoadBalancingFuture =
            ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
                loadBalancingExecutor,
                this::updateNodeLoadStatistic,
                0,
                HEARTBEAT_INTERVAL,
                TimeUnit.MILLISECONDS);
        LOGGER.info("LoadBalancing service is started successfully.");
      }
      loadManagerMetrics.addMetrics();
    }
  }

  /** Stop the heartbeat service and the load balancing service */
  public void stop() {
    loadManagerMetrics.removeMetrics();
    LOGGER.debug("Stop Heartbeat Service and LoadBalancing Service of LoadManager");
    synchronized (scheduleMonitor) {
      if (currentHeartbeatFuture != null) {
        currentHeartbeatFuture.cancel(false);
        currentHeartbeatFuture = null;
        LOGGER.info("Heartbeat service is stopped successfully.");
        currentLoadBalancingFuture.cancel(false);
        currentLoadBalancingFuture = null;
        LOGGER.info("LoadBalancing service is stopped successfully.");
      }
    }
  }

  private void updateNodeLoadStatistic() {
    AtomicBoolean existFailDownDataNode = new AtomicBoolean(false);
    AtomicBoolean existChangeLeaderSchemaRegionGroup = new AtomicBoolean(false);
    AtomicBoolean existChangeLeaderDataRegionGroup = new AtomicBoolean(false);
    boolean isNeedBroadcast = false;

    nodeCacheMap
        .values()
        .forEach(
            nodeCache -> {
              boolean updateResult = nodeCache.updateLoadStatistic();
              if (nodeCache instanceof DataNodeHeartbeatCache) {
                // Check if some DataNodes fail down
                existFailDownDataNode.compareAndSet(false, updateResult);
              }
            });

    regionGroupCacheMap
        .values()
        .forEach(
            regionGroupCache -> {
              boolean updateResult = regionGroupCache.updateLoadStatistic();
              switch (regionGroupCache.getConsensusGroupId().getType()) {
                  // Check if some RegionGroups change their leader
                case SchemaRegion:
                  existChangeLeaderSchemaRegionGroup.compareAndSet(false, updateResult);
                  break;
                case DataRegion:
                  existChangeLeaderDataRegionGroup.compareAndSet(false, updateResult);
                  break;
              }
            });

    if (existFailDownDataNode.get()) {
      // The RegionRouteMap must be broadcast if some DataNodes fail down
      isNeedBroadcast = true;
    }

    if (RouteBalancer.leaderPolicy.equals(CONF.getRoutingPolicy())) {
      // Check the condition of leader routing policy
      if (existChangeLeaderSchemaRegionGroup.get()) {
        // Broadcast the RegionRouteMap if some SchemaRegionGroups change their leader
        isNeedBroadcast = true;
      }
      if (!ConsensusFactory.MultiLeaderConsensus.equals(CONF.getDataRegionConsensusProtocolClass())
          && existChangeLeaderDataRegionGroup.get()) {
        // Broadcast the RegionRouteMap if some DataRegionGroups change their leader
        // and the consensus protocol isn't MultiLeader
        isNeedBroadcast = true;
      }
    }

    if (isNeedBroadcast) {
      broadcastLatestRegionRouteMap();
    }
  }

  public void broadcastLatestRegionRouteMap() {
    Map<TConsensusGroupId, TRegionReplicaSet> latestRegionRouteMap = genLatestRegionRouteMap();
    Map<Integer, TDataNodeLocation> dataNodeLocationMap = new ConcurrentHashMap<>();
    getOnlineDataNodes()
        .forEach(
            onlineDataNode ->
                dataNodeLocationMap.put(
                    onlineDataNode.getLocation().getDataNodeId(), onlineDataNode.getLocation()));

    LOGGER.info("[latestRegionRouteMap] Begin to broadcast RegionRouteMap:");
    long broadcastTime = System.currentTimeMillis();
    printRegionRouteMap(broadcastTime, latestRegionRouteMap);
    AsyncDataNodeClientPool.getInstance()
        .sendAsyncRequestToDataNodeWithRetry(
            new TRegionRouteReq(broadcastTime, latestRegionRouteMap),
            dataNodeLocationMap,
            DataNodeRequestType.UPDATE_REGION_ROUTE_MAP,
            null);
    LOGGER.info("[latestRegionRouteMap] Broadcast the latest RegionRouteMap finished.");
  }

  /** loop body of the heartbeat thread */
  private void heartbeatLoopBody() {
    if (getConsensusManager().isLeader()) {
      // Generate HeartbeatReq
      THeartbeatReq heartbeatReq = genHeartbeatReq();
      // Send heartbeat requests to all the registered DataNodes
      pingRegisteredDataNodes(heartbeatReq, getNodeManager().getRegisteredDataNodes());
      // Send heartbeat requests to all the registered ConfigNodes
      pingRegisteredConfigNodes(heartbeatReq, getNodeManager().getRegisteredConfigNodes());
    }
  }

  private THeartbeatReq genHeartbeatReq() {
    /* Generate heartbeat request */
    THeartbeatReq heartbeatReq = new THeartbeatReq();
    heartbeatReq.setHeartbeatTimestamp(System.currentTimeMillis());
    // We update RegionGroups' leadership in every 5 heartbeat loop
    heartbeatReq.setNeedJudgeLeader(heartbeatCounter.get() % 5 == 0);
    // We sample DataNode's load in every 10 heartbeat loop
    heartbeatReq.setNeedSamplingLoad(heartbeatCounter.get() % 10 == 0);

    /* Update heartbeat counter */
    heartbeatCounter.getAndUpdate((x) -> (x + 1) % 10);
    return heartbeatReq;
  }

  /**
   * Send heartbeat requests to all the Registered DataNodes
   *
   * @param registeredDataNodes DataNodes that registered in cluster
   */
  private void pingRegisteredDataNodes(
      THeartbeatReq heartbeatReq, List<TDataNodeConfiguration> registeredDataNodes) {
    // Send heartbeat requests
    for (TDataNodeConfiguration dataNodeInfo : registeredDataNodes) {
      DataNodeHeartbeatHandler handler =
          new DataNodeHeartbeatHandler(
              dataNodeInfo.getLocation(),
              (DataNodeHeartbeatCache)
                  nodeCacheMap.computeIfAbsent(
                      dataNodeInfo.getLocation().getDataNodeId(),
                      empty -> new DataNodeHeartbeatCache()),
              regionGroupCacheMap);
      AsyncDataNodeHeartbeatClientPool.getInstance()
          .getDataNodeHeartBeat(
              dataNodeInfo.getLocation().getInternalEndPoint(), heartbeatReq, handler);
    }
  }

  /**
   * Send heartbeat requests to all the Registered ConfigNodes
   *
   * @param registeredConfigNodes ConfigNodes that registered in cluster
   */
  private void pingRegisteredConfigNodes(
      THeartbeatReq heartbeatReq, List<TConfigNodeLocation> registeredConfigNodes) {
    // Send heartbeat requests
    for (TConfigNodeLocation configNodeLocation : registeredConfigNodes) {
      if (configNodeLocation.getInternalEndPoint().equals(CURRENT_NODE)) {
        // Skip itself
        nodeCacheMap.putIfAbsent(
            configNodeLocation.getConfigNodeId(), new ConfigNodeHeartbeatCache(configNodeLocation));
        continue;
      }

      ConfigNodeHeartbeatHandler handler =
          new ConfigNodeHeartbeatHandler(
              (ConfigNodeHeartbeatCache)
                  nodeCacheMap.computeIfAbsent(
                      configNodeLocation.getConfigNodeId(),
                      empty -> new ConfigNodeHeartbeatCache(configNodeLocation)));
      AsyncConfigNodeHeartbeatClientPool.getInstance()
          .getConfigNodeHeartBeat(
              configNodeLocation.getInternalEndPoint(),
              heartbeatReq.getHeartbeatTimestamp(),
              handler);
    }
  }

  /**
   * When a node is removed, clear the node's cache
   *
   * @param nodeId removed node id
   */
  public void removeNodeHeartbeatHandCache(Integer nodeId) {
    nodeCacheMap.remove(nodeId);
  }

  public List<TConfigNodeLocation> getOnlineConfigNodes() {
    return getNodeManager().getRegisteredConfigNodes().stream()
        .filter(
            registeredConfigNode -> {
              int configNodeId = registeredConfigNode.getConfigNodeId();
              return nodeCacheMap.containsKey(configNodeId)
                  && NodeStatus.Running.equals(nodeCacheMap.get(configNodeId).getNodeStatus());
            })
        .collect(Collectors.toList());
  }

  public List<TDataNodeConfiguration> getOnlineDataNodes() {
    return getNodeManager().getRegisteredDataNodes().stream()
        .filter(
            registeredDataNode -> {
              int id = registeredDataNode.getLocation().getDataNodeId();
              return nodeCacheMap.containsKey(id)
                  && NodeStatus.Running.equals(nodeCacheMap.get(id).getNodeStatus());
            })
        .collect(Collectors.toList());
  }

  public List<TConfigNodeLocation> getUnknownConfigNodes() {
    return getNodeManager().getRegisteredConfigNodes().stream()
        .filter(
            registeredConfigNode -> {
              int configNodeId = registeredConfigNode.getConfigNodeId();
              return nodeCacheMap.containsKey(configNodeId)
                  && NodeStatus.Unknown.equals(nodeCacheMap.get(configNodeId).getNodeStatus());
            })
        .collect(Collectors.toList());
  }

  public List<TDataNodeConfiguration> getUnknownDataNodes() {
    return getNodeManager().getRegisteredDataNodes().stream()
        .filter(
            registeredDataNode -> {
              int id = registeredDataNode.getLocation().getDataNodeId();
              return nodeCacheMap.containsKey(id)
                  && NodeStatus.Unknown.equals(nodeCacheMap.get(id).getNodeStatus());
            })
        .collect(Collectors.toList());
  }

  public static void printRegionRouteMap(
      long timestamp, Map<TConsensusGroupId, TRegionReplicaSet> regionRouteMap) {
    LOGGER.info("[latestRegionRouteMap] timestamp:{}", timestamp);
    LOGGER.info("[latestRegionRouteMap] RegionRouteMap:");
    for (Map.Entry<TConsensusGroupId, TRegionReplicaSet> entry : regionRouteMap.entrySet()) {
      LOGGER.info(
          "[latestRegionRouteMap]\t {}={}",
          entry.getKey(),
          entry.getValue().getDataNodeLocations().stream()
              .map(TDataNodeLocation::getDataNodeId)
              .collect(Collectors.toList()));
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

  private PartitionManager getPartitionManager() {
    return configManager.getPartitionManager();
  }

  public Map<Integer, INodeCache> getNodeCacheMap() {
    return nodeCacheMap;
  }
}
