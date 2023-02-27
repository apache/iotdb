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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.partition.DataPartitionTable;
import org.apache.iotdb.commons.partition.SchemaPartitionTable;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.async.AsyncDataNodeClientPool;
import org.apache.iotdb.confignode.client.async.handlers.AsyncClientHandler;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.write.region.CreateRegionGroupsPlan;
import org.apache.iotdb.confignode.exception.DatabaseNotExistsException;
import org.apache.iotdb.confignode.exception.NoAvailableRegionGroupException;
import org.apache.iotdb.confignode.exception.NotEnoughDataNodeException;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.load.balancer.PartitionBalancer;
import org.apache.iotdb.confignode.manager.load.balancer.RegionBalancer;
import org.apache.iotdb.confignode.manager.load.balancer.RouteBalancer;
import org.apache.iotdb.confignode.manager.load.balancer.router.RegionRouteMap;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.confignode.manager.node.heartbeat.NodeStatistics;
import org.apache.iotdb.confignode.manager.observer.NodeStatisticsEvent;
import org.apache.iotdb.confignode.manager.partition.PartitionManager;
import org.apache.iotdb.confignode.manager.partition.heartbeat.RegionGroupStatistics;
import org.apache.iotdb.confignode.manager.partition.heartbeat.RegionStatistics;
import org.apache.iotdb.confignode.rpc.thrift.TTimeSlotList;
import org.apache.iotdb.mpp.rpc.thrift.TRegionRouteReq;
import org.apache.iotdb.tsfile.utils.Pair;

import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.EventBus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
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

  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();
  private static final long HEARTBEAT_INTERVAL = CONF.getHeartbeatIntervalInMs();

  private final IManager configManager;

  /** Balancers */
  private final RegionBalancer regionBalancer;

  private final PartitionBalancer partitionBalancer;
  private final RouteBalancer routeBalancer;

  /** Load statistics executor service */
  private Future<?> currentLoadStatisticsFuture;

  private final ScheduledExecutorService loadStatisticsExecutor =
      IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor("Cluster-LoadStatistics-Service");
  private final Object scheduleMonitor = new Object();

  private final EventBus eventBus =
      new AsyncEventBus("LoadManager-EventBus", Executors.newFixedThreadPool(5));

  public LoadManager(IManager configManager) {
    this.configManager = configManager;

    this.regionBalancer = new RegionBalancer(configManager);
    this.partitionBalancer = new PartitionBalancer(configManager);
    this.routeBalancer = new RouteBalancer(configManager);

    eventBus.register(configManager.getClusterSchemaManager());
    eventBus.register(configManager.getSyncManager());
  }

  /**
   * Generate an optimal CreateRegionGroupsPlan
   *
   * @param allotmentMap Map<StorageGroupName, Region allotment>
   * @param consensusGroupType TConsensusGroupType of RegionGroup to be allocated
   * @return CreateRegionGroupsPlan
   * @throws NotEnoughDataNodeException If there are not enough DataNodes
   * @throws DatabaseNotExistsException If some specific StorageGroups don't exist
   */
  public CreateRegionGroupsPlan allocateRegionGroups(
      Map<String, Integer> allotmentMap, TConsensusGroupType consensusGroupType)
      throws NotEnoughDataNodeException, DatabaseNotExistsException {
    return regionBalancer.genRegionGroupsAllocationPlan(allotmentMap, consensusGroupType);
  }

  /**
   * Allocate SchemaPartitions
   *
   * @param unassignedSchemaPartitionSlotsMap SchemaPartitionSlots that should be assigned
   * @return Map<StorageGroupName, SchemaPartitionTable>, the allocating result
   */
  public Map<String, SchemaPartitionTable> allocateSchemaPartition(
      Map<String, List<TSeriesPartitionSlot>> unassignedSchemaPartitionSlotsMap)
      throws NoAvailableRegionGroupException {
    return partitionBalancer.allocateSchemaPartition(unassignedSchemaPartitionSlotsMap);
  }

  /**
   * Allocate DataPartitions
   *
   * @param unassignedDataPartitionSlotsMap DataPartitionSlots that should be assigned
   * @return Map<StorageGroupName, DataPartitionTable>, the allocating result
   */
  public Map<String, DataPartitionTable> allocateDataPartition(
      Map<String, Map<TSeriesPartitionSlot, TTimeSlotList>> unassignedDataPartitionSlotsMap)
      throws NoAvailableRegionGroupException {
    return partitionBalancer.allocateDataPartition(unassignedDataPartitionSlotsMap);
  }

  /** @return Map<RegionGroupId, DataNodeId where the leader resides> */
  public Map<TConsensusGroupId, Integer> getLatestRegionLeaderMap() {
    return routeBalancer.getLatestRegionLeaderMap();
  }

  /**
   * Get the number of RegionGroup-leaders in the specified DataNode.
   *
   * @param dataNodeId The specified DataNode
   * @param type SchemaRegion or DataRegion
   * @return The number of RegionGroup-leaders
   */
  public int getRegionGroupLeaderCount(int dataNodeId, TConsensusGroupType type) {
    AtomicInteger result = new AtomicInteger(0);
    routeBalancer
        .getLatestRegionLeaderMap()
        .forEach(
            ((consensusGroupId, leaderId) -> {
              if (dataNodeId == leaderId && type.equals(consensusGroupId.getType())) {
                result.getAndIncrement();
              }
            }));
    return result.get();
  }

  /**
   * Generate an optimal real-time read/write requests routing policy.
   *
   * @return Map<TConsensusGroupId, TRegionReplicaSet>, The routing policy of read/write requests
   *     for each Region is based on the order in the TRegionReplicaSet. The replica with higher
   *     sorting result have higher priority.
   */
  public Map<TConsensusGroupId, TRegionReplicaSet> getLatestRegionRouteMap() {
    return routeBalancer.getLatestRegionPriorityMap();
  }

  /** Start the load statistics service */
  public void startLoadStatisticsService() {
    synchronized (scheduleMonitor) {
      if (currentLoadStatisticsFuture == null) {
        currentLoadStatisticsFuture =
            ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
                loadStatisticsExecutor,
                this::updateLoadStatistics,
                0,
                HEARTBEAT_INTERVAL,
                TimeUnit.MILLISECONDS);
        LOGGER.info("LoadStatistics service is started successfully.");
      }
    }
  }

  /** Stop the load statistics service */
  public void stopLoadStatisticsService() {
    synchronized (scheduleMonitor) {
      if (currentLoadStatisticsFuture != null) {
        currentLoadStatisticsFuture.cancel(false);
        currentLoadStatisticsFuture = null;
        LOGGER.info("LoadStatistics service is stopped successfully.");
      }
    }
  }

  private void updateLoadStatistics() {
    // Broadcast the RegionRouteMap if some LoadStatistics has changed
    boolean isNeedBroadcast = false;

    // Update NodeStatistics:
    // Pair<NodeStatistics, NodeStatistics>:left one means the current NodeStatistics, right one
    // means the previous NodeStatistics
    Map<Integer, Pair<NodeStatistics, NodeStatistics>> differentNodeStatisticsMap =
        new ConcurrentHashMap<>();
    getNodeManager()
        .getNodeCacheMap()
        .forEach(
            (nodeId, nodeCache) -> {
              NodeStatistics preNodeStatistics = nodeCache.getPreviousStatistics().deepCopy();
              if (nodeCache.periodicUpdate()) {
                // Update and record the changed NodeStatistics
                differentNodeStatisticsMap.put(
                    nodeId, new Pair<>(nodeCache.getStatistics(), preNodeStatistics));
              }
            });
    if (!differentNodeStatisticsMap.isEmpty()) {
      isNeedBroadcast = true;
      recordNodeStatistics(differentNodeStatisticsMap);
      eventBus.post(new NodeStatisticsEvent(differentNodeStatisticsMap));
    }

    // Update RegionGroupStatistics
    Map<TConsensusGroupId, RegionGroupStatistics> differentRegionGroupStatisticsMap =
        new ConcurrentHashMap<>();
    getPartitionManager()
        .getRegionGroupCacheMap()
        .forEach(
            (regionGroupId, regionGroupCache) -> {
              if (regionGroupCache.periodicUpdate()) {
                // Update and record the changed RegionGroupStatistics
                differentRegionGroupStatisticsMap.put(
                    regionGroupId, regionGroupCache.getStatistics());
              }
            });
    if (!differentRegionGroupStatisticsMap.isEmpty()) {
      isNeedBroadcast = true;
      recordRegionGroupStatistics(differentRegionGroupStatisticsMap);
    }

    // Update RegionRouteMap
    if (routeBalancer.updateRegionRouteMap()) {
      isNeedBroadcast = true;
      recordRegionRouteMap(routeBalancer.getRegionRouteMap());
    }

    if (isNeedBroadcast) {
      broadcastLatestRegionRouteMap();
    }
  }

  private void recordNodeStatistics(
      Map<Integer, Pair<NodeStatistics, NodeStatistics>> differentNodeStatisticsMap) {
    LOGGER.info("[UpdateLoadStatistics] NodeStatisticsMap: ");
    for (Map.Entry<Integer, Pair<NodeStatistics, NodeStatistics>> nodeCacheEntry :
        differentNodeStatisticsMap.entrySet()) {
      LOGGER.info(
          "[UpdateLoadStatistics]\t {}={}",
          "nodeId{" + nodeCacheEntry.getKey() + "}",
          nodeCacheEntry.getValue().left);
    }
  }

  private void recordRegionGroupStatistics(
      Map<TConsensusGroupId, RegionGroupStatistics> differentRegionGroupStatisticsMap) {
    LOGGER.info("[UpdateLoadStatistics] RegionGroupStatisticsMap: ");
    for (Map.Entry<TConsensusGroupId, RegionGroupStatistics> regionGroupStatisticsEntry :
        differentRegionGroupStatisticsMap.entrySet()) {
      LOGGER.info("[UpdateLoadStatistics]\t RegionGroup: {}", regionGroupStatisticsEntry.getKey());
      LOGGER.info("[UpdateLoadStatistics]\t {}", regionGroupStatisticsEntry.getValue());
      for (Map.Entry<Integer, RegionStatistics> regionStatisticsEntry :
          regionGroupStatisticsEntry.getValue().getRegionStatisticsMap().entrySet()) {
        LOGGER.info(
            "[UpdateLoadStatistics]\t dataNodeId{}={}",
            regionStatisticsEntry.getKey(),
            regionStatisticsEntry.getValue());
      }
    }
  }

  private void recordRegionRouteMap(RegionRouteMap regionRouteMap) {
    LOGGER.info("[UpdateLoadStatistics] RegionLeaderMap: ");
    for (Map.Entry<TConsensusGroupId, Integer> regionLeaderEntry :
        regionRouteMap.getRegionLeaderMap().entrySet()) {
      LOGGER.info(
          "[UpdateLoadStatistics]\t {}={}",
          regionLeaderEntry.getKey(),
          regionLeaderEntry.getValue());
    }

    LOGGER.info("[UpdateLoadStatistics] RegionPriorityMap: ");
    for (Map.Entry<TConsensusGroupId, TRegionReplicaSet> regionPriorityEntry :
        regionRouteMap.getRegionPriorityMap().entrySet()) {
      LOGGER.info(
          "[UpdateLoadStatistics]\t {}={}",
          regionPriorityEntry.getKey(),
          regionPriorityEntry.getValue().getDataNodeLocations().stream()
              .map(TDataNodeLocation::getDataNodeId)
              .collect(Collectors.toList()));
    }
  }

  public void broadcastLatestRegionRouteMap() {
    Map<TConsensusGroupId, TRegionReplicaSet> latestRegionRouteMap = getLatestRegionRouteMap();
    Map<Integer, TDataNodeLocation> dataNodeLocationMap = new ConcurrentHashMap<>();
    // Broadcast the RegionRouteMap to all DataNodes except the unknown ones
    getNodeManager()
        .filterDataNodeThroughStatus(NodeStatus.Running, NodeStatus.Removing, NodeStatus.ReadOnly)
        .forEach(
            onlineDataNode ->
                dataNodeLocationMap.put(
                    onlineDataNode.getLocation().getDataNodeId(), onlineDataNode.getLocation()));

    LOGGER.info("[UpdateLoadStatistics] Begin to broadcast RegionRouteMap:");
    long broadcastTime = System.currentTimeMillis();

    AsyncClientHandler<TRegionRouteReq, TSStatus> clientHandler =
        new AsyncClientHandler<>(
            DataNodeRequestType.UPDATE_REGION_ROUTE_MAP,
            new TRegionRouteReq(broadcastTime, latestRegionRouteMap),
            dataNodeLocationMap);
    AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);
    LOGGER.info("[UpdateLoadStatistics] Broadcast the latest RegionRouteMap finished.");
  }

  /** Initialize all kinds of the HeartbeatCache when the ConfigNode-Leader is switched */
  public void initHeartbeatCache() {
    getNodeManager().initNodeHeartbeatCache();
    getPartitionManager().initRegionGroupHeartbeatCache();
    routeBalancer.initRegionRouteMap();
  }

  public RouteBalancer getRouteBalancer() {
    return routeBalancer;
  }

  private NodeManager getNodeManager() {
    return configManager.getNodeManager();
  }

  private PartitionManager getPartitionManager() {
    return configManager.getPartitionManager();
  }

  public EventBus getEventBus() {
    return eventBus;
  }
}
