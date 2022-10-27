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
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
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
import org.apache.iotdb.confignode.consensus.request.write.statistics.UpdateLoadStatisticsPlan;
import org.apache.iotdb.confignode.exception.NotAvailableRegionGroupException;
import org.apache.iotdb.confignode.exception.NotEnoughDataNodeException;
import org.apache.iotdb.confignode.exception.StorageGroupNotExistsException;
import org.apache.iotdb.confignode.manager.ClusterSchemaManager;
import org.apache.iotdb.confignode.manager.ConsensusManager;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.load.balancer.PartitionBalancer;
import org.apache.iotdb.confignode.manager.load.balancer.RegionBalancer;
import org.apache.iotdb.confignode.manager.load.balancer.RouteBalancer;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.confignode.manager.partition.PartitionManager;
import org.apache.iotdb.db.service.metrics.MetricService;
import org.apache.iotdb.mpp.rpc.thrift.TRegionRouteReq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * The LoadManager at ConfigNodeGroup-Leader is active. It proactively implements the cluster
 * dynamic load balancing policy and passively accepts the PartitionTable expansion request.
 */
public class LoadManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadManager.class);

  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();
  private static final long HEARTBEAT_INTERVAL = CONF.getHeartbeatInterval();

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

  public LoadManager(IManager configManager) {
    this.configManager = configManager;

    this.regionBalancer = new RegionBalancer(configManager);
    this.partitionBalancer = new PartitionBalancer(configManager);
    this.routeBalancer = new RouteBalancer(configManager);
    MetricService.getInstance().addMetricSet(new LoadManagerMetrics(configManager));
  }

  /**
   * Generate an optimal CreateRegionGroupsPlan
   *
   * @param allotmentMap Map<StorageGroupName, Region allotment>
   * @param consensusGroupType TConsensusGroupType of RegionGroup to be allocated
   * @return CreateRegionGroupsPlan
   * @throws NotEnoughDataNodeException If there are not enough DataNodes
   * @throws StorageGroupNotExistsException If some specific StorageGroups don't exist
   */
  public CreateRegionGroupsPlan allocateRegionGroups(
      Map<String, Integer> allotmentMap, TConsensusGroupType consensusGroupType)
      throws NotEnoughDataNodeException, StorageGroupNotExistsException {
    return regionBalancer.genRegionsAllocationPlan(allotmentMap, consensusGroupType);
  }

  /**
   * Allocate SchemaPartitions
   *
   * @param unassignedSchemaPartitionSlotsMap SchemaPartitionSlots that should be assigned
   * @return Map<StorageGroupName, SchemaPartitionTable>, the allocating result
   */
  public Map<String, SchemaPartitionTable> allocateSchemaPartition(
      Map<String, List<TSeriesPartitionSlot>> unassignedSchemaPartitionSlotsMap)
      throws NotAvailableRegionGroupException {
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
          unassignedDataPartitionSlotsMap)
      throws NotAvailableRegionGroupException {
    return partitionBalancer.allocateDataPartition(unassignedDataPartitionSlotsMap);
  }

  public Map<TConsensusGroupId, Integer> getLatestRegionLeaderMap() {
    return routeBalancer.getLatestRegionLeaderMap();
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
    UpdateLoadStatisticsPlan updateLoadStatisticsPlan = new UpdateLoadStatisticsPlan();

    // Update NodeStatistics
    getNodeManager()
        .getNodeCacheMap()
        .forEach(
            (nodeId, nodeCache) -> {
              // Update and check if NodeStatistics needs consensus
              nodeCache.updateNodeStatistics();
              if (!nodeCache
                  .getStatistics()
                  .equals(getNodeManager().getNodeStatistics(nodeId, false))) {
                updateLoadStatisticsPlan.putNodeStatistics(nodeId, nodeCache.getStatistics());
              }
            });

    // Update RegionGroupStatistics
    getPartitionManager()
        .getRegionGroupCacheMap()
        .forEach(
            (regionGroupId, regionGroupCache) -> {
              // Update and check if RegionGroupStatistics needs consensus
              regionGroupCache.updateRegionGroupStatistics();
              if (!regionGroupCache
                  .getStatistics()
                  .equals(getPartitionManager().getRegionGroupStatistics(regionGroupId, false))) {
                updateLoadStatisticsPlan.putRegionGroupStatistics(
                    regionGroupId, regionGroupCache.getStatistics());
              }
            });

    // Update RegionRouteMap
    routeBalancer.updateRegionRouteMap();
    if (!routeBalancer
        .getLatestRegionRouteMap()
        .equals(getPartitionManager().getRegionRouteMap())) {
      updateLoadStatisticsPlan.setRegionRouteMap(routeBalancer.getLatestRegionRouteMap());
    }

    // Consensus-write LoadStatistics if not empty
    if (!updateLoadStatisticsPlan.isEmpty()) {
      getConsensusManager().write(updateLoadStatisticsPlan);
      broadcastLatestRegionRouteMap();
    }
  }

  public void broadcastLatestRegionRouteMap() {
    Map<TConsensusGroupId, TRegionReplicaSet> latestRegionRouteMap = getLatestRegionRouteMap();
    Map<Integer, TDataNodeLocation> dataNodeLocationMap = new ConcurrentHashMap<>();
    getNodeManager()
        .filterDataNodeThroughStatus(NodeStatus.Running)
        .forEach(
            onlineDataNode ->
                dataNodeLocationMap.put(
                    onlineDataNode.getLocation().getDataNodeId(), onlineDataNode.getLocation()));

    LOGGER.info("[latestRegionRouteMap] Begin to broadcast RegionRouteMap:");
    long broadcastTime = System.currentTimeMillis();
    printRegionRouteMap(broadcastTime, latestRegionRouteMap);

    AsyncClientHandler<TRegionRouteReq, TSStatus> clientHandler =
        new AsyncClientHandler<>(
            DataNodeRequestType.UPDATE_REGION_ROUTE_MAP,
            new TRegionRouteReq(broadcastTime, latestRegionRouteMap),
            dataNodeLocationMap);
    AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);
    LOGGER.info("[latestRegionRouteMap] Broadcast the latest RegionRouteMap finished.");
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

  /**
   * Recover the cluster heartbeat cache through loadStatistics when the ConfigNode-Leader is
   * switched
   */
  public void recoverHeartbeatCache() {
    getNodeManager().recoverNodeCacheMap();
    getPartitionManager().recoverRegionGroupCacheMap();
    routeBalancer.recoverRegionRouteMap();
  }

  public RouteBalancer getRouteBalancer() {
    return routeBalancer;
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
}
