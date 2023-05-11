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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.async.AsyncDataNodeClientPool;
import org.apache.iotdb.confignode.client.async.handlers.AsyncClientHandler;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.load.balancer.RouteBalancer;
import org.apache.iotdb.confignode.manager.load.cache.LoadCache;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeStatistics;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionGroupStatistics;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionStatistics;
import org.apache.iotdb.confignode.manager.load.subscriber.IClusterStatusSubscriber;
import org.apache.iotdb.confignode.manager.load.subscriber.RouteChangeEvent;
import org.apache.iotdb.confignode.manager.load.subscriber.StatisticsChangeEvent;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.mpp.rpc.thrift.TRegionRouteReq;
import org.apache.iotdb.tsfile.utils.Pair;

import com.google.common.eventbus.EventBus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class StatisticsService implements IClusterStatusSubscriber {

  private static final Logger LOGGER = LoggerFactory.getLogger(StatisticsService.class);

  private static final long HEARTBEAT_INTERVAL =
      ConfigNodeDescriptor.getInstance().getConf().getHeartbeatIntervalInMs();

  private final IManager configManager;
  private final RouteBalancer routeBalancer;
  private final LoadCache loadCache;
  private final EventBus eventBus;

  public StatisticsService(
      IManager configManager, RouteBalancer routeBalancer, LoadCache loadCache, EventBus eventBus) {
    this.configManager = configManager;
    this.routeBalancer = routeBalancer;
    this.loadCache = loadCache;
    this.eventBus = eventBus;
  }

  /** Load statistics executor service. */
  private final Object statisticsScheduleMonitor = new Object();

  private Future<?> currentLoadStatisticsFuture;
  private final ScheduledExecutorService loadStatisticsExecutor =
      IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor("Cluster-LoadStatistics-Service");

  /** Start the load statistics service. */
  public void startLoadStatisticsService() {
    synchronized (statisticsScheduleMonitor) {
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

  /** Stop the load statistics service. */
  public void stopLoadStatisticsService() {
    synchronized (statisticsScheduleMonitor) {
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

    // Update NodeStatistics
    // Map<NodeId, Pair<old NodeStatistics, new NodeStatistics>>
    Map<Integer, Pair<NodeStatistics, NodeStatistics>> differentNodeStatisticsMap =
        loadCache.updateNodeStatistics();
    if (!differentNodeStatisticsMap.isEmpty()) {
      isNeedBroadcast = true;
    }

    // Update RegionGroupStatistics
    // Map<RegionGroupId, Pair<old RegionGroupStatistics, new RegionGroupStatistics>>
    Map<TConsensusGroupId, Pair<RegionGroupStatistics, RegionGroupStatistics>>
        differentRegionGroupStatisticsMap = loadCache.updateRegionGroupStatistics();
    if (!differentRegionGroupStatisticsMap.isEmpty()) {
      isNeedBroadcast = true;
    }

    // Update RegionGroupLeaders
    // Map<RegionGroupId, Pair<old leader index, new leader index>>
    Map<TConsensusGroupId, Pair<Integer, Integer>> differentRegionLeaderMap =
        loadCache.updateRegionGroupLeader();
    if (!differentRegionLeaderMap.isEmpty()) {
      isNeedBroadcast = true;
    }

    if (isNeedBroadcast) {
      differentRegionLeaderMap.putAll(routeBalancer.balanceRegionLeader());
      // Update RegionPriority
      // Map<RegionGroupId, Pair<old priority, new priority>>
      Map<TConsensusGroupId, Pair<TRegionReplicaSet, TRegionReplicaSet>>
          differentRegionPriorityMap = routeBalancer.balanceRegionPriority();

      eventBus.post(
          new StatisticsChangeEvent(differentNodeStatisticsMap, differentRegionGroupStatisticsMap));
      eventBus.post(new RouteChangeEvent(differentRegionLeaderMap, differentRegionPriorityMap));
      broadcastLatestRegionRouteMap();
    }
  }

  public void broadcastLatestRegionRouteMap() {
    Map<TConsensusGroupId, TRegionReplicaSet> regionPriorityMap =
        getLoadManager().getRegionPriorityMap();
    Map<Integer, TDataNodeLocation> dataNodeLocationMap = new ConcurrentHashMap<>();
    // Broadcast the RegionRouteMap to all DataNodes except the unknown ones
    getNodeManager()
        .filterDataNodeThroughStatus(NodeStatus.Running, NodeStatus.Removing, NodeStatus.ReadOnly)
        .forEach(
            onlineDataNode ->
                dataNodeLocationMap.put(
                    onlineDataNode.getLocation().getDataNodeId(), onlineDataNode.getLocation()));

    long broadcastTime = System.currentTimeMillis();
    AsyncClientHandler<TRegionRouteReq, TSStatus> clientHandler =
        new AsyncClientHandler<>(
            DataNodeRequestType.UPDATE_REGION_ROUTE_MAP,
            new TRegionRouteReq(broadcastTime, regionPriorityMap),
            dataNodeLocationMap);
    AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);
  }

  private void recordNodeStatistics(
      Map<Integer, Pair<NodeStatistics, NodeStatistics>> differentNodeStatisticsMap) {
    LOGGER.info("[NodeStatistics] NodeStatisticsMap: ");
    for (Map.Entry<Integer, Pair<NodeStatistics, NodeStatistics>> nodeCacheEntry :
        differentNodeStatisticsMap.entrySet()) {
      LOGGER.info(
          "[NodeStatistics]\t {}={}",
          "nodeId{" + nodeCacheEntry.getKey() + "}",
          nodeCacheEntry.getValue().getRight());
    }
  }

  private void recordRegionGroupStatistics(
      Map<TConsensusGroupId, Pair<RegionGroupStatistics, RegionGroupStatistics>>
          differentRegionGroupStatisticsMap) {
    LOGGER.info("[RegionGroupStatistics] RegionGroupStatisticsMap: ");
    for (Map.Entry<TConsensusGroupId, Pair<RegionGroupStatistics, RegionGroupStatistics>>
        regionGroupStatisticsEntry : differentRegionGroupStatisticsMap.entrySet()) {
      LOGGER.info("[RegionGroupStatistics]\t RegionGroup: {}", regionGroupStatisticsEntry.getKey());
      LOGGER.info("[RegionGroupStatistics]\t {}", regionGroupStatisticsEntry.getValue());
      for (Map.Entry<Integer, RegionStatistics> regionStatisticsEntry :
          regionGroupStatisticsEntry.getValue().getRight().getRegionStatisticsMap().entrySet()) {
        LOGGER.info(
            "[RegionGroupStatistics]\t dataNodeId{}={}",
            regionStatisticsEntry.getKey(),
            regionStatisticsEntry.getValue());
      }
    }
  }

  @Override
  public synchronized void onClusterStatisticsChanged(StatisticsChangeEvent event) {
    recordNodeStatistics(event.getNodeStatisticsMap());
    recordRegionGroupStatistics(event.getRegionGroupStatisticsMap());
  }

  private void recordRegionLeaderMap(Map<TConsensusGroupId, Pair<Integer, Integer>> leaderMap) {
    LOGGER.info("[RegionLeader] RegionLeaderMap: ");
    for (Map.Entry<TConsensusGroupId, Pair<Integer, Integer>> regionLeaderEntry :
        leaderMap.entrySet()) {
      LOGGER.info(
          "[RegionLeader]\t {}={}",
          regionLeaderEntry.getKey(),
          regionLeaderEntry.getValue().getRight());
    }
  }

  private void recordRegionPriorityMap(
      Map<TConsensusGroupId, Pair<TRegionReplicaSet, TRegionReplicaSet>> priorityMap) {
    LOGGER.info("[RegionPriority] RegionPriorityMap: ");
    for (Map.Entry<TConsensusGroupId, Pair<TRegionReplicaSet, TRegionReplicaSet>>
        regionPriorityEntry : priorityMap.entrySet()) {
      LOGGER.info(
          "[RegionPriority]\t {}={}",
          regionPriorityEntry.getKey(),
          regionPriorityEntry.getValue().getRight().getDataNodeLocations().stream()
              .map(TDataNodeLocation::getDataNodeId)
              .collect(Collectors.toList()));
    }
  }

  @Override
  public synchronized void onRegionGroupLeaderChanged(RouteChangeEvent event) {
    recordRegionLeaderMap(event.getLeaderMap());
    recordRegionPriorityMap(event.getPriorityMap());
  }

  private NodeManager getNodeManager() {
    return configManager.getNodeManager();
  }

  private LoadManager getLoadManager() {
    return configManager.getLoadManager();
  }
}
