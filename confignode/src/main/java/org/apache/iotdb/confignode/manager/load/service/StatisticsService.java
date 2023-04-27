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
import org.apache.iotdb.confignode.manager.load.balancer.RouteBalancer;
import org.apache.iotdb.confignode.manager.load.cache.LoadCache;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeStatistics;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionGroupStatistics;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionStatistics;
import org.apache.iotdb.confignode.manager.load.subscriber.IClusterStatusSubscriber;
import org.apache.iotdb.confignode.manager.load.subscriber.RouteChangeEvent;
import org.apache.iotdb.confignode.manager.load.subscriber.StatisticsChangeEvent;
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

    // Update NodeStatistics:
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

    if (isNeedBroadcast) {
      StatisticsChangeEvent statisticsChangeEvent =
          new StatisticsChangeEvent(differentNodeStatisticsMap, differentRegionGroupStatisticsMap);
      eventBus.post(statisticsChangeEvent);
    }

    // Update RegionRouteMap
    RouteChangeEvent routeChangeEvent = routeBalancer.updateRegionRouteMap();
    if (routeChangeEvent.isNeedBroadcast()) {
      isNeedBroadcast = true;
      eventBus.post(routeChangeEvent);
    }

    if (isNeedBroadcast) {
      broadcastLatestRegionRouteMap();
    }
  }

  public void broadcastLatestRegionRouteMap() {
    Map<TConsensusGroupId, TRegionReplicaSet> latestRegionRouteMap =
        routeBalancer.getLatestRegionPriorityMap();
    Map<Integer, TDataNodeLocation> dataNodeLocationMap = new ConcurrentHashMap<>();
    // Broadcast the RegionRouteMap to all DataNodes except the unknown ones
    configManager
        .getNodeManager()
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

  private void recordNodeStatistics(
      Map<Integer, Pair<NodeStatistics, NodeStatistics>> differentNodeStatisticsMap) {
    LOGGER.info("[UpdateLoadStatistics] NodeStatisticsMap: ");
    for (Map.Entry<Integer, Pair<NodeStatistics, NodeStatistics>> nodeCacheEntry :
        differentNodeStatisticsMap.entrySet()) {
      LOGGER.info(
          "[UpdateLoadStatistics]\t {}={}",
          "nodeId{" + nodeCacheEntry.getKey() + "}",
          nodeCacheEntry.getValue().getRight());
    }
  }

  private void recordRegionGroupStatistics(
      Map<TConsensusGroupId, Pair<RegionGroupStatistics, RegionGroupStatistics>>
          differentRegionGroupStatisticsMap) {
    LOGGER.info("[UpdateLoadStatistics] RegionGroupStatisticsMap: ");
    for (Map.Entry<TConsensusGroupId, Pair<RegionGroupStatistics, RegionGroupStatistics>>
        regionGroupStatisticsEntry : differentRegionGroupStatisticsMap.entrySet()) {
      LOGGER.info("[UpdateLoadStatistics]\t RegionGroup: {}", regionGroupStatisticsEntry.getKey());
      LOGGER.info("[UpdateLoadStatistics]\t {}", regionGroupStatisticsEntry.getValue());
      for (Map.Entry<Integer, RegionStatistics> regionStatisticsEntry :
          regionGroupStatisticsEntry.getValue().getRight().getRegionStatisticsMap().entrySet()) {
        LOGGER.info(
            "[UpdateLoadStatistics]\t dataNodeId{}={}",
            regionStatisticsEntry.getKey(),
            regionStatisticsEntry.getValue());
      }
    }
  }

  @Override
  public void onClusterStatisticsChanged(StatisticsChangeEvent event) {
    recordNodeStatistics(event.getNodeStatisticsMap());
    recordRegionGroupStatistics(event.getRegionGroupStatisticsMap());
  }

  private void recordRegionLeaderMap(Map<TConsensusGroupId, Pair<Integer, Integer>> leaderMap) {
    LOGGER.info("[UpdateLoadStatistics] RegionLeaderMap: ");
    for (Map.Entry<TConsensusGroupId, Pair<Integer, Integer>> regionLeaderEntry :
        leaderMap.entrySet()) {
      LOGGER.info(
          "[UpdateLoadStatistics]\t {}={}",
          regionLeaderEntry.getKey(),
          regionLeaderEntry.getValue().getRight());
    }
  }

  private void recordRegionPriorityMap(
      Map<TConsensusGroupId, Pair<TRegionReplicaSet, TRegionReplicaSet>> priorityMap) {
    LOGGER.info("[UpdateLoadStatistics] RegionPriorityMap: ");
    for (Map.Entry<TConsensusGroupId, Pair<TRegionReplicaSet, TRegionReplicaSet>>
        regionPriorityEntry : priorityMap.entrySet()) {
      LOGGER.info(
          "[UpdateLoadStatistics]\t {}={}",
          regionPriorityEntry.getKey(),
          regionPriorityEntry.getValue().getRight().getDataNodeLocations().stream()
              .map(TDataNodeLocation::getDataNodeId)
              .collect(Collectors.toList()));
    }
  }

  @Override
  public void onRegionGroupLeaderChanged(RouteChangeEvent event) {
    recordRegionLeaderMap(event.getLeaderMap());
    recordRegionPriorityMap(event.getPriorityMap());
  }
}
