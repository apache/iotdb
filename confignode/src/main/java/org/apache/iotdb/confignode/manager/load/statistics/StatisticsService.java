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

package org.apache.iotdb.confignode.manager.load.statistics;

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
import org.apache.iotdb.confignode.manager.load.LoadCache;
import org.apache.iotdb.confignode.manager.load.balancer.RouteBalancer;
import org.apache.iotdb.confignode.manager.load.balancer.router.RegionRouteMap;
import org.apache.iotdb.confignode.manager.observer.NodeStatisticsEvent;
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

public class StatisticsService {

  private static final Logger LOGGER = LoggerFactory.getLogger(StatisticsService.class);

  private static final long HEARTBEAT_INTERVAL =
      ConfigNodeDescriptor.getInstance().getConf().getHeartbeatIntervalInMs();

  private final IManager configManager;
  private final RouteBalancer routeBalancer;
  private final LoadCache loadCache;
  private final EventBus eventBus;

  public StatisticsService(IManager configManager, LoadCache loadCache, EventBus eventBus) {
    this.configManager = configManager;
    this.routeBalancer = configManager.getLoadManager().getRouteBalancer();
    this.loadCache = loadCache;
    this.eventBus = eventBus;
  }

  /** Load statistics executor service */
  private final Object statisticsScheduleMonitor = new Object();

  private Future<?> currentLoadStatisticsFuture;
  private final ScheduledExecutorService loadStatisticsExecutor =
      IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor("Cluster-LoadStatistics-Service");

  /** Start the load statistics service */
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

  /** Stop the load statistics service */
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
    // Pair<NodeStatistics, NodeStatistics>:left one means the current NodeStatistics, right one
    // means the previous NodeStatistics
    Map<Integer, Pair<NodeStatistics, NodeStatistics>> differentNodeStatisticsMap =
        loadCache.updateNodeStatistics();
    if (!differentNodeStatisticsMap.isEmpty()) {
      isNeedBroadcast = true;
      recordNodeStatistics(differentNodeStatisticsMap);
      eventBus.post(new NodeStatisticsEvent(differentNodeStatisticsMap));
    }

    // Update RegionGroupStatistics
    Map<TConsensusGroupId, RegionGroupStatistics> differentRegionGroupStatisticsMap =
        loadCache.updateRegionGroupStatistics();
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
}
