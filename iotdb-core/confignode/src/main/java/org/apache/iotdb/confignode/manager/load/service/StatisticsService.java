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
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.load.balancer.RouteBalancer;
import org.apache.iotdb.confignode.manager.load.cache.LoadCache;
import org.apache.iotdb.confignode.manager.load.subscriber.ConsensusStatisticsChangeEvent;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
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

  public StatisticsService(
      IManager configManager, RouteBalancer routeBalancer, LoadCache loadCache) {
    this.configManager = configManager;
    this.routeBalancer = routeBalancer;
    this.loadCache = loadCache;
  }

  /** Load statistics executor service. */
  private final Object statisticsScheduleMonitor = new Object();

  private Future<?> currentLoadStatisticsFuture;
  private final ScheduledExecutorService loadStatisticsExecutor =
      IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
          ThreadName.CONFIG_NODE_LOAD_STATISTIC.getName());

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
    loadCache.updateNodeStatistics();
    loadCache.updateRegionGroupStatistics();
    loadCache.updateConsensusStatistics();
  }

  public void broadcastRouteChangeEventIfNecessary(
      Map<TConsensusGroupId, Pair<Integer, Integer>> differentRegionLeaderMap,
      Map<TConsensusGroupId, Pair<TRegionReplicaSet, TRegionReplicaSet>>
          differentRegionPriorityMap) {
    if (containsChangeEvent(differentRegionLeaderMap)
        || containsChangeEvent(differentRegionPriorityMap)) {
      eventBus.post(
          new ConsensusStatisticsChangeEvent(differentRegionLeaderMap, differentRegionPriorityMap));
    }
  }

  private static <T> boolean containsChangeEvent(Map<TConsensusGroupId, Pair<T, T>> map) {
    return !map.isEmpty()
        && map.values().stream().anyMatch(pair -> !Objects.equals(pair.getLeft(), pair.getRight()));
  }

  private void recordRegionPriorityMap(
      Map<TConsensusGroupId, Pair<TRegionReplicaSet, TRegionReplicaSet>> priorityMap) {
    LOGGER.info("[RegionPriority] RegionPriorityMap: ");
    for (Map.Entry<TConsensusGroupId, Pair<TRegionReplicaSet, TRegionReplicaSet>>
        regionPriorityEntry : priorityMap.entrySet()) {
      if (!Objects.equals(
          regionPriorityEntry.getValue().getRight(), regionPriorityEntry.getValue().getLeft())) {
        try {
          LOGGER.info(
              "[RegionPriority]\t {}: {}->{}",
              regionPriorityEntry.getKey(),
              regionPriorityEntry.getValue().getLeft() == null
                  ? "null"
                  : regionPriorityEntry.getValue().getLeft().getDataNodeLocations().stream()
                      .map(TDataNodeLocation::getDataNodeId)
                      .collect(Collectors.toList()),
              regionPriorityEntry.getValue().getRight().getDataNodeLocations().stream()
                  .map(TDataNodeLocation::getDataNodeId)
                  .collect(Collectors.toList()));
        } catch (Exception e) {
          LOGGER.error("unexcepted exception", e);
        }
      }
    }
  }

  private NodeManager getNodeManager() {
    return configManager.getNodeManager();
  }

  private LoadManager getLoadManager() {
    return configManager.getLoadManager();
  }
}
