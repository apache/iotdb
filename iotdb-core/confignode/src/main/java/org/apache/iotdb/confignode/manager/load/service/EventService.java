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
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.async.AsyncDataNodeClientPool;
import org.apache.iotdb.confignode.client.async.handlers.AsyncClientHandler;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.load.cache.LoadCache;
import org.apache.iotdb.confignode.manager.load.cache.consensus.ConsensusStatistics;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeStatistics;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionGroupStatistics;
import org.apache.iotdb.confignode.manager.load.subscriber.ConsensusStatisticsChangeEvent;
import org.apache.iotdb.mpp.rpc.thrift.TRegionRouteReq;
import org.apache.iotdb.tsfile.utils.Pair;

import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.EventBus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class EventService {

  private static final Logger LOGGER = LoggerFactory.getLogger(EventService.class);

  private static final long HEARTBEAT_INTERVAL =
      ConfigNodeDescriptor.getInstance().getConf().getHeartbeatIntervalInMs();

  // Event executor service
  private final Object eventServiceMonitor = new Object();

  private Future<?> currentEventServiceFuture;
  private final ScheduledExecutorService eventServiceExecutor =
      IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
          ThreadName.CONFIG_NODE_EVENT_SERVICE.getName());

  private final LoadCache loadCache;
  private final Map<Integer, NodeStatistics> previousNodeStatisticsMap;
  private final Map<TConsensusGroupId, RegionGroupStatistics> previousRegionGroupStatisticsMap;
  private final Map<TConsensusGroupId, ConsensusStatistics> previousConsensusStatisticsMap;
  private final EventBus eventPublisher;

  public EventService(IManager configManager, LoadCache loadCache) {
    this.loadCache = loadCache;
    this.previousNodeStatisticsMap = new TreeMap<>();
    this.previousRegionGroupStatisticsMap = new TreeMap<>();
    this.previousConsensusStatisticsMap = new TreeMap<>();

    this.eventPublisher =
        new AsyncEventBus(
            ThreadName.CONFIG_NODE_LOAD_PUBLISHER.getName(),
            IoTDBThreadPoolFactory.newFixedThreadPool(
                5, ThreadName.CONFIG_NODE_LOAD_PUBLISHER.getName()));
    eventPublisher.register(configManager.getPipeManager().getPipeRuntimeCoordinator());
  }

  /** Start the event service. */
  public void startEventService() {
    synchronized (eventServiceMonitor) {
      if (currentEventServiceFuture == null) {
        currentEventServiceFuture =
            ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
                eventServiceExecutor,
                this::broadcastChangeEventIfNecessary,
                0,
                HEARTBEAT_INTERVAL,
                TimeUnit.MILLISECONDS);
        LOGGER.info("Event service is started successfully.");
      }
    }
  }

  /** Stop the event service. */
  public void stopEventService() {
    synchronized (eventServiceMonitor) {
      if (currentEventServiceFuture != null) {
        currentEventServiceFuture.cancel(false);
        currentEventServiceFuture = null;
        LOGGER.info("Event service is stopped successfully.");
      }
    }
  }

  private synchronized void broadcastChangeEventIfNecessary() {
    checkAndBroadcastNodeStatisticsChangeEventIfNecessary();
    checkAndBroadcastRegionGroupStatisticsChangeEventIfNecessary();
    checkAndBroadcastConsensusStatisticsChangeEventIfNecessary();
  }

  public void checkAndBroadcastNodeStatisticsChangeEventIfNecessary() {
    Map<Integer, NodeStatistics> currentNodeStatisticsMap = loadCache.getCurrentNodeStatisticsMap();
    Map<Integer, Pair<NodeStatistics, NodeStatistics>> differentNodeStatisticsMap = new TreeMap<>();
    currentNodeStatisticsMap.forEach(
        (nodeId, currentNodeStatistics) -> {
          NodeStatistics previousNodeStatistics = previousNodeStatisticsMap.get(nodeId);
          if (previousNodeStatistics == null
              || (currentNodeStatistics.isNewerThan(previousNodeStatistics)
                  && !currentNodeStatistics.equals(previousNodeStatistics))) {
            differentNodeStatisticsMap.put(
                nodeId, new Pair<>(previousNodeStatistics, currentNodeStatistics));
            previousNodeStatisticsMap.put(nodeId, currentNodeStatistics);
          }
        });
    if (!differentNodeStatisticsMap.isEmpty()) {
      // TODO: A NodeStatistics change event can be broadcast in the future
      recordNodeStatistics(differentNodeStatisticsMap);
    }
  }

  private void recordNodeStatistics(
      Map<Integer, Pair<NodeStatistics, NodeStatistics>> differentNodeStatisticsMap) {
    LOGGER.info("[NodeStatistics] NodeStatisticsMap: ");
    for (Map.Entry<Integer, Pair<NodeStatistics, NodeStatistics>> nodeCacheEntry :
        differentNodeStatisticsMap.entrySet()) {
      if (!Objects.equals(
          nodeCacheEntry.getValue().getRight(), nodeCacheEntry.getValue().getLeft())) {
        LOGGER.info(
            "[NodeStatistics]\t {}: {}->{}",
            "nodeId{" + nodeCacheEntry.getKey() + "}",
            nodeCacheEntry.getValue().getLeft(),
            nodeCacheEntry.getValue().getRight());
      }
    }
  }

  public void checkAndBroadcastRegionGroupStatisticsChangeEventIfNecessary() {
    Map<TConsensusGroupId, RegionGroupStatistics> currentRegionGroupStatisticsMap =
        loadCache.getCurrentRegionGroupStatisticsMap();
    Map<TConsensusGroupId, Pair<RegionGroupStatistics, RegionGroupStatistics>>
        differentRegionGroupStatisticsMap = new TreeMap<>();
    currentRegionGroupStatisticsMap.forEach(
        (regionGroupId, currentRegionGroupStatistics) -> {
          RegionGroupStatistics previousRegionGroupStatistics =
              previousRegionGroupStatisticsMap.get(regionGroupId);
          if (previousRegionGroupStatistics == null
              || (currentRegionGroupStatistics.isNewerThan(previousRegionGroupStatistics)
                  && !currentRegionGroupStatistics.equals(previousRegionGroupStatistics))) {
            differentRegionGroupStatisticsMap.put(
                regionGroupId,
                new Pair<>(previousRegionGroupStatistics, currentRegionGroupStatistics));
            previousRegionGroupStatisticsMap.put(regionGroupId, currentRegionGroupStatistics);
          }
        });
    if (!differentRegionGroupStatisticsMap.isEmpty()) {
      // TODO: A RegionGroupStatistics change event can be broadcast in the future
      recordRegionGroupStatistics(differentRegionGroupStatisticsMap);
    }
  }

  private void recordRegionGroupStatistics(
      Map<TConsensusGroupId, Pair<RegionGroupStatistics, RegionGroupStatistics>>
          differentRegionGroupStatisticsMap) {
    LOGGER.info("[RegionGroupStatistics] RegionGroupStatisticsMap: ");
    for (Map.Entry<TConsensusGroupId, Pair<RegionGroupStatistics, RegionGroupStatistics>>
        regionGroupStatisticsEntry : differentRegionGroupStatisticsMap.entrySet()) {
      if (!Objects.equals(
          regionGroupStatisticsEntry.getValue().getRight(),
          regionGroupStatisticsEntry.getValue().getLeft())) {
        LOGGER.info(
            "[RegionGroupStatistics]\t RegionGroup {}: {} -> {}",
            regionGroupStatisticsEntry.getKey(),
            regionGroupStatisticsEntry.getValue().getLeft().getRegionGroupStatus(),
            regionGroupStatisticsEntry.getValue().getRight().getRegionGroupStatus());

        List<Integer> leftIds = regionGroupStatisticsEntry.getValue().getLeft().getRegionIds();
        List<Integer> rightIds = regionGroupStatisticsEntry.getValue().getRight().getRegionIds();
        for (Integer leftId : leftIds) {
          if (!rightIds.contains(leftId)) {
            LOGGER.info(
                "[RegionGroupStatistics]\t Region in DataNode {}: {} -> null",
                leftId,
                regionGroupStatisticsEntry.getValue().getLeft().getRegionStatus(leftId));
          } else {
            LOGGER.info(
                "[RegionGroupStatistics]\t Region in DataNode {}: {} -> {}",
                leftId,
                regionGroupStatisticsEntry.getValue().getLeft().getRegionStatus(leftId),
                regionGroupStatisticsEntry.getValue().getRight().getRegionStatus(leftId));
          }
        }
        for (Integer rightId : rightIds) {
          if (!leftIds.contains(rightId)) {
            LOGGER.info(
                "[RegionGroupStatistics]\t Region in DataNode {}: null -> {}",
                rightId,
                regionGroupStatisticsEntry.getValue().getRight().getRegionStatus(rightId));
          }
        }
      }
    }
  }

  public void checkAndBroadcastConsensusStatisticsChangeEventIfNecessary() {
    Map<TConsensusGroupId, ConsensusStatistics> currentConsensusStatisticsMap =
        loadCache.getCurrentConsensusStatisticsMap();
    Map<TConsensusGroupId, Pair<ConsensusStatistics, ConsensusStatistics>>
        differentConsensusStatisticsMap = new TreeMap<>();
    currentConsensusStatisticsMap.forEach(
        (consensusGroupId, currentConsensusStatistics) -> {
          ConsensusStatistics previousConsensusStatistics =
              previousConsensusStatisticsMap.get(consensusGroupId);
          if (previousConsensusStatistics == null
              || (currentConsensusStatistics.isNewerThan(previousConsensusStatistics)
                  && !currentConsensusStatistics.equals(previousConsensusStatistics))) {
            differentConsensusStatisticsMap.put(
                consensusGroupId,
                new Pair<>(previousConsensusStatistics, currentConsensusStatistics));
            previousConsensusStatisticsMap.put(consensusGroupId, currentConsensusStatistics);
          }
        });
    if (!differentConsensusStatisticsMap.isEmpty()) {
      eventPublisher.post(new ConsensusStatisticsChangeEvent(differentConsensusStatisticsMap));
      recordConsensusStatistics(differentConsensusStatisticsMap);
    }
  }

  private void recordConsensusStatistics(
      Map<TConsensusGroupId, Pair<ConsensusStatistics, ConsensusStatistics>>
          differentConsensusStatisticsMap) {
    LOGGER.info("[ConsensusStatistics] ConsensusStatisticsMap: ");
    for (Map.Entry<TConsensusGroupId, Pair<ConsensusStatistics, ConsensusStatistics>>
        consensusStatisticsEntry : differentConsensusStatisticsMap.entrySet()) {
      if (!Objects.equals(
          consensusStatisticsEntry.getValue().getRight(),
          consensusStatisticsEntry.getValue().getLeft())) {
        LOGGER.info(
            "[ConsensusStatistics]\t {}: {}->{}",
            consensusStatisticsEntry.getKey(),
            consensusStatisticsEntry.getValue().getLeft(),
            consensusStatisticsEntry.getValue().getRight());
      }
    }
  }

  private void broadcastLatestRegionRouteMap() {
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
}
