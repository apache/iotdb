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
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.load.balancer.RouteBalancer;
import org.apache.iotdb.confignode.manager.load.cache.LoadCache;
import org.apache.iotdb.confignode.manager.load.cache.consensus.ConsensusGroupStatistics;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeStatistics;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionGroupStatistics;
import org.apache.iotdb.confignode.manager.load.subscriber.ConsensusGroupStatisticsChangeEvent;
import org.apache.iotdb.confignode.manager.load.subscriber.NodeStatisticsChangeEvent;
import org.apache.iotdb.confignode.manager.load.subscriber.RegionGroupStatisticsChangeEvent;

import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.EventBus;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * EventService periodically check statistics and broadcast corresponding change event if necessary.
 */
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
  private final Map<TConsensusGroupId, ConsensusGroupStatistics>
      previousConsensusGroupStatisticsMap;
  private final EventBus eventPublisher;

  public EventService(IManager configManager, LoadCache loadCache, RouteBalancer routeBalancer) {
    this.loadCache = loadCache;
    this.previousNodeStatisticsMap = new TreeMap<>();
    this.previousRegionGroupStatisticsMap = new TreeMap<>();
    this.previousConsensusGroupStatisticsMap = new TreeMap<>();

    this.eventPublisher =
        new AsyncEventBus(
            ThreadName.CONFIG_NODE_LOAD_PUBLISHER.getName(),
            IoTDBThreadPoolFactory.newFixedThreadPool(
                5, ThreadName.CONFIG_NODE_LOAD_PUBLISHER.getName()));
    eventPublisher.register(configManager.getPipeManager().getPipeRuntimeCoordinator());
    eventPublisher.register(routeBalancer);
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

  private void broadcastChangeEventIfNecessary() {
    checkAndBroadcastNodeStatisticsChangeEventIfNecessary();
    checkAndBroadcastRegionGroupStatisticsChangeEventIfNecessary();
    checkAndBroadcastConsensusGroupStatisticsChangeEventIfNecessary();
  }

  public synchronized void checkAndBroadcastNodeStatisticsChangeEventIfNecessary() {
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
    previousNodeStatisticsMap.forEach(
        (nodeId, previousNodeStatistics) -> {
          if (!currentNodeStatisticsMap.containsKey(nodeId)) {
            differentNodeStatisticsMap.put(nodeId, new Pair<>(previousNodeStatistics, null));
          }
        });
    previousNodeStatisticsMap.keySet().retainAll(currentNodeStatisticsMap.keySet());
    if (!differentNodeStatisticsMap.isEmpty()) {
      eventPublisher.post(new NodeStatisticsChangeEvent(differentNodeStatisticsMap));
      recordNodeStatistics(differentNodeStatisticsMap);
    }
  }

  private void recordNodeStatistics(
      Map<Integer, Pair<NodeStatistics, NodeStatistics>> differentNodeStatisticsMap) {
    LOGGER.info("[NodeStatistics] NodeStatisticsMap: ");
    for (Map.Entry<Integer, Pair<NodeStatistics, NodeStatistics>> nodeCacheEntry :
        differentNodeStatisticsMap.entrySet()) {
      LOGGER.info(
          "[NodeStatistics]\t {}: {} -> {}",
          "nodeId{" + nodeCacheEntry.getKey() + "}",
          nodeCacheEntry.getValue().getLeft(),
          nodeCacheEntry.getValue().getRight());
    }
  }

  public synchronized void checkAndBroadcastRegionGroupStatisticsChangeEventIfNecessary() {
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
    previousRegionGroupStatisticsMap.forEach(
        (regionGroupId, previousRegionGroupStatistics) -> {
          if (!currentRegionGroupStatisticsMap.containsKey(regionGroupId)) {
            differentRegionGroupStatisticsMap.put(
                regionGroupId, new Pair<>(previousRegionGroupStatistics, null));
          }
        });
    previousRegionGroupStatisticsMap.keySet().retainAll(currentRegionGroupStatisticsMap.keySet());
    if (!differentRegionGroupStatisticsMap.isEmpty()) {
      eventPublisher.post(new RegionGroupStatisticsChangeEvent(differentRegionGroupStatisticsMap));
      recordRegionGroupStatistics(differentRegionGroupStatisticsMap);
    }
  }

  @SuppressWarnings("java:S2259")
  private void recordRegionGroupStatistics(
      Map<TConsensusGroupId, Pair<RegionGroupStatistics, RegionGroupStatistics>>
          differentRegionGroupStatisticsMap) {
    LOGGER.info("[RegionGroupStatistics] RegionGroupStatisticsMap: ");
    for (Map.Entry<TConsensusGroupId, Pair<RegionGroupStatistics, RegionGroupStatistics>>
        regionGroupStatisticsEntry : differentRegionGroupStatisticsMap.entrySet()) {
      RegionGroupStatistics previousStatistics = regionGroupStatisticsEntry.getValue().getLeft();
      RegionGroupStatistics currentStatistics = regionGroupStatisticsEntry.getValue().getRight();
      LOGGER.info(
          "[RegionGroupStatistics]\t RegionGroup {}: {} -> {}",
          regionGroupStatisticsEntry.getKey(),
          previousStatistics == null ? null : previousStatistics.getRegionGroupStatus(),
          currentStatistics == null ? null : currentStatistics.getRegionGroupStatus());

      List<Integer> leftIds =
          previousStatistics == null ? Collections.emptyList() : previousStatistics.getRegionIds();
      List<Integer> rightIds =
          currentStatistics == null ? Collections.emptyList() : currentStatistics.getRegionIds();
      for (Integer leftId : leftIds) {
        if (rightIds.contains(leftId)) {
          LOGGER.info(
              "[RegionGroupStatistics]\t Region in DataNode {}: {} -> {}",
              leftId,
              previousStatistics.getRegionStatus(leftId),
              currentStatistics.getRegionStatus(leftId));
        } else {
          LOGGER.info(
              "[RegionGroupStatistics]\t Region in DataNode {}: {} -> null",
              leftId,
              previousStatistics.getRegionStatus(leftId));
        }
      }
      for (Integer rightId : rightIds) {
        if (!leftIds.contains(rightId)) {
          LOGGER.info(
              "[RegionGroupStatistics]\t Region in DataNode {}: null -> {}",
              rightId,
              currentStatistics.getRegionStatus(rightId));
        }
      }
    }
  }

  public synchronized void checkAndBroadcastConsensusGroupStatisticsChangeEventIfNecessary() {
    Map<TConsensusGroupId, ConsensusGroupStatistics> currentConsensusGroupStatisticsMap =
        loadCache.getCurrentConsensusGroupStatisticsMap();
    Map<TConsensusGroupId, Pair<ConsensusGroupStatistics, ConsensusGroupStatistics>>
        differentConsensusGroupStatisticsMap = new TreeMap<>();
    currentConsensusGroupStatisticsMap.forEach(
        (consensusGroupId, currentConsensusGroupStatistics) -> {
          ConsensusGroupStatistics previousConsensusGroupStatistics =
              previousConsensusGroupStatisticsMap.get(consensusGroupId);
          if (previousConsensusGroupStatistics == null
              || (currentConsensusGroupStatistics.isNewerThan(previousConsensusGroupStatistics)
                  && !currentConsensusGroupStatistics.equals(previousConsensusGroupStatistics))) {
            differentConsensusGroupStatisticsMap.put(
                consensusGroupId,
                new Pair<>(previousConsensusGroupStatistics, currentConsensusGroupStatistics));
            previousConsensusGroupStatisticsMap.put(
                consensusGroupId, currentConsensusGroupStatistics);
          }
        });
    previousConsensusGroupStatisticsMap.forEach(
        (consensusGroupId, previousConsensusGroupStatistics) -> {
          if (!currentConsensusGroupStatisticsMap.containsKey(consensusGroupId)) {
            differentConsensusGroupStatisticsMap.put(
                consensusGroupId, new Pair<>(previousConsensusGroupStatistics, null));
          }
        });
    previousConsensusGroupStatisticsMap
        .keySet()
        .retainAll(currentConsensusGroupStatisticsMap.keySet());
    if (!differentConsensusGroupStatisticsMap.isEmpty()) {
      eventPublisher.post(
          new ConsensusGroupStatisticsChangeEvent(differentConsensusGroupStatisticsMap));
      recordConsensusGroupStatistics(differentConsensusGroupStatisticsMap);
    }
  }

  private void recordConsensusGroupStatistics(
      Map<TConsensusGroupId, Pair<ConsensusGroupStatistics, ConsensusGroupStatistics>>
          differentConsensusGroupStatisticsMap) {
    LOGGER.info("[ConsensusGroupStatistics] ConsensusGroupStatisticsMap: ");
    for (Map.Entry<TConsensusGroupId, Pair<ConsensusGroupStatistics, ConsensusGroupStatistics>>
        consensusGroupStatisticsEntry : differentConsensusGroupStatisticsMap.entrySet()) {
      if (!Objects.equals(
          consensusGroupStatisticsEntry.getValue().getRight(),
          consensusGroupStatisticsEntry.getValue().getLeft())) {
        LOGGER.info(
            "[ConsensusGroupStatistics]\t {}: {} -> {}",
            consensusGroupStatisticsEntry.getKey(),
            consensusGroupStatisticsEntry.getValue().getLeft(),
            consensusGroupStatisticsEntry.getValue().getRight());
      }
    }
  }

  @TestOnly
  public EventBus getEventPublisher() {
    return eventPublisher;
  }
}
