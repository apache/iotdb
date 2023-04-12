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
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.commons.partition.DataPartitionTable;
import org.apache.iotdb.commons.partition.SchemaPartitionTable;
import org.apache.iotdb.confignode.consensus.request.write.region.CreateRegionGroupsPlan;
import org.apache.iotdb.confignode.exception.DatabaseNotExistsException;
import org.apache.iotdb.confignode.exception.NoAvailableRegionGroupException;
import org.apache.iotdb.confignode.exception.NotEnoughDataNodeException;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.load.balancer.PartitionBalancer;
import org.apache.iotdb.confignode.manager.load.balancer.RegionBalancer;
import org.apache.iotdb.confignode.manager.load.balancer.RouteBalancer;
import org.apache.iotdb.confignode.manager.load.heartbeat.HeartbeatService;
import org.apache.iotdb.confignode.manager.load.statistics.StatisticsService;
import org.apache.iotdb.confignode.rpc.thrift.TTimeSlotList;

import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.EventBus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The LoadManager at ConfigNodeGroup-Leader is active. It proactively implements the cluster
 * dynamic load balancing policy and passively accepts the PartitionTable expansion request.
 */
public class LoadManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadManager.class);

  private final IManager configManager;

  /** Balancers */
  private final RegionBalancer regionBalancer;

  private final PartitionBalancer partitionBalancer;
  private final RouteBalancer routeBalancer;

  /** Cluster load services */
  private final LoadCache loadCache;

  private final HeartbeatService heartbeatService;
  private final StatisticsService statisticsService;

  private final EventBus eventBus =
      new AsyncEventBus("LoadManager-EventBus", Executors.newFixedThreadPool(5));

  public LoadManager(IManager configManager) {
    this.configManager = configManager;

    this.regionBalancer = new RegionBalancer(configManager);
    this.partitionBalancer = new PartitionBalancer(configManager);
    this.routeBalancer = new RouteBalancer(configManager);

    this.loadCache = new LoadCache();
    this.heartbeatService = new HeartbeatService(configManager, loadCache);
    this.statisticsService = new StatisticsService(configManager, loadCache, eventBus);

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

  public void startLoadServices() {
    loadCache.initHeartbeatCache(configManager);
    routeBalancer.initRegionRouteMap();
    heartbeatService.startHeartbeatService();
    statisticsService.startLoadStatisticsService();
  }

  public void stopLoadServices() {
    heartbeatService.stopHeartbeatService();
    statisticsService.stopLoadStatisticsService();
    loadCache.clearHeartbeatCache();
  }

  public RouteBalancer getRouteBalancer() {
    return routeBalancer;
  }
}
