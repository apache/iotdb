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
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.cluster.NodeType;
import org.apache.iotdb.commons.cluster.RegionStatus;
import org.apache.iotdb.commons.partition.DataPartitionTable;
import org.apache.iotdb.commons.partition.SchemaPartitionTable;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.consensus.request.write.region.CreateRegionGroupsPlan;
import org.apache.iotdb.confignode.exception.DatabaseNotExistsException;
import org.apache.iotdb.confignode.exception.NoAvailableRegionGroupException;
import org.apache.iotdb.confignode.exception.NotEnoughDataNodeException;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.load.balancer.PartitionBalancer;
import org.apache.iotdb.confignode.manager.load.balancer.RegionBalancer;
import org.apache.iotdb.confignode.manager.load.balancer.RouteBalancer;
import org.apache.iotdb.confignode.manager.load.cache.LoadCache;
import org.apache.iotdb.confignode.manager.load.cache.consensus.ConsensusGroupHeartbeatSample;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeHeartbeatSample;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionHeartbeatSample;
import org.apache.iotdb.confignode.manager.load.service.EventService;
import org.apache.iotdb.confignode.manager.load.service.HeartbeatService;
import org.apache.iotdb.confignode.manager.load.service.StatisticsService;
import org.apache.iotdb.confignode.manager.partition.RegionGroupStatus;
import org.apache.iotdb.confignode.rpc.thrift.TTimeSlotList;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The {@link LoadManager} at ConfigNodeGroup-Leader is active. It proactively implements the
 * cluster dynamic load balancing policy and passively accepts the PartitionTable expansion request.
 */
public class LoadManager {

  protected final IManager configManager;

  /** Balancers. */
  private final RegionBalancer regionBalancer;

  private final PartitionBalancer partitionBalancer;
  private final RouteBalancer routeBalancer;

  /** Cluster load services. */
  protected final LoadCache loadCache;

  protected HeartbeatService heartbeatService;
  private final StatisticsService statisticsService;
  private final EventService eventService;

  public LoadManager(IManager configManager) {
    this.configManager = configManager;

    this.regionBalancer = new RegionBalancer(configManager);
    this.partitionBalancer = new PartitionBalancer(configManager);
    this.routeBalancer = new RouteBalancer(configManager);

    this.loadCache = new LoadCache();
    setHeartbeatService(configManager, loadCache);
    this.statisticsService = new StatisticsService(loadCache);
    this.eventService = new EventService(configManager, loadCache, routeBalancer);
  }

  protected void setHeartbeatService(IManager configManager, LoadCache loadCache) {
    this.heartbeatService = new HeartbeatService(configManager, loadCache);
  }

  /**
   * Generate an optimal CreateRegionGroupsPlan.
   *
   * @param allotmentMap Map<DatabaseName, Region allotment>
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
   * Allocate SchemaPartitions.
   *
   * @param unassignedSchemaPartitionSlotsMap SchemaPartitionSlots that should be assigned
   * @return Map<DatabaseName, SchemaPartitionTable>, the allocating result
   */
  public Map<String, SchemaPartitionTable> allocateSchemaPartition(
      Map<String, List<TSeriesPartitionSlot>> unassignedSchemaPartitionSlotsMap)
      throws NoAvailableRegionGroupException {
    return partitionBalancer.allocateSchemaPartition(unassignedSchemaPartitionSlotsMap);
  }

  /**
   * Allocate DataPartitions.
   *
   * @param unassignedDataPartitionSlotsMap DataPartitionSlots that should be assigned
   * @throws DatabaseNotExistsException If some specific Databases don't exist
   * @throws NoAvailableRegionGroupException If there are no available RegionGroups
   * @return Map<DatabaseName, DataPartitionTable>, the allocating result
   */
  public Map<String, DataPartitionTable> allocateDataPartition(
      Map<String, Map<TSeriesPartitionSlot, TTimeSlotList>> unassignedDataPartitionSlotsMap)
      throws DatabaseNotExistsException, NoAvailableRegionGroupException {
    return partitionBalancer.allocateDataPartition(unassignedDataPartitionSlotsMap);
  }

  /**
   * Re-balance the DataPartitionPolicyTable.
   *
   * @param database Database name
   */
  public void reBalanceDataPartitionPolicy(String database) {
    partitionBalancer.reBalanceDataPartitionPolicy(database);
  }

  public void startLoadServices() {
    loadCache.initHeartbeatCache(configManager);
    heartbeatService.startHeartbeatService();
    statisticsService.startLoadStatisticsService();
    eventService.startEventService();
    partitionBalancer.setupPartitionBalancer();
  }

  public void stopLoadServices() {
    heartbeatService.stopHeartbeatService();
    statisticsService.stopLoadStatisticsService();
    eventService.stopEventService();
    loadCache.clearHeartbeatCache();
    partitionBalancer.clearPartitionBalancer();
    routeBalancer.clearRegionPriority();
  }

  public void clearDataPartitionPolicyTable(String database) {
    partitionBalancer.clearDataPartitionPolicyTable(database);
  }

  /**
   * Safely get NodeStatus by NodeId.
   *
   * @param nodeId The specified NodeId
   * @return NodeStatus of the specified Node. Unknown if cache doesn't exist.
   */
  public NodeStatus getNodeStatus(int nodeId) {
    return loadCache.getNodeStatus(nodeId);
  }

  /**
   * Safely get the specified Node's current status with reason.
   *
   * @param nodeId The specified NodeId
   * @return The specified Node's current status if the nodeCache contains it, Unknown otherwise
   */
  public String getNodeStatusWithReason(int nodeId) {
    return loadCache.getNodeStatusWithReason(nodeId);
  }

  /**
   * Get all Node's current status with reason.
   *
   * @return Map<NodeId, NodeStatus with reason>
   */
  public Map<Integer, String> getNodeStatusWithReason() {
    return loadCache.getNodeStatusWithReason();
  }

  /**
   * Filter ConfigNodes through the specified NodeStatus.
   *
   * @param status The specified NodeStatus
   * @return Filtered ConfigNodes with the specified NodeStatus
   */
  public List<Integer> filterConfigNodeThroughStatus(NodeStatus... status) {
    return loadCache.filterConfigNodeThroughStatus(status);
  }

  /**
   * Filter DataNodes through the specified NodeStatus.
   *
   * @param status The specified NodeStatus
   * @return Filtered DataNodes with the specified NodeStatus
   */
  public List<Integer> filterDataNodeThroughStatus(NodeStatus... status) {
    return loadCache.filterDataNodeThroughStatus(status);
  }

  /**
   * Get the free disk space of the specified DataNode.
   *
   * @param dataNodeId The index of the specified DataNode
   * @return The free disk space that sample through heartbeat, 0 if no heartbeat received
   */
  public double getFreeDiskSpace(int dataNodeId) {
    return loadCache.getFreeDiskSpace(dataNodeId);
  }

  /**
   * Get the loadScore of each DataNode.
   *
   * @return Map<DataNodeId, loadScore>
   */
  public Map<Integer, Long> getAllDataNodeLoadScores() {
    return loadCache.getAllDataNodeLoadScores();
  }

  /**
   * Get the lowest loadScore DataNode.
   *
   * @return The index of the lowest loadScore DataNode. -1 if no DataNode heartbeat received.
   */
  public int getLowestLoadDataNode() {
    return loadCache.getLowestLoadDataNode();
  }

  /**
   * Get the lowest loadScore DataNode from the specified DataNodes.
   *
   * @param dataNodeIds The specified DataNodes
   * @return The index of the lowest loadScore DataNode. -1 if no DataNode heartbeat received.
   */
  public int getLowestLoadDataNode(List<Integer> dataNodeIds) {
    return loadCache.getLowestLoadDataNode(dataNodeIds);
  }

  /**
   * Force update the specified Node's cache, update statistics and broadcast statistics change
   * event if necessary.
   *
   * @param nodeType Specified NodeType
   * @param nodeId Specified NodeId
   * @param heartbeatSample Specified NodeHeartbeatSample
   */
  public void forceUpdateNodeCache(
      NodeType nodeType, int nodeId, NodeHeartbeatSample heartbeatSample) {
    switch (nodeType) {
      case ConfigNode:
        loadCache.cacheConfigNodeHeartbeatSample(nodeId, heartbeatSample);
        break;
      case DataNode:
        loadCache.cacheDataNodeHeartbeatSample(nodeId, heartbeatSample);
        break;
      case AINode:
        loadCache.cacheAINodeHeartbeatSample(nodeId, heartbeatSample);
        break;
      default:
        break;
    }
    loadCache.updateNodeStatistics(true);
    eventService.checkAndBroadcastNodeStatisticsChangeEventIfNecessary();
  }

  /**
   * Remove the NodeHeartbeatCache of the specified Node, update statistics and broadcast statistics
   * change event if necessary.
   *
   * @param nodeId the index of the specified Node
   */
  public void removeNodeCache(int nodeId) {
    loadCache.removeNodeCache(nodeId);
    loadCache.updateNodeStatistics(true);
    eventService.checkAndBroadcastNodeStatisticsChangeEventIfNecessary();
  }

  /**
   * Safely get RegionStatus.
   *
   * @param consensusGroupId Specified RegionGroupId
   * @param dataNodeId Specified RegionReplicaId
   * @return Corresponding RegionStatus if cache exists, Unknown otherwise
   */
  public RegionStatus getRegionStatus(TConsensusGroupId consensusGroupId, int dataNodeId) {
    return loadCache.getRegionStatus(consensusGroupId, dataNodeId);
  }

  /**
   * Safely get RegionGroupStatus.
   *
   * @param consensusGroupId Specified RegionGroupId
   * @return Corresponding RegionGroupStatus if cache exists, Disabled otherwise
   */
  public RegionGroupStatus getRegionGroupStatus(TConsensusGroupId consensusGroupId) {
    return loadCache.getRegionGroupStatus(consensusGroupId);
  }

  /**
   * Safely get RegionGroupStatus.
   *
   * @param consensusGroupIds Specified RegionGroupIds
   * @return Corresponding RegionGroupStatus if cache exists, Disabled otherwise
   */
  public Map<TConsensusGroupId, RegionGroupStatus> getRegionGroupStatus(
      List<TConsensusGroupId> consensusGroupIds) {
    return loadCache.getRegionGroupStatus(consensusGroupIds);
  }

  /**
   * Filter the RegionGroups through the RegionGroupStatus.
   *
   * @param status The specified RegionGroupStatus
   * @return Filtered RegionGroups with the specified RegionGroupStatus
   */
  public List<TConsensusGroupId> filterRegionGroupThroughStatus(RegionGroupStatus... status) {
    return loadCache.filterRegionGroupThroughStatus(status);
  }

  /**
   * Count the number of cluster Regions with specified RegionStatus.
   *
   * @param type The specified RegionGroupType
   * @param status The specified statues
   * @return The number of cluster Regions with specified RegionStatus
   */
  public int countRegionWithSpecifiedStatus(TConsensusGroupType type, RegionStatus... status) {
    return loadCache.countRegionWithSpecifiedStatus(type, status);
  }

  /**
   * Force update the specified RegionGroups' cache, update statistics and broadcast statistics
   * change event if necessary.
   *
   * @param heartbeatSampleMap Map<RegionGroupId, Map<DataNodeId, RegionHeartbeatSample>>
   */
  public void forceUpdateRegionGroupCache(
      Map<TConsensusGroupId, Map<Integer, RegionHeartbeatSample>> heartbeatSampleMap) {
    heartbeatSampleMap.forEach(
        (regionGroupId, regionHeartbeatSampleMap) ->
            regionHeartbeatSampleMap.forEach(
                (dataNodeId, regionHeartbeatSample) ->
                    loadCache.cacheRegionHeartbeatSample(
                        regionGroupId, dataNodeId, regionHeartbeatSample, true)));
    loadCache.updateRegionGroupStatistics();
    eventService.checkAndBroadcastRegionGroupStatisticsChangeEventIfNecessary();
  }

  /**
   * Force update the specified Region's cache, update statistics and broadcast statistics change
   * event if necessary.
   *
   * @param regionGroupId The specified RegionGroup
   * @param dataNodeId The DataNodeId where the specified Region is located
   * @param regionStatus The specified RegionStatus
   */
  public void forceUpdateRegionCache(
      TConsensusGroupId regionGroupId, int dataNodeId, RegionStatus regionStatus) {
    loadCache.cacheRegionHeartbeatSample(
        regionGroupId,
        dataNodeId,
        new RegionHeartbeatSample(System.nanoTime(), regionStatus),
        true);
    loadCache.updateRegionGroupStatistics();
    eventService.checkAndBroadcastRegionGroupStatisticsChangeEventIfNecessary();
  }

  /**
   * Remove the cache of the specified Region in the specified RegionGroup, update statistics and
   * broadcast statistics change event if necessary.
   *
   * @param regionGroupId the specified RegionGroup
   * @param dataNodeId the specified DataNode
   */
  public void removeRegionCache(TConsensusGroupId regionGroupId, int dataNodeId) {
    loadCache.removeRegionCache(regionGroupId, dataNodeId);
    loadCache.updateRegionGroupStatistics();
    eventService.checkAndBroadcastRegionGroupStatisticsChangeEventIfNecessary();
  }

  /**
   * Remove the specified RegionGroup's related cache, update statistics and broadcast statistics
   * change event if necessary.
   *
   * @param consensusGroupId The specified RegionGroup
   */
  public void removeRegionGroupRelatedCache(TConsensusGroupId consensusGroupId) {
    loadCache.removeRegionGroupCache(consensusGroupId);
    routeBalancer.removeRegionPriority(consensusGroupId);
    loadCache.updateRegionGroupStatistics();
    loadCache.updateConsensusGroupStatistics();
    eventService.checkAndBroadcastRegionGroupStatisticsChangeEventIfNecessary();
    eventService.checkAndBroadcastConsensusGroupStatisticsChangeEventIfNecessary();
  }

  /**
   * Get the latest RegionLeaderMap.
   *
   * @return Map<RegionGroupId, leaderId>
   */
  public Map<TConsensusGroupId, Integer> getRegionLeaderMap() {
    return loadCache.getRegionLeaderMap();
  }

  /**
   * Get the latest RegionPriorityMap.
   *
   * @return Map<RegionGroupId, RegionPriority>.
   */
  public Map<TConsensusGroupId, TRegionReplicaSet> getRegionPriorityMap() {
    return routeBalancer.getRegionPriorityMap();
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
    getRegionLeaderMap()
        .forEach(
            ((consensusGroupId, leaderId) -> {
              if (dataNodeId == leaderId && type.equals(consensusGroupId.getType())) {
                result.getAndIncrement();
              }
            }));
    return result.get();
  }

  /**
   * Wait for the specified RegionGroups to finish leader election and priority update.
   *
   * @param regionGroupIds Specified RegionGroupIds
   */
  public void waitForRegionGroupReady(List<TConsensusGroupId> regionGroupIds) {
    loadCache.waitForLeaderElection(regionGroupIds);
    routeBalancer.waitForPriorityUpdate(regionGroupIds);
  }

  /**
   * Force update the specified ConsensusGroups' cache.
   *
   * @param heartbeatSampleMap Map<RegionGroupId, ConsensusGroupHeartbeatSample>
   */
  public void forceUpdateConsensusGroupCache(
      Map<TConsensusGroupId, ConsensusGroupHeartbeatSample> heartbeatSampleMap) {
    heartbeatSampleMap.forEach(loadCache::cacheConsensusSample);
    loadCache.updateConsensusGroupStatistics();
    eventService.checkAndBroadcastConsensusGroupStatisticsChangeEventIfNecessary();
  }

  public LoadCache getLoadCache() {
    return loadCache;
  }

  public RouteBalancer getRouteBalancer() {
    return routeBalancer;
  }

  @TestOnly
  public EventService getEventService() {
    return eventService;
  }
}
