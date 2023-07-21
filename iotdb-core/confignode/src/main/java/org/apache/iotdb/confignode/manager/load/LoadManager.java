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
import org.apache.iotdb.confignode.consensus.request.write.region.CreateRegionGroupsPlan;
import org.apache.iotdb.confignode.exception.DatabaseNotExistsException;
import org.apache.iotdb.confignode.exception.NoAvailableRegionGroupException;
import org.apache.iotdb.confignode.exception.NotEnoughDataNodeException;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.load.balancer.PartitionBalancer;
import org.apache.iotdb.confignode.manager.load.balancer.RegionBalancer;
import org.apache.iotdb.confignode.manager.load.balancer.RouteBalancer;
import org.apache.iotdb.confignode.manager.load.cache.LoadCache;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeHeartbeatSample;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionHeartbeatSample;
import org.apache.iotdb.confignode.manager.load.service.HeartbeatService;
import org.apache.iotdb.confignode.manager.load.service.StatisticsService;
import org.apache.iotdb.confignode.manager.partition.RegionGroupStatus;
import org.apache.iotdb.confignode.rpc.thrift.TTimeSlotList;

import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.EventBus;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The LoadManager at ConfigNodeGroup-Leader is active. It proactively implements the cluster
 * dynamic load balancing policy and passively accepts the PartitionTable expansion request.
 */
public class LoadManager {

  private final IManager configManager;

  /** Balancers. */
  private final RegionBalancer regionBalancer;

  private final PartitionBalancer partitionBalancer;
  private final RouteBalancer routeBalancer;

  /** Cluster load services. */
  private final LoadCache loadCache;

  private final HeartbeatService heartbeatService;
  private final StatisticsService statisticsService;

  private final EventBus loadPublisher =
      new AsyncEventBus("Cluster-LoadPublisher-Thread", Executors.newFixedThreadPool(5));

  public LoadManager(IManager configManager) {
    this.configManager = configManager;

    this.regionBalancer = new RegionBalancer(configManager);
    this.partitionBalancer = new PartitionBalancer(configManager);
    this.routeBalancer = new RouteBalancer(configManager);

    this.loadCache = new LoadCache();
    this.heartbeatService = new HeartbeatService(configManager, loadCache);
    this.statisticsService =
        new StatisticsService(configManager, routeBalancer, loadCache, loadPublisher);

    loadPublisher.register(statisticsService);
    loadPublisher.register(configManager.getPipeManager().getPipeRuntimeCoordinator());
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
   * @return Map<DatabaseName, DataPartitionTable>, the allocating result
   */
  public Map<String, DataPartitionTable> allocateDataPartition(
      Map<String, Map<TSeriesPartitionSlot, TTimeSlotList>> unassignedDataPartitionSlotsMap)
      throws NoAvailableRegionGroupException {
    return partitionBalancer.allocateDataPartition(unassignedDataPartitionSlotsMap);
  }

  /**
   * Re-balance runtime status cached in the PartitionBalancer. This method may shift the
   * currentTimePartition or update the DataAllotTable.
   */
  public void reBalancePartitionPolicyIfNecessary(
      Map<String, DataPartitionTable> assignedDataPartition) {
    partitionBalancer.reBalanceDataPartitionPolicyIfNecessary(assignedDataPartition);
  }

  public void updateDataAllotTable(String database) {
    partitionBalancer.updateDataAllotTable(database);
  }

  public void broadcastLatestRegionRouteMap() {
    statisticsService.broadcastLatestRegionRouteMap();
  }

  public void startLoadServices() {
    loadCache.initHeartbeatCache(configManager);
    heartbeatService.startHeartbeatService();
    statisticsService.startLoadStatisticsService();
    partitionBalancer.setupPartitionBalancer();
  }

  public void stopLoadServices() {
    heartbeatService.stopHeartbeatService();
    statisticsService.stopLoadStatisticsService();
    loadCache.clearHeartbeatCache();
    partitionBalancer.clearPartitionBalancer();
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
   * Force update the specified Node's cache.
   *
   * @param nodeType Specified NodeType
   * @param nodeId Specified NodeId
   * @param heartbeatSample Specified NodeHeartbeatSample
   */
  public void forceUpdateNodeCache(
      NodeType nodeType, int nodeId, NodeHeartbeatSample heartbeatSample) {
    loadCache.forceUpdateNodeCache(nodeType, nodeId, heartbeatSample);
  }

  /** Remove the specified Node's cache. */
  public void removeNodeCache(int nodeId) {
    loadCache.removeNodeCache(nodeId);
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
   * Force update the specified RegionGroup's cache.
   *
   * @param regionGroupId Specified RegionGroupId
   * @param heartbeatSampleMap Specified RegionHeartbeatSampleMap
   */
  public void forceUpdateRegionGroupCache(
      TConsensusGroupId regionGroupId, Map<Integer, RegionHeartbeatSample> heartbeatSampleMap) {
    loadCache.forceUpdateRegionGroupCache(regionGroupId, heartbeatSampleMap);
  }

  /** Remove the specified RegionGroup's cache. */
  public void removeRegionGroupCache(TConsensusGroupId consensusGroupId) {
    loadCache.removeRegionGroupCache(consensusGroupId);
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
    return loadCache.getRegionPriorityMap();
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
   * Wait for the specified RegionGroups to finish leader election
   *
   * @param regionGroupIds Specified RegionGroupIds
   */
  public void waitForLeaderElection(List<TConsensusGroupId> regionGroupIds) {
    loadCache.waitForLeaderElection(regionGroupIds);
  }

  /**
   * Force update the specified RegionGroup's leader.
   *
   * @param regionGroupId Specified RegionGroupId
   * @param leaderId Leader DataNodeId
   */
  public void forceUpdateRegionLeader(TConsensusGroupId regionGroupId, int leaderId) {
    loadCache.forceUpdateRegionLeader(regionGroupId, leaderId);
  }

  /**
   * Force update the specified RegionGroup's priority.
   *
   * @param regionGroupId Specified RegionGroupId
   * @param regionPriority Region route priority
   */
  public void forceUpdateRegionPriority(
      TConsensusGroupId regionGroupId, TRegionReplicaSet regionPriority) {
    loadCache.forceUpdateRegionPriority(regionGroupId, regionPriority);
  }

  /**
   * Remove the specified RegionGroup's route cache.
   *
   * @param regionGroupId Specified RegionGroupId
   */
  public void removeRegionRouteCache(TConsensusGroupId regionGroupId) {
    loadCache.removeRegionRouteCache(regionGroupId);
  }
}
