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

package org.apache.iotdb.confignode.manager.load.cache;

import org.apache.iotdb.common.rpc.thrift.TAINodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.cluster.NodeType;
import org.apache.iotdb.commons.cluster.RegionStatus;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.ProcedureManager;
import org.apache.iotdb.confignode.manager.load.cache.consensus.ConsensusGroupCache;
import org.apache.iotdb.confignode.manager.load.cache.consensus.ConsensusGroupHeartbeatSample;
import org.apache.iotdb.confignode.manager.load.cache.consensus.ConsensusGroupStatistics;
import org.apache.iotdb.confignode.manager.load.cache.node.AINodeHeartbeatCache;
import org.apache.iotdb.confignode.manager.load.cache.node.BaseNodeCache;
import org.apache.iotdb.confignode.manager.load.cache.node.ConfigNodeHeartbeatCache;
import org.apache.iotdb.confignode.manager.load.cache.node.DataNodeHeartbeatCache;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeHeartbeatSample;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeStatistics;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionGroupCache;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionGroupStatistics;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionHeartbeatSample;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionStatistics;
import org.apache.iotdb.confignode.manager.partition.RegionGroupStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/** Maintain all kinds of heartbeat samples and statistics. */
public class LoadCache {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadCache.class);

  private static final long WAIT_LEADER_INTERVAL = 10;
  private static final long LEADER_ELECTION_WAITING_TIMEOUT =
      Math.max(
          ProcedureManager.PROCEDURE_WAIT_TIME_OUT - TimeUnit.SECONDS.toMillis(2),
          TimeUnit.SECONDS.toMillis(10));

  // Map<NodeId, is heartbeat processing>
  // False indicates there is no processing heartbeat request, true otherwise
  private final Map<Integer, AtomicBoolean> heartbeatProcessingMap;
  // Map<NodeId, BaseNodeCache>
  private final Map<Integer, BaseNodeCache> nodeCacheMap;
  // Map<RegionGroupId, RegionGroupCache>
  private final Map<TConsensusGroupId, RegionGroupCache> regionGroupCacheMap;
  // Map<RegionGroupId, ConsensusGroupCache>
  private final Map<TConsensusGroupId, ConsensusGroupCache> consensusGroupCacheMap;
  // Map<DataNodeId, confirmedConfigNodes>
  private final Map<Integer, Set<TEndPoint>> confirmedConfigNodeMap;

  public LoadCache() {
    this.nodeCacheMap = new ConcurrentHashMap<>();
    this.heartbeatProcessingMap = new ConcurrentHashMap<>();
    this.regionGroupCacheMap = new ConcurrentHashMap<>();
    this.consensusGroupCacheMap = new ConcurrentHashMap<>();
    this.confirmedConfigNodeMap = new ConcurrentHashMap<>();
  }

  public void initHeartbeatCache(IManager configManager) {
    initNodeHeartbeatCache(
        configManager.getNodeManager().getRegisteredConfigNodes(),
        configManager.getNodeManager().getRegisteredDataNodes(),
        configManager.getNodeManager().getRegisteredAINodes());
    initRegionGroupHeartbeatCache(
        configManager.getClusterSchemaManager().getDatabaseNames(null).stream()
            .collect(
                Collectors.toMap(
                    database -> database,
                    database -> configManager.getPartitionManager().getAllReplicaSets(database))));
  }

  /** Initialize the nodeCacheMap when the ConfigNode-Leader is switched. */
  private void initNodeHeartbeatCache(
      List<TConfigNodeLocation> registeredConfigNodes,
      List<TDataNodeConfiguration> registeredDataNodes,
      List<TAINodeConfiguration> registeredAINodes) {

    final int CURRENT_NODE_ID = ConfigNodeHeartbeatCache.CURRENT_NODE_ID;
    nodeCacheMap.clear();
    heartbeatProcessingMap.clear();

    // Init ConfigNodeHeartbeatCache
    registeredConfigNodes.forEach(
        configNodeLocation -> {
          int configNodeId = configNodeLocation.getConfigNodeId();
          if (configNodeId != CURRENT_NODE_ID) {
            createNodeHeartbeatCache(NodeType.ConfigNode, configNodeId);
          }
        });
    // Force set itself and never update
    nodeCacheMap.put(
        ConfigNodeHeartbeatCache.CURRENT_NODE_ID,
        new ConfigNodeHeartbeatCache(
            CURRENT_NODE_ID, ConfigNodeHeartbeatCache.CURRENT_NODE_STATISTICS));

    // Init DataNodeHeartbeatCache
    registeredDataNodes.forEach(
        dataNodeConfiguration -> {
          int dataNodeId = dataNodeConfiguration.getLocation().getDataNodeId();
          createNodeHeartbeatCache(NodeType.DataNode, dataNodeId);
        });

    // Init AiNodeHeartbeatCache
    registeredAINodes.forEach(
        aiNodeConfiguration -> {
          int aiNodeId = aiNodeConfiguration.getLocation().getAiNodeId();
          createNodeHeartbeatCache(NodeType.AINode, aiNodeId);
        });
  }

  /**
   * Initialize the regionGroupCacheMap and regionRouteCacheMap when the ConfigNode-Leader is
   * switched.
   */
  private void initRegionGroupHeartbeatCache(
      Map<String, List<TRegionReplicaSet>> regionReplicaMap) {
    regionGroupCacheMap.clear();
    consensusGroupCacheMap.clear();
    regionReplicaMap.forEach(
        (database, regionReplicaSets) ->
            regionReplicaSets.forEach(
                regionReplicaSet -> {
                  TConsensusGroupId regionGroupId = regionReplicaSet.getRegionId();
                  regionGroupCacheMap.put(
                      regionGroupId,
                      new RegionGroupCache(
                          database,
                          regionReplicaSet.getDataNodeLocations().stream()
                              .map(TDataNodeLocation::getDataNodeId)
                              .collect(Collectors.toSet())));
                  consensusGroupCacheMap.put(regionGroupId, new ConsensusGroupCache());
                }));
  }

  public void clearHeartbeatCache() {
    heartbeatProcessingMap.clear();
    nodeCacheMap.clear();
    regionGroupCacheMap.clear();
    consensusGroupCacheMap.clear();
  }

  /**
   * Check if the specified Node is processing heartbeat. And set the processing flag to true.
   *
   * @param nodeId The specified NodeId.
   * @return False if the previous heartbeat is completed, true otherwise.
   */
  public boolean checkAndSetHeartbeatProcessing(int nodeId) {
    return heartbeatProcessingMap
        .computeIfAbsent(nodeId, empty -> new AtomicBoolean(false))
        .getAndSet(true);
  }

  /**
   * Create a new NodeHeartbeatCache for the specified Node.
   *
   * @param nodeType The specified NodeType
   * @param nodeId The specified NodeId
   */
  public void createNodeHeartbeatCache(NodeType nodeType, int nodeId) {
    switch (nodeType) {
      case ConfigNode:
        nodeCacheMap.put(nodeId, new ConfigNodeHeartbeatCache(nodeId));
        break;
      case DataNode:
        nodeCacheMap.put(nodeId, new DataNodeHeartbeatCache(nodeId));
        break;
      case AINode:
        nodeCacheMap.put(nodeId, new AINodeHeartbeatCache(nodeId));
        break;
    }
    heartbeatProcessingMap.put(nodeId, new AtomicBoolean(false));
  }

  /**
   * Cache the latest heartbeat sample of a ConfigNode.
   *
   * @param nodeId the id of the ConfigNode
   * @param sample the latest heartbeat sample
   */
  public void cacheConfigNodeHeartbeatSample(int nodeId, NodeHeartbeatSample sample) {
    // Only cache sample when the corresponding loadCache exists
    Optional.ofNullable(nodeCacheMap.get(nodeId))
        .ifPresent(node -> node.cacheHeartbeatSample(sample));
    Optional.ofNullable(heartbeatProcessingMap.get(nodeId)).ifPresent(node -> node.set(false));
  }

  /**
   * Cache the latest heartbeat sample of a DataNode.
   *
   * @param nodeId the id of the DataNode
   * @param sample the latest heartbeat sample
   */
  public void cacheDataNodeHeartbeatSample(int nodeId, NodeHeartbeatSample sample) {
    // Only cache sample when the corresponding loadCache exists
    Optional.ofNullable(nodeCacheMap.get(nodeId))
        .ifPresent(node -> node.cacheHeartbeatSample(sample));
    Optional.ofNullable(heartbeatProcessingMap.get(nodeId)).ifPresent(node -> node.set(false));
  }

  /**
   * Cache the latest heartbeat sample of a AINode.
   *
   * @param nodeId the id of the AINode
   * @param sample the latest heartbeat sample
   */
  public void cacheAINodeHeartbeatSample(int nodeId, NodeHeartbeatSample sample) {
    nodeCacheMap
        .computeIfAbsent(nodeId, empty -> new AINodeHeartbeatCache(nodeId))
        .cacheHeartbeatSample(sample);
    Optional.ofNullable(heartbeatProcessingMap.get(nodeId)).ifPresent(node -> node.set(false));
  }

  public void resetHeartbeatProcessing(int nodeId) {
    Optional.ofNullable(heartbeatProcessingMap.get(nodeId)).ifPresent(node -> node.set(false));
  }

  /**
   * Remove the NodeHeartbeatCache of the specified Node.
   *
   * @param nodeId the index of the specified Node
   */
  public void removeNodeCache(int nodeId) {
    nodeCacheMap.remove(nodeId);
    heartbeatProcessingMap.remove(nodeId);
  }

  /**
   * Create a new RegionGroupCache and a new ConsensusGroupCache for the specified RegionGroup.
   *
   * @param database the Database where the RegionGroup belonged
   * @param regionGroupId the index of the RegionGroup
   * @param dataNodeIds the index of the DataNodes where the Regions resided
   */
  public void createRegionGroupHeartbeatCache(
      String database, TConsensusGroupId regionGroupId, Set<Integer> dataNodeIds) {
    regionGroupCacheMap.put(regionGroupId, new RegionGroupCache(database, dataNodeIds));
    consensusGroupCacheMap.put(regionGroupId, new ConsensusGroupCache());
  }

  /**
   * Create a new RegionCache for the specified Region in the specified RegionGroup.
   *
   * @param regionGroupId the index of the RegionGroup
   * @param dataNodeId the index of the DataNode where the Region resides
   */
  public void createRegionCache(TConsensusGroupId regionGroupId, int dataNodeId) {
    Optional.ofNullable(regionGroupCacheMap.get(regionGroupId))
        .ifPresent(cache -> cache.createRegionCache(dataNodeId));
  }

  /**
   * Cache the latest heartbeat sample of a RegionGroup.
   *
   * @param regionGroupId the id of the RegionGroup
   * @param nodeId the id of the DataNode where specified Region resides
   * @param sample the latest heartbeat sample
   */
  public void cacheRegionHeartbeatSample(
      TConsensusGroupId regionGroupId,
      int nodeId,
      RegionHeartbeatSample sample,
      boolean overwrite) {
    // Only cache sample when the corresponding loadCache exists
    Optional.ofNullable(regionGroupCacheMap.get(regionGroupId))
        .ifPresent(group -> group.cacheHeartbeatSample(nodeId, sample, overwrite));
  }

  public RegionStatus getRegionCacheLastSampleStatus(TConsensusGroupId regionGroupId, int nodeId) {
    return Optional.ofNullable(regionGroupCacheMap.get(regionGroupId))
        .map(regionGroupCache -> regionGroupCache.getRegionCache(nodeId))
        .map(regionCache -> (RegionHeartbeatSample) regionCache.getLastSample())
        .map(RegionHeartbeatSample::getStatus)
        .orElse(RegionStatus.Unknown);
  }

  /**
   * Remove the cache of the specified Region in the specified RegionGroup.
   *
   * @param regionGroupId the specified RegionGroup
   * @param dataNodeId the specified DataNode
   */
  public void removeRegionCache(TConsensusGroupId regionGroupId, int dataNodeId) {
    Optional.ofNullable(regionGroupCacheMap.get(regionGroupId))
        .ifPresent(cache -> cache.removeRegionCache(dataNodeId));
  }

  /**
   * Cache the latest leader of a RegionGroup.
   *
   * @param regionGroupId the id of the RegionGroup
   * @param sample the latest heartbeat sample
   */
  public void cacheConsensusSample(
      TConsensusGroupId regionGroupId, ConsensusGroupHeartbeatSample sample) {
    // Only cache sample when the corresponding loadCache exists
    Optional.ofNullable(consensusGroupCacheMap.get(regionGroupId))
        .ifPresent(group -> group.cacheHeartbeatSample(sample));
  }

  /** Update the NodeStatistics of all Nodes. */
  public void updateNodeStatistics(boolean forceUpdate) {
    nodeCacheMap
        .values()
        .forEach(baseNodeCache -> baseNodeCache.updateCurrentStatistics(forceUpdate));
  }

  /** Update the RegionGroupStatistics of all RegionGroups. */
  public void updateRegionGroupStatistics() {
    regionGroupCacheMap.values().forEach(RegionGroupCache::updateCurrentStatistics);
  }

  /** Update the ConsensusGroupStatistics of all RegionGroups. */
  public void updateConsensusGroupStatistics() {
    consensusGroupCacheMap
        .values()
        .forEach(consensusGroupCache -> consensusGroupCache.updateCurrentStatistics(false));
  }

  /**
   * Get the NodeStatistics of all Nodes.
   *
   * @return a map of NodeStatistics
   */
  public Map<Integer, NodeStatistics> getCurrentNodeStatisticsMap() {
    Map<Integer, NodeStatistics> nodeStatisticsMap = new TreeMap<>();
    nodeCacheMap.forEach(
        (nodeId, nodeCache) ->
            nodeStatisticsMap.put(nodeId, (NodeStatistics) nodeCache.getCurrentStatistics()));
    return nodeStatisticsMap;
  }

  /**
   * Get the NodeStatistics of all DataNodes.
   *
   * @return a map of all DataNodes' NodeStatistics
   */
  public Map<Integer, NodeStatistics> getCurrentDataNodeStatisticsMap() {
    Map<Integer, NodeStatistics> dataNodeStatisticsMap = new TreeMap<>();
    nodeCacheMap.forEach(
        (nodeId, nodeCache) -> {
          if (nodeCache instanceof DataNodeHeartbeatCache) {
            dataNodeStatisticsMap.put(nodeId, (NodeStatistics) nodeCache.getCurrentStatistics());
          }
        });
    return dataNodeStatisticsMap;
  }

  /**
   * Get a map of cached RegionGroups of all Databases.
   *
   * @param type SchemaRegion or DataRegion
   * @return Map<Database, List<RegionGroupId>>
   */
  public Map<String, List<TConsensusGroupId>> getCurrentDatabaseRegionGroupMap(
      TConsensusGroupType type) {
    Map<String, List<TConsensusGroupId>> databaseRegionGroupMap = new TreeMap<>();
    regionGroupCacheMap.forEach(
        (regionGroupId, regionGroupCache) -> {
          if (type.equals(regionGroupId.getType())) {
            databaseRegionGroupMap
                .computeIfAbsent(regionGroupCache.getDatabase(), empty -> new ArrayList<>())
                .add(regionGroupId);
          }
        });
    return databaseRegionGroupMap;
  }

  /**
   * Get a map of cached RegionGroups
   *
   * @param type SchemaRegion or DataRegion
   * @return Map<RegionGroupId, Set<DataNodeId>>
   */
  public Map<TConsensusGroupId, Set<Integer>> getCurrentRegionLocationMap(
      TConsensusGroupType type) {
    Map<TConsensusGroupId, Set<Integer>> regionGroupIdsMap = new TreeMap<>();
    regionGroupCacheMap.forEach(
        (regionGroupId, regionGroupCache) -> {
          if (type.equals(regionGroupId.getType())) {
            regionGroupIdsMap.put(regionGroupId, regionGroupCache.getRegionLocations());
          }
        });
    return regionGroupIdsMap;
  }

  /**
   * Get the RegionGroupStatistics of all RegionGroups.
   *
   * @return a map of RegionGroupStatistics
   */
  public Map<TConsensusGroupId, RegionGroupStatistics> getCurrentRegionGroupStatisticsMap() {
    Map<TConsensusGroupId, RegionGroupStatistics> regionGroupStatisticsMap = new TreeMap<>();
    regionGroupCacheMap.forEach(
        (regionGroupId, regionGroupCache) ->
            regionGroupStatisticsMap.put(regionGroupId, regionGroupCache.getCurrentStatistics()));
    return regionGroupStatisticsMap;
  }

  /**
   * Get the RegionStatistics of all Regions.
   *
   * @param type DataRegion or SchemaRegion
   * @return a map of RegionStatistics
   */
  public Map<TConsensusGroupId, Map<Integer, RegionStatistics>> getCurrentRegionStatisticsMap(
      TConsensusGroupType type) {
    Map<TConsensusGroupId, Map<Integer, RegionStatistics>> regionStatisticsMap = new TreeMap<>();
    regionGroupCacheMap.forEach(
        (regionGroupId, regionGroupCache) -> {
          if (type.equals(regionGroupId.getType())) {
            regionStatisticsMap.put(
                regionGroupId, regionGroupCache.getCurrentStatistics().getRegionStatisticsMap());
          }
        });
    return regionStatisticsMap;
  }

  /**
   * Get the ConsensusGroupStatistics of all RegionGroups.
   *
   * @return a map of ConsensusGroupStatistics
   */
  public Map<TConsensusGroupId, ConsensusGroupStatistics> getCurrentConsensusGroupStatisticsMap() {
    Map<TConsensusGroupId, ConsensusGroupStatistics> consensusGroupStatisticsMap = new TreeMap<>();
    consensusGroupCacheMap.forEach(
        (regionGroupId, consensusGroupCache) ->
            consensusGroupStatisticsMap.put(
                regionGroupId, consensusGroupCache.getCurrentStatistics()));
    return consensusGroupStatisticsMap;
  }

  /**
   * Safely get NodeStatus by NodeId.
   *
   * @param nodeId The specified NodeId
   * @return NodeStatus of the specified Node. Unknown if cache doesn't exist.
   */
  public NodeStatus getNodeStatus(int nodeId) {
    return Optional.ofNullable(nodeCacheMap.get(nodeId))
        .map(BaseNodeCache::getNodeStatus)
        .orElse(NodeStatus.Unknown);
  }

  /**
   * Safely get the specified Node's current status with reason.
   *
   * @param nodeId The specified NodeId
   * @return The specified Node's current status if the nodeCache contains it, Unknown otherwise
   */
  public String getNodeStatusWithReason(int nodeId) {
    return Optional.ofNullable(nodeCacheMap.get(nodeId))
        .map(BaseNodeCache::getNodeStatusWithReason)
        .orElseGet(() -> NodeStatus.Unknown.getStatus() + "(NoHeartbeat)");
  }

  /**
   * Get all Node's current status with reason.
   *
   * @return Map<NodeId, NodeStatus with reason>
   */
  public Map<Integer, String> getNodeStatusWithReason() {
    return nodeCacheMap.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getNodeStatusWithReason()));
  }

  /**
   * Filter ConfigNodes through the specified NodeStatus.
   *
   * @param status The specified NodeStatus
   * @return Filtered ConfigNodes with the specified NodeStatus
   */
  public List<Integer> filterConfigNodeThroughStatus(NodeStatus... status) {
    return nodeCacheMap.entrySet().stream()
        .filter(
            nodeCacheEntry ->
                nodeCacheEntry.getValue() instanceof ConfigNodeHeartbeatCache
                    && Arrays.stream(status)
                        .anyMatch(s -> s.equals(nodeCacheEntry.getValue().getNodeStatus())))
        .map(Map.Entry::getKey)
        .collect(Collectors.toList());
  }

  /**
   * Filter DataNodes through the specified NodeStatus.
   *
   * @param status The specified NodeStatus
   * @return Filtered DataNodes with the specified NodeStatus
   */
  public List<Integer> filterDataNodeThroughStatus(NodeStatus... status) {
    return nodeCacheMap.entrySet().stream()
        .filter(
            nodeCacheEntry ->
                nodeCacheEntry.getValue() instanceof DataNodeHeartbeatCache
                    && Arrays.stream(status)
                        .anyMatch(s -> s.equals(nodeCacheEntry.getValue().getNodeStatus())))
        .map(Map.Entry::getKey)
        .collect(Collectors.toList());
  }

  /**
   * Get the free disk space of the specified DataNode.
   *
   * @param dataNodeId The index of the specified DataNode
   * @return The free disk space that sample through heartbeat, 0 if no heartbeat received
   */
  public double getFreeDiskSpace(int dataNodeId) {
    return Optional.ofNullable((DataNodeHeartbeatCache) nodeCacheMap.get(dataNodeId))
        .map(DataNodeHeartbeatCache::getFreeDiskSpace)
        .orElse(0d);
  }

  /**
   * Get the loadScore of each DataNode.
   *
   * @return Map<DataNodeId, loadScore>
   */
  public Map<Integer, Long> getAllDataNodeLoadScores() {
    Map<Integer, Long> result = new ConcurrentHashMap<>();
    nodeCacheMap.forEach(
        (dataNodeId, heartbeatCache) -> {
          if (heartbeatCache instanceof DataNodeHeartbeatCache) {
            result.put(dataNodeId, heartbeatCache.getLoadScore());
          }
        });
    return result;
  }

  /**
   * Get the lowest loadScore DataNode.
   *
   * @return The index of the lowest loadScore DataNode. -1 if no DataNode heartbeat received.
   */
  public int getLowestLoadDataNode() {
    return nodeCacheMap.entrySet().stream()
        .filter(nodeCacheEntry -> nodeCacheEntry.getValue() instanceof DataNodeHeartbeatCache)
        .min(Comparator.comparingLong(nodeCacheEntry -> nodeCacheEntry.getValue().getLoadScore()))
        .map(Map.Entry::getKey)
        .orElse(-1);
  }

  /**
   * Get the lowest loadScore DataNode from the specified DataNodes.
   *
   * @param dataNodeIds The specified DataNodes
   * @return The index of the lowest loadScore DataNode. -1 if no DataNode heartbeat received.
   */
  public int getLowestLoadDataNode(List<Integer> dataNodeIds) {
    return dataNodeIds.stream()
        .map(nodeCacheMap::get)
        .filter(Objects::nonNull)
        .min(Comparator.comparingLong(BaseNodeCache::getLoadScore))
        .map(BaseNodeCache::getNodeId)
        .orElse(-1);
  }

  /**
   * Safely get RegionStatus.
   *
   * @param consensusGroupId Specified RegionGroupId
   * @param dataNodeId Specified RegionReplicaId
   * @return Corresponding RegionStatus if cache exists, Unknown otherwise
   */
  public RegionStatus getRegionStatus(TConsensusGroupId consensusGroupId, int dataNodeId) {
    return Optional.ofNullable(regionGroupCacheMap.get(consensusGroupId))
        .map(x -> x.getCurrentStatistics().getRegionStatus(dataNodeId))
        .orElse(RegionStatus.Unknown);
  }

  /**
   * Safely get RegionGroupStatus.
   *
   * @param consensusGroupId Specified RegionGroupId
   * @return Corresponding RegionGroupStatus if cache exists, Disabled otherwise
   */
  public RegionGroupStatus getRegionGroupStatus(TConsensusGroupId consensusGroupId) {
    return Optional.ofNullable(regionGroupCacheMap.get(consensusGroupId))
        .map(x -> x.getCurrentStatistics().getRegionGroupStatus())
        .orElse(RegionGroupStatus.Disabled);
  }

  /**
   * Safely get RegionGroupStatus.
   *
   * @param consensusGroupIds Specified RegionGroupIds
   * @return Corresponding RegionGroupStatus if cache exists, Disabled otherwise
   */
  public Map<TConsensusGroupId, RegionGroupStatus> getRegionGroupStatus(
      List<TConsensusGroupId> consensusGroupIds) {
    Map<TConsensusGroupId, RegionGroupStatus> regionGroupStatusMap = new TreeMap<>();
    for (TConsensusGroupId consensusGroupId : consensusGroupIds) {
      regionGroupStatusMap.put(consensusGroupId, getRegionGroupStatus(consensusGroupId));
    }
    return regionGroupStatusMap;
  }

  /**
   * Filter the RegionGroups through the RegionGroupStatus.
   *
   * @param status The specified RegionGroupStatus
   * @return Filtered RegionGroups with the specified RegionGroupStatus
   */
  public List<TConsensusGroupId> filterRegionGroupThroughStatus(RegionGroupStatus... status) {
    return regionGroupCacheMap.entrySet().stream()
        .filter(
            regionGroupCacheEntry ->
                Arrays.stream(status)
                    .anyMatch(
                        s ->
                            s.equals(
                                regionGroupCacheEntry
                                    .getValue()
                                    .getCurrentStatistics()
                                    .getRegionGroupStatus())))
        .map(Map.Entry::getKey)
        .collect(Collectors.toList());
  }

  /**
   * Count the number of cluster Regions with specified RegionStatus.
   *
   * @param type The specified RegionGroupType
   * @param status The specified statues
   * @return The number of cluster Regions with specified RegionStatus
   */
  public int countRegionWithSpecifiedStatus(TConsensusGroupType type, RegionStatus... status) {
    AtomicInteger result = new AtomicInteger(0);
    regionGroupCacheMap.forEach(
        (regionGroupId, regionGroupCache) -> {
          if (type.equals(regionGroupId.getType())) {
            regionGroupCache
                .getCurrentStatistics()
                .getRegionStatisticsMap()
                .values()
                .forEach(
                    regionStatistics -> {
                      if (Arrays.stream(status)
                          .anyMatch(s -> s.equals(regionStatistics.getRegionStatus()))) {
                        result.getAndIncrement();
                      }
                    });
          }
        });
    return result.get();
  }

  /** Remove the specified RegionGroup's cache. */
  public void removeRegionGroupCache(TConsensusGroupId consensusGroupId) {
    regionGroupCacheMap.remove(consensusGroupId);
    consensusGroupCacheMap.remove(consensusGroupId);
  }

  /**
   * Safely get the latest RegionLeaderMap.
   *
   * @return Map<RegionGroupId, leaderId>, leaderId will be -1 if the RegionGroup has no leader yet.
   */
  public Map<TConsensusGroupId, Integer> getRegionLeaderMap() {
    Map<TConsensusGroupId, Integer> regionLeaderMap = new ConcurrentHashMap<>();
    consensusGroupCacheMap.forEach(
        (regionGroupId, consensusGroupCache) ->
            regionLeaderMap.put(regionGroupId, consensusGroupCache.getLeaderId()));
    return regionLeaderMap;
  }

  /**
   * Safely get the latest RegionLeaderMap.
   *
   * @param consensusGroupType DataRegion or SchemaRegion
   * @return Map<RegionGroupId, leaderId>, leaderId will be -1 if the RegionGroup has no leader yet.
   */
  public Map<TConsensusGroupId, Integer> getRegionLeaderMap(
      TConsensusGroupType consensusGroupType) {
    Map<TConsensusGroupId, Integer> regionLeaderMap = new ConcurrentHashMap<>();
    consensusGroupCacheMap.forEach(
        (regionGroupId, consensusGroupCache) -> {
          if (regionGroupId.getType().equals(consensusGroupType)) {
            regionLeaderMap.put(regionGroupId, consensusGroupCache.getLeaderId());
          }
        });
    return regionLeaderMap;
  }

  /**
   * Wait for the specified RegionGroups to finish leader election.
   *
   * @param regionGroupIds Specified RegionGroupIds
   */
  public void waitForLeaderElection(List<TConsensusGroupId> regionGroupIds) {
    long startTime = System.currentTimeMillis();
    LOGGER.info("[RegionElection] Wait for leader election of RegionGroups: {}", regionGroupIds);
    while (System.currentTimeMillis() - startTime <= LEADER_ELECTION_WAITING_TIMEOUT) {
      AtomicBoolean allRegionLeaderElected = new AtomicBoolean(true);
      regionGroupIds.forEach(
          regionGroupId -> {
            if (!consensusGroupCacheMap.containsKey(regionGroupId)
                || consensusGroupCacheMap.get(regionGroupId).isLeaderUnSelected()) {
              allRegionLeaderElected.set(false);
            }
          });
      if (allRegionLeaderElected.get()) {
        LOGGER.info("[RegionElection] The leader of RegionGroups: {} is elected.", regionGroupIds);
        return;
      }
      try {
        TimeUnit.MILLISECONDS.sleep(WAIT_LEADER_INTERVAL);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.warn("Interrupt when wait for leader election", e);
        return;
      }
    }

    LOGGER.warn(
        "[RegionElection] The leader of RegionGroups: {} is not determined after 10 heartbeat interval. Some function might fail.",
        regionGroupIds);
  }

  public void updateConfirmedConfigNodeEndPoints(
      int dataNodeId, Set<TEndPoint> configNodeEndPoints) {
    confirmedConfigNodeMap.put(dataNodeId, configNodeEndPoints);
  }

  public Set<TEndPoint> getConfirmedConfigNodeEndPoints(int dataNodeId) {
    return confirmedConfigNodeMap.get(dataNodeId);
  }
}
