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

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.cluster.NodeType;
import org.apache.iotdb.commons.cluster.RegionStatus;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.load.cache.node.BaseNodeCache;
import org.apache.iotdb.confignode.manager.load.cache.node.ConfigNodeHeartbeatCache;
import org.apache.iotdb.confignode.manager.load.cache.node.DataNodeHeartbeatCache;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeHeartbeatSample;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeStatistics;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionGroupCache;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionGroupStatistics;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionHeartbeatSample;
import org.apache.iotdb.confignode.manager.load.cache.route.RegionRouteCache;
import org.apache.iotdb.confignode.manager.partition.RegionGroupStatus;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/** Maintain all kinds of heartbeat samples. */
public class LoadCache {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadCache.class);

  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();
  private static final long HEARTBEAT_INTERVAL = CONF.getHeartbeatIntervalInMs();

  // Map<NodeId, INodeCache>
  private final Map<Integer, BaseNodeCache> nodeCacheMap;
  // Map<RegionGroupId, RegionGroupCache>
  private final Map<TConsensusGroupId, RegionGroupCache> regionGroupCacheMap;
  // Map<RegionGroupId, RegionRouteCache>
  private final Map<TConsensusGroupId, RegionRouteCache> regionRouteCacheMap;

  public LoadCache() {
    this.nodeCacheMap = new ConcurrentHashMap<>();
    this.regionGroupCacheMap = new ConcurrentHashMap<>();
    this.regionRouteCacheMap = new ConcurrentHashMap<>();
  }

  public void initHeartbeatCache(IManager configManager) {
    initNodeHeartbeatCache(
        configManager.getNodeManager().getRegisteredConfigNodes(),
        configManager.getNodeManager().getRegisteredDataNodes());
    initRegionGroupHeartbeatCache(configManager.getPartitionManager().getAllReplicaSets());
  }

  /** Initialize the nodeCacheMap when the ConfigNode-Leader is switched. */
  private void initNodeHeartbeatCache(
      List<TConfigNodeLocation> registeredConfigNodes,
      List<TDataNodeConfiguration> registeredDataNodes) {

    final int CURRENT_NODE_ID = ConfigNodeHeartbeatCache.CURRENT_NODE_ID;
    nodeCacheMap.clear();

    // Init ConfigNodeHeartbeatCache
    registeredConfigNodes.forEach(
        configNodeLocation -> {
          int configNodeId = configNodeLocation.getConfigNodeId();
          if (configNodeId != CURRENT_NODE_ID) {
            nodeCacheMap.put(configNodeId, new ConfigNodeHeartbeatCache(configNodeId));
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
          nodeCacheMap.put(dataNodeId, new DataNodeHeartbeatCache(dataNodeId));
        });
  }

  /** Initialize the regionGroupCacheMap when the ConfigNode-Leader is switched. */
  private void initRegionGroupHeartbeatCache(List<TRegionReplicaSet> regionReplicaSets) {
    regionGroupCacheMap.clear();
    regionReplicaSets.forEach(
        regionReplicaSet -> {
          TConsensusGroupId consensusGroupId = regionReplicaSet.getRegionId();
          regionGroupCacheMap.put(consensusGroupId, new RegionGroupCache(consensusGroupId));
          regionRouteCacheMap.put(consensusGroupId, new RegionRouteCache(consensusGroupId));
        });
  }

  public void clearHeartbeatCache() {
    nodeCacheMap.clear();
    regionGroupCacheMap.clear();
  }

  /**
   * Cache the latest heartbeat sample of a ConfigNode.
   *
   * @param nodeId the id of the ConfigNode
   * @param sample the latest heartbeat sample
   */
  public void cacheConfigNodeHeartbeatSample(int nodeId, NodeHeartbeatSample sample) {
    nodeCacheMap
        .computeIfAbsent(nodeId, empty -> new ConfigNodeHeartbeatCache(nodeId))
        .cacheHeartbeatSample(sample);
  }

  /**
   * Cache the latest heartbeat sample of a DataNode.
   *
   * @param nodeId the id of the DataNode
   * @param sample the latest heartbeat sample
   */
  public void cacheDataNodeHeartbeatSample(int nodeId, NodeHeartbeatSample sample) {
    nodeCacheMap
        .computeIfAbsent(nodeId, empty -> new DataNodeHeartbeatCache(nodeId))
        .cacheHeartbeatSample(sample);
  }

  /**
   * Cache the latest heartbeat sample of a RegionGroup.
   *
   * @param regionGroupId the id of the RegionGroup
   * @param nodeId the id of the DataNode where specified Region resides
   * @param sample the latest heartbeat sample
   */
  public void cacheRegionHeartbeatSample(
      TConsensusGroupId regionGroupId, int nodeId, RegionHeartbeatSample sample) {
    regionGroupCacheMap
        .computeIfAbsent(regionGroupId, empty -> new RegionGroupCache(regionGroupId))
        .cacheHeartbeatSample(nodeId, sample);
  }

  /**
   * Cache the latest leader of a RegionGroup.
   *
   * @param regionGroupId the id of the RegionGroup
   * @param leaderSample the latest leader of a RegionGroup
   */
  public void cacheLeaderSample(TConsensusGroupId regionGroupId, Pair<Long, Integer> leaderSample) {
    regionRouteCacheMap
        .computeIfAbsent(regionGroupId, empty -> new RegionRouteCache(regionGroupId))
        .cacheLeaderSample(leaderSample);
  }

  /**
   * Periodic invoke to update the NodeStatistics of all Nodes.
   *
   * @return a map of changed NodeStatistics
   */
  public Map<Integer, Pair<NodeStatistics, NodeStatistics>> updateNodeStatistics() {
    Map<Integer, Pair<NodeStatistics, NodeStatistics>> differentNodeStatisticsMap =
        new ConcurrentHashMap<>();
    nodeCacheMap.forEach(
        (nodeId, nodeCache) -> {
          NodeStatistics preNodeStatistics = nodeCache.getPreviousStatistics().deepCopy();
          if (nodeCache.periodicUpdate()) {
            // Update and record the changed NodeStatistics
            differentNodeStatisticsMap.put(
                nodeId, new Pair<>(preNodeStatistics, nodeCache.getStatistics()));
          }
        });
    return differentNodeStatisticsMap;
  }

  /**
   * Periodic invoke to update the RegionGroupStatistics of all RegionGroups.
   *
   * @return a map of changed RegionGroupStatistics
   */
  public Map<TConsensusGroupId, Pair<RegionGroupStatistics, RegionGroupStatistics>>
      updateRegionGroupStatistics() {
    Map<TConsensusGroupId, Pair<RegionGroupStatistics, RegionGroupStatistics>>
        differentRegionGroupStatisticsMap = new ConcurrentHashMap<>();
    regionGroupCacheMap.forEach(
        (regionGroupId, regionGroupCache) -> {
          RegionGroupStatistics preRegionGroupStatistics =
              regionGroupCache.getPreviousStatistics().deepCopy();
          if (regionGroupCache.periodicUpdate()) {
            // Update and record the changed RegionGroupStatistics
            differentRegionGroupStatisticsMap.put(
                regionGroupId,
                new Pair<>(preRegionGroupStatistics, regionGroupCache.getStatistics()));
          }
        });
    return differentRegionGroupStatisticsMap;
  }

  public Map<TConsensusGroupId, Pair<Integer, Integer>> updateRegionGroupLeader() {
    Map<TConsensusGroupId, Pair<Integer, Integer>> differentRegionGroupLeaderMap =
        new ConcurrentHashMap<>();
    regionRouteCacheMap.forEach(
        (regionGroupId, regionRouteCache) -> {
          int prevLeader = regionRouteCache.getLeaderId();
          if (regionRouteCache.periodicUpdate()) {
            // Update and record the changed RegionGroupStatistics
            differentRegionGroupLeaderMap.put(
                regionGroupId, new Pair<>(prevLeader, regionRouteCache.getLeaderId()));
          }
        });
    return differentRegionGroupLeaderMap;
  }

  /**
   * Safely get NodeStatus by NodeId.
   *
   * @param nodeId The specified NodeId
   * @return NodeStatus of the specified Node. Unknown if cache doesn't exist.
   */
  public NodeStatus getNodeStatus(int nodeId) {
    BaseNodeCache nodeCache = nodeCacheMap.get(nodeId);
    return nodeCache == null ? NodeStatus.Unknown : nodeCache.getNodeStatus();
  }

  /**
   * Safely get the specified Node's current status with reason.
   *
   * @param nodeId The specified NodeId
   * @return The specified Node's current status if the nodeCache contains it, Unknown otherwise
   */
  public String getNodeStatusWithReason(int nodeId) {
    BaseNodeCache nodeCache = nodeCacheMap.get(nodeId);
    return nodeCache == null
        ? NodeStatus.Unknown.getStatus() + "(NoHeartbeat)"
        : nodeCache.getNodeStatusWithReason();
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
    DataNodeHeartbeatCache dataNodeHeartbeatCache =
        (DataNodeHeartbeatCache) nodeCacheMap.get(dataNodeId);
    return dataNodeHeartbeatCache == null ? 0d : dataNodeHeartbeatCache.getFreeDiskSpace();
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
   * Force update the specified Node's cache.
   *
   * @param nodeType Specified NodeType
   * @param nodeId Specified NodeId
   * @param heartbeatSample Specified NodeHeartbeatSample
   */
  public void forceUpdateNodeCache(
      NodeType nodeType, int nodeId, NodeHeartbeatSample heartbeatSample) {
    switch (nodeType) {
      case ConfigNode:
        nodeCacheMap
            .computeIfAbsent(nodeId, empty -> new ConfigNodeHeartbeatCache(nodeId))
            .forceUpdate(heartbeatSample);
        break;
      case DataNode:
      default:
        nodeCacheMap
            .computeIfAbsent(nodeId, empty -> new DataNodeHeartbeatCache(nodeId))
            .forceUpdate(heartbeatSample);
        break;
    }
  }

  /** Remove the specified Node's cache. */
  public void removeNodeCache(int nodeId) {
    nodeCacheMap.remove(nodeId);
  }

  /**
   * Safely get RegionStatus.
   *
   * @param consensusGroupId Specified RegionGroupId
   * @param dataNodeId Specified RegionReplicaId
   * @return Corresponding RegionStatus if cache exists, Unknown otherwise
   */
  public RegionStatus getRegionStatus(TConsensusGroupId consensusGroupId, int dataNodeId) {
    return regionGroupCacheMap.containsKey(consensusGroupId)
        ? regionGroupCacheMap.get(consensusGroupId).getStatistics().getRegionStatus(dataNodeId)
        : RegionStatus.Unknown;
  }

  /**
   * Safely get RegionGroupStatus.
   *
   * @param consensusGroupId Specified RegionGroupId
   * @return Corresponding RegionGroupStatus if cache exists, Disabled otherwise
   */
  public RegionGroupStatus getRegionGroupStatus(TConsensusGroupId consensusGroupId) {
    return regionGroupCacheMap.containsKey(consensusGroupId)
        ? regionGroupCacheMap.get(consensusGroupId).getStatistics().getRegionGroupStatus()
        : RegionGroupStatus.Disabled;
  }

  /**
   * Safely get RegionGroupStatus.
   *
   * @param consensusGroupIds Specified RegionGroupIds
   * @return Corresponding RegionGroupStatus if cache exists, Disabled otherwise
   */
  public Map<TConsensusGroupId, RegionGroupStatus> getRegionGroupStatus(
      List<TConsensusGroupId> consensusGroupIds) {
    Map<TConsensusGroupId, RegionGroupStatus> regionGroupStatusMap = new ConcurrentHashMap<>();
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
                                    .getStatistics()
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
                .getStatistics()
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

  /**
   * Force update the specified RegionGroup's cache.
   *
   * @param regionGroupId Specified RegionGroupId
   * @param heartbeatSampleMap Specified RegionHeartbeatSampleMap
   */
  public void forceUpdateRegionGroupCache(
      TConsensusGroupId regionGroupId, Map<Integer, RegionHeartbeatSample> heartbeatSampleMap) {
    regionGroupCacheMap
        .computeIfAbsent(regionGroupId, empty -> new RegionGroupCache(regionGroupId))
        .forceUpdate(heartbeatSampleMap);
  }

  /** Remove the specified RegionGroup's cache. */
  public void removeRegionGroupCache(TConsensusGroupId consensusGroupId) {
    regionGroupCacheMap.remove(consensusGroupId);
  }

  /**
   * Safely get the latest RegionLeaderMap.
   *
   * @return Map<RegionGroupId, leaderId>
   */
  public Map<TConsensusGroupId, Integer> getRegionLeaderMap() {
    Map<TConsensusGroupId, Integer> regionLeaderMap = new ConcurrentHashMap<>();
    regionRouteCacheMap.forEach(
        (regionGroupId, regionRouteCache) ->
            regionLeaderMap.put(regionGroupId, regionRouteCache.getLeaderId()));
    return regionLeaderMap;
  }

  /**
   * Safely get the latest RegionPriorityMap.
   *
   * @return Map<RegionGroupId, RegionPriority>
   */
  public Map<TConsensusGroupId, TRegionReplicaSet> getRegionPriorityMap() {
    Map<TConsensusGroupId, TRegionReplicaSet> regionPriorityMap = new ConcurrentHashMap<>();
    regionRouteCacheMap.forEach(
        (regionGroupId, regionRouteCache) ->
            regionPriorityMap.put(regionGroupId, regionRouteCache.getRegionPriority()));
    return regionPriorityMap;
  }

  /**
   * Wait for the specified RegionGroups to finish leader election
   *
   * @param regionGroupIds Specified RegionGroupIds
   */
  public void waitForLeaderElection(List<TConsensusGroupId> regionGroupIds) {
    for (int retry = 0; retry < 10; retry++) {
      AtomicBoolean allRegionLeaderElected = new AtomicBoolean(true);
      regionGroupIds.forEach(
          regionGroupId -> {
            if (!regionRouteCacheMap.containsKey(regionGroupId)
                || regionRouteCacheMap.get(regionGroupId).getLeaderId()
                    == RegionRouteCache.unReadyLeaderId
                || RegionRouteCache.unReadyRegionPriority.equals(
                    regionRouteCacheMap.get(regionGroupId).getRegionPriority())) {
              allRegionLeaderElected.set(false);
            }
          });
      if (allRegionLeaderElected.get()) {
        LOGGER.info("[RegionElection] The leader of RegionGroups: {} is elected.", regionGroupIds);
        return;
      }
      try {
        TimeUnit.MILLISECONDS.sleep(HEARTBEAT_INTERVAL);
      } catch (InterruptedException e) {
        LOGGER.warn("Interrupt when wait for leader election", e);
      }
    }

    LOGGER.warn(
        "[RegionElection] The leader of RegionGroups: {} is not elected after 10 seconds. Some function might fail.",
        regionGroupIds);
  }

  /**
   * Force update the specified RegionGroup's leader.
   *
   * @param regionGroupId Specified RegionGroupId
   * @param leaderId Leader DataNodeId
   */
  public void forceUpdateRegionLeader(TConsensusGroupId regionGroupId, int leaderId) {
    regionRouteCacheMap
        .computeIfAbsent(regionGroupId, empty -> new RegionRouteCache(regionGroupId))
        .forceUpdateRegionLeader(leaderId);
  }

  /**
   * Force update the specified RegionGroup's priority.
   *
   * @param regionGroupId Specified RegionGroupId
   * @param regionPriority Region route priority
   */
  public void forceUpdateRegionPriority(
      TConsensusGroupId regionGroupId, TRegionReplicaSet regionPriority) {
    regionRouteCacheMap
        .computeIfAbsent(regionGroupId, empty -> new RegionRouteCache(regionGroupId))
        .forceUpdateRegionPriority(regionPriority);
  }

  /**
   * Remove the specified RegionGroup's route cache.
   *
   * @param regionGroupId Specified RegionGroupId
   */
  public void removeRegionRouteCache(TConsensusGroupId regionGroupId) {
    regionRouteCacheMap.remove(regionGroupId);
  }
}
