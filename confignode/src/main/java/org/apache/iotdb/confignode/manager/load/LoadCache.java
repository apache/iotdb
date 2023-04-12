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

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.load.heartbeat.node.BaseNodeCache;
import org.apache.iotdb.confignode.manager.load.heartbeat.node.ConfigNodeHeartbeatCache;
import org.apache.iotdb.confignode.manager.load.heartbeat.node.DataNodeHeartbeatCache;
import org.apache.iotdb.confignode.manager.load.heartbeat.node.NodeHeartbeatSample;
import org.apache.iotdb.confignode.manager.load.heartbeat.region.RegionGroupCache;
import org.apache.iotdb.confignode.manager.load.heartbeat.region.RegionHeartbeatSample;
import org.apache.iotdb.confignode.manager.load.statistics.NodeStatistics;
import org.apache.iotdb.confignode.manager.load.statistics.RegionGroupStatistics;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Maintain all kinds of heartbeat samples */
public class LoadCache {

  private static final boolean IS_DATA_REGION_IOT_CONSENSUS =
      ConsensusFactory.IOT_CONSENSUS.equals(
          ConfigNodeDescriptor.getInstance().getConf().getDataRegionConsensusProtocolClass());

  // Map<NodeId, INodeCache>
  private final Map<Integer, BaseNodeCache> nodeCacheMap;
  // Map<RegionGroupId, RegionGroupCache>
  private final Map<TConsensusGroupId, RegionGroupCache> regionGroupCacheMap;

  public LoadCache() {
    this.nodeCacheMap = new ConcurrentHashMap<>();
    this.regionGroupCacheMap = new ConcurrentHashMap<>();
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
        .computeIfAbsent(nodeId, empty -> new DataNodeHeartbeatCache())
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
                nodeId, new Pair<>(nodeCache.getStatistics(), preNodeStatistics));
          }
        });
    return differentNodeStatisticsMap;
  }

  /**
   * Periodic invoke to update the RegionGroupStatistics of all RegionGroups.
   *
   * @return a map of changed RegionGroupStatistics
   */
  public Map<TConsensusGroupId, RegionGroupStatistics> updateRegionGroupStatistics() {
    Map<TConsensusGroupId, RegionGroupStatistics> differentRegionGroupStatisticsMap =
        new ConcurrentHashMap<>();
    regionGroupCacheMap.forEach(
        (regionGroupId, regionGroupCache) -> {
          if (regionGroupCache.periodicUpdate()) {
            // Update and record the changed RegionGroupStatistics
            differentRegionGroupStatisticsMap.put(regionGroupId, regionGroupCache.getStatistics());
          }
        });
    return differentRegionGroupStatisticsMap;
  }

  public void initHeartbeatCache(IManager configManager) {
    initNodeHeartbeatCache(
        configManager.getNodeManager().getRegisteredConfigNodes(),
        configManager.getNodeManager().getRegisteredDataNodes());
    initRegionGroupHeartbeatCache(configManager.getPartitionManager().getAllReplicaSets());
  }

  /** Initialize the nodeCacheMap when the ConfigNode-Leader is switched */
  private void initNodeHeartbeatCache(
      List<TConfigNodeLocation> registeredConfigNodes,
      List<TDataNodeConfiguration> registeredDataNodes) {
    final int CURRENT_NODE_ID = ConfigNodeHeartbeatCache.CURRENT_NODE_ID;
    nodeCacheMap.clear();

    // Init ConfigNodeHeartbeatCache
    registeredConfigNodes.forEach(
        configNodeLocation -> {
          if (configNodeLocation.getConfigNodeId() != CURRENT_NODE_ID) {
            nodeCacheMap.put(
                configNodeLocation.getConfigNodeId(),
                new ConfigNodeHeartbeatCache(configNodeLocation.getConfigNodeId()));
          }
        });
    // Force set itself and never update
    nodeCacheMap.put(
        ConfigNodeHeartbeatCache.CURRENT_NODE_ID,
        new ConfigNodeHeartbeatCache(
            CURRENT_NODE_ID, ConfigNodeHeartbeatCache.CURRENT_NODE_STATISTICS));

    // Init DataNodeHeartbeatCache
    registeredDataNodes.forEach(
        dataNodeConfiguration ->
            nodeCacheMap.put(
                dataNodeConfiguration.getLocation().getDataNodeId(), new DataNodeHeartbeatCache()));
  }

  /** Initialize the regionGroupCacheMap when the ConfigNode-Leader is switched. */
  private void initRegionGroupHeartbeatCache(List<TRegionReplicaSet> regionReplicaSets) {
    regionGroupCacheMap.clear();
    regionReplicaSets.forEach(
        regionReplicaSet ->
            regionGroupCacheMap.put(
                regionReplicaSet.getRegionId(),
                new RegionGroupCache(regionReplicaSet.getRegionId())));
  }

  public void clearHeartbeatCache() {
    nodeCacheMap.clear();
    regionGroupCacheMap.clear();
  }
}
