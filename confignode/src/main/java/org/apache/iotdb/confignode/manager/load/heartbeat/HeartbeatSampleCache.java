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

package org.apache.iotdb.confignode.manager.load.heartbeat;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.manager.load.heartbeat.node.BaseNodeCache;
import org.apache.iotdb.confignode.manager.load.heartbeat.node.ConfigNodeHeartbeatCache;
import org.apache.iotdb.confignode.manager.load.heartbeat.node.DataNodeHeartbeatCache;
import org.apache.iotdb.confignode.manager.load.heartbeat.node.NodeHeartbeatSample;
import org.apache.iotdb.confignode.manager.load.heartbeat.region.RegionGroupCache;
import org.apache.iotdb.confignode.manager.load.heartbeat.region.RegionHeartbeatSample;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.mpp.rpc.thrift.THeartbeatResp;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Maintain all kinds of heartbeat samples */
public class HeartbeatSampleCache {

  private static final boolean IS_DATA_REGION_IOT_CONSENSUS =
    ConsensusFactory.IOT_CONSENSUS.equals(ConfigNodeDescriptor.getInstance().getConf().getDataRegionConsensusProtocolClass());

  // Map<NodeId, INodeCache>
  private final Map<Integer, BaseNodeCache> nodeCacheMap;
  // Map<RegionGroupId, RegionGroupCache>
  private final Map<TConsensusGroupId, RegionGroupCache> regionGroupCacheMap;
  // Key: RegionGroupId
  // Value: Pair<Timestamp, LeaderDataNodeId>, where
  // the left value stands for sampling timestamp
  // and the right value stands for the index of DataNode that leader resides.
  private final Map<TConsensusGroupId, Pair<Long, Integer>> leaderCache;

  public HeartbeatSampleCache() {
    this.nodeCacheMap = new ConcurrentHashMap<>();
    this.regionGroupCacheMap = new ConcurrentHashMap<>();
    this.leaderCache = new ConcurrentHashMap<>();
  }

  /**
   * Cache the latest heartbeat sample of a ConfigNode.
   *
   * @param nodeId the id of the ConfigNode
   * @param sample the latest heartbeat sample
   */
  public void cacheConfigNodeHeartbeatSample(int nodeId, NodeHeartbeatSample sample) {
    nodeCacheMap.computeIfAbsent(nodeId,
      empty -> new ConfigNodeHeartbeatCache(nodeId))
      .cacheHeartbeatSample(sample);
  }

  /**
   * Cache the latest heartbeat sample of a DataNode.
   *
   * @param nodeId the id of the DataNode
   * @param sample the latest heartbeat sample
   */
  public void cacheDataNodeHeartbeatSample(int nodeId, NodeHeartbeatSample sample) {
    nodeCacheMap.computeIfAbsent(nodeId,
      empty -> new DataNodeHeartbeatCache())
      .cacheHeartbeatSample(sample);
  }

  /**
   * Cache the latest heartbeat sample of a RegionGroup.
   *
   * @param regionGroupId the id of the RegionGroup
   * @param nodeId the id of the DataNode where specified Region resides
   * @param sample the latest heartbeat sample
   */
  public void cacheRegionHeartbeatSample(TConsensusGroupId regionGroupId, int nodeId, RegionHeartbeatSample sample) {
    regionGroupCacheMap.computeIfAbsent(regionGroupId,
      empty -> new RegionGroupCache(regionGroupId))
      .cacheHeartbeatSample(nodeId, sample);
  }

  /**
   * Cache the latest leader of a RegionGroup.
   *
   * @param regionGroupId the id of the RegionGroup
   * @param leaderSample the latest leader of a RegionGroup
   */
  public void cacheLeaderSample(TConsensusGroupId regionGroupId, Pair<Long, Integer> leaderSample) {
    if (TConsensusGroupType.DataRegion.equals(regionGroupId.getType())
      && IS_DATA_REGION_IOT_CONSENSUS) {
      // The leadership of IoTConsensus protocol is decided by ConfigNode-leader
      return;
    }

    leaderCache.putIfAbsent(regionGroupId, leaderSample);
    synchronized (leaderCache.get(regionGroupId)) {
      if (leaderCache.get(regionGroupId).getLeft() < leaderSample.getLeft()) {
        leaderCache.replace(regionGroupId, leaderSample);
      }
    }
  }

  public void clear() {
    nodeCacheMap.clear();
    regionGroupCacheMap.clear();
    leaderCache.clear();
  }
}
