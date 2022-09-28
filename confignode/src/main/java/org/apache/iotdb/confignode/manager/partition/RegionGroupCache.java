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
package org.apache.iotdb.confignode.manager.partition;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.commons.cluster.RegionStatus;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RegionGroupCache {

  private static final Logger LOGGER = LoggerFactory.getLogger(RegionGroupCache.class);

  private final TConsensusGroupId consensusGroupId;

  // Map<DataNodeId(where a RegionReplica resides), RegionCache>
  private final Map<Integer, RegionCache> regionCacheMap;

  // The DataNode where the leader resides
  private volatile int leaderDataNodeId;

  public RegionGroupCache(TConsensusGroupId consensusGroupId) {
    this.consensusGroupId = consensusGroupId;
    this.regionCacheMap = new ConcurrentHashMap<>();
    this.leaderDataNodeId = -1;
  }

  public void cacheHeartbeatSample(RegionHeartbeatSample newHeartbeatSample) {
    regionCacheMap
        .computeIfAbsent(newHeartbeatSample.getBelongedDataNodeId(), empty -> new RegionCache())
        .cacheHeartbeatSample(newHeartbeatSample);
  }

  /**
   * Update RegionReplicas' statistics, including:
   *
   * <p>1. RegionStatus
   *
   * <p>2. Leadership
   *
   * @return True if the leader changed, false otherwise
   */
  public boolean updateRegionStatistics() {
    long updateVersion = Long.MIN_VALUE;
    int originLeaderDataNodeId = leaderDataNodeId;

    for (Map.Entry<Integer, RegionCache> cacheEntry : regionCacheMap.entrySet()) {
      cacheEntry.getValue().updateStatistics();
      Pair<Long, Boolean> isLeader = cacheEntry.getValue().isLeader();
      if (isLeader.getLeft() > updateVersion && isLeader.getRight()) {
        updateVersion = isLeader.getLeft();
        leaderDataNodeId = cacheEntry.getKey();
      }
    }

    return originLeaderDataNodeId != leaderDataNodeId;
  }

  public void removeCacheIfExists(int dataNodeId) {
    regionCacheMap.remove(dataNodeId);
  }

  public int getLeaderDataNodeId() {
    return leaderDataNodeId;
  }

  /**
   * Get the specified Region's status
   *
   * @param dataNodeId Where the Region resides
   * @return Region's latest status if received heartbeat recently, Unknown otherwise
   */
  public RegionStatus getRegionStatus(int dataNodeId) {
    return regionCacheMap.containsKey(dataNodeId)
        ? regionCacheMap.get(dataNodeId).getStatus()
        : RegionStatus.Unknown;
  }

  public RegionGroupStatus getRegionGroupStatus() {
    int unknownCount = 0;
    for (RegionCache regionCache : regionCacheMap.values()) {
      if (RegionStatus.ReadOnly.equals(regionCache.getStatus())
          || RegionStatus.Removing.equals(regionCache.getStatus())) {
        return RegionGroupStatus.Disabled;
      }
      unknownCount += RegionStatus.Unknown.equals(regionCache.getStatus()) ? 1 : 0;
    }

    if (unknownCount == 0) {
      return RegionGroupStatus.Running;
    } else {
      return unknownCount <= ((regionCacheMap.size() - 1) / 2)
          ? RegionGroupStatus.Available
          : RegionGroupStatus.Disabled;
    }
  }

  public TConsensusGroupId getConsensusGroupId() {
    return consensusGroupId;
  }
}
