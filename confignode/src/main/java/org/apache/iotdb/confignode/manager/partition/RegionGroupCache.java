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
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.commons.cluster.RegionStatus;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.persistence.partition.statistics.RegionGroupStatistics;
import org.apache.iotdb.confignode.persistence.partition.statistics.RegionStatistics;
import org.apache.iotdb.consensus.ConsensusFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RegionGroupCache {

  private final TConsensusGroupId consensusGroupId;

  // Map<DataNodeId(where a RegionReplica resides), RegionCache>
  private final Map<Integer, RegionCache> regionCacheMap;

  private volatile RegionGroupStatistics statistics;

  /** Constructor for create RegionGroupCache with default RegionGroupStatistics */
  public RegionGroupCache(TConsensusGroupId consensusGroupId) {
    this.consensusGroupId = consensusGroupId;
    this.regionCacheMap = new ConcurrentHashMap<>();

    this.statistics = RegionGroupStatistics.generateDefaultRegionGroupStatistics();
  }

  public RegionGroupCache(
      TConsensusGroupId consensusGroupId, RegionGroupStatistics regionGroupStatistics) {
    this.consensusGroupId = consensusGroupId;
    this.regionCacheMap = new ConcurrentHashMap<>();

    this.statistics = regionGroupStatistics;
  }

  public void cacheHeartbeatSample(int dataNodeId, RegionHeartbeatSample newHeartbeatSample) {
    regionCacheMap
        .computeIfAbsent(dataNodeId, empty -> new RegionCache())
        .cacheHeartbeatSample(newHeartbeatSample);
  }

  /**
   * Update RegionReplicas' statistics, including:
   *
   * <p>1. RegionGroupStatus
   *
   * <p>2. RegionStatus
   *
   * @return RegionGroupStatistics if some fields of statistics changed, null otherwise
   */
  public RegionGroupStatistics updateRegionGroupStatistics() {
    Map<Integer, RegionStatistics> regionStatisticsMap = new HashMap<>();
    for (Map.Entry<Integer, RegionCache> cacheEntry : regionCacheMap.entrySet()) {
      // Update RegionStatistics
      RegionStatistics regionStatistics = cacheEntry.getValue().getRegionStatistics();
      regionStatisticsMap.put(cacheEntry.getKey(), regionStatistics);
    }

    // Update RegionGroupStatus
    RegionGroupStatus status = updateRegionGroupStatus(regionStatisticsMap);

    RegionGroupStatistics newRegionGroupStatistics =
        new RegionGroupStatistics(status, regionStatisticsMap);
    return newRegionGroupStatistics.equals(statistics)
        ? null
        : (statistics = newRegionGroupStatistics);
  }

  private RegionGroupStatus updateRegionGroupStatus(
      Map<Integer, RegionStatistics> regionStatisticsMap) {
    int unknownCount = 0;
    for (RegionStatistics regionStatistics : regionStatisticsMap.values()) {
      if (RegionStatus.ReadOnly.equals(regionStatistics.getRegionStatus())
          || RegionStatus.Removing.equals(regionStatistics.getRegionStatus())) {
        // The RegionGroup is considered as Disabled when
        // at least one Region is in the ReadOnly or Removing status
        return RegionGroupStatus.Disabled;
      }
      unknownCount += RegionStatus.Unknown.equals(regionStatistics.getRegionStatus()) ? 1 : 0;
    }

    if (unknownCount == 0) {
      // The RegionGroup is considered as Running only if
      // all Regions are in the Running status
      return RegionGroupStatus.Running;
    } else {
      return unknownCount <= ((regionCacheMap.size() - 1) / 2)
          // The RegionGroup is considered as Available when the number of Unknown Regions is less
          // than half
          ? RegionGroupStatus.Available
          // Disabled otherwise
          : RegionGroupStatus.Disabled;
    }
  }

  public void removeCacheIfExists(int dataNodeId) {
    regionCacheMap.remove(dataNodeId);
  }

  public RegionGroupStatistics getStatistics() {
    return statistics;
  }
}
