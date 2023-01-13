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
package org.apache.iotdb.confignode.manager.partition.heartbeat;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.commons.cluster.RegionStatus;
import org.apache.iotdb.confignode.manager.partition.RegionGroupStatus;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RegionGroupCache {

  private final TConsensusGroupId consensusGroupId;

  // Map<DataNodeId(where a RegionReplica resides in), RegionCache>
  private final Map<Integer, RegionCache> regionCacheMap;

  // The previous RegionGroupStatistics, used for comparing with
  // the current RegionGroupStatistics to initiate notification when they are different
  protected volatile RegionGroupStatistics previousStatistics;
  // The current RegionGroupStatistics, used for providing statistics to other services
  private volatile RegionGroupStatistics currentStatistics;

  /** Constructor for create RegionGroupCache with default RegionGroupStatistics */
  public RegionGroupCache(TConsensusGroupId consensusGroupId) {
    this.consensusGroupId = consensusGroupId;
    this.regionCacheMap = new ConcurrentHashMap<>();

    this.previousStatistics = RegionGroupStatistics.generateDefaultRegionGroupStatistics();
    this.currentStatistics = RegionGroupStatistics.generateDefaultRegionGroupStatistics();
  }

  /**
   * Cache the newest RegionHeartbeatSample
   *
   * @param dataNodeId Where the specified Region resides
   * @param newHeartbeatSample The newest RegionHeartbeatSample
   */
  public void cacheHeartbeatSample(int dataNodeId, RegionHeartbeatSample newHeartbeatSample) {
    regionCacheMap
        .computeIfAbsent(dataNodeId, empty -> new RegionCache())
        .cacheHeartbeatSample(newHeartbeatSample);
  }

  /**
   * Invoking periodically in the Cluster-LoadStatistics-Service to update currentStatistics and
   * compare with the previousStatistics, in order to detect whether the RegionGroup's statistics
   * has changed
   *
   * @return True if the currentStatistics has changed recently(compare with the
   *     previousStatistics), false otherwise
   */
  public boolean periodicUpdate() {
    updateCurrentStatistics();
    if (!currentStatistics.equals(previousStatistics)) {
      previousStatistics = currentStatistics.deepCopy();
      return true;
    } else {
      return false;
    }
  }

  /**
   * Actively append custom NodeHeartbeatSamples to force a change in the RegionGroupStatistics.
   *
   * <p>For example, this interface can be invoked in RegionGroup creating process to forcibly
   * activate the corresponding RegionGroup's status to Available without waiting for heartbeat
   * sampling
   *
   * <p>Notice: The ConfigNode-leader doesn't know the specified RegionGroup's statistics has
   * changed even if this interface is invoked, since the ConfigNode-leader only detect cluster
   * RegionGroups' statistics by periodicUpdate interface. However, other service can still read the
   * update of currentStatistics by invoking getters below.
   *
   * @param newHeartbeatSamples Custom RegionHeartbeatSamples that will lead to needed
   *     RegionGroupStatistics
   */
  public void forceUpdate(Map<Integer, RegionHeartbeatSample> newHeartbeatSamples) {
    newHeartbeatSamples.forEach(this::cacheHeartbeatSample);
    updateCurrentStatistics();
  }

  /**
   * Update currentStatistics based on recent NodeHeartbeatSamples that cached in the slidingWindow
   */
  protected void updateCurrentStatistics() {
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
    if (!currentStatistics.equals(newRegionGroupStatistics)) {
      // Update RegionGroupStatistics if necessary
      currentStatistics = newRegionGroupStatistics;
    }
  }

  private RegionGroupStatus updateRegionGroupStatus(
      Map<Integer, RegionStatistics> regionStatisticsMap) {
    int unknownCount = 0;
    int readonlyCount = 0;
    for (RegionStatistics regionStatistics : regionStatisticsMap.values()) {
      if (RegionStatus.Removing.equals(regionStatistics.getRegionStatus())) {
        // The RegionGroup is considered as Disabled when
        // at least one Region is in the ReadOnly or Removing status
        return RegionGroupStatus.Disabled;
      }
      unknownCount += RegionStatus.Unknown.equals(regionStatistics.getRegionStatus()) ? 1 : 0;
      readonlyCount += RegionStatus.ReadOnly.equals(regionStatistics.getRegionStatus()) ? 1 : 0;
    }

    if (unknownCount + readonlyCount == 0) {
      // The RegionGroup is considered as Running only if
      // all Regions are in the Running status
      return RegionGroupStatus.Running;
    } else if (readonlyCount == 0) {
      return unknownCount <= ((regionCacheMap.size() - 1) / 2)
          // The RegionGroup is considered as Available when the number of Unknown Regions is less
          // than half
          ? RegionGroupStatus.Available
          // Disabled otherwise
          : RegionGroupStatus.Disabled;
    } else {
      return unknownCount + readonlyCount <= ((regionCacheMap.size() - 1) / 2)
          // The RegionGroup is considered as Discouraged when the number of Unknown or ReadOnly
          // Regions is less
          // than half, and there are at least 1 ReadOnly Region
          ? RegionGroupStatus.Discouraged
          // Disabled otherwise
          : RegionGroupStatus.Disabled;
    }
  }

  public void removeCacheIfExists(int dataNodeId) {
    regionCacheMap.remove(dataNodeId);
  }

  public RegionGroupStatistics getStatistics() {
    return currentStatistics;
  }
}
