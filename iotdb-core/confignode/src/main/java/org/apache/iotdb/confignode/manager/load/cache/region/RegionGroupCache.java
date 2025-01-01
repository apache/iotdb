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

package org.apache.iotdb.confignode.manager.load.cache.region;

import org.apache.iotdb.commons.cluster.RegionStatus;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.manager.partition.RegionGroupStatus;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * RegionGroupCache caches the RegionHeartbeatSamples of all Regions in the same RegionGroup. Update
 * and cache the current statistics of the RegionGroup based on the latest RegionHeartbeatSamples
 * from all Regions it contains.
 */
public class RegionGroupCache {

  private final String database;
  // Map<DataNodeId(where a RegionReplica resides in), RegionCache>
  private final Map<Integer, RegionCache> regionCacheMap;
  // The current RegionGroupStatistics, used for providing statistics to other services
  private final AtomicReference<RegionGroupStatistics> currentStatistics;

  /** Constructor for create RegionGroupCache with default RegionGroupStatistics. */
  public RegionGroupCache(String database, Set<Integer> dataNodeIds) {
    this.database = database;
    this.regionCacheMap = new ConcurrentHashMap<>();
    dataNodeIds.forEach(dataNodeId -> regionCacheMap.put(dataNodeId, new RegionCache()));
    this.currentStatistics =
        new AtomicReference<>(RegionGroupStatistics.generateDefaultRegionGroupStatistics());
  }

  /**
   * Cache the newest RegionHeartbeatSample.
   *
   * @param dataNodeId Where the specified Region resides
   * @param newHeartbeatSample The newest RegionHeartbeatSample
   * @param overwrite Able to overwrite Adding or Removing
   */
  public void cacheHeartbeatSample(
      int dataNodeId, RegionHeartbeatSample newHeartbeatSample, boolean overwrite) {
    // Only cache sample when the corresponding loadCache exists
    Optional.ofNullable(regionCacheMap.get(dataNodeId))
        .ifPresent(region -> region.cacheHeartbeatSample(newHeartbeatSample, overwrite));
  }

  @TestOnly
  public void cacheHeartbeatSample(int dataNodeId, RegionHeartbeatSample newHeartbeatSample) {
    cacheHeartbeatSample(dataNodeId, newHeartbeatSample, false);
  }

  /**
   * Create the cache of the specified Region.
   *
   * @param dataNodeId the specified DataNode
   */
  public void createRegionCache(int dataNodeId) {
    regionCacheMap.put(dataNodeId, new RegionCache());
  }

  /**
   * Remove the cache of the specified Region in the specified RegionGroup.
   *
   * @param dataNodeId the specified DataNode
   */
  public void removeRegionCache(int dataNodeId) {
    regionCacheMap.remove(dataNodeId);
  }

  /**
   * Update currentStatistics based on the latest NodeHeartbeatSamples that cached in the
   * slidingWindow.
   */
  public void updateCurrentStatistics() {
    regionCacheMap.values().forEach(regionCache -> regionCache.updateCurrentStatistics(false));
    Map<Integer, RegionStatistics> regionStatisticsMap =
        regionCacheMap.entrySet().stream()
            .collect(
                TreeMap::new,
                (map, entry) -> map.put(entry.getKey(), entry.getValue().getCurrentStatistics()),
                TreeMap::putAll);
    currentStatistics.set(
        new RegionGroupStatistics(
            caculateRegionGroupStatus(regionStatisticsMap), regionStatisticsMap));
  }

  private RegionGroupStatus caculateRegionGroupStatus(
      Map<Integer, RegionStatistics> regionStatisticsMap) {
    int unknownCount = 0;
    int readonlyCount = 0;
    int removingCount = 0;
    for (RegionStatistics regionStatistics : regionStatisticsMap.values()) {
      unknownCount += RegionStatus.Unknown.equals(regionStatistics.getRegionStatus()) ? 1 : 0;
      readonlyCount += RegionStatus.ReadOnly.equals(regionStatistics.getRegionStatus()) ? 1 : 0;
      removingCount += RegionStatus.Removing.equals(regionStatistics.getRegionStatus()) ? 1 : 0;
    }

    if (unknownCount + readonlyCount + removingCount == 0) {
      // The RegionGroup is considered as Running only if
      // all Regions are in the Running status
      return RegionGroupStatus.Running;
    } else if (readonlyCount == 0) {
      return (unknownCount + removingCount) <= ((regionCacheMap.size() - 1) / 2)
          // The RegionGroup is considered as Available when the number of Unknown Regions is less
          // than half
          ? RegionGroupStatus.Available
          // Disabled otherwise
          : RegionGroupStatus.Disabled;
    } else {
      return (unknownCount + readonlyCount + removingCount) <= ((regionCacheMap.size() - 1) / 2)
          // The RegionGroup is considered as Discouraged when the number of Unknown or ReadOnly
          // Regions is less
          // than half, and there are at least 1 ReadOnly Region
          ? RegionGroupStatus.Discouraged
          // Disabled otherwise
          : RegionGroupStatus.Disabled;
    }
  }

  public RegionGroupStatistics getCurrentStatistics() {
    return currentStatistics.get();
  }

  public String getDatabase() {
    return database;
  }

  public Set<Integer> getRegionLocations() {
    return regionCacheMap.keySet();
  }

  public RegionCache getRegionCache(int nodeId) {
    return regionCacheMap.get(nodeId);
  }
}
