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
import org.apache.iotdb.confignode.manager.partition.RegionGroupStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/** RegionGroupStatistics indicates the statistics of a RegionGroup. */
public class RegionGroupStatistics {

  private final RegionGroupStatus regionGroupStatus;
  private final Map<Integer, RegionStatistics> regionStatisticsMap;
  private long diskUsage = 0;

  public RegionGroupStatistics(
      RegionGroupStatus regionGroupStatus, Map<Integer, RegionStatistics> regionStatisticsMap) {
    this.regionGroupStatus = regionGroupStatus;
    this.regionStatisticsMap = regionStatisticsMap;
  }

  public static RegionGroupStatistics generateDefaultRegionGroupStatistics() {
    return new RegionGroupStatistics(RegionGroupStatus.Disabled, new TreeMap<>());
  }

  public RegionGroupStatus getRegionGroupStatus() {
    return regionGroupStatus;
  }

  public long getDiskUsage() {
    return diskUsage;
  }

  public void setDiskUsage(long diskUsage) {
    this.diskUsage = diskUsage;
  }

  /**
   * Get the specified Region's status.
   *
   * @param dataNodeId Where the Region resides
   * @return Region's latest status if received heartbeat recently, Unknown otherwise
   */
  public RegionStatus getRegionStatus(int dataNodeId) {
    return regionStatisticsMap.containsKey(dataNodeId)
        ? regionStatisticsMap.get(dataNodeId).getRegionStatus()
        : RegionStatus.Unknown;
  }

  public Map<Integer, RegionStatistics> getRegionStatisticsMap() {
    return regionStatisticsMap;
  }

  public List<Integer> getRegionIds() {
    return new ArrayList<>(regionStatisticsMap.keySet());
  }

  /**
   * Check if the current statistics is newer than the given one.
   *
   * @param o The other RegionGroupStatistics.
   * @return True if one of the RegionStatistics in the current RegionGroupStatistics is newer than
   *     the corresponding one in the other RegionGroupStatistics, or the current
   *     RegionGroupStatistics contains newer Region's statistics, False otherwise.
   */
  public boolean isNewerThan(RegionGroupStatistics o) {
    for (Integer dataNodeId : regionStatisticsMap.keySet()) {
      if (!o.regionStatisticsMap.containsKey(dataNodeId)
          || (regionStatisticsMap
              .get(dataNodeId)
              .isNewerThan(o.regionStatisticsMap.get(dataNodeId)))) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RegionGroupStatistics that = (RegionGroupStatistics) o;
    return regionGroupStatus == that.regionGroupStatus
        && regionStatisticsMap.equals(that.regionStatisticsMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(regionGroupStatus, regionStatisticsMap);
  }

  @Override
  public String toString() {
    return "RegionGroupStatistics{" + "regionGroupStatus=" + regionGroupStatus + '}';
  }
}
