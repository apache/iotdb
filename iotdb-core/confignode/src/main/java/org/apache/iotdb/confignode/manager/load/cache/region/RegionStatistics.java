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
import org.apache.iotdb.confignode.manager.load.cache.AbstractStatistics;

import java.util.Objects;

/** RegionStatistics indicates the statistics of a Region. */
public class RegionStatistics extends AbstractStatistics {

  private final RegionStatus regionStatus;
  private final long diskUsage;

  public RegionStatistics(long statisticsNanoTimestamp, RegionStatus regionStatus, long diskUsage) {
    super(statisticsNanoTimestamp);
    this.regionStatus = regionStatus;
    this.diskUsage = diskUsage;
  }

  @TestOnly
  public RegionStatistics(RegionStatus regionStatus) {
    super(System.nanoTime());
    this.regionStatus = regionStatus;
    this.diskUsage = 0;
  }

  public static RegionStatistics generateDefaultRegionStatistics() {
    return new RegionStatistics(0, RegionStatus.Unknown, 0);
  }

  public RegionStatus getRegionStatus() {
    return regionStatus;
  }

  public long getDiskUsage() {
    return diskUsage;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RegionStatistics that = (RegionStatistics) o;
    return regionStatus == that.regionStatus;
  }

  @Override
  public int hashCode() {
    return Objects.hash(regionStatus);
  }

  @Override
  public String toString() {
    return "RegionStatistics{" + "regionStatus=" + regionStatus + '}';
  }
}
