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

import org.apache.iotdb.commons.cluster.RegionStatus;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionGroupCache;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionHeartbeatSample;
import org.apache.iotdb.confignode.manager.partition.RegionGroupStatus;

import org.junit.Assert;
import org.junit.Test;

import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RegionGroupCacheTest {

  private static final String DATABASE = "root.db";

  @Test
  public void getRegionStatusTest() {
    long currentTime = System.nanoTime();
    RegionGroupCache regionGroupCache =
        new RegionGroupCache(DATABASE, Stream.of(0, 1, 2, 3).collect(Collectors.toSet()));
    regionGroupCache.cacheHeartbeatSample(
        0, new RegionHeartbeatSample(currentTime, RegionStatus.Running));
    regionGroupCache.cacheHeartbeatSample(
        1, new RegionHeartbeatSample(currentTime, RegionStatus.Unknown));
    regionGroupCache.cacheHeartbeatSample(
        2, new RegionHeartbeatSample(currentTime, RegionStatus.Removing));
    regionGroupCache.cacheHeartbeatSample(
        3, new RegionHeartbeatSample(currentTime, RegionStatus.ReadOnly));
    regionGroupCache.updateCurrentStatistics();

    Assert.assertEquals(
        RegionStatus.Running, regionGroupCache.getCurrentStatistics().getRegionStatus(0));
    Assert.assertEquals(
        RegionStatus.Unknown, regionGroupCache.getCurrentStatistics().getRegionStatus(1));
    Assert.assertEquals(
        RegionStatus.Removing, regionGroupCache.getCurrentStatistics().getRegionStatus(2));
    Assert.assertEquals(
        RegionStatus.ReadOnly, regionGroupCache.getCurrentStatistics().getRegionStatus(3));
  }

  @Test
  public void getRegionGroupStatusTest() {
    long currentTime = System.nanoTime();
    RegionGroupCache runningRegionGroup =
        new RegionGroupCache(DATABASE, Stream.of(0, 1, 2).collect(Collectors.toSet()));
    runningRegionGroup.cacheHeartbeatSample(
        0, new RegionHeartbeatSample(currentTime, RegionStatus.Running));
    runningRegionGroup.cacheHeartbeatSample(
        1, new RegionHeartbeatSample(currentTime, RegionStatus.Running));
    runningRegionGroup.cacheHeartbeatSample(
        2, new RegionHeartbeatSample(currentTime, RegionStatus.Running));
    runningRegionGroup.updateCurrentStatistics();
    Assert.assertEquals(
        RegionGroupStatus.Running,
        runningRegionGroup.getCurrentStatistics().getRegionGroupStatus());

    RegionGroupCache availableRegionGroup =
        new RegionGroupCache(DATABASE, Stream.of(0, 1, 2).collect(Collectors.toSet()));
    availableRegionGroup.cacheHeartbeatSample(
        0, new RegionHeartbeatSample(currentTime, RegionStatus.Running));
    availableRegionGroup.cacheHeartbeatSample(
        1, new RegionHeartbeatSample(currentTime, RegionStatus.Unknown));
    availableRegionGroup.cacheHeartbeatSample(
        2, new RegionHeartbeatSample(currentTime, RegionStatus.Running));
    availableRegionGroup.updateCurrentStatistics();
    Assert.assertEquals(
        RegionGroupStatus.Available,
        availableRegionGroup.getCurrentStatistics().getRegionGroupStatus());

    RegionGroupCache disabledRegionGroup0 =
        new RegionGroupCache(DATABASE, Stream.of(0, 1, 2).collect(Collectors.toSet()));
    disabledRegionGroup0.cacheHeartbeatSample(
        0, new RegionHeartbeatSample(currentTime, RegionStatus.Running));
    disabledRegionGroup0.cacheHeartbeatSample(
        1, new RegionHeartbeatSample(currentTime, RegionStatus.ReadOnly));
    disabledRegionGroup0.cacheHeartbeatSample(
        2, new RegionHeartbeatSample(currentTime, RegionStatus.Running));
    disabledRegionGroup0.updateCurrentStatistics();
    Assert.assertEquals(
        RegionGroupStatus.Discouraged,
        disabledRegionGroup0.getCurrentStatistics().getRegionGroupStatus());

    RegionGroupCache disabledRegionGroup1 =
        new RegionGroupCache(DATABASE, Stream.of(0, 1, 2).collect(Collectors.toSet()));
    disabledRegionGroup1.cacheHeartbeatSample(
        0, new RegionHeartbeatSample(currentTime, RegionStatus.Running));
    disabledRegionGroup1.cacheHeartbeatSample(
        1, new RegionHeartbeatSample(currentTime, RegionStatus.Unknown));
    disabledRegionGroup1.cacheHeartbeatSample(
        2, new RegionHeartbeatSample(currentTime, RegionStatus.Unknown));
    disabledRegionGroup1.updateCurrentStatistics();
    Assert.assertEquals(
        RegionGroupStatus.Disabled,
        disabledRegionGroup1.getCurrentStatistics().getRegionGroupStatus());

    RegionGroupCache disabledRegionGroup2 =
        new RegionGroupCache(DATABASE, Stream.of(0, 1, 2).collect(Collectors.toSet()));
    disabledRegionGroup2.cacheHeartbeatSample(
        0, new RegionHeartbeatSample(currentTime, RegionStatus.Running));
    disabledRegionGroup2.cacheHeartbeatSample(
        1, new RegionHeartbeatSample(currentTime, RegionStatus.Running));
    disabledRegionGroup2.cacheHeartbeatSample(
        2, new RegionHeartbeatSample(currentTime, RegionStatus.Removing));
    disabledRegionGroup2.updateCurrentStatistics();
    Assert.assertEquals(
        RegionGroupStatus.Available,
        disabledRegionGroup2.getCurrentStatistics().getRegionGroupStatus());
  }
}
