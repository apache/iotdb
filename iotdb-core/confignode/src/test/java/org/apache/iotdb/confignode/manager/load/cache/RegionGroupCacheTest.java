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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
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
  private static final TConsensusGroupId GROUP_ID =
      new TConsensusGroupId(TConsensusGroupType.DataRegion, 1);

  @Test
  public void getRegionStatusTest() {
    long currentTime = System.nanoTime();
    RegionGroupCache regionGroupCache =
        new RegionGroupCache(
            DATABASE, GROUP_ID, Stream.of(0, 1, 2, 3, 4).collect(Collectors.toSet()), false);
    regionGroupCache.cacheHeartbeatSample(
        0, new RegionHeartbeatSample(currentTime, RegionStatus.Running));
    regionGroupCache.cacheHeartbeatSample(
        1, new RegionHeartbeatSample(currentTime, RegionStatus.Unknown));
    regionGroupCache.cacheHeartbeatSample(
        2, new RegionHeartbeatSample(currentTime, RegionStatus.Removing));
    regionGroupCache.cacheHeartbeatSample(
        3, new RegionHeartbeatSample(currentTime, RegionStatus.ReadOnly));
    regionGroupCache.cacheHeartbeatSample(
        4, new RegionHeartbeatSample(currentTime, RegionStatus.Adding));
    regionGroupCache.updateCurrentStatistics();

    Assert.assertEquals(
        RegionStatus.Running, regionGroupCache.getCurrentStatistics().getRegionStatus(0));
    Assert.assertEquals(
        RegionStatus.Unknown, regionGroupCache.getCurrentStatistics().getRegionStatus(1));
    Assert.assertEquals(
        RegionStatus.Removing, regionGroupCache.getCurrentStatistics().getRegionStatus(2));
    Assert.assertEquals(
        RegionStatus.ReadOnly, regionGroupCache.getCurrentStatistics().getRegionStatus(3));
    Assert.assertEquals(
        RegionStatus.Adding, regionGroupCache.getCurrentStatistics().getRegionStatus(4));
  }

  @Test
  public void weakConsistencyRegionGroupStatusTest() {
    long currentTime = System.nanoTime();
    RegionGroupCache regionGroupCache =
        new RegionGroupCache(
            DATABASE, GROUP_ID, Stream.of(0, 1, 2).collect(Collectors.toSet()), false);
    regionGroupCache.cacheHeartbeatSample(
        0, new RegionHeartbeatSample(currentTime, RegionStatus.Running));
    regionGroupCache.cacheHeartbeatSample(
        1, new RegionHeartbeatSample(currentTime, RegionStatus.Running));
    regionGroupCache.cacheHeartbeatSample(
        2, new RegionHeartbeatSample(currentTime, RegionStatus.Running));
    regionGroupCache.updateCurrentStatistics();
    Assert.assertEquals(
        RegionGroupStatus.Running, regionGroupCache.getCurrentStatistics().getRegionGroupStatus());

    regionGroupCache.cacheHeartbeatSample(
        0, new RegionHeartbeatSample(currentTime, RegionStatus.Unknown));
    regionGroupCache.updateCurrentStatistics();
    Assert.assertEquals(
        RegionGroupStatus.Available,
        regionGroupCache.getCurrentStatistics().getRegionGroupStatus());

    regionGroupCache.cacheHeartbeatSample(
        1, new RegionHeartbeatSample(currentTime, RegionStatus.Unknown));
    regionGroupCache.updateCurrentStatistics();
    Assert.assertEquals(
        RegionGroupStatus.Available,
        regionGroupCache.getCurrentStatistics().getRegionGroupStatus());

    regionGroupCache.cacheHeartbeatSample(
        2, new RegionHeartbeatSample(currentTime, RegionStatus.Unknown));
    regionGroupCache.updateCurrentStatistics();
    Assert.assertEquals(
        RegionGroupStatus.Disabled, regionGroupCache.getCurrentStatistics().getRegionGroupStatus());
  }

  @Test
  public void strongConsistencyRegionGroupStatusTest() {
    long currentTime = System.nanoTime();
    RegionGroupCache regionGroupCache =
        new RegionGroupCache(
            DATABASE, GROUP_ID, Stream.of(0, 1, 2).collect(Collectors.toSet()), true);
    regionGroupCache.cacheHeartbeatSample(
        0, new RegionHeartbeatSample(currentTime, RegionStatus.Running));
    regionGroupCache.cacheHeartbeatSample(
        1, new RegionHeartbeatSample(currentTime, RegionStatus.Running));
    regionGroupCache.cacheHeartbeatSample(
        2, new RegionHeartbeatSample(currentTime, RegionStatus.Running));
    regionGroupCache.updateCurrentStatistics();
    Assert.assertEquals(
        RegionGroupStatus.Running, regionGroupCache.getCurrentStatistics().getRegionGroupStatus());

    regionGroupCache.cacheHeartbeatSample(
        0, new RegionHeartbeatSample(currentTime, RegionStatus.Unknown));
    regionGroupCache.updateCurrentStatistics();
    Assert.assertEquals(
        RegionGroupStatus.Available,
        regionGroupCache.getCurrentStatistics().getRegionGroupStatus());

    regionGroupCache.cacheHeartbeatSample(
        1, new RegionHeartbeatSample(currentTime, RegionStatus.Unknown));
    regionGroupCache.updateCurrentStatistics();
    Assert.assertEquals(
        RegionGroupStatus.Disabled, regionGroupCache.getCurrentStatistics().getRegionGroupStatus());

    regionGroupCache.cacheHeartbeatSample(
        2, new RegionHeartbeatSample(currentTime, RegionStatus.Unknown));
    regionGroupCache.updateCurrentStatistics();
    Assert.assertEquals(
        RegionGroupStatus.Disabled, regionGroupCache.getCurrentStatistics().getRegionGroupStatus());
  }

  @Test
  public void migrateRegionRegionGroupStatusTest() {
    long currentTime = System.nanoTime();
    RegionGroupCache regionGroupCache =
        new RegionGroupCache(DATABASE, GROUP_ID, Stream.of(0).collect(Collectors.toSet()), true);
    regionGroupCache.cacheHeartbeatSample(
        0, new RegionHeartbeatSample(currentTime, RegionStatus.Running));
    regionGroupCache.updateCurrentStatistics();
    Assert.assertEquals(
        RegionGroupStatus.Running, regionGroupCache.getCurrentStatistics().getRegionGroupStatus());

    regionGroupCache =
        new RegionGroupCache(DATABASE, GROUP_ID, Stream.of(0, 1).collect(Collectors.toSet()), true);
    regionGroupCache.cacheHeartbeatSample(
        0, new RegionHeartbeatSample(currentTime, RegionStatus.Running));
    regionGroupCache.cacheHeartbeatSample(
        1, new RegionHeartbeatSample(currentTime, RegionStatus.Adding));
    regionGroupCache.updateCurrentStatistics();
    Assert.assertEquals(
        RegionGroupStatus.Running, regionGroupCache.getCurrentStatistics().getRegionGroupStatus());

    regionGroupCache =
        new RegionGroupCache(DATABASE, GROUP_ID, Stream.of(0, 1).collect(Collectors.toSet()), true);
    regionGroupCache.cacheHeartbeatSample(
        0, new RegionHeartbeatSample(currentTime, RegionStatus.Running));
    regionGroupCache.cacheHeartbeatSample(
        1, new RegionHeartbeatSample(currentTime, RegionStatus.Removing));
    regionGroupCache.updateCurrentStatistics();
    Assert.assertEquals(
        RegionGroupStatus.Running, regionGroupCache.getCurrentStatistics().getRegionGroupStatus());
  }
}
