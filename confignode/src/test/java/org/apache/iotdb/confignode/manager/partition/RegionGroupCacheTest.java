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
import org.apache.iotdb.confignode.manager.partition.heartbeat.RegionGroupCache;
import org.apache.iotdb.confignode.manager.partition.heartbeat.RegionHeartbeatSample;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class RegionGroupCacheTest {

  @Test
  public void getRegionStatusTest() {
    long currentTime = System.currentTimeMillis();
    RegionGroupCache regionGroupCache =
        new RegionGroupCache(new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 1));
    regionGroupCache.cacheHeartbeatSample(
        0, new RegionHeartbeatSample(currentTime, currentTime, RegionStatus.Running));
    regionGroupCache.cacheHeartbeatSample(
        1, new RegionHeartbeatSample(currentTime, currentTime, RegionStatus.Unknown));
    regionGroupCache.cacheHeartbeatSample(
        2, new RegionHeartbeatSample(currentTime, currentTime, RegionStatus.Removing));
    regionGroupCache.cacheHeartbeatSample(
        3, new RegionHeartbeatSample(currentTime, currentTime, RegionStatus.ReadOnly));
    Assert.assertTrue(regionGroupCache.periodicUpdate());

    Assert.assertEquals(RegionStatus.Running, regionGroupCache.getStatistics().getRegionStatus(0));
    Assert.assertEquals(RegionStatus.Unknown, regionGroupCache.getStatistics().getRegionStatus(1));
    Assert.assertEquals(RegionStatus.Removing, regionGroupCache.getStatistics().getRegionStatus(2));
    Assert.assertEquals(RegionStatus.ReadOnly, regionGroupCache.getStatistics().getRegionStatus(3));
  }

  @Test
  public void getRegionGroupStatusTest() {
    long currentTime = System.currentTimeMillis();
    RegionGroupCache runningRegionGroup =
        new RegionGroupCache(new TConsensusGroupId(TConsensusGroupType.DataRegion, 0));
    runningRegionGroup.cacheHeartbeatSample(
        0, new RegionHeartbeatSample(currentTime, currentTime, RegionStatus.Running));
    runningRegionGroup.cacheHeartbeatSample(
        1, new RegionHeartbeatSample(currentTime, currentTime, RegionStatus.Running));
    runningRegionGroup.cacheHeartbeatSample(
        2, new RegionHeartbeatSample(currentTime, currentTime, RegionStatus.Running));
    Assert.assertTrue(runningRegionGroup.periodicUpdate());
    Assert.assertEquals(
        RegionGroupStatus.Running, runningRegionGroup.getStatistics().getRegionGroupStatus());

    RegionGroupCache availableRegionGroup =
        new RegionGroupCache(new TConsensusGroupId(TConsensusGroupType.DataRegion, 1));
    availableRegionGroup.cacheHeartbeatSample(
        0, new RegionHeartbeatSample(currentTime, currentTime, RegionStatus.Running));
    availableRegionGroup.cacheHeartbeatSample(
        1, new RegionHeartbeatSample(currentTime, currentTime, RegionStatus.Unknown));
    availableRegionGroup.cacheHeartbeatSample(
        2, new RegionHeartbeatSample(currentTime, currentTime, RegionStatus.Running));
    Assert.assertTrue(availableRegionGroup.periodicUpdate());
    Assert.assertEquals(
        RegionGroupStatus.Available, availableRegionGroup.getStatistics().getRegionGroupStatus());

    RegionGroupCache disabledRegionGroup0 =
        new RegionGroupCache(new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 2));
    disabledRegionGroup0.cacheHeartbeatSample(
        0, new RegionHeartbeatSample(currentTime, currentTime, RegionStatus.Running));
    disabledRegionGroup0.cacheHeartbeatSample(
        1, new RegionHeartbeatSample(currentTime, currentTime, RegionStatus.ReadOnly));
    disabledRegionGroup0.cacheHeartbeatSample(
        2, new RegionHeartbeatSample(currentTime, currentTime, RegionStatus.Running));
    Assert.assertTrue(disabledRegionGroup0.periodicUpdate());
    Assert.assertEquals(
        RegionGroupStatus.Discouraged, disabledRegionGroup0.getStatistics().getRegionGroupStatus());

    RegionGroupCache disabledRegionGroup1 =
        new RegionGroupCache(new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 3));
    disabledRegionGroup1.cacheHeartbeatSample(
        0, new RegionHeartbeatSample(currentTime, currentTime, RegionStatus.Running));
    disabledRegionGroup1.cacheHeartbeatSample(
        1, new RegionHeartbeatSample(currentTime, currentTime, RegionStatus.Unknown));
    disabledRegionGroup1.cacheHeartbeatSample(
        2, new RegionHeartbeatSample(currentTime, currentTime, RegionStatus.Unknown));
    Assert.assertTrue(disabledRegionGroup1.periodicUpdate());
    Assert.assertEquals(
        RegionGroupStatus.Disabled, disabledRegionGroup1.getStatistics().getRegionGroupStatus());

    RegionGroupCache disabledRegionGroup2 =
        new RegionGroupCache(new TConsensusGroupId(TConsensusGroupType.DataRegion, 4));
    disabledRegionGroup2.cacheHeartbeatSample(
        0, new RegionHeartbeatSample(currentTime, currentTime, RegionStatus.Running));
    disabledRegionGroup2.cacheHeartbeatSample(
        1, new RegionHeartbeatSample(currentTime, currentTime, RegionStatus.Running));
    disabledRegionGroup2.cacheHeartbeatSample(
        2, new RegionHeartbeatSample(currentTime, currentTime, RegionStatus.Removing));
    Assert.assertTrue(disabledRegionGroup2.periodicUpdate());
    Assert.assertEquals(
        RegionGroupStatus.Disabled, disabledRegionGroup2.getStatistics().getRegionGroupStatus());
  }

  @Test
  public void forceUpdateTest() {
    long currentTime = System.currentTimeMillis();
    Map<Integer, RegionHeartbeatSample> heartbeatSampleMap = new HashMap<>();
    for (int i = 0; i < 3; i++) {
      heartbeatSampleMap.put(
          i, new RegionHeartbeatSample(currentTime, currentTime, RegionStatus.Running));
    }

    RegionGroupCache regionGroupCache =
        new RegionGroupCache(new TConsensusGroupId(TConsensusGroupType.DataRegion, 10));
    regionGroupCache.forceUpdate(heartbeatSampleMap);
    Assert.assertEquals(
        RegionGroupStatus.Running, regionGroupCache.getStatistics().getRegionGroupStatus());
  }
}
