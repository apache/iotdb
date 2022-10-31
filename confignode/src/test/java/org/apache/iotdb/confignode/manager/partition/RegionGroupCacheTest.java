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

import org.junit.Assert;
import org.junit.Test;

public class RegionGroupCacheTest {

  @Test
  public void getLeaderDataNodeIdTest() {
    final int leaderId = 1;
    long currentTime = System.currentTimeMillis();
    RegionGroupCache regionGroupCache =
        new RegionGroupCache(new TConsensusGroupId(TConsensusGroupType.DataRegion, 0));
    for (int i = 0; i < 3; i++) {
      regionGroupCache.cacheHeartbeatSample(
          i,
          new RegionHeartbeatSample(currentTime, currentTime, i == leaderId, RegionStatus.Running));
    }
    Assert.assertNotNull(regionGroupCache.updateRegionGroupStatistics());
    Assert.assertEquals(leaderId, regionGroupCache.getStatistics().getLeaderDataNodeId());
  }

  @Test
  public void getRegionStatusTest() {
    long currentTime = System.currentTimeMillis();
    RegionGroupCache regionGroupCache =
        new RegionGroupCache(new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 1));
    regionGroupCache.cacheHeartbeatSample(
        0, new RegionHeartbeatSample(currentTime, currentTime, false, RegionStatus.Running));
    regionGroupCache.cacheHeartbeatSample(
        1, new RegionHeartbeatSample(currentTime, currentTime, false, RegionStatus.Unknown));
    regionGroupCache.cacheHeartbeatSample(
        2, new RegionHeartbeatSample(currentTime, currentTime, false, RegionStatus.Removing));
    regionGroupCache.cacheHeartbeatSample(
        3, new RegionHeartbeatSample(currentTime, currentTime, false, RegionStatus.ReadOnly));
    regionGroupCache.updateRegionGroupStatistics();

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
        0, new RegionHeartbeatSample(currentTime, currentTime, false, RegionStatus.Running));
    runningRegionGroup.cacheHeartbeatSample(
        1, new RegionHeartbeatSample(currentTime, currentTime, false, RegionStatus.Running));
    runningRegionGroup.cacheHeartbeatSample(
        2, new RegionHeartbeatSample(currentTime, currentTime, false, RegionStatus.Running));
    runningRegionGroup.updateRegionGroupStatistics();
    Assert.assertEquals(
        RegionGroupStatus.Running, runningRegionGroup.getStatistics().getRegionGroupStatus());

    RegionGroupCache availableRegionGroup =
        new RegionGroupCache(new TConsensusGroupId(TConsensusGroupType.DataRegion, 1));
    availableRegionGroup.cacheHeartbeatSample(
        0, new RegionHeartbeatSample(currentTime, currentTime, false, RegionStatus.Running));
    availableRegionGroup.cacheHeartbeatSample(
        1, new RegionHeartbeatSample(currentTime, currentTime, false, RegionStatus.Unknown));
    availableRegionGroup.cacheHeartbeatSample(
        2, new RegionHeartbeatSample(currentTime, currentTime, false, RegionStatus.Running));
    availableRegionGroup.updateRegionGroupStatistics();
    Assert.assertEquals(
        RegionGroupStatus.Available, availableRegionGroup.getStatistics().getRegionGroupStatus());

    RegionGroupCache disabledRegionGroup0 =
        new RegionGroupCache(new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 2));
    disabledRegionGroup0.cacheHeartbeatSample(
        0, new RegionHeartbeatSample(currentTime, currentTime, false, RegionStatus.Running));
    disabledRegionGroup0.cacheHeartbeatSample(
        1, new RegionHeartbeatSample(currentTime, currentTime, false, RegionStatus.ReadOnly));
    disabledRegionGroup0.cacheHeartbeatSample(
        2, new RegionHeartbeatSample(currentTime, currentTime, false, RegionStatus.Running));
    disabledRegionGroup0.updateRegionGroupStatistics();
    Assert.assertEquals(
        RegionGroupStatus.Disabled, disabledRegionGroup0.getStatistics().getRegionGroupStatus());

    RegionGroupCache disabledRegionGroup1 =
        new RegionGroupCache(new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 3));
    disabledRegionGroup1.cacheHeartbeatSample(
        0, new RegionHeartbeatSample(currentTime, currentTime, false, RegionStatus.Running));
    disabledRegionGroup1.cacheHeartbeatSample(
        1, new RegionHeartbeatSample(currentTime, currentTime, false, RegionStatus.Unknown));
    disabledRegionGroup1.cacheHeartbeatSample(
        2, new RegionHeartbeatSample(currentTime, currentTime, false, RegionStatus.Unknown));
    disabledRegionGroup1.updateRegionGroupStatistics();
    Assert.assertEquals(
        RegionGroupStatus.Disabled, disabledRegionGroup1.getStatistics().getRegionGroupStatus());

    RegionGroupCache disabledRegionGroup2 =
        new RegionGroupCache(new TConsensusGroupId(TConsensusGroupType.DataRegion, 4));
    disabledRegionGroup2.cacheHeartbeatSample(
        0, new RegionHeartbeatSample(currentTime, currentTime, false, RegionStatus.Running));
    disabledRegionGroup2.cacheHeartbeatSample(
        1, new RegionHeartbeatSample(currentTime, currentTime, false, RegionStatus.Running));
    disabledRegionGroup2.cacheHeartbeatSample(
        2, new RegionHeartbeatSample(currentTime, currentTime, false, RegionStatus.Removing));
    disabledRegionGroup2.updateRegionGroupStatistics();
    Assert.assertEquals(
        RegionGroupStatus.Disabled, disabledRegionGroup2.getStatistics().getRegionGroupStatus());
  }
}
