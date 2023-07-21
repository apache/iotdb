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

package org.apache.iotdb.confignode.manager.load.balancer.partition;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class DataAllotTableTest {

  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();
  private static final int SERIES_SLOT_NUM = CONF.getSeriesSlotNum();

  @Test
  public void testUpdateCurrentTimePartition() {
    final int regionGroupNum = 5;
    final int threshold = DataAllotTable.timePartitionThreshold(regionGroupNum);
    final long timePartitionInterval = 1000;
    DataAllotTable dataAllotTable = new DataAllotTable();

    // Test 1: currentTimePartition is the first one
    TTimePartitionSlot nextTimePartition = new TTimePartitionSlot(1000);
    Map<TTimePartitionSlot, Integer> timePartitionCountMap = new HashMap<>();
    timePartitionCountMap.put(new TTimePartitionSlot(nextTimePartition), threshold);
    timePartitionCountMap.put(
        new TTimePartitionSlot(nextTimePartition.getStartTime() + timePartitionInterval),
        threshold - 100);
    timePartitionCountMap.put(
        new TTimePartitionSlot(nextTimePartition.getStartTime() + 2 * timePartitionInterval),
        threshold - 200);
    dataAllotTable.addTimePartitionCount(timePartitionCountMap);
    dataAllotTable.updateCurrentTimePartition(regionGroupNum);
    Assert.assertEquals(nextTimePartition, dataAllotTable.getCurrentTimePartition());

    // Test 2: currentTimePartition in the middle
    timePartitionCountMap.clear();
    nextTimePartition = new TTimePartitionSlot(5000);
    timePartitionCountMap.put(
        new TTimePartitionSlot(nextTimePartition.getStartTime() - timePartitionInterval),
        threshold - 100);
    timePartitionCountMap.put(new TTimePartitionSlot(nextTimePartition), threshold);
    timePartitionCountMap.put(
        new TTimePartitionSlot(nextTimePartition.getStartTime() + timePartitionInterval),
        threshold - 100);
    dataAllotTable.addTimePartitionCount(timePartitionCountMap);
    dataAllotTable.updateCurrentTimePartition(regionGroupNum);
    Assert.assertEquals(nextTimePartition, dataAllotTable.getCurrentTimePartition());

    // Test 3: currentTimePartition will be the maximum timePartitionSlot that greater or equal to
    // threshold
    int offset = 200;
    Random random = new Random();
    timePartitionCountMap.clear();
    TTimePartitionSlot baseSlot = new TTimePartitionSlot(10000);
    nextTimePartition = baseSlot;
    timePartitionCountMap.put(nextTimePartition, threshold);
    for (int i = 1; i < 100; i++) {
      TTimePartitionSlot slot =
          new TTimePartitionSlot(baseSlot.getStartTime() + i * timePartitionInterval);
      int count = threshold + random.nextInt(offset) - offset / 2;
      timePartitionCountMap.put(slot, count);
      if (count >= threshold) {
        nextTimePartition = slot;
      }
    }
    dataAllotTable.addTimePartitionCount(timePartitionCountMap);
    dataAllotTable.updateCurrentTimePartition(regionGroupNum);
    Assert.assertEquals(nextTimePartition, dataAllotTable.getCurrentTimePartition());
  }

  @Test
  public void testUpdateDataAllotTable() {
    DataAllotTable dataAllotTable = new DataAllotTable();
    List<TConsensusGroupId> dataRegionGroups = new ArrayList<>();

    // Test 1: construct DataAllotTable from scratch
    TConsensusGroupId group1 = new TConsensusGroupId(TConsensusGroupType.DataRegion, 1);
    dataRegionGroups.add(group1);
    dataAllotTable.updateDataAllotTable(dataRegionGroups, new HashMap<>());
    for (int i = 0; i < SERIES_SLOT_NUM; i++) {
      TSeriesPartitionSlot seriesPartitionSlot = new TSeriesPartitionSlot(i);
      // All SeriesPartitionSlots belong to group1
      Assert.assertEquals(group1, dataAllotTable.getRegionGroupId(seriesPartitionSlot));
    }

    // Test2: extend DataRegionGroups
    Map<TSeriesPartitionSlot, TConsensusGroupId> lastDataAllotTable = new HashMap<>();
    dataRegionGroups.add(new TConsensusGroupId(TConsensusGroupType.DataRegion, 2));
    dataRegionGroups.add(new TConsensusGroupId(TConsensusGroupType.DataRegion, 3));
    dataAllotTable.updateDataAllotTable(dataRegionGroups, new HashMap<>());
    int mu = SERIES_SLOT_NUM / 3;
    Map<TConsensusGroupId, AtomicInteger> counter = new HashMap<>();
    for (int i = 0; i < SERIES_SLOT_NUM; i++) {
      TSeriesPartitionSlot seriesPartitionSlot = new TSeriesPartitionSlot(i);
      TConsensusGroupId groupId = dataAllotTable.getRegionGroupId(seriesPartitionSlot);
      lastDataAllotTable.put(seriesPartitionSlot, groupId);
      counter.computeIfAbsent(groupId, empty -> new AtomicInteger(0)).incrementAndGet();
    }
    // All DataRegionGroups divide SeriesPartitionSlots evenly
    for (Map.Entry<TConsensusGroupId, AtomicInteger> counterEntry : counter.entrySet()) {
      Assert.assertTrue(Math.abs(counterEntry.getValue().get() - mu) <= 1);
    }

    // Test 3: extend DataRegionGroups while inherit future allocate result
    dataRegionGroups.add(new TConsensusGroupId(TConsensusGroupType.DataRegion, 4));
    dataRegionGroups.add(new TConsensusGroupId(TConsensusGroupType.DataRegion, 5));
    Random random = new Random();
    Map<TSeriesPartitionSlot, TConsensusGroupId> allocatedTable = new HashMap<>();
    Map<TConsensusGroupId, AtomicInteger> unchangedSlots = new HashMap<>();
    for (int i = 0; i < 50; i++) {
      TSeriesPartitionSlot seriesPartitionSlot =
          new TSeriesPartitionSlot(random.nextInt(SERIES_SLOT_NUM));
      while (allocatedTable.containsKey(seriesPartitionSlot)) {
        seriesPartitionSlot = new TSeriesPartitionSlot(random.nextInt(SERIES_SLOT_NUM));
      }
      allocatedTable.put(
          seriesPartitionSlot,
          new TConsensusGroupId(TConsensusGroupType.DataRegion, random.nextInt(2) + 4));
    }
    dataAllotTable.updateDataAllotTable(dataRegionGroups, allocatedTable);
    mu = SERIES_SLOT_NUM / 5;
    counter.clear();
    for (int i = 0; i < SERIES_SLOT_NUM; i++) {
      TSeriesPartitionSlot seriesPartitionSlot = new TSeriesPartitionSlot(i);
      TConsensusGroupId groupId = dataAllotTable.getRegionGroupId(seriesPartitionSlot);
      counter.computeIfAbsent(groupId, empty -> new AtomicInteger(0)).incrementAndGet();
      if (groupId.getId() < 4) {
        // Most of SeriesPartitionSlots in the first three DataRegionGroups should remain unchanged
        Assert.assertEquals(lastDataAllotTable.get(seriesPartitionSlot), groupId);
        unchangedSlots.computeIfAbsent(groupId, empty -> new AtomicInteger(0)).incrementAndGet();
      }
    }
    // All DataRegionGroups divide SeriesPartitionSlots evenly
    for (Map.Entry<TConsensusGroupId, AtomicInteger> counterEntry : counter.entrySet()) {
      Assert.assertTrue(Math.abs(counterEntry.getValue().get() - mu) <= 1);
    }
    // All SeriesPartitionSlots that have been allocated before should be allocated to the same
    // DataRegionGroup
    allocatedTable.forEach(
        (seriesPartitionSlot, groupId) ->
            Assert.assertEquals(groupId, dataAllotTable.getRegionGroupId(seriesPartitionSlot)));
    // Most of SeriesPartitionSlots in the first three DataRegionGroups should remain unchanged
    for (Map.Entry<TConsensusGroupId, AtomicInteger> counterEntry : unchangedSlots.entrySet()) {
      Assert.assertEquals(mu, counterEntry.getValue().get());
    }
  }
}
