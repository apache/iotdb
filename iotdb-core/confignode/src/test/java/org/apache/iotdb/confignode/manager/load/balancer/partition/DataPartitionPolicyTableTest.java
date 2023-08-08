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
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class DataPartitionPolicyTableTest {

  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();
  private static final int SERIES_SLOT_NUM = CONF.getSeriesSlotNum();

  @Test
  public void testUpdateDataAllotTable() {
    DataPartitionPolicyTable dataPartitionPolicyTable = new DataPartitionPolicyTable();
    List<TConsensusGroupId> dataRegionGroups = new ArrayList<>();

    // Test 1: construct DataAllotTable from scratch
    TConsensusGroupId group1 = new TConsensusGroupId(TConsensusGroupType.DataRegion, 1);
    dataRegionGroups.add(group1);
    dataPartitionPolicyTable.reBalanceDataPartitionPolicy(dataRegionGroups);
    for (int i = 0; i < SERIES_SLOT_NUM; i++) {
      TSeriesPartitionSlot seriesPartitionSlot = new TSeriesPartitionSlot(i);
      // All SeriesPartitionSlots belong to group1
      Assert.assertEquals(
          group1,
          dataPartitionPolicyTable.getRegionGroupIdOrActivateIfNecessary(seriesPartitionSlot));
    }

    // Test2: extend DataRegionGroups
    Map<TSeriesPartitionSlot, TConsensusGroupId> lastDataAllotTable = new HashMap<>();
    dataRegionGroups.add(new TConsensusGroupId(TConsensusGroupType.DataRegion, 2));
    dataRegionGroups.add(new TConsensusGroupId(TConsensusGroupType.DataRegion, 3));
    dataPartitionPolicyTable.reBalanceDataPartitionPolicy(dataRegionGroups);
    int mu = SERIES_SLOT_NUM / 3;
    Map<TConsensusGroupId, AtomicInteger> counter = new HashMap<>();
    for (int i = 0; i < SERIES_SLOT_NUM; i++) {
      TSeriesPartitionSlot seriesPartitionSlot = new TSeriesPartitionSlot(i);
      TConsensusGroupId groupId =
          dataPartitionPolicyTable.getRegionGroupIdOrActivateIfNecessary(seriesPartitionSlot);
      lastDataAllotTable.put(seriesPartitionSlot, groupId);
      counter.computeIfAbsent(groupId, empty -> new AtomicInteger(0)).incrementAndGet();
    }
    // All DataRegionGroups divide SeriesPartitionSlots evenly
    for (Map.Entry<TConsensusGroupId, AtomicInteger> counterEntry : counter.entrySet()) {
      Assert.assertTrue(Math.abs(counterEntry.getValue().get() - mu) <= 1);
    }

    // Test 3: extend DataRegionGroups while inherit as much SeriesPartitionSlots as possible
    dataRegionGroups.add(new TConsensusGroupId(TConsensusGroupType.DataRegion, 4));
    dataRegionGroups.add(new TConsensusGroupId(TConsensusGroupType.DataRegion, 5));
    dataPartitionPolicyTable.reBalanceDataPartitionPolicy(dataRegionGroups);
    Map<TConsensusGroupId, AtomicInteger> unchangedSlots = new HashMap<>();
    mu = SERIES_SLOT_NUM / 5;
    counter.clear();
    for (int i = 0; i < SERIES_SLOT_NUM; i++) {
      TSeriesPartitionSlot seriesPartitionSlot = new TSeriesPartitionSlot(i);
      TConsensusGroupId groupId =
          dataPartitionPolicyTable.getRegionGroupIdOrActivateIfNecessary(seriesPartitionSlot);
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
    // Most of SeriesPartitionSlots in the first three DataRegionGroups should remain unchanged
    for (Map.Entry<TConsensusGroupId, AtomicInteger> counterEntry : unchangedSlots.entrySet()) {
      Assert.assertEquals(mu, counterEntry.getValue().get());
    }
  }
}
