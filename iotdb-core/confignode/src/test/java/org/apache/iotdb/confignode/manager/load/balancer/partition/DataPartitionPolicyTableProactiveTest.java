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
import org.apache.iotdb.confignode.manager.partition.RegionGroupExtensionPolicy;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class DataPartitionPolicyTableProactiveTest {

  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();
  private RegionGroupExtensionPolicy originalPolicy;
  private int originalSlotCount;

  @Before
  public void setUp() {
    originalPolicy = CONF.getDataRegionGroupExtensionPolicy();
    originalSlotCount = CONF.getSeriesSlotNum();
    CONF.setDataRegionGroupExtensionPolicy(RegionGroupExtensionPolicy.PROACTIVE);
    CONF.setSeriesSlotNum(1000);
  }

  @After
  public void tearDown() {
    CONF.setDataRegionGroupExtensionPolicy(originalPolicy);
    CONF.setSeriesSlotNum(originalSlotCount);
  }

  @Test
  public void testSixExistingSlotsUseNewGroupsAfterSwitchingFromAuto() {
    DataPartitionPolicyTable table = new DataPartitionPolicyTable();
    CONF.setDataRegionGroupExtensionPolicy(RegionGroupExtensionPolicy.AUTO);
    table.reBalanceDataPartitionPolicy(groups(2));
    Assert.assertEquals(2, new HashSet<>(activate(table, 6).values()).size());

    CONF.setDataRegionGroupExtensionPolicy(RegionGroupExtensionPolicy.PROACTIVE);
    table.reBalanceDataPartitionPolicy(groups(6));
    assertBalanced(groups(6), activate(table, 6));
  }

  @Test
  public void testCustomExpansionIsRebalancedWhenEnteringProactive() {
    CONF.setDataRegionGroupExtensionPolicy(RegionGroupExtensionPolicy.CUSTOM);
    DataPartitionPolicyTable table = new DataPartitionPolicyTable();
    table.reBalanceDataPartitionPolicy(groups(2));
    Map<TSeriesPartitionSlot, TConsensusGroupId> original = activate(table, 6);
    table.reBalanceDataPartitionPolicy(groups(6));
    Assert.assertEquals(original, activate(table, 6));

    CONF.setDataRegionGroupExtensionPolicy(RegionGroupExtensionPolicy.PROACTIVE);
    table.reBalanceDataPartitionPolicy(groups(6));
    assertBalanced(groups(6), activate(table, 6));
  }

  @Test
  public void testUnevenSlotCountStillUsesEveryNewGroup() {
    DataPartitionPolicyTable table = new DataPartitionPolicyTable();
    table.reBalanceDataPartitionPolicy(groups(2));
    activate(table, 5);
    table.reBalanceDataPartitionPolicy(groups(4));
    assertBalanced(groups(4), activate(table, 5));
  }

  @Test
  public void testGrowingAheadOfPendingSlotsPreservesExistingAssignments() {
    DataPartitionPolicyTable table = new DataPartitionPolicyTable();
    for (int slot = 0; slot < 6; slot++) {
      Map<TSeriesPartitionSlot, TConsensusGroupId> existing = activate(table, slot);
      table.reBalanceDataPartitionPolicy(groups(slot + 1));
      Assert.assertEquals(existing, activate(table, slot));
      assertBalanced(groups(slot + 1), activate(table, slot + 1));
    }
  }

  @Test
  public void testLeaderRecoveryRebalancesSparseSlotsAndIgnoresEmptyEntries() {
    DataPartitionPolicyTable table = new DataPartitionPolicyTable();
    table.reBalanceDataPartitionPolicy(groups(6));
    Map<TSeriesPartitionSlot, TConsensusGroupId> previous = new HashMap<>();
    for (int slot = 0; slot < 6; slot++) {
      previous.put(new TSeriesPartitionSlot(slot), groups(2).get(slot % 2));
    }
    // DataPartitionTable can retain empty entries after filtering or time-partition cleanup.
    for (int slot = 6; slot < 100; slot++) {
      previous.put(new TSeriesPartitionSlot(slot), null);
    }
    table.setDataAllotMap(previous);
    assertBalanced(groups(6), activate(table, 6));
  }

  @Test
  public void testLeaderRecoveryPreservesAlreadyBalancedAssignments() {
    DataPartitionPolicyTable original = new DataPartitionPolicyTable();
    original.reBalanceDataPartitionPolicy(groups(6));
    Map<TSeriesPartitionSlot, TConsensusGroupId> previous = activate(original, 6);
    DataPartitionPolicyTable recovered = new DataPartitionPolicyTable();
    recovered.reBalanceDataPartitionPolicy(groups(6));
    recovered.setDataAllotMap(previous);
    Assert.assertEquals(previous, activate(recovered, 6));
  }

  @Test
  public void testAutoAndCustomKeepExistingSparseAssignments() {
    for (RegionGroupExtensionPolicy policy :
        Arrays.asList(RegionGroupExtensionPolicy.AUTO, RegionGroupExtensionPolicy.CUSTOM)) {
      CONF.setDataRegionGroupExtensionPolicy(policy);
      DataPartitionPolicyTable table = new DataPartitionPolicyTable();
      table.reBalanceDataPartitionPolicy(groups(2));
      Map<TSeriesPartitionSlot, TConsensusGroupId> previous = activate(table, 6);
      table.reBalanceDataPartitionPolicy(groups(6));
      Assert.assertEquals(previous, activate(table, 6));

      DataPartitionPolicyTable recovered = new DataPartitionPolicyTable();
      recovered.reBalanceDataPartitionPolicy(groups(6));
      recovered.setDataAllotMap(previous);
      Assert.assertEquals(previous, activate(recovered, 6));
    }
  }

  @Test
  public void testEmptyRecoveryCanActivateFirstSlot() {
    DataPartitionPolicyTable table = new DataPartitionPolicyTable();
    table.reBalanceDataPartitionPolicy(Collections.emptyList());
    table.setDataAllotMap(Collections.emptyMap());
    table.reBalanceDataPartitionPolicy(groups(1));
    table.setDataAllotMap(Collections.emptyMap());
    assertBalanced(groups(1), activate(table, 1));
  }

  private static List<TConsensusGroupId> groups(int count) {
    List<TConsensusGroupId> groups = new ArrayList<>();
    for (int id = 0; id < count; id++) {
      groups.add(new TConsensusGroupId(TConsensusGroupType.DataRegion, id));
    }
    return groups;
  }

  private static Map<TSeriesPartitionSlot, TConsensusGroupId> activate(
      DataPartitionPolicyTable table, int slotCount) {
    Map<TSeriesPartitionSlot, TConsensusGroupId> assignments = new HashMap<>();
    for (int id = 0; id < slotCount; id++) {
      TSeriesPartitionSlot slot = new TSeriesPartitionSlot(id);
      assignments.put(slot, table.getRegionGroupIdOrActivateIfNecessary(slot));
    }
    return assignments;
  }

  private static void assertBalanced(
      List<TConsensusGroupId> groups, Map<TSeriesPartitionSlot, TConsensusGroupId> assignments) {
    Map<TConsensusGroupId, Integer> counts = new HashMap<>();
    assignments.values().forEach(group -> counts.merge(group, 1, Integer::sum));
    Assert.assertEquals(new HashSet<>(groups), counts.keySet());
    Assert.assertTrue(
        counts.toString(),
        Collections.max(counts.values()) - Collections.min(counts.values()) <= 1);
  }
}
