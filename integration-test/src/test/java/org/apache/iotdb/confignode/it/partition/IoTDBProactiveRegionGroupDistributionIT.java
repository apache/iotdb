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

package org.apache.iotdb.confignode.it.partition;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.cluster.RegionStatus;
import org.apache.iotdb.commons.partition.executor.hash.BKDRHashExecutor;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchemaResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetDatabaseReq;
import org.apache.iotdb.confignode.rpc.thrift.TRegionInfo;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionResp;
import org.apache.iotdb.confignode.rpc.thrift.TTimeSlotList;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.commons.schema.SchemaConstant.ALL_MATCH_SCOPE_BINARY;
import static org.apache.iotdb.confignode.it.utils.ConfigNodeTestUtils.generatePatternTreeBuffer;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBProactiveRegionGroupDistributionIT {

  private static final String AUTO_DATABASE = "root.auto";
  private static final String PROACTIVE_DATABASE = "root.proactive";
  private static final String CUSTOM_DATABASE = "root.custom";
  // 1000 possible hash slots, but the six devices deliberately activate only slots 0 through 5.
  // Each device has one measurement; a new time partition adds no new series slot.
  private static final int SERIES_SLOT_NUM = 1000;
  private static final int DEVICE_COUNT = 6;
  // Group counts below are logical groups: G groups correspond to 3 * G replica rows.
  private static final int REPLICATION_FACTOR = 3;
  private static final long TIME_PARTITION_INTERVAL = 10;
  private static final BKDRHashExecutor PARTITION_EXECUTOR = new BKDRHashExecutor(SERIES_SLOT_NUM);

  /**
   * Clean up the test cluster; for example, stop one ConfigNode and three DataNodes after a 1C3D
   * case.
   */
  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  /**
   * Verify six distinct slots on 1C3D: PROACTIVE uses three groups per type with two slots each;
   * AUTO uses one Schema group and two Data groups.
   */
  @Test
  public void testSixDevicesOnThreeDataNodes() throws Exception {
    // Two databases: cap = 3 groups per type per database. PROACTIVE: 6 slots / 3 groups = 2.
    checkSparseDeviceDistribution(3);
  }

  /**
   * Verify six distinct slots on 1C6D: PROACTIVE uses six groups per type with one slot each,
   * placing 18 replicas as three per node.
   */
  @Test
  public void testSixDevicesOnSixDataNodes() throws Exception {
    // Cap = 6; PROACTIVE uses 6 groups per type, each owning one of the six slots.
    checkSparseDeviceDistribution(6);
  }

  /**
   * Verify sparse allocation on 1C9D: six active slots create only six groups despite a cap of
   * nine, placing 18 replicas as two per node.
   */
  @Test
  public void testSixDevicesOnNineDataNodes() throws Exception {
    // Cap = 9, but only 6 slots are active: PROACTIVE stops at 6 groups per type.
    // Each type has 6 * 3 = 18 replicas, balanced across 9 DataNodes: 2 replicas per node.
    checkSparseDeviceDistribution(9);
  }

  /**
   * Verify AUTO-to-PROACTIVE redistribution: two groups with three slots each become six groups
   * with one slot each. Each old group retains one slot and each of four new groups receives one.
   * Also verify historical mappings and SQL values.
   */
  @Test
  public void testAutoToProactiveHotReloadRedistributesSixSlots() throws Exception {
    // Step 1: one database on 1C3D; AUTO starts with minimums of 1 schema group / 2 data groups.
    initAutoCluster(3);
    try (SyncConfigNodeIServiceClient client =
            (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection();
        Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // Expect successful creation (200): Schema/Data minimums 1/2 are valid, and three nodes
      // support three replicas.
      assertSuccess(
          client.setDatabase(
              new TDatabaseSchema(AUTO_DATABASE)
                  .setMinSchemaRegionGroupNum(1)
                  .setMinDataRegionGroupNum(2)));
      // Cap per type = ceil(6 groups per node * 3 nodes / (1 database * 3 replicas)) = 6.
      // Expect both caps to be 6: ceil(6*3/(1 database*3 replicas))=6.
      assertMaximum(client, AUTO_DATABASE, DEVICE_COUNT);
      // Creates 6 devices / measurements / series slots and writes t=0 and t=10.
      // Result: 1 schema group, 2 data groups, 6 schema partitions and 6 * 2 = 12 data partitions.
      checkSixDevices(client, statement, AUTO_DATABASE, 3, false);
      List<String> devices = generateDeviceNamesForSlots(AUTO_DATABASE, DEVICE_COUNT);
      Set<TConsensusGroupId> oldGroups =
          // Expect 2 Data groups and 6 replicas: AUTO has reached its minimum; six sparse slots
          // do not trigger further growth.
          assertRegionGroupsAndReplicas(client, AUTO_DATABASE, TConsensusGroupType.DataRegion, 2)
              .keySet();
      Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TConsensusGroupId>>> original =
          readDataPartitions(client, AUTO_DATABASE, devices, 0);
      List<TConsensusGroupId> originalAssignments = groupsAtTime(original, 0);
      oldGroups.forEach(
          // Expect 3 slots per old group: six distinct slots are evenly split between two AUTO
          // groups, 6/2=3.
          group -> Assert.assertEquals(3, Collections.frequency(originalAssignments, group)));

      // Step 2: t=20 raises the data-partition count to 6 * 3 = 18, still only 6 series slots.
      // AUTO keeps 2 data groups with 3 slots each; verify the same slot-to-group routing as t=0.
      writeAtTime(statement, devices, 2 * TIME_PARTITION_INTERVAL, 200);
      // Expect the same two group IDs: adding only a time partition does not make AUTO extend
      // or replace groups.
      Assert.assertEquals(
          oldGroups,
          // Expect 2 Data groups: t=20 adds no series slots, so the active-slot count remains
          // 6.
          assertRegionGroupsAndReplicas(client, AUTO_DATABASE, TConsensusGroupType.DataRegion, 2)
              .keySet());
      Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TConsensusGroupId>>> beforeReload =
          readDataPartitions(client, AUTO_DATABASE, devices, 2 * TIME_PARTITION_INTERVAL);
      original.forEach(
          (slot, times) ->
              // Expect the same owner at t=20 and t=0: AUTO has not extended, so the routing
              // stays unchanged.
              Assert.assertEquals(
                  times.get(new TTimePartitionSlot(0)),
                  beforeReload.get(slot).get(new TTimePartitionSlot(2 * TIME_PARTITION_INTERVAL))));

      // Step 3: hot-reload only the data policy. Schema stays AUTO; no node is restarted.
      statement.execute("SET CONFIGURATION 'data_region_group_extension_policy'='PROACTIVE'");
      // Reload alone leaves 2 data groups. The first missing partition at t=30 triggers extension.
      // Expect the original two IDs: reload alone creates no groups; a later missing-partition
      // request triggers extension.
      Assert.assertEquals(
          oldGroups,
          // Expect 2 Data groups: only the policy has changed; no new time partition has been
          // written.
          assertRegionGroupsAndReplicas(client, AUTO_DATABASE, TConsensusGroupType.DataRegion, 2)
              .keySet());
      writeAtTime(statement, devices, 3 * TIME_PARTITION_INTERVAL, 300);
      // Target = min(6 active slots, cap 6) = 6: keep the old 2 groups and create 6 - 2 = 4.
      // t=0/10/20/30 now give 24 data partitions in total, still derived from 6 series slots.
      Set<TConsensusGroupId> allGroups =
          // Expect 6 Data groups and 18 replicas: PROACTIVE targets min(6 active slots, cap
          // 6)=6.
          assertRegionGroupsAndReplicas(
                  client, AUTO_DATABASE, TConsensusGroupType.DataRegion, DEVICE_COUNT)
              .keySet();
      // Expect true: all six groups must include the two old groups; extension must not delete
      // or replace them.
      Assert.assertTrue(allGroups.containsAll(oldGroups));
      Set<TConsensusGroupId> newGroups = new HashSet<>(allGroups);
      newGroups.removeAll(oldGroups);
      // Expect 4 new groups: target 6 minus the existing 2 gives 6-2=4.
      Assert.assertEquals(4, newGroups.size());
      Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TConsensusGroupId>>> afterReload =
          readDataPartitions(client, AUTO_DATABASE, devices, 3 * TIME_PARTITION_INTERVAL);
      // Expect all six groups to be used, with one slot each: 6 slots / 6 groups=1.
      assertSlotDistribution(allGroups, groupsAtTime(afterReload, 3 * TIME_PARTITION_INTERVAL));

      // Step 4: verify ownership, not just the final group count. At rebalance,
      // PROACTIVE retains mu = max(1, floor(6 active slots / 6 groups)) = 1 slot per old group;
      // AUTO would retain 166.
      // Each old group unassigns 3 - 1 = 2 slots: 2 * 2 = 4 slots activate four distinct new
      // groups.
      // Which slot is retained is randomized, so compare its original owner rather than its ID.
      Map<TConsensusGroupId, Integer> retainedByOldGroup = new HashMap<>();
      Set<TConsensusGroupId> reassignedGroups = new HashSet<>();
      original.forEach(
          (slot, times) -> {
            TConsensusGroupId previous = times.get(new TTimePartitionSlot(0)).get(0);
            TConsensusGroupId current =
                afterReload
                    .get(slot)
                    .get(new TTimePartitionSlot(3 * TIME_PARTITION_INTERVAL))
                    .get(0);
            if (oldGroups.contains(current)) {
              // Expect a retained slot to keep its previous owner: an old group may retain only
              // its own slots.
              Assert.assertEquals(previous, current);
              retainedByOldGroup.merge(current, 1, Integer::sum);
            } else {
              // Expect true: each reassigned slot uses a distinct new group, giving one slot to
              // each of four new groups.
              Assert.assertTrue(reassignedGroups.add(current));
            }
          });
      // Expect exactly the original two groups to retain slots: both old groups must retain
      // one.
      Assert.assertEquals(oldGroups, retainedByOldGroup.keySet());
      // Expect 1 retained slot per old group: PROACTIVE uses mu=max(1,floor(6/6))=1.
      retainedByOldGroup.values().forEach(count -> Assert.assertEquals(1, count.intValue()));
      // Expect reassignment to use exactly the four new groups, without leaving any unused or
      // including old groups.
      Assert.assertEquals(newGroups, reassignedGroups);

      // Step 5: repeat the reload and write t=40: 6 data groups, 6 slots, 6 * 5 = 30 partitions.
      // Verify stable future routing, unchanged historical mappings, and all five values per
      // device.
      statement.execute("SET CONFIGURATION 'data_region_group_extension_policy'='PROACTIVE'");
      writeAtTime(statement, devices, 4 * TIME_PARTITION_INTERVAL, 400);
      Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TConsensusGroupId>>> nextPartitions =
          readDataPartitions(client, AUTO_DATABASE, devices, 4 * TIME_PARTITION_INTERVAL);
      afterReload.forEach(
          (slot, times) ->
              // Expect identical routing at t=40 and t=30: reapplying the same policy preserves
              // the balanced mapping.
              Assert.assertEquals(
                  times.get(new TTimePartitionSlot(3 * TIME_PARTITION_INTERVAL)),
                  nextPartitions
                      .get(slot)
                      .get(new TTimePartitionSlot(4 * TIME_PARTITION_INTERVAL))));
      // Expect the six historical t=0 mappings to match the snapshot: rebalancing affects
      // future allocation only.
      Assert.assertEquals(original, readDataPartitions(client, AUTO_DATABASE, devices, 0));
      // Expect the six t=20 mappings to remain unchanged, preserving the last time partition
      // before reload.
      Assert.assertEquals(
          beforeReload,
          readDataPartitions(client, AUTO_DATABASE, devices, 2 * TIME_PARTITION_INTERVAL));
      // Expect the same six group IDs after another reload and time partition, with no further
      // group creation.
      Assert.assertEquals(
          allGroups,
          // Expect 6 Data groups: there are still six active series slots, and the database cap
          // has been reached.
          assertRegionGroupsAndReplicas(
                  client, AUTO_DATABASE, TConsensusGroupType.DataRegion, DEVICE_COUNT)
              .keySet());
      // Expect 1 Schema group and 3 replicas: only the data policy changed, with no new schema
      // slots.
      assertRegionGroupsAndReplicas(client, AUTO_DATABASE, TConsensusGroupType.SchemaRegion, 1);
      // Expect both caps to remain 6: node count, database count, replication and quotas are
      // unchanged.
      assertMaximum(client, AUTO_DATABASE, DEVICE_COUNT);
      for (int time = 0; time <= 4; time++) {
        // Expect one row per device at t=0/10/20/30/40, with slot+0/100/200/300/400 from the
        // five writes.
        assertValuesAtTime(statement, devices, time * TIME_PARTITION_INTERVAL, time * 100);
      }
    }
  }

  /**
   * Verify switching a low CUSTOM cap to PROACTIVE: refresh the cap from 2 to 6 and use six groups
   * with one slot each for the next time partition.
   */
  @Test
  public void testCustomBelowProactiveHotReloadRedistributesSixSlots() throws Exception {
    checkCustomToResourcePolicy("PROACTIVE");
  }

  /**
   * Verify CUSTOM-to-AUTO cap refresh: increase the cap from 2 to 6 while the six sparse slots
   * remain in the original two groups.
   */
  @Test
  public void testCustomToAutoHotReloadRefreshesMaximum() throws Exception {
    checkCustomToResourcePolicy("AUTO");
  }

  /**
   * Compare switching CUSTOM to resource-based policies. Starting with two groups of three slots,
   * PROACTIVE uses six groups of one slot, while AUTO keeps two groups of three. Both caps must
   * refresh from 2 to 6.
   */
  private void checkCustomToResourcePolicy(String policy) throws Exception {
    // Step 1: 1C3D / one database gives a resource cap of 6, but CUSTOM explicitly limits data to
    // 2.
    // At t=0, six devices activate six slots: 1 schema group, 2 data groups, 3 data slots per
    // group.
    initAutoCluster(3);
    try (SyncConfigNodeIServiceClient client =
            (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection();
        Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("SET CONFIGURATION 'data_region_group_extension_policy'='CUSTOM'");
      // Expect successful creation (200): CUSTOM cap 2 equals the Data minimum, and three nodes
      // support three replicas.
      assertSuccess(
          client.setDatabase(
              new TDatabaseSchema(CUSTOM_DATABASE)
                  .setMinSchemaRegionGroupNum(1)
                  .setMinDataRegionGroupNum(2)
                  .setMaxDataRegionGroupNum(2)));
      // Expect Schema cap 6 and Data cap 2: Schema uses ceil(6*3/(1*3)); Data uses the explicit
      // CUSTOM cap.
      assertMaximum(client, CUSTOM_DATABASE, 6, 2);
      List<String> devices = createSixDevices(statement, CUSTOM_DATABASE);
      Set<TConsensusGroupId> oldGroups =
          // Expect 2 Data groups and 6 replicas: CUSTOM allocates directly to its configured
          // cap of 2.
          assertRegionGroupsAndReplicas(client, CUSTOM_DATABASE, TConsensusGroupType.DataRegion, 2)
              .keySet();
      Map<TSeriesPartitionSlot, TConsensusGroupId> original =
          readDataAssignments(client, CUSTOM_DATABASE, devices, 0);
      oldGroups.forEach(
          // Expect 3 slots per group: six active slots are evenly split between two CUSTOM
          // groups.
          group -> Assert.assertEquals(3, Collections.frequency(original.values(), group)));

      // Step 2: expect the data cap to refresh from 2 to 6 when leaving CUSTOM; groups remain at 2.
      // Change only the policy: no quota edits, database creation or node registration may
      // accidentally refresh the CUSTOM cap before the next partition request.
      statement.execute("SET CONFIGURATION 'data_region_group_extension_policy'='" + policy + "'");
      // Expect the same two IDs: the policy switch refreshes the cap but has not yet triggered
      // partition allocation.
      Assert.assertEquals(
          oldGroups,
          // Expect 2 Data groups immediately after reload: t=10 has not been written, so
          // extension has not run.
          assertRegionGroupsAndReplicas(client, CUSTOM_DATABASE, TConsensusGroupType.DataRegion, 2)
              .keySet());
      writeAtTime(statement, devices, TIME_PARTITION_INTERVAL, 100);
      // Step 3: t=10 adds six data partitions (12 total), but still only six active series slots.
      // PROACTIVE grows to min(6 slots, cap 6)=6 groups; AUTO stays at its minimum of 2.
      Set<TConsensusGroupId> currentGroups =
          // Expect 6 groups for PROACTIVE and 2 for AUTO: only PROACTIVE grows for the six
          // active slots.
          assertRegionGroupsAndReplicas(
                  client,
                  CUSTOM_DATABASE,
                  TConsensusGroupType.DataRegion,
                  "PROACTIVE".equals(policy) ? DEVICE_COUNT : 2)
              .keySet();
      // Expect both caps to be 6: leaving CUSTOM recalculates the Data cap from resources
      // instead of retaining 2.
      assertMaximum(client, CUSTOM_DATABASE, 6);
      Map<TSeriesPartitionSlot, TConsensusGroupId> current =
          readDataAssignments(client, CUSTOM_DATABASE, devices, TIME_PARTITION_INTERVAL);
      if ("PROACTIVE".equals(policy)) {
        // Retain mu=max(1,6/6)=1 original slot in each old group; reassign 2*(3-1)=4 to new groups.
        // Expect one slot in each of six groups: PROACTIVE must actually use the newly created
        // groups.
        assertSlotDistribution(currentGroups, new ArrayList<>(current.values()));
        Set<TConsensusGroupId> newGroups = new HashSet<>(currentGroups);
        newGroups.removeAll(oldGroups);
        // Expect 4 new groups: PROACTIVE target 6 minus the original 2 CUSTOM groups.
        Assert.assertEquals(4, newGroups.size());
        Map<TConsensusGroupId, Integer> retained = new HashMap<>();
        Set<TConsensusGroupId> reassigned = new HashSet<>();
        current.forEach(
            (slot, group) -> {
              if (oldGroups.contains(group)) {
                // Expect a retained slot to keep its original owner, without swapping slots
                // between old groups.
                Assert.assertEquals(original.get(slot), group);
                retained.merge(group, 1, Integer::sum);
              } else {
                // Expect true: the four unassigned slots must each enter a different new group.
                Assert.assertTrue(reassigned.add(group));
              }
            });
        // Expect both original groups to retain slots; their ID set must match the pre-switch
        // snapshot.
        Assert.assertEquals(oldGroups, retained.keySet());
        // Expect one retained slot per old group: mu=max(1,6/6)=1; the other four slots enter
        // new groups.
        retained.values().forEach(count -> Assert.assertEquals(1, count.intValue()));
        // Expect exactly the four new groups to receive reassigned slots, verifying that every
        // new group is used.
        Assert.assertEquals(newGroups, reassigned);
      } else {
        // AUTO has the same resource cap as PROACTIVE, but six sparse slots still need two groups.
        // Expect AUTO routing to remain unchanged: raising the cap from 2 to 6 does not require
        // more groups for sparse load.
        Assert.assertEquals(original, current);
      }
      // Step 4: data-policy reload leaves schema at 1 group and preserves t=0 mappings and values.
      // Expect the six t=0 mappings to match the snapshot: neither target policy may rewrite
      // historical partitions.
      Assert.assertEquals(original, readDataAssignments(client, CUSTOM_DATABASE, devices, 0));
      // Expect 1 Schema group and 3 replicas: only the data policy changed; schema load and
      // minimum are unchanged.
      assertRegionGroupsAndReplicas(client, CUSTOM_DATABASE, TConsensusGroupType.SchemaRegion, 1);
      // Expect one row per device at t=0 with values 0..5, verifying that pre-switch data
      // remains readable.
      assertValuesAtTime(statement, devices, 0, 0);
      // Expect one row per device at t=10 with values 100..105, verifying the new time
      // partition.
      assertValuesAtTime(statement, devices, TIME_PARTITION_INTERVAL, 100);
    }
  }

  /**
   * Verify that existing CUSTOM groups exceeding the PROACTIVE target are preserved. For example,
   * eight groups with six active slots and a resource cap of six retain all eight groups and
   * existing slot owners.
   */
  @Test
  public void testCustomAboveProactiveHotReloadPreservesGroups() throws Exception {
    // Step 1: CUSTOM creates 8 data groups although six active slots only use six of them.
    // There are 8*3=24 data replicas; two logical groups initially have no series slots.
    initAutoCluster(3);
    try (SyncConfigNodeIServiceClient client =
            (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection();
        Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("SET CONFIGURATION 'data_region_group_extension_policy'='CUSTOM'");
      // Expect successful creation (200): CUSTOM permits an explicit Data cap of 8 above the
      // minimum of 2.
      assertSuccess(
          client.setDatabase(
              new TDatabaseSchema(CUSTOM_DATABASE)
                  .setMinSchemaRegionGroupNum(1)
                  .setMinDataRegionGroupNum(2)
                  .setMaxDataRegionGroupNum(8)));
      List<String> devices = createSixDevices(statement, CUSTOM_DATABASE);
      Set<TConsensusGroupId> originalGroups =
          // Expect 8 Data groups and 24 replicas: CUSTOM creates up to its cap even with only
          // six active slots.
          assertRegionGroupsAndReplicas(client, CUSTOM_DATABASE, TConsensusGroupType.DataRegion, 8)
              .keySet();
      Map<TSeriesPartitionSlot, TConsensusGroupId> original =
          readDataAssignments(client, CUSTOM_DATABASE, devices, 0);
      // Expect 6 used groups: each slot occupies a distinct group, leaving two of the eight
      // groups unused.
      Assert.assertEquals(DEVICE_COUNT, new HashSet<>(original.values()).size());

      statement.execute("SET CONFIGURATION 'data_region_group_extension_policy'='PROACTIVE'");
      // Step 2: recalculated data cap=max(minimum 2, resource quota 6, existing groups 8)=8.
      // Switching to PROACTIVE does not shrink to the six-group active-slot target.
      // Expect Schema cap 6 and Data cap 8: max(minimum 2, resource cap 6, existing groups 8)=8
      // prevents shrinking.
      assertMaximum(client, CUSTOM_DATABASE, 6, 8);
      // Expect all eight IDs to remain: switching to PROACTIVE must not delete groups to match
      // six active slots.
      Assert.assertEquals(
          originalGroups,
          // Expect 8 Data groups and 24 replicas: a policy reload must not shrink existing
          // groups.
          assertRegionGroupsAndReplicas(client, CUSTOM_DATABASE, TConsensusGroupType.DataRegion, 8)
              .keySet());
      writeAtTime(statement, devices, TIME_PARTITION_INTERVAL, 100);
      // Step 3: t=10 gives 12 data partitions; all 8 group IDs and all six slot owners stay
      // unchanged.
      // Expect the same eight IDs after t=10: existing groups already outnumber the six active
      // slots.
      Assert.assertEquals(
          originalGroups,
          // Expect 8 Data groups: a new time partition adds no active series slots and does not
          // trigger shrinking.
          assertRegionGroupsAndReplicas(client, CUSTOM_DATABASE, TConsensusGroupType.DataRegion, 8)
              .keySet());
      // Expect t=10 routing to match t=0: without new groups, assigned slots keep their
      // original owners.
      Assert.assertEquals(
          original, readDataAssignments(client, CUSTOM_DATABASE, devices, TIME_PARTITION_INTERVAL));
      // Expect the six historical t=0 mappings to remain unchanged: switching policies does not
      // migrate partitions.
      Assert.assertEquals(original, readDataAssignments(client, CUSTOM_DATABASE, devices, 0));
      // Expect 1 Schema group: only the data policy changed; schema load and minimum are
      // unchanged.
      assertRegionGroupsAndReplicas(client, CUSTOM_DATABASE, TConsensusGroupType.SchemaRegion, 1);
      // Expect one row per device at t=0 with values 0..5, confirming historical reads after
      // retaining eight groups.
      assertValuesAtTime(statement, devices, 0, 0);
      // Expect one row per device at t=10 with values 100..105, verifying writes through the
      // retained routing.
      assertValuesAtTime(statement, devices, TIME_PARTITION_INTERVAL, 100);
    }
  }

  /**
   * Verify that entering PROACTIVE balances slots even when CUSTOM has already created enough
   * groups. Existing time partitions and repeated policy reloads retain their assignments.
   */
  @Test
  public void testExpandedCustomGroupsRebalanceOnProactiveSwitchWithoutFurtherGrowth()
      throws Exception {
    initAutoCluster(3);
    try (SyncConfigNodeIServiceClient client =
            (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection();
        Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("SET CONFIGURATION 'data_region_group_extension_policy'='CUSTOM'");
      assertSuccess(
          client.setDatabase(
              new TDatabaseSchema(CUSTOM_DATABASE)
                  .setMinSchemaRegionGroupNum(1)
                  .setMinDataRegionGroupNum(2)
                  .setMaxDataRegionGroupNum(2)));
      List<String> devices = createSixDevices(statement, CUSTOM_DATABASE);
      Map<TSeriesPartitionSlot, TConsensusGroupId> original =
          readDataAssignments(client, CUSTOM_DATABASE, devices, 0);
      Assert.assertEquals(2, new HashSet<>(original.values()).size());

      statement.execute("ALTER DATABASE " + CUSTOM_DATABASE + " WITH MAX_DATA_REGION_GROUP_NUM=6");
      writeAtTime(statement, devices, TIME_PARTITION_INTERVAL, 100);
      Set<TConsensusGroupId> expandedGroups =
          assertRegionGroupsAndReplicas(client, CUSTOM_DATABASE, TConsensusGroupType.DataRegion, 6)
              .keySet();
      // CUSTOM keeps all six slots on the original two groups despite creating four more groups.
      Assert.assertEquals(
          original, readDataAssignments(client, CUSTOM_DATABASE, devices, TIME_PARTITION_INTERVAL));

      statement.execute("SET CONFIGURATION 'data_region_group_extension_policy'='PROACTIVE'");
      writeAtTime(statement, devices, 2 * TIME_PARTITION_INTERVAL, 200);
      Assert.assertEquals(
          expandedGroups,
          assertRegionGroupsAndReplicas(client, CUSTOM_DATABASE, TConsensusGroupType.DataRegion, 6)
              .keySet());
      Map<TSeriesPartitionSlot, TConsensusGroupId> rebalanced =
          readDataAssignments(client, CUSTOM_DATABASE, devices, 2 * TIME_PARTITION_INTERVAL);
      assertSlotDistribution(expandedGroups, new ArrayList<>(rebalanced.values()));

      statement.execute("SET CONFIGURATION 'data_region_group_extension_policy'='PROACTIVE'");
      writeAtTime(statement, devices, 3 * TIME_PARTITION_INTERVAL, 300);
      Assert.assertEquals(
          rebalanced,
          readDataAssignments(client, CUSTOM_DATABASE, devices, 3 * TIME_PARTITION_INTERVAL));
      Assert.assertEquals(original, readDataAssignments(client, CUSTOM_DATABASE, devices, 0));
      Assert.assertEquals(
          original, readDataAssignments(client, CUSTOM_DATABASE, devices, TIME_PARTITION_INTERVAL));
      for (int time = 0; time <= 3; time++) {
        assertValuesAtTime(statement, devices, time * TIME_PARTITION_INTERVAL, time * 100);
      }
    }
  }

  /**
   * Verify PROACTIVE-to-CUSTOM growth to the cap: grow from six to nine groups, then to ten after
   * raising the cap. The six slots keep their owners; also verify rejection of a lower cap and SQL
   * values.
   */
  @Test
  public void testProactiveToCustomHotReloadAllocatesMaximum() throws Exception {
    // Step 1: quota 9 per node gives cap=ceil(9*3/(1*3))=9; PROACTIVE activates only 6 groups.
    // At t=0, six distinct slots each own one group: 6 data partitions and 6*3=18 data replicas.
    initAutoCluster(3, 9);
    try (SyncConfigNodeIServiceClient client =
            (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection();
        Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("SET CONFIGURATION 'data_region_group_extension_policy'='PROACTIVE'");
      // Expect successful creation (200): minimums 1/2 are valid, and three nodes support three
      // replicas.
      assertSuccess(
          client.setDatabase(
              new TDatabaseSchema(PROACTIVE_DATABASE)
                  .setMinSchemaRegionGroupNum(1)
                  .setMinDataRegionGroupNum(2)));
      // Expect both caps to be 9: the explicit per-node quota is 9, so ceil(9*3/(1*3))=9.
      assertMaximum(client, PROACTIVE_DATABASE, 9);
      List<String> devices = createSixDevices(statement, PROACTIVE_DATABASE);
      Set<TConsensusGroupId> oldGroups =
          // Expect 6 Data groups and 18 replicas: PROACTIVE creates groups for six active
          // slots, below the cap of 9.
          assertRegionGroupsAndReplicas(
                  client, PROACTIVE_DATABASE, TConsensusGroupType.DataRegion, DEVICE_COUNT)
              .keySet();
      Map<TSeriesPartitionSlot, TConsensusGroupId> original =
          readDataAssignments(client, PROACTIVE_DATABASE, devices, 0);
      // Expect one slot in each of six groups: six active slots are evenly assigned to six
      // PROACTIVE groups.
      assertSlotDistribution(oldGroups, new ArrayList<>(original.values()));

      // Step 2: CUSTOM inherits the saved cap of 9; changing the policy alone leaves 6 groups.
      statement.execute("SET CONFIGURATION 'data_region_group_extension_policy'='CUSTOM'");
      // Expect both caps to remain 9: CUSTOM uses the database's current cap rather than
      // resetting to a default minimum.
      assertMaximum(client, PROACTIVE_DATABASE, 9);
      // Expect the original six IDs: changing the policy alone does not create the three
      // additional groups.
      Assert.assertEquals(
          oldGroups,
          // Expect 6 Data groups: the missing t=10 partition has not yet been requested, so
          // CUSTOM has not extended.
          assertRegionGroupsAndReplicas(
                  client, PROACTIVE_DATABASE, TConsensusGroupType.DataRegion, DEVICE_COUNT)
              .keySet());
      writeAtTime(statement, devices, TIME_PARTITION_INTERVAL, 100);
      Set<TConsensusGroupId> customGroups =
          // Expect 9 Data groups and 27 replicas: the first missing t=10 partition makes CUSTOM
          // fill the cap of 9.
          assertRegionGroupsAndReplicas(
                  client, PROACTIVE_DATABASE, TConsensusGroupType.DataRegion, 9)
              .keySet();
      // Expect true: the nine groups must include the original six, adding exactly 9-6=3
      // groups.
      Assert.assertTrue(customGroups.containsAll(oldGroups));
      // Step 3: t=10 triggers 9-6=3 new groups (27 replicas total), giving 12 data partitions.
      // CUSTOM retains floor(1000/9)=111 slots per group, so each old group's single slot stays.
      // All six slots keep their original groups, leaving the three new groups unused for now.
      // Expect all six slots to retain their owners: CUSTOM's threshold floor(1000/9)=111
      // exceeds one slot per old group.
      Assert.assertEquals(
          original,
          readDataAssignments(client, PROACTIVE_DATABASE, devices, TIME_PARTITION_INTERVAL));

      // Step 4: repeating CUSTOM and writing t=20 preserves 9 groups / 6 slots / the same owners.
      statement.execute("SET CONFIGURATION 'data_region_group_extension_policy'='CUSTOM'");
      writeAtTime(statement, devices, 2 * TIME_PARTITION_INTERVAL, 200);
      // Expect the same nine IDs: reapplying CUSTOM finds both the cap and current group count
      // already satisfied.
      Assert.assertEquals(
          customGroups,
          // Expect 9 Data groups: t=20 only adds a time partition without raising the cap.
          assertRegionGroupsAndReplicas(
                  client, PROACTIVE_DATABASE, TConsensusGroupType.DataRegion, 9)
              .keySet());
      // Expect t=20 routing to match the original six groups: one slot per group remains below
      // threshold 111.
      Assert.assertEquals(
          original,
          readDataAssignments(client, PROACTIVE_DATABASE, devices, 2 * TIME_PARTITION_INTERVAL));

      // Step 5: explicitly raise the CUSTOM cap to 10; t=30 creates one more group (30 replicas).
      // mu=1000/10=100 still retains all six slot owners; 6 slots * 4 times = 24 data partitions.
      statement.execute(
          "ALTER DATABASE " + PROACTIVE_DATABASE + " WITH MAX_DATA_REGION_GROUP_NUM=10");
      writeAtTime(statement, devices, 3 * TIME_PARTITION_INTERVAL, 300);
      // Expect true: extending the cap from 9 to 10 must preserve all nine existing groups.
      Assert.assertTrue(
          // Expect 10 Data groups and 30 replicas: allocation at t=30 fills the new explicit
          // CUSTOM cap.
          assertRegionGroupsAndReplicas(
                  client, PROACTIVE_DATABASE, TConsensusGroupType.DataRegion, 10)
              .keySet()
              .containsAll(customGroups));
      // Expect unchanged routing: the new threshold 1000/10=100 still exceeds one slot per old
      // group.
      Assert.assertEquals(
          original,
          readDataAssignments(client, PROACTIVE_DATABASE, devices, 3 * TIME_PARTITION_INTERVAL));
      // Step 6: lowering the cap to 2 must fail; check 10 groups remain and all four times are
      // readable.
      // Expect SQLException: requested cap 2 is below the current cap and existing group count
      // of 10.
      Assert.assertThrows(
          SQLException.class,
          () ->
              statement.execute(
                  "ALTER DATABASE " + PROACTIVE_DATABASE + " WITH MAX_DATA_REGION_GROUP_NUM=2"));
      // Expect Schema cap 9 and Data cap 10: only the Data cap increased, and the rejected
      // decrease changes nothing.
      assertMaximum(client, PROACTIVE_DATABASE, 9, 10);
      // Expect 10 Data groups and 30 replicas: rejecting the lower cap must not delete existing
      // groups.
      assertRegionGroupsAndReplicas(client, PROACTIVE_DATABASE, TConsensusGroupType.DataRegion, 10);
      // Expect the six t=0 mappings to stay unchanged through growth to 9/10 groups and the
      // rejected decrease.
      Assert.assertEquals(original, readDataAssignments(client, PROACTIVE_DATABASE, devices, 0));
      for (int time = 0; time <= 3; time++) {
        // Expect one row per device at t=0/10/20/30, with slot+0/100/200/300 from the four
        // writes.
        assertValuesAtTime(statement, devices, time * TIME_PARTITION_INTERVAL, time * 100);
      }
    }
  }

  /**
   * Verify independent Schema policy reload. Six CUSTOM slots initially share one group; after
   * switching to PROACTIVE, new slot 6 triggers growth to cap 6. Existing schema mappings and the
   * two Data groups remain unchanged.
   */
  @Test
  public void testSchemaCustomToProactiveHotReloadRefreshesMaximum() throws Exception {
    // Step 1: schema CUSTOM cap=1, data AUTO cap=6; six schema slots all belong to one schema
    // group.
    initAutoCluster(3);
    try (SyncConfigNodeIServiceClient client =
            (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection();
        Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("SET CONFIGURATION 'schema_region_group_extension_policy'='CUSTOM'");
      // Expect successful creation (200): CUSTOM Schema cap 1 equals its minimum; Data minimum
      // 2 is valid.
      assertSuccess(
          client.setDatabase(
              new TDatabaseSchema(CUSTOM_DATABASE)
                  .setMinSchemaRegionGroupNum(1)
                  .setMaxSchemaRegionGroupNum(1)
                  .setMinDataRegionGroupNum(2)));
      // Expect Schema cap 1 and Data cap 6: the former is explicit CUSTOM; the latter uses
      // ceil(6*3/(1*3)).
      assertMaximum(client, CUSTOM_DATABASE, 1, 6);
      List<String> devices = createSixDevices(statement, CUSTOM_DATABASE);
      TSchemaPartitionTableResp original = client.getSchemaPartitionTable(schemaRequest(devices));
      // Expect successful query (200): schema partitions for all six existing measurements must
      // be readable.
      assertSuccess(original.getStatus());
      Set<TConsensusGroupId> oldGroups =
          // Expect 1 Schema group and 3 replicas: CUSTOM cap 1 places all six schema slots in
          // that group.
          assertRegionGroupsAndReplicas(
                  client, CUSTOM_DATABASE, TConsensusGroupType.SchemaRegion, 1)
              .keySet();
      statement.execute("SET CONFIGURATION 'schema_region_group_extension_policy'='PROACTIVE'");
      // Expect the original Schema group ID: reloading the policy has not yet requested a new
      // schema slot.
      Assert.assertEquals(
          oldGroups,
          // Expect 1 Schema group: extension waits for the seventh device's new slot.
          assertRegionGroupsAndReplicas(
                  client, CUSTOM_DATABASE, TConsensusGroupType.SchemaRegion, 1)
              .keySet());
      // Step 2: reload only the schema policy, then create a seventh device in a seventh slot.
      // Target=min(7 schema slots, cap 6)=6 groups: add 5 groups, while the original six slots stay
      // put.
      // The new device has no data write, so data stays at 6 active slots and 2 groups.
      String newDevice = findDeviceInSlot(CUSTOM_DATABASE, DEVICE_COUNT);
      statement.execute("CREATE TIMESERIES " + newDevice + ".s WITH DATATYPE=INT32, ENCODING=RLE");
      // Expect true: the six Schema groups must include the original group, preserving its
      // existing mappings.
      Assert.assertTrue(
          // Expect 6 Schema groups and 18 replicas: the new device raises active slots to 7, so
          // min(7,cap 6)=6.
          assertRegionGroupsAndReplicas(
                  client, CUSTOM_DATABASE, TConsensusGroupType.SchemaRegion, 6)
              .keySet()
              .containsAll(oldGroups));
      // Expect both caps to be 6: Schema resumes resource-based calculation; the Data cap stays
      // unchanged.
      assertMaximum(client, CUSTOM_DATABASE, 6);
      TSchemaPartitionTableResp restored = client.getSchemaPartitionTable(schemaRequest(devices));
      // Step 3: verify the original schema mappings and SQL values survive the schema-only change.
      // Expect successful query (200): all six original devices' schema partitions remain
      // readable after extension.
      assertSuccess(restored.getStatus());
      // Expect unchanged mappings for the original six schema slots: extension assigns new
      // slots without moving old ones.
      Assert.assertEquals(original.getSchemaPartitionTable(), restored.getSchemaPartitionTable());
      // Expect 2 Data groups and 6 replicas: Data stays AUTO, and no data was written to the
      // seventh device.
      assertRegionGroupsAndReplicas(client, CUSTOM_DATABASE, TConsensusGroupType.DataRegion, 2);
      // Expect one t=0 row per original device with values 0..5: Schema reload must preserve
      // existing reads.
      assertValuesAtTime(statement, devices, 0, 0);
    }
  }

  /**
   * Initialize AUTO with an explicit per-node quota of 6. For example, dataNodeCount=3 starts 1C3D
   * with three replicas per Region type and 1000 series slots.
   */
  private void initAutoCluster(int dataNodeCount) throws Exception {
    initAutoCluster(dataNodeCount, 6);
  }

  /**
   * Initialize AUTO with a specified node quota. For example, three nodes, regionPerDataNode=9 and
   * one database with three replicas yield ceil(9*3/3)=9 groups. The quota is not a simulated CPU
   * count.
   */
  private void initAutoCluster(int dataNodeCount, int regionPerDataNode) throws Exception {
    // Shared setup: 1 ConfigNode, N DataNodes, 3 replicas, INHERIT, and 10 ms time partitions.
    // Per-type cap depends on regionPerDataNode (normally 6), node count, databases and replicas.
    // A nonzero Data quota is used directly without the 0.5 CPU multiplier; quota 6 matches
    // automatic sizing with 12 reported cores per node.
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.SIMPLE_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaReplicationFactor(REPLICATION_FACTOR)
        .setDataReplicationFactor(REPLICATION_FACTOR)
        .setSchemaRegionGroupExtensionPolicy("AUTO")
        .setDataRegionGroupExtensionPolicy("AUTO")
        .setDataPartitionAllocationStrategy("INHERIT")
        .setSchemaRegionPerDataNode(regionPerDataNode)
        .setDataRegionPerDataNode(regionPerDataNode)
        .setSeriesSlotNum(SERIES_SLOT_NUM)
        .setSeriesPartitionExecutorClass(BKDRHashExecutor.class.getName())
        .setTimePartitionInterval(TIME_PARTITION_INTERVAL);
    EnvFactory.getEnv().initClusterEnvironment(1, dataNodeCount);
  }

  /**
   * Run the two-database distribution, reload, cap and recovery scenarios. For example, on 3DN each
   * cap is 3: AUTO uses two Data groups of three slots, then PROACTIVE uses three groups of two
   * while preserving historical mappings.
   */
  private void checkSparseDeviceDistribution(int dataNodeCount) throws Exception {
    initAutoCluster(dataNodeCount);
    Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TConsensusGroupId>>>
        originalAutoPartitions;
    try (SyncConfigNodeIServiceClient client =
            (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection();
        Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // Step 1: create both databases before allocating any regions, ensuring the same cap for
      // each.
      // Cap per type = ceil(6 groups per node * N nodes / (2 databases * 3 replicas)) = N.
      // Thus the 3D / 6D / 9D cases have caps of 3 / 6 / 9; minimums are schema=1 and data=2.
      for (String database : Arrays.asList(AUTO_DATABASE, PROACTIVE_DATABASE)) {
        // Expect both creations to succeed (200): minimums 1/2 are valid, and 3/6/9DN all
        // support three replicas.
        assertSuccess(
            client.setDatabase(
                new TDatabaseSchema(database)
                    .setMinSchemaRegionGroupNum(1)
                    .setMinDataRegionGroupNum(2)));
      }
      // Expect both root.auto caps to be 3/6/9 on 3/6/9DN: ceil(6*N/(2 databases*3
      // replicas))=N.
      assertMaximum(client, AUTO_DATABASE, dataNodeCount);
      // Expect root.proactive caps to be 3/6/9 as well: both databases share the same resource
      // calculation.
      assertMaximum(client, PROACTIVE_DATABASE, dataNodeCount);

      // Step 2: AUTO database, t=0/10: 6 slots, 1 schema group, 2 data groups, 12 data partitions.
      checkSixDevices(client, statement, AUTO_DATABASE, dataNodeCount, false);

      // Step 3: switch both policies, then populate the second database with the same workload.
      // K = min(6, N) gives 3 / 6 / 6 groups per type for 3D / 6D / 9D, respectively.
      statement.execute("SET CONFIGURATION 'schema_region_group_extension_policy'='PROACTIVE'");
      statement.execute("SET CONFIGURATION 'data_region_group_extension_policy'='PROACTIVE'");
      // Expect caps to remain 3/6/9: AUTO and PROACTIVE share the resource formula; nodes and
      // database count are unchanged.
      assertMaximum(client, PROACTIVE_DATABASE, dataNodeCount);
      checkSixDevices(client, statement, PROACTIVE_DATABASE, dataNodeCount, true);

      // Step 4: write the existing AUTO database's six devices at t=20 after the global switch.
      // Its data groups grow 2 -> K (3 / 6 / 6); six slots spread as 2 / 1 / 1 slots per group.
      // Its schema group stays at 1 because these devices need no new schema partitions.
      // Verify all K data groups are used and t=0 keeps its original group IDs (no data migration).
      List<String> autoDevices = generateDeviceNamesForSlots(AUTO_DATABASE, DEVICE_COUNT);
      originalAutoPartitions = readDataPartitions(client, AUTO_DATABASE, autoDevices, 0);
      writeAtTime(statement, autoDevices, 2 * TIME_PARTITION_INTERVAL, 200);
      // Expect 3/6/6 Data groups on 3/6/9DN, with 2/1/1 slots per group: target=min(6,N).
      // Expect replicas on all 3/6/9 nodes, with 3/3/2 per node: divide 9/18/18 total replicas
      // by node count.
      assertNewTimeDistribution(
          client, AUTO_DATABASE, autoDevices, 2 * TIME_PARTITION_INTERVAL, dataNodeCount);
      // Expect one t=20 row per device with slot+200 (200..205): identical writes on 3/6/9DN.
      assertValuesAtTime(statement, autoDevices, 2 * TIME_PARTITION_INTERVAL, 200);
      // Expect the six t=0 mappings to match the snapshot: growing to three or six groups must
      // preserve history.
      Assert.assertEquals(
          originalAutoPartitions, readDataPartitions(client, AUTO_DATABASE, autoDevices, 0));

      // Step 5: add distinct slots starting at 6 to the PROACTIVE database only.
      // The 3D / 6D / 9D cases add 2 / 2 / 5 slots, giving 8 / 8 / 11 active slots in total.
      // Both region types reach/stay at their caps of 3 / 6 / 9 despite exceeding the cap in slots.
      List<String> extraDevices = new ArrayList<>();
      for (int slot = DEVICE_COUNT; slot < Math.max(DEVICE_COUNT, dataNodeCount) + 2; slot++) {
        extraDevices.add(findDeviceInSlot(PROACTIVE_DATABASE, slot));
      }
      TSchemaPartitionTableResp schemaResponse =
          client.getOrCreateSchemaPartitionTable(schemaRequest(extraDevices));
      // Expect schema allocation to succeed (200): assign the extra distinct slots, reusing
      // groups at the cap.
      assertSuccess(schemaResponse.getStatus());
      TDataPartitionTableResp dataResponse =
          client.getOrCreateDataPartitionTable(dataRequest(PROACTIVE_DATABASE, extraDevices, 0));
      // Expect data allocation to succeed (200): once capped, new slots must still be assigned
      // to existing groups.
      assertSuccess(dataResponse.getStatus());
      // Expect 2/2/5 series slots on 3/6/9DN: this request contains only the extra devices, not
      // the original six.
      Assert.assertEquals(
          extraDevices.size(), dataResponse.getDataPartitionTable().get(PROACTIVE_DATABASE).size());
      // Expect 3/6/9 Schema groups: 8/8/11 active slots exceed the respective caps of 3/6/9.
      assertRegionGroupsAndReplicas(
          client, PROACTIVE_DATABASE, TConsensusGroupType.SchemaRegion, dataNodeCount);
      // Expect 3/6/9 Data groups: the cap prevents growing to match all 8/8/11 active slots.
      assertRegionGroupsAndReplicas(
          client, PROACTIVE_DATABASE, TConsensusGroupType.DataRegion, dataNodeCount);
    }

    // Step 6: restart the ConfigNode and rebuild its in-memory allot map from persisted partitions.
    // In root.auto, t=30 must still use K=3/6/6 data groups for the same 6 slots (24 partitions
    // total).
    // Wait for all K * 3 replicas to become Running, then check routing, SQL values and t=0
    // history.
    EnvFactory.getEnv().shutdownConfigNode(0);
    EnvFactory.getEnv().startConfigNode(0);
    try (SyncConfigNodeIServiceClient client =
            (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection();
        Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // Expect all replicas of 3/6/6 groups to recover on 3/6/9DN, yielding 9/18/18 Running
      // records.
      awaitRunningDataRegions(client, AUTO_DATABASE, Math.min(DEVICE_COUNT, dataNodeCount));
      List<String> autoDevices = generateDeviceNamesForSlots(AUTO_DATABASE, DEVICE_COUNT);
      writeAtTime(statement, autoDevices, 3 * TIME_PARTITION_INTERVAL, 300);
      // Expect 3/6/6 Data groups with 2/1/1 slots each after restart: root.auto still has six
      // active slots.
      // Expect replicas on all 3/6/9 nodes, with 3/3/2 per node: divide 9/18/18 total replicas
      // by node count.
      assertNewTimeDistribution(
          client, AUTO_DATABASE, autoDevices, 3 * TIME_PARTITION_INTERVAL, dataNodeCount);
      // Expect one t=30 row per device with slot+300 (300..305), verifying routing and reads
      // after recovery.
      assertValuesAtTime(statement, autoDevices, 3 * TIME_PARTITION_INTERVAL, 300);
      // Expect the six t=0 mappings to remain unchanged: rebuilding the allocation map must
      // preserve persisted history.
      Assert.assertEquals(
          originalAutoPartitions, readDataPartitions(client, AUTO_DATABASE, autoDevices, 0));
    }
  }

  /**
   * Create six devices in distinct slots one by one and write t=0 and t=10. Verify groups,
   * replicas, routing and SQL values. For example, on 3DN PROACTIVE ends with three groups of two
   * slots; AUTO Data uses two groups of three.
   */
  private void checkSixDevices(
      SyncConfigNodeIServiceClient client,
      Statement statement,
      String database,
      int dataNodeCount,
      boolean proactive)
      throws Exception {
    // Insert devices one at a time so each iteration activates exactly one distinct series slot.
    // At iteration i (1..6): AUTO schema=1, data=min(i,2); PROACTIVE schema=data=min(i,N).
    List<String> devices = new ArrayList<>();
    for (int slot = 0; slot < DEVICE_COUNT; slot++) {
      String device = findDeviceInSlot(database, slot);
      devices.add(device);
      statement.execute("CREATE TIMESERIES " + device + ".s WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("INSERT INTO " + device + "(time,s) VALUES (0," + slot + ")");
      int expectedSchemaGroups = proactive ? Math.min(slot + 1, dataNodeCount) : 1;
      int expectedDataGroups =
          proactive ? Math.min(slot + 1, dataNodeCount) : Math.min(slot + 1, 2);
      // Let i=slot+1. Expect one AUTO Schema group: its minimum is 1 and six slots do not
      // trigger proportional growth.
      // Expect PROACTIVE counts 1,2,3,3,3,3 on 3DN and 1,2,3,4,5,6 on 6/9DN: min(i,N).
      assertRegionGroupsAndReplicas(
          client, database, TConsensusGroupType.SchemaRegion, expectedSchemaGroups);
      // Expect AUTO Data counts 1,2,2,2,2,2: successive requests gradually reach the minimum of
      // 2.
      // Expect PROACTIVE counts 1,2,3,3,3,3 on 3DN and 1,2,3,4,5,6 on 6/9DN, bounded by active
      // slots and the cap.
      assertRegionGroupsAndReplicas(
          client, database, TConsensusGroupType.DataRegion, expectedDataGroups);
    }

    // After t=0: 6 measurements, 6 schema slots and 6 (series slot, time slot) data partitions.
    // For each type, G logical groups produce G * 3 replica rows on distinct nodes within a group.
    int expectedSchemaGroups = proactive ? Math.min(DEVICE_COUNT, dataNodeCount) : 1;
    int expectedDataGroups = proactive ? Math.min(DEVICE_COUNT, dataNodeCount) : 2;
    Map<TConsensusGroupId, Set<Integer>> schemaReplicas =
        // Expect Schema counts: AUTO 1 on all 3/6/9DN cases; PROACTIVE 3/6/6, or min(6
        // slots,N).
        assertRegionGroupsAndReplicas(
            client, database, TConsensusGroupType.SchemaRegion, expectedSchemaGroups);
    Map<TConsensusGroupId, Set<Integer>> dataReplicas =
        // Expect Data counts: AUTO 2 on all 3/6/9DN cases; PROACTIVE 3/6/6 after activating all
        // six slots.
        assertRegionGroupsAndReplicas(
            client, database, TConsensusGroupType.DataRegion, expectedDataGroups);
    // Expect Schema replicas to cover 3/3/3 nodes for AUTO (one three-replica group), and 3/6/9
    // for PROACTIVE.
    // Expect 3/3/2 replicas of this type per node for PROACTIVE: 9/18/18 replicas divided by
    // 3/6/9 nodes.
    assertReplicaDistribution(
        schemaReplicas, proactive ? dataNodeCount : REPLICATION_FACTOR, proactive);
    // Expect Data replicas to cover 3/6/6 nodes for AUTO (six replicas across two groups), and
    // 3/6/9 for PROACTIVE.
    // Expect 3/3/2 replicas per node for PROACTIVE; AUTO checks node coverage here without
    // requiring balance.
    assertReplicaDistribution(
        dataReplicas,
        proactive ? dataNodeCount : Math.min(dataNodeCount, 2 * REPLICATION_FACTOR),
        proactive);

    // Check actual partition owners as well as created groups: no expected group may be unused.
    // AUTO data distribution is 3+3; PROACTIVE is 2+2+2 (3D), or six groups with one slot (6D/9D).
    TSchemaPartitionTableResp schemaResponse =
        client.getSchemaPartitionTable(schemaRequest(devices));
    // Expect successful schema query (200): all six measurements exist on every 3/6/9DN
    // configuration.
    assertSuccess(schemaResponse.getStatus());
    Map<TSeriesPartitionSlot, TConsensusGroupId> schemaPartitions =
        schemaResponse.getSchemaPartitionTable().get(database);
    // Expect exactly 6 schema slots: hash-selected device names avoid collisions regardless of
    // node count.
    Assert.assertEquals(DEVICE_COUNT, schemaPartitions.size());
    // Expect Schema slots: AUTO has one group of 6; PROACTIVE has three groups of 2 on 3DN or
    // six of 1 on 6/9DN.
    assertSlotDistribution(schemaReplicas.keySet(), new ArrayList<>(schemaPartitions.values()));
    Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TConsensusGroupId>>> firstPartitions =
        readDataPartitions(client, database, devices, 0);
    // Expect Data slots: AUTO has two groups of 3; PROACTIVE has three groups of 2 on 3DN or
    // six of 1 on 6/9DN.
    assertSlotDistribution(dataReplicas.keySet(), groupsAtTime(firstPartitions, 0));

    // At t=10: 6 series slots * 2 time slots = 12 data partitions; schema partitions stay at 6.
    // No extra groups are needed here; verify unchanged replica placement and per-slot routing.
    writeAtTime(statement, devices, TIME_PARTITION_INTERVAL, 100);
    // Expect identical Schema groups and replica placements to t=0: writing t=10 adds no
    // devices or schema slots.
    Assert.assertEquals(
        schemaReplicas,
        // Expect Schema counts AUTO=1 or PROACTIVE=3/6/6 on 3/6/9DN: schema partitions are
        // unchanged.
        assertRegionGroupsAndReplicas(
            client, database, TConsensusGroupType.SchemaRegion, expectedSchemaGroups));
    // Expect identical Data groups and replica placements to t=0: the same six series slots
    // need no further growth.
    Assert.assertEquals(
        dataReplicas,
        // Expect Data counts AUTO=2 or PROACTIVE=3/6/6 on 3/6/9DN: only the time slot is new.
        assertRegionGroupsAndReplicas(
            client, database, TConsensusGroupType.DataRegion, expectedDataGroups));
    Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TConsensusGroupId>>> secondPartitions =
        readDataPartitions(client, database, devices, TIME_PARTITION_INTERVAL);
    firstPartitions.forEach(
        (slot, times) ->
            // Expect each slot's owner at t=10 to match t=0: no new group triggers rebalancing,
            // so INHERIT keeps its mapping.
            Assert.assertEquals(
                times.get(new TTimePartitionSlot(0)),
                secondPartitions.get(slot).get(new TTimePartitionSlot(TIME_PARTITION_INTERVAL))));

    // Finally verify SQL reads: exactly two rows per device, values slotId and slotId + 100.
    for (int slot = 0; slot < DEVICE_COUNT; slot++) {
      try (ResultSet rows = statement.executeQuery("SELECT s FROM " + devices.get(slot))) {
        // Expect true: each device has a t=0 row on all 3/6/9DN configurations.
        Assert.assertTrue(rows.next());
        // Expect first timestamp 0: default ascending time order returns the earliest write
        // first.
        Assert.assertEquals(0, rows.getLong(1));
        // Expect first value slot (0..5), matching this device's INSERT at t=0.
        Assert.assertEquals(slot, rows.getInt(2));
        // Expect true: each device also has a t=10 write, so a second row must exist.
        Assert.assertTrue(rows.next());
        // Expect second timestamp 10: TIME_PARTITION_INTERVAL=10 places the second write in the
        // next partition.
        Assert.assertEquals(TIME_PARTITION_INTERVAL, rows.getLong(1));
        // Expect second value slot+100 (100..105), matching the INSERT at t=10.
        Assert.assertEquals(slot + 100, rows.getInt(2));
        // Expect false: only t=0 and t=10 were written, so no third or duplicate row may exist.
        Assert.assertFalse(rows.next());
      }
    }
  }

  /**
   * Select device names with the real BKDR hash; this only generates names, without creating
   * measurements or partitions or mocking the executor.
   *
   * <p>For database=root.auto and slotCount=3, return three names whose indices i hash to slots i:
   * 0, 1 and 2. Their d suffixes need not be 0, 1 and 2.
   *
   * @param database database containing the device paths
   * @param slotCount number of distinct series slots to cover, from 0 to SERIES_SLOT_NUM; target
   *     slots are 0 through slotCount-1
   * @return names ordered by target slot, for subsequent SQL/RPC calls to allocate actual
   *     partitions
   */
  private static List<String> generateDeviceNamesForSlots(String database, int slotCount) {
    List<String> devices = new ArrayList<>();
    for (int slot = 0; slot < slotCount; slot++) {
      devices.add(findDeviceInSlot(database, slot));
    }
    return devices;
  }

  /**
   * Create six devices with one INT32 measurement s each and write initial data. For example, the
   * devices hash to slots 0..5 and receive values 0..5 at t=0.
   */
  private static List<String> createSixDevices(Statement statement, String database)
      throws Exception {
    List<String> devices = generateDeviceNamesForSlots(database, DEVICE_COUNT);
    for (String device : devices) {
      statement.execute("CREATE TIMESERIES " + device + ".s WITH DATATYPE=INT32, ENCODING=RLE");
    }
    writeAtTime(statement, devices, 0, 0);
    return devices;
  }

  /**
   * Read the unique logical owner of each series slot at a given time partition. For example, six
   * devices at t=10 yield six slot-to-DataRegionGroup mappings; three replicas do not count as
   * three groups.
   */
  private static Map<TSeriesPartitionSlot, TConsensusGroupId> readDataAssignments(
      SyncConfigNodeIServiceClient client, String database, List<String> devices, long time)
      throws Exception {
    Map<TSeriesPartitionSlot, TConsensusGroupId> assignments = new HashMap<>();
    readDataPartitions(client, database, devices, time)
        .forEach(
            (slot, times) -> {
              List<TConsensusGroupId> groups = times.get(new TTimePartitionSlot(time));
              // Expect one logical Data group per (series slot,time slot), not three replica
              // records.
              Assert.assertEquals(1, groups.size());
              assignments.put(slot, groups.get(0));
            });
    return assignments;
  }

  /**
   * Write one value per device, using its list index plus valueOffset. For example, six devices
   * with time=10 and valueOffset=100 receive values 100..105.
   */
  private static void writeAtTime(
      Statement statement, List<String> devices, long time, int valueOffset) throws Exception {
    for (int slot = 0; slot < devices.size(); slot++) {
      statement.execute(
          "INSERT INTO "
              + devices.get(slot)
              + "(time,s) VALUES ("
              + time
              + ","
              + (slot + valueOffset)
              + ")");
    }
  }

  /**
   * Verify exactly one row per device at the specified time, including timestamp and value. For
   * example, six devices with time=20 and valueOffset=200 must return values 200..205; callers
   * describe each scenario.
   */
  private static void assertValuesAtTime(
      Statement statement, List<String> devices, long time, int valueOffset) throws Exception {
    for (int slot = 0; slot < devices.size(); slot++) {
      try (ResultSet rows =
          statement.executeQuery("SELECT s FROM " + devices.get(slot) + " WHERE time=" + time)) {
        // Expect true: filtering by the exact time must return the row written for each device,
        // regardless of node count.
        Assert.assertTrue(rows.next());
        // Expect the supplied timestamp: WHERE time selects the exact write batch requested by
        // the caller.
        Assert.assertEquals(time, rows.getLong(1));
        // Expect device index slot + valueOffset, matching the writeAtTime rule.
        Assert.assertEquals(slot + valueOffset, rows.getInt(2));
        // Expect false: one value was written per device at this time, so no second row may
        // exist.
        Assert.assertFalse(rows.next());
      }
    }
  }

  /**
   * Check a new time partition in the two-database PROACTIVE scenario, assuming six distinct slots
   * and a per-database cap equal to dataNodeCount. For example, 3DN uses three groups of two slots
   * and nine replicas across three nodes.
   */
  private static void assertNewTimeDistribution(
      SyncConfigNodeIServiceClient client,
      String database,
      List<String> devices,
      long time,
      int dataNodeCount)
      throws Exception {
    Map<TConsensusGroupId, Set<Integer>> replicas =
        // Expect min(DEVICE_COUNT, dataNodeCount) Data groups; callers list the concrete counts
        // for each cluster size.
        assertRegionGroupsAndReplicas(
            client,
            database,
            TConsensusGroupType.DataRegion,
            Math.min(DEVICE_COUNT, dataNodeCount));
    // Expect replicas on dataNodeCount nodes, with counts differing by at most one.
    assertReplicaDistribution(replicas, dataNodeCount, true);
    // Expect this time partition to use every expected group, with slot counts differing by at
    // most one.
    assertSlotDistribution(
        replicas.keySet(), groupsAtTime(readDataPartitions(client, database, devices, time), time));
  }

  /**
   * Wait for all expected Data replicas in the database to become Running. For example,
   * expectedRegionGroupCount=6 with three replicas requires 18 Running records; query at most 30
   * times.
   */
  private static void awaitRunningDataRegions(
      SyncConfigNodeIServiceClient client, String database, int expectedRegionGroupCount)
      throws Exception {
    TShowRegionResp response = null;
    for (int retry = 0; retry < 30; retry++) {
      response =
          client.showRegion(
              new TShowRegionReq().setConsensusGroupType(TConsensusGroupType.DataRegion));
      // Expect successful SHOW REGIONS (200): statuses may change during recovery, but
      // readiness requires a valid query.
      assertSuccess(response.getStatus());
      long runningReplicas =
          response.getRegionInfoList().stream()
              .filter(region -> database.equals(region.getDatabase()))
              .filter(region -> RegionStatus.Running.getStatus().equals(region.getStatus()))
              .count();
      if (runningReplicas == (long) expectedRegionGroupCount * REPLICATION_FACTOR) {
        return;
      }
      TimeUnit.SECONDS.sleep(1);
    }
    // Expect expectedRegionGroupCount * REPLICATION_FACTOR Running replicas within 30 attempts.
    Assert.fail(
        "DataRegion replicas did not become Running after ConfigNode recovery: " + response);
  }

  /**
   * Enumerate names and use the real BKDR hash to find the target slot. For example, slot=2 returns
   * a root.auto.dN that hashes to 2, without assuming d2 hashes to slot 2.
   */
  private static String findDeviceInSlot(String database, int slot) {
    // Control hashing instead of assuming d0...d5 occupy different slots; measurements share a
    // slot.
    for (int candidate = 0; candidate < SERIES_SLOT_NUM * 100; candidate++) {
      String device = database + ".d" + candidate;
      if (PARTITION_EXECUTOR.getSeriesPartitionSlot(device).getSlotId() == slot) {
        return device;
      }
    }
    throw new AssertionError("No device found in series slot " + slot);
  }

  /**
   * Build a schema partition request for measurement s of each device. For example, root.auto.d8
   * becomes root.auto.d8.s; this method only constructs the request.
   */
  private static TSchemaPartitionReq schemaRequest(List<String> devices) throws Exception {
    return new TSchemaPartitionReq(
        generatePatternTreeBuffer(
            devices.stream().map(device -> device + ".s").toArray(String[]::new)));
  }

  /**
   * Build a request for the database, series slots and time partition start. For example, two
   * devices in distinct slots and time=10 produce two entries requesting t=10; time must be a
   * partition start.
   */
  private static TDataPartitionReq dataRequest(String database, List<String> devices, long time) {
    Map<TSeriesPartitionSlot, TTimeSlotList> slots = new HashMap<>();
    for (String device : devices) {
      slots.put(
          PARTITION_EXECUTOR.getSeriesPartitionSlot(device),
          new TTimeSlotList(Collections.singletonList(new TTimePartitionSlot(time)), false, false));
    }
    return new TDataPartitionReq(Collections.singletonMap(database, slots));
  }

  /**
   * Query allocated data partitions, requiring devices in distinct series slots. For example, six
   * distinct slots at time=10 must return six slot entries; callers verify their owners.
   */
  private static Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TConsensusGroupId>>>
      readDataPartitions(
          SyncConfigNodeIServiceClient client, String database, List<String> devices, long time)
          throws Exception {
    TDataPartitionTableResp response =
        client.getDataPartitionTable(dataRequest(database, devices, time));
    // Expect successful partition query (200): the requested partitions must already have been
    // allocated.
    assertSuccess(response.getStatus());
    Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TConsensusGroupId>>> partitions =
        response.getDataPartitionTable().get(database);
    // Expect devices.size() distinct slots: callers must provide devices in different slots
    // with existing partitions.
    Assert.assertEquals(devices.size(), partitions.size());
    return partitions;
  }

  /**
   * Extract group IDs at the requested time, retaining duplicates for slot counting. For example,
   * six slots owned by A and B may produce three A entries and three B entries.
   */
  private static List<TConsensusGroupId> groupsAtTime(
      Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TConsensusGroupId>>> partitions,
      long time) {
    List<TConsensusGroupId> groups = new ArrayList<>();
    partitions
        .values()
        .forEach(
            times -> {
              List<TConsensusGroupId> regionIds = times.get(new TTimePartitionSlot(time));
              // Expect one group ID per slot/time: this checks logical ownership; the three
              // replicas are checked separately.
              Assert.assertEquals(1, regionIds.size());
              groups.add(regionIds.get(0));
            });
    return groups;
  }

  /**
   * Verify that every expected group is used and slot counts differ by at most one. For example,
   * expected groups A/B with assignments A,A,A,B,B,B pass; an unused B fails.
   */
  private static void assertSlotDistribution(
      Set<TConsensusGroupId> groups, List<TConsensusGroupId> assigned) {
    // One assignment per series slot at a fixed time: all groups must be used, counts differ by
    // <=1.
    Map<TConsensusGroupId, Integer> counts = new HashMap<>();
    assigned.forEach(group -> counts.merge(group, 1, Integer::sum));
    // Expect exactly the caller's group set to be used: every expected group must receive a
    // slot.
    Assert.assertEquals(groups, counts.keySet());
    // Expect slot counts to differ by at most one; callers describe the concrete slot and group
    // counts.
    Assert.assertTrue(
        counts.toString(),
        Collections.max(counts.values()) - Collections.min(counts.values()) <= 1);
  }

  /**
   * Check the number of distinct DataNodes hosting replicas and optionally require counts to differ
   * by at most one. For example, six groups with three replicas balanced across nine nodes yield
   * two replicas per node.
   *
   * @param replicas group IDs mapped to the DataNode IDs hosting their replicas
   * @param expectedHostingDataNodeCount expected number of distinct DataNodes actually hosting
   *     these replicas
   * @param requireBalancedReplicaCounts whether to require balanced replica counts among hosting
   *     nodes
   */
  private static void assertReplicaDistribution(
      Map<TConsensusGroupId, Set<Integer>> replicas,
      int expectedHostingDataNodeCount,
      boolean requireBalancedReplicaCounts) {
    Map<Integer, Integer> counts = new HashMap<>();
    replicas.values().forEach(nodes -> nodes.forEach(node -> counts.merge(node, 1, Integer::sum)));
    // Expect expectedHostingDataNodeCount distinct DataNodes to host these replicas.
    Assert.assertEquals(counts.toString(), expectedHostingDataNodeCount, counts.size());
    if (requireBalancedReplicaCounts) {
      // Expect replica counts among hosting nodes to differ by at most one, only when balance
      // is requested.
      Assert.assertTrue(
          counts.toString(),
          Collections.max(counts.values()) - Collections.min(counts.values()) <= 1);
    }
  }

  /**
   * Check logical group count, total replica rows and distinct DataNodes per group for one database
   * and Region type.
   *
   * <p>For root.auto, DataRegion and expectedRegionGroupCount=2, expect two logical groups, 2*3=6
   * replica rows and three distinct DataNodes per group. Slot allocation and balance across groups
   * are checked separately.
   *
   * @param database database whose Regions are counted
   * @param regionType SchemaRegion or DataRegion to check
   * @param expectedRegionGroupCount expected number of created logical groups for this database and
   *     type, not nodes or slots
   * @return group IDs mapped to the DataNode IDs hosting their replicas
   */
  private static Map<TConsensusGroupId, Set<Integer>> assertRegionGroupsAndReplicas(
      SyncConfigNodeIServiceClient client,
      String database,
      TConsensusGroupType regionType,
      int expectedRegionGroupCount)
      throws Exception {
    // SHOW REGIONS returns replicas, not logical groups: deduplicate IDs before checking G and 3*G.
    TShowRegionResp response =
        client.showRegion(new TShowRegionReq().setConsensusGroupType(regionType));
    // Expect successful SHOW REGIONS (200) before counting replicas by database, type and group
    // ID.
    assertSuccess(response.getStatus());
    Map<TConsensusGroupId, Set<Integer>> replicas = new HashMap<>();
    int replicaCount = 0;
    for (TRegionInfo region : response.getRegionInfoList()) {
      if (database.equals(region.getDatabase())) {
        replicas
            .computeIfAbsent(region.getConsensusGroupId(), ignored -> new HashSet<>())
            .add(region.getDataNodeId());
        replicaCount++;
      }
    }
    // Expect expectedRegionGroupCount distinct groups for the specified database and Region
    // type.
    Assert.assertEquals(database + " " + regionType, expectedRegionGroupCount, replicas.size());
    // Expect expectedRegionGroupCount * REPLICATION_FACTOR replica rows.
    Assert.assertEquals(expectedRegionGroupCount * REPLICATION_FACTOR, replicaCount);
    // Expect each group's replicas on REPLICATION_FACTOR distinct DataNodes, preventing
    // colocated replicas of one group.
    replicas.values().forEach(nodes -> Assert.assertEquals(REPLICATION_FACTOR, nodes.size()));
    return replicas;
  }

  /**
   * Check that both database group caps equal the same expected value. For example, 6 means both
   * Schema and Data caps are 6, without requiring six groups to have been created.
   */
  private static void assertMaximum(
      SyncConfigNodeIServiceClient client, String database, int expectedRegionGroupLimit)
      throws Exception {
    // Expect both Schema and Data caps to equal the caller's expectedRegionGroupLimit.
    assertMaximum(client, database, expectedRegionGroupLimit, expectedRegionGroupLimit);
  }

  /**
   * Check database group caps independently. For example, expectedSchemaRegionGroupLimit=6 and
   * expectedDataRegionGroupLimit=2 require Schema cap 6 and Data cap 2; actual group counts are
   * checked separately.
   */
  private static void assertMaximum(
      SyncConfigNodeIServiceClient client,
      String database,
      int expectedSchemaRegionGroupLimit,
      int expectedDataRegionGroupLimit)
      throws Exception {
    TDatabaseSchemaResp response =
        client.getMatchedDatabaseSchemas(
            new TGetDatabaseReq(Arrays.asList(database.split("\\.")), ALL_MATCH_SCOPE_BINARY));
    // Expect successful database metadata query (200) before checking both group caps.
    assertSuccess(response.getStatus());
    TDatabaseSchema schema = response.getDatabaseSchemaMap().get(database);
    // Expect Schema cap expectedSchemaRegionGroupLimit; a cap is not the number of groups
    // already created.
    Assert.assertEquals(expectedSchemaRegionGroupLimit, schema.getMaxSchemaRegionGroupNum());
    // Expect Data cap expectedDataRegionGroupLimit, checked independently of the Schema cap.
    Assert.assertEquals(expectedDataRegionGroupLimit, schema.getMaxDataRegionGroupNum());
  }

  /**
   * Check RPC success; for example, normal database creation returns SUCCESS_STATUS (200). Callers
   * assert expected failures separately.
   */
  private static void assertSuccess(TSStatus status) {
    // Expect SUCCESS_STATUS (200) for normal database creation, allocation and queries; failure
    // cases use assertThrows.
    Assert.assertEquals(
        status.getMessage(), TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
  }
}
