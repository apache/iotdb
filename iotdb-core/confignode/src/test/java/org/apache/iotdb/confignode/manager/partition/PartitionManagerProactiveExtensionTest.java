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
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.partition.DataPartitionTable;
import org.apache.iotdb.commons.partition.SchemaPartitionTable;
import org.apache.iotdb.commons.partition.SeriesPartitionTable;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetDataPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetOrCreateDataPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.PreDeleteDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.write.partition.CreateDataPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.write.partition.CreateSchemaPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.write.region.CreateRegionGroupsPlan;
import org.apache.iotdb.confignode.exception.NotEnoughDataNodeException;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.ProcedureManager;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.schema.ClusterSchemaManager;
import org.apache.iotdb.confignode.persistence.partition.PartitionInfo;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.confignode.rpc.thrift.TTimeSlotList;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class PartitionManagerProactiveExtensionTest {

  private static final String DATABASE = "root.proactive";
  private static final String SECOND_DATABASE = "root.other";
  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> parameters() {
    return Arrays.asList(
        new Object[] {TConsensusGroupType.SchemaRegion},
        new Object[] {TConsensusGroupType.DataRegion});
  }

  private final TConsensusGroupType type;
  private PartitionInfo partitionInfo;
  private PartitionManager partitionManager;
  private ClusterSchemaManager schemaManager;
  private LoadManager loadManager;
  private ProcedureManager procedureManager;
  private ConsensusManager consensusManager;
  private RegionGroupExtensionPolicy originalSchemaPolicy;
  private RegionGroupExtensionPolicy originalDataPolicy;
  private int originalSeriesSlotNum;

  public PartitionManagerProactiveExtensionTest(TConsensusGroupType type) {
    this.type = type;
  }

  @Before
  public void setUp() throws Exception {
    originalSchemaPolicy = CONF.getSchemaRegionGroupExtensionPolicy();
    originalDataPolicy = CONF.getDataRegionGroupExtensionPolicy();
    originalSeriesSlotNum = CONF.getSeriesSlotNum();
    CONF.setSchemaRegionGroupExtensionPolicy(RegionGroupExtensionPolicy.PROACTIVE);
    CONF.setDataRegionGroupExtensionPolicy(RegionGroupExtensionPolicy.PROACTIVE);
    CONF.setSeriesSlotNum(100);

    partitionInfo = new PartitionInfo();
    for (String database : Arrays.asList(DATABASE, SECOND_DATABASE)) {
      partitionInfo.createDatabase(
          new DatabaseSchemaPlan(
              ConfigPhysicalPlanType.CreateDatabase, new TDatabaseSchema(database)));
    }
    IManager manager = mock(IManager.class);
    schemaManager = mock(ClusterSchemaManager.class);
    loadManager = mock(LoadManager.class);
    procedureManager = mock(ProcedureManager.class);
    consensusManager = mock(ConsensusManager.class);
    when(manager.getClusterSchemaManager()).thenReturn(schemaManager);
    when(manager.getLoadManager()).thenReturn(loadManager);
    when(manager.getProcedureManager()).thenReturn(procedureManager);
    when(manager.getConsensusManager()).thenReturn(consensusManager);
    when(schemaManager.getMinRegionGroupNum(DATABASE, type)).thenReturn(4);
    when(schemaManager.getMaxRegionGroupNum(DATABASE, type)).thenReturn(5);
    when(schemaManager.getMaxRegionGroupNum(SECOND_DATABASE, type)).thenReturn(3);
    when(loadManager.allocateRegionGroups(anyMap(), any(TConsensusGroupType.class)))
        .thenAnswer(
            invocation -> {
              Map<String, Integer> allotments = invocation.getArgument(0);
              TConsensusGroupType allocatedType = invocation.getArgument(1);
              return regionPlan(allotments, allocatedType);
            });
    when(procedureManager.createRegionGroups(any(TConsensusGroupType.class), any()))
        .thenAnswer(
            invocation -> {
              partitionInfo.createRegionGroups(invocation.getArgument(1));
              return RpcUtils.SUCCESS_STATUS;
            });
    partitionManager = new PartitionManager(manager, partitionInfo);
  }

  @After
  public void tearDown() {
    if (partitionManager != null) {
      partitionManager.stopRegionCleaner();
    }
    CONF.setSchemaRegionGroupExtensionPolicy(originalSchemaPolicy);
    CONF.setDataRegionGroupExtensionPolicy(originalDataPolicy);
    CONF.setSeriesSlotNum(originalSeriesSlotNum);
  }

  @Test
  public void testEachNewSlotCreatesOneRegionUntilMaximum() throws Exception {
    for (int slot = 0; slot < 8; slot++) {
      assertSuccess(extend(DATABASE, slots(slot)));
      // A configured minimum of four is reached gradually, as with AUTO.
      assertEquals(Math.min(slot + 1, 5), partitionInfo.getRegionGroupCount(DATABASE, type));
      persistSlots(DATABASE, type, 0, slot);
    }
    verify(loadManager, never()).allocateRegionGroups(anyMap(), eq(otherType()));
  }

  @Test
  public void testDuplicatePendingSlotsDoNotInflateMinimumGrowth() throws Exception {
    List<TSeriesPartitionSlot> pending = slots(0, 0);
    if (type == TConsensusGroupType.SchemaRegion) {
      pending =
          partitionInfo
              .filterUnassignedSchemaPartitionSlots(Collections.singletonMap(DATABASE, pending))
              .get(DATABASE);
    }
    assertSuccess(extend(DATABASE, pending));
    assertEquals(1, partitionInfo.getRegionGroupCount(DATABASE, type));
    persistSlots(DATABASE, type, 0, 0);
    assertSuccess(extend(DATABASE, slots(1, 1)));
    assertEquals(2, partitionInfo.getRegionGroupCount(DATABASE, type));
  }

  @Test
  public void testMixedDataRequestOnlyCountsSlotsWithMissingTimePartitions() throws Exception {
    assumeTrue(type == TConsensusGroupType.DataRegion);
    assertSuccess(extend(DATABASE, slots(0, 1)));
    persistSlots(DATABASE, type, 0, 0, 1);
    configureDataPartitionConsensusAndAllocation();
    clearInvocations(loadManager);

    GetOrCreateDataPartitionPlan request =
        new GetOrCreateDataPartitionPlan(
            Collections.singletonMap(DATABASE, dataRequest(0, 0, 1, 2)));
    assertSuccess(partitionManager.getOrCreateDataPartition(request).getStatus());
    assertEquals(3, partitionInfo.getRegionGroupCount(DATABASE, type));
    verify(loadManager).allocateRegionGroups(Collections.singletonMap(DATABASE, 1), type);

    clearInvocations(loadManager);
    assertSuccess(partitionManager.getOrCreateDataPartition(request).getStatus());
    verify(loadManager, never()).allocateRegionGroups(anyMap(), any());
  }

  @Test
  public void testSatisfiedDatabaseDoesNotGrowForAnotherDatabasesPendingSlots() throws Exception {
    assumeTrue(type == TConsensusGroupType.DataRegion);
    assertSuccess(extend(DATABASE, slots(0)));
    persistSlots(DATABASE, type, 0, 0);
    configureDataPartitionConsensusAndAllocation();
    clearInvocations(loadManager);

    Map<String, Map<TSeriesPartitionSlot, TTimeSlotList>> request = new HashMap<>();
    request.put(DATABASE, dataRequest(0, 0));
    request.put(SECOND_DATABASE, dataRequest(0, 1));
    assertSuccess(
        partitionManager
            .getOrCreateDataPartition(new GetOrCreateDataPartitionPlan(request))
            .getStatus());
    assertEquals(1, partitionInfo.getRegionGroupCount(DATABASE, type));
    assertEquals(1, partitionInfo.getRegionGroupCount(SECOND_DATABASE, type));
    verify(loadManager).allocateRegionGroups(Collections.singletonMap(SECOND_DATABASE, 1), type);
  }

  @Test
  public void testNewTimePartitionsStillGrowSingleSlotToMinimum() throws Exception {
    assumeTrue(type == TConsensusGroupType.DataRegion);
    configureDataPartitionConsensusAndAllocation();
    for (int time = 0; time < 6; time++) {
      assertSuccess(
          partitionManager
              .getOrCreateDataPartition(
                  new GetOrCreateDataPartitionPlan(
                      Collections.singletonMap(DATABASE, dataRequest(time, 0))))
              .getStatus());
      assertEquals(Math.min(4, time + 1), partitionInfo.getRegionGroupCount(DATABASE, type));
    }
  }

  private Map<TSeriesPartitionSlot, TTimeSlotList> dataRequest(long time, int... ids) {
    Map<TSeriesPartitionSlot, TTimeSlotList> request = new HashMap<>();
    for (TSeriesPartitionSlot slot : slots(ids)) {
      request.put(
          slot,
          new TTimeSlotList(Collections.singletonList(new TTimePartitionSlot(time)), false, false));
    }
    return request;
  }

  private void configureDataPartitionConsensusAndAllocation() throws Exception {
    when(consensusManager.read(any(GetDataPartitionPlan.class)))
        .thenAnswer(invocation -> partitionInfo.getDataPartition(invocation.getArgument(0)));
    when(consensusManager.confirmLeader()).thenReturn(RpcUtils.SUCCESS_STATUS);
    when(consensusManager.write(any(CreateDataPartitionPlan.class)))
        .thenAnswer(invocation -> partitionInfo.createDataPartition(invocation.getArgument(0)));
    when(loadManager.allocateDataPartition(anyMap()))
        .thenAnswer(
            invocation -> {
              Map<String, Map<TSeriesPartitionSlot, TTimeSlotList>> pending =
                  invocation.getArgument(0);
              Map<String, DataPartitionTable> assigned = new HashMap<>();
              for (Map.Entry<String, Map<TSeriesPartitionSlot, TTimeSlotList>> database :
                  pending.entrySet()) {
                DataPartitionTable table = new DataPartitionTable();
                TConsensusGroupId region =
                    partitionInfo.getAllRegionGroupIds(database.getKey(), type).get(0);
                database
                    .getValue()
                    .forEach(
                        (slot, times) -> {
                          SeriesPartitionTable series = new SeriesPartitionTable();
                          times
                              .getTimePartitionSlots()
                              .forEach(time -> series.putDataPartition(time, region));
                          table.getDataPartitionMap().put(slot, series);
                        });
                assigned.put(database.getKey(), table);
              }
              return assigned;
            });
  }

  @Test
  public void testBatchCountsDistinctPersistedAndIncomingSlots() throws Exception {
    when(schemaManager.getMinRegionGroupNum(DATABASE, type)).thenReturn(1);
    assertSuccess(extend(DATABASE, slots(1)));
    persistSlots(DATABASE, type, 0, 1);
    clearInvocations(loadManager);

    assertSuccess(extend(DATABASE, slots(1, 2, 2, 3, 3)));
    assertEquals(3, partitionInfo.getRegionGroupCount(DATABASE, type));
    verify(loadManager).allocateRegionGroups(Collections.singletonMap(DATABASE, 2), type);
    persistSlots(DATABASE, type, 0, 2, 3);
    assertEquals(3, partitionInfo.getSeriesPartitionSlotsCount(DATABASE, type, slots(1, 1)));

    clearInvocations(loadManager);
    for (int time = 1; time <= 3; time++) {
      assertSuccess(extend(DATABASE, slots(1, 2)));
      persistSlots(DATABASE, type, time, 1, 2);
    }
    assertEquals(3, partitionInfo.getRegionGroupCount(DATABASE, type));
    verify(loadManager, never()).allocateRegionGroups(anyMap(), any());
  }

  @Test
  public void testSchemaAndDataActivationAreIndependent() throws Exception {
    partitionInfo.createRegionGroups(
        regionPlan(Collections.singletonMap(DATABASE, 1), otherType()));
    persistSlots(DATABASE, otherType(), 0, 10, 11, 12, 13);

    assertSuccess(extend(DATABASE, slots(10)));
    assertEquals(1, partitionInfo.getRegionGroupCount(DATABASE, type));
    persistSlots(DATABASE, type, 0, 10);
    assertEquals(1, partitionInfo.getSeriesPartitionSlotsCount(DATABASE, type, slots(10)));
    assertEquals(4, partitionInfo.getSeriesPartitionSlotsCount(DATABASE, otherType(), slots(10)));

    assertSuccess(extend(DATABASE, slots(11)));
    assertEquals(2, partitionInfo.getRegionGroupCount(DATABASE, type));
    assertEquals(1, partitionInfo.getRegionGroupCount(DATABASE, otherType()));
  }

  @Test
  public void testBatchUsesEachDatabaseMaximum() throws Exception {
    Map<String, List<TSeriesPartitionSlot>> requested = new HashMap<>();
    requested.put(DATABASE, slots(0, 1, 2, 3));
    requested.put(SECOND_DATABASE, slots(0, 1, 2, 3, 4, 5));
    assertSuccess(partitionManager.extendRegionGroupIfNecessary(requested, type));
    assertEquals(4, partitionInfo.getRegionGroupCount(DATABASE, type));
    assertEquals(3, partitionInfo.getRegionGroupCount(SECOND_DATABASE, type));
    Map<String, Integer> expected = new HashMap<>();
    expected.put(DATABASE, 4);
    expected.put(SECOND_DATABASE, 3);
    verify(loadManager).allocateRegionGroups(expected, type);
  }

  @Test
  public void testNoPreallocationWithoutActivatedSlots() throws Exception {
    assertSuccess(partitionManager.extendRegionGroupIfNecessary(Collections.emptyMap(), type));
    assertSuccess(extend(DATABASE, Collections.emptyList()));
    assertEquals(0, partitionInfo.getRegionGroupCount(DATABASE, type));
    verify(loadManager, never()).allocateRegionGroups(anyMap(), any());
  }

  @Test
  public void testSingleSlotTimePartitionsReachDefaultDataMinimumLikeAuto() throws Exception {
    assumeTrue(type == TConsensusGroupType.DataRegion);
    checkSingleSlotTimePartitionsReachMinimumLikeAuto(2);
  }

  @Test
  public void testSingleSlotTimePartitionsReachConfiguredDataMinimumLikeAuto() throws Exception {
    assumeTrue(type == TConsensusGroupType.DataRegion);
    checkSingleSlotTimePartitionsReachMinimumLikeAuto(4);
  }

  @Test
  public void testEachDatabaseReachesItsOwnMinimum() throws Exception {
    assumeTrue(type == TConsensusGroupType.DataRegion);
    when(schemaManager.getMinRegionGroupNum(SECOND_DATABASE, type)).thenReturn(2);

    for (int time = 0; time < 6; time++) {
      Map<String, Collection<TSeriesPartitionSlot>> pending = new HashMap<>();
      for (String database : Arrays.asList(DATABASE, SECOND_DATABASE)) {
        pending.put(database, pendingDataSlots(database, time));
      }
      assertSuccess(partitionManager.extendRegionGroupIfNecessary(pending, type));
      assertEquals(Math.min(time + 1, 4), partitionInfo.getRegionGroupCount(DATABASE, type));
      assertEquals(Math.min(time + 1, 2), partitionInfo.getRegionGroupCount(SECOND_DATABASE, type));
      persistSlots(DATABASE, type, time, 0);
      persistSlots(SECOND_DATABASE, type, time, 0);
    }
  }

  @Test
  public void testNoMinimumGrowthWithoutPendingSlots() throws Exception {
    assertSuccess(extend(DATABASE, slots(0)));
    persistSlots(DATABASE, type, 0, 0);
    clearInvocations(loadManager);

    assertSuccess(partitionManager.extendRegionGroupIfNecessary(Collections.emptyMap(), type));
    assertSuccess(extend(DATABASE, Collections.emptyList()));
    assertEquals(1, partitionInfo.getRegionGroupCount(DATABASE, type));
    verify(loadManager, never()).allocateRegionGroups(anyMap(), any());
  }

  @Test
  public void testExistingRegionsAreNotRemovedOrExceeded() throws Exception {
    partitionInfo.createRegionGroups(regionPlan(Collections.singletonMap(DATABASE, 4), type));
    when(schemaManager.getMaxRegionGroupNum(DATABASE, type)).thenReturn(3);
    assertSuccess(extend(DATABASE, slots(0)));
    assertSuccess(extend(DATABASE, slots(0, 1, 2, 3, 4)));
    assertEquals(4, partitionInfo.getRegionGroupCount(DATABASE, type));
    verify(loadManager, never()).allocateRegionGroups(anyMap(), any());
  }

  @Test
  public void testAllDisabledRegionsCreateOneReplacement() throws Exception {
    when(schemaManager.getMinRegionGroupNum(DATABASE, type)).thenReturn(1);
    assertSuccess(extend(DATABASE, slots(0)));
    persistSlots(DATABASE, type, 0, 0);
    TConsensusGroupId disabledGroup = partitionInfo.getAllRegionGroupIds(DATABASE, type).get(0);
    when(loadManager.getRegionGroupStatus(disabledGroup)).thenReturn(RegionGroupStatus.Disabled);
    clearInvocations(loadManager);

    assertSuccess(extend(DATABASE, slots(0)));
    assertEquals(2, partitionInfo.getRegionGroupCount(DATABASE, type));
    verify(loadManager).allocateRegionGroups(Collections.singletonMap(DATABASE, 1), type);
    for (TConsensusGroupId group : partitionInfo.getAllRegionGroupIds(DATABASE, type)) {
      if (!disabledGroup.equals(group)) {
        when(loadManager.getRegionGroupStatus(group)).thenReturn(RegionGroupStatus.Running);
      }
    }
    clearInvocations(loadManager);
    assertSuccess(extend(DATABASE, slots(0)));
    assertSuccess(extend(DATABASE, slots(1)));
    assertEquals(2, partitionInfo.getRegionGroupCount(DATABASE, type));
    verify(loadManager, never()).allocateRegionGroups(anyMap(), any());
  }

  @Test
  public void testAllDisabledRegionsRespectMaximum() throws Exception {
    assertSuccess(extend(DATABASE, slots(0, 1, 2, 3, 4)));
    persistSlots(DATABASE, type, 0, 0, 1, 2, 3, 4);
    for (TConsensusGroupId group : partitionInfo.getAllRegionGroupIds(DATABASE, type)) {
      when(loadManager.getRegionGroupStatus(group)).thenReturn(RegionGroupStatus.Disabled);
    }
    clearInvocations(loadManager);
    assertSuccess(extend(DATABASE, slots(0)));
    assertEquals(5, partitionInfo.getRegionGroupCount(DATABASE, type));
    verify(loadManager, never()).allocateRegionGroups(anyMap(), any());
  }

  @Test
  public void testDisabledStatusOfOtherTypeDoesNotAffectReplacement() throws Exception {
    when(schemaManager.getMinRegionGroupNum(DATABASE, type)).thenReturn(1);
    assertSuccess(extend(DATABASE, slots(0)));
    persistSlots(DATABASE, type, 0, 0);
    partitionInfo.createRegionGroups(
        regionPlan(Collections.singletonMap(DATABASE, 1), otherType()));
    TConsensusGroupId currentGroup = partitionInfo.getAllRegionGroupIds(DATABASE, type).get(0);
    TConsensusGroupId otherGroup = partitionInfo.getAllRegionGroupIds(DATABASE, otherType()).get(0);
    when(loadManager.getRegionGroupStatus(currentGroup)).thenReturn(RegionGroupStatus.Running);
    when(loadManager.getRegionGroupStatus(otherGroup)).thenReturn(RegionGroupStatus.Disabled);
    clearInvocations(loadManager);
    assertSuccess(extend(DATABASE, slots(0)));
    verify(loadManager, never()).allocateRegionGroups(anyMap(), any());

    when(loadManager.getRegionGroupStatus(currentGroup)).thenReturn(RegionGroupStatus.Disabled);
    when(loadManager.getRegionGroupStatus(otherGroup)).thenReturn(RegionGroupStatus.Running);
    assertSuccess(extend(DATABASE, slots(0)));
    assertEquals(2, partitionInfo.getRegionGroupCount(DATABASE, type));
    assertEquals(1, partitionInfo.getRegionGroupCount(DATABASE, otherType()));
    verify(loadManager).allocateRegionGroups(Collections.singletonMap(DATABASE, 1), type);
  }

  @Test
  public void testNoReplacementWithoutPendingSlots() throws Exception {
    assertSuccess(extend(DATABASE, slots(0)));
    persistSlots(DATABASE, type, 0, 0);
    TConsensusGroupId group = partitionInfo.getAllRegionGroupIds(DATABASE, type).get(0);
    when(loadManager.getRegionGroupStatus(group)).thenReturn(RegionGroupStatus.Disabled);
    clearInvocations(loadManager);
    assertSuccess(extend(DATABASE, Collections.emptyList()));
    assertEquals(1, partitionInfo.getRegionGroupCount(DATABASE, type));
    verify(loadManager, never()).allocateRegionGroups(anyMap(), any());
  }

  @Test
  public void testCustomStillAllocatesMaximumImmediately() throws Exception {
    setPolicy(RegionGroupExtensionPolicy.CUSTOM);
    assertSuccess(extend(DATABASE, slots(0)));
    assertEquals(5, partitionInfo.getRegionGroupCount(DATABASE, type));
    setPolicy(RegionGroupExtensionPolicy.PROACTIVE);
    clearInvocations(loadManager);
    assertSuccess(extend(DATABASE, slots(1)));
    assertEquals(5, partitionInfo.getRegionGroupCount(DATABASE, type));
    verify(loadManager, never()).allocateRegionGroups(anyMap(), any());
  }

  @Test
  public void testAutoKeepsSlowerGrowthAndSwitchCatchesUp() throws Exception {
    setPolicy(RegionGroupExtensionPolicy.AUTO);
    when(schemaManager.getMinRegionGroupNum(DATABASE, type)).thenReturn(1);
    for (int slot = 0; slot < 3; slot++) {
      assertSuccess(extend(DATABASE, slots(slot)));
      persistSlots(DATABASE, type, 0, slot);
    }
    assertEquals(1, partitionInfo.getRegionGroupCount(DATABASE, type));

    setPolicy(RegionGroupExtensionPolicy.PROACTIVE);
    assertSuccess(extend(DATABASE, slots(3)));
    assertEquals(4, partitionInfo.getRegionGroupCount(DATABASE, type));
  }

  @Test
  public void testAutoAndProactiveReachSameMaximum() throws Exception {
    setPolicy(RegionGroupExtensionPolicy.AUTO);
    List<TSeriesPartitionSlot> allSlots =
        IntStream.range(0, CONF.getSeriesSlotNum())
            .mapToObj(TSeriesPartitionSlot::new)
            .collect(Collectors.toList());
    assertSuccess(extend(DATABASE, allSlots));
    assertEquals(5, partitionInfo.getRegionGroupCount(DATABASE, type));

    setPolicy(RegionGroupExtensionPolicy.PROACTIVE);
    when(schemaManager.getMaxRegionGroupNum(SECOND_DATABASE, type)).thenReturn(5);
    assertSuccess(extend(SECOND_DATABASE, allSlots));
    assertEquals(5, partitionInfo.getRegionGroupCount(SECOND_DATABASE, type));
  }

  @Test
  public void testSixSlotsGrowFasterThanAutoWithOneThousandSlots() throws Exception {
    configureSparseSlotComparison();
    int minimum = type == TConsensusGroupType.SchemaRegion ? 1 : 2;
    setPolicy(RegionGroupExtensionPolicy.AUTO);
    for (int slot = 0; slot < 6; slot++) {
      assertSuccess(extend(DATABASE, slots(slot)));
      persistSlots(DATABASE, type, 0, slot);
      assertEquals(Math.min(slot + 1, minimum), partitionInfo.getRegionGroupCount(DATABASE, type));
    }

    setPolicy(RegionGroupExtensionPolicy.PROACTIVE);
    for (int slot = 0; slot < 6; slot++) {
      assertSuccess(extend(SECOND_DATABASE, slots(slot)));
      persistSlots(SECOND_DATABASE, type, 0, slot);
      assertEquals(slot + 1, partitionInfo.getRegionGroupCount(SECOND_DATABASE, type));
    }
    // Six active slots are still below the nine-group resource limit.
    assertEquals(6, partitionInfo.getRegionGroupCount(SECOND_DATABASE, type));
    clearInvocations(loadManager);
    assertSuccess(extend(SECOND_DATABASE, slots(0, 1, 2, 3, 4, 5)));
    verify(loadManager, never()).allocateRegionGroups(anyMap(), any());

    for (int slot = 6; slot < 11; slot++) {
      assertSuccess(extend(SECOND_DATABASE, slots(slot)));
      persistSlots(SECOND_DATABASE, type, 0, slot);
      assertEquals(Math.min(slot + 1, 9), partitionInfo.getRegionGroupCount(SECOND_DATABASE, type));
    }
  }

  @Test
  public void testSixSlotBatchGrowsFasterThanAutoWithOneThousandSlots() throws Exception {
    configureSparseSlotComparison();
    setPolicy(RegionGroupExtensionPolicy.AUTO);
    assertSuccess(extend(DATABASE, slots(0, 1, 2, 3, 4, 5)));
    assertEquals(
        type == TConsensusGroupType.SchemaRegion ? 1 : 2,
        partitionInfo.getRegionGroupCount(DATABASE, type));

    setPolicy(RegionGroupExtensionPolicy.PROACTIVE);
    assertSuccess(extend(SECOND_DATABASE, slots(0, 1, 2, 3, 4, 5)));
    assertEquals(6, partitionInfo.getRegionGroupCount(SECOND_DATABASE, type));
    verify(loadManager).allocateRegionGroups(Collections.singletonMap(SECOND_DATABASE, 6), type);
  }

  @Test
  public void testFilteredRequestCountsEachSeriesSlotOnce() throws Exception {
    when(schemaManager.getMinRegionGroupNum(DATABASE, type)).thenReturn(1);
    assertSuccess(extend(DATABASE, slots(0)));
    persistSlots(DATABASE, type, 0, 0);

    Collection<TSeriesPartitionSlot> pendingSlots;
    if (type == TConsensusGroupType.SchemaRegion) {
      pendingSlots =
          partitionInfo
              .filterUnassignedSchemaPartitionSlots(
                  Collections.singletonMap(DATABASE, slots(0, 1, 2)))
              .get(DATABASE);
      assertEquals(slots(1, 2), pendingSlots);
    } else {
      Map<TSeriesPartitionSlot, TTimeSlotList> request = new HashMap<>();
      for (TSeriesPartitionSlot slot : slots(0, 1, 2)) {
        request.put(
            slot,
            new TTimeSlotList(Collections.singletonList(new TTimePartitionSlot(1)), false, false));
      }
      pendingSlots =
          partitionInfo
              .filterUnassignedDataPartitionSlots(Collections.singletonMap(DATABASE, request))
              .get(DATABASE)
              .keySet();
      // Filtering inserted empty entries for slots 1 and 2, but only slot 0 is assigned.
      assertEquals(
          1, partitionInfo.getSeriesPartitionSlotsCount(DATABASE, type, Collections.emptyList()));
      assertEquals(2, partitionInfo.getSeriesPartitionSlotsCount(DATABASE, type, slots(3, 3)));
    }
    assertSuccess(extend(DATABASE, pendingSlots));
    assertEquals(3, partitionInfo.getRegionGroupCount(DATABASE, type));
  }

  @Test
  public void testPreDeletedDatabaseIsReported() throws Exception {
    partitionInfo.preDeleteDatabase(
        new PreDeleteDatabasePlan(DATABASE, PreDeleteDatabasePlan.PreDeleteType.EXECUTE));
    assertEquals(
        TSStatusCode.DATABASE_NOT_EXIST.getStatusCode(), extend(DATABASE, slots(0)).getCode());
    verify(loadManager, never()).allocateRegionGroups(anyMap(), any());
  }

  @Test
  public void testHigherResourceMaximumAllowsFurtherGrowth() throws Exception {
    when(schemaManager.getMaxRegionGroupNum(DATABASE, type)).thenReturn(3);
    assertSuccess(extend(DATABASE, slots(0, 1, 2, 3, 4, 5)));
    persistSlots(DATABASE, type, 0, 0, 1, 2, 3, 4, 5);
    assertEquals(3, partitionInfo.getRegionGroupCount(DATABASE, type));
    clearInvocations(loadManager);

    when(schemaManager.getMaxRegionGroupNum(DATABASE, type)).thenReturn(5);
    assertSuccess(extend(DATABASE, slots(6)));
    assertEquals(5, partitionInfo.getRegionGroupCount(DATABASE, type));
    verify(loadManager).allocateRegionGroups(Collections.singletonMap(DATABASE, 2), type);
  }

  @Test
  public void testAllocationFailureIsReturnedWithoutPersistingRegions() throws Exception {
    doThrow(new NotEnoughDataNodeException(Collections.emptyList(), 3))
        .when(loadManager)
        .allocateRegionGroups(anyMap(), eq(type));
    assertEquals(
        TSStatusCode.NO_ENOUGH_DATANODE.getStatusCode(), extend(DATABASE, slots(0)).getCode());
    assertEquals(0, partitionInfo.getRegionGroupCount(DATABASE, type));
    verify(procedureManager, never()).createRegionGroups(any(), any());
  }

  @Test
  public void testProcedureFailureIsReturned() throws Exception {
    TSStatus failure = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
    doReturn(failure).when(procedureManager).createRegionGroups(eq(type), any());
    assertEquals(failure, extend(DATABASE, slots(0)));
    assertEquals(0, partitionInfo.getRegionGroupCount(DATABASE, type));
  }

  @Test
  public void testRetryOnlyCreatesRemainingRegionsAfterPartialSuccess() throws Exception {
    TSStatus failure = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
    doAnswer(
            invocation -> {
              CreateRegionGroupsPlan requested = invocation.getArgument(1);
              CreateRegionGroupsPlan partial = new CreateRegionGroupsPlan();
              partial.addRegionGroup(DATABASE, requested.getRegionGroupMap().get(DATABASE).get(0));
              partitionInfo.createRegionGroups(partial);
              return failure;
            })
        .doAnswer(
            invocation -> {
              partitionInfo.createRegionGroups(invocation.getArgument(1));
              return RpcUtils.SUCCESS_STATUS;
            })
        .when(procedureManager)
        .createRegionGroups(eq(type), any());

    assertEquals(failure, extend(DATABASE, slots(0, 1, 2)));
    assertEquals(1, partitionInfo.getRegionGroupCount(DATABASE, type));
    clearInvocations(loadManager);
    assertSuccess(extend(DATABASE, slots(0, 1, 2)));
    // The three pending slots can now complete the configured minimum of four.
    assertEquals(4, partitionInfo.getRegionGroupCount(DATABASE, type));
    verify(loadManager).allocateRegionGroups(Collections.singletonMap(DATABASE, 3), type);
    persistSlots(DATABASE, type, 0, 0, 1, 2);
    clearInvocations(loadManager);
    assertSuccess(extend(DATABASE, slots(0, 1, 2)));
    verify(loadManager, never()).allocateRegionGroups(anyMap(), any());
  }

  @Test
  public void testMissingDatabaseIsReported() throws Exception {
    assertEquals(
        TSStatusCode.DATABASE_NOT_EXIST.getStatusCode(),
        extend("root.missing", slots(0)).getCode());
    verify(loadManager, never()).allocateRegionGroups(anyMap(), any());
  }

  private TSStatus extend(String database, Collection<TSeriesPartitionSlot> requested) {
    return partitionManager.extendRegionGroupIfNecessary(
        Collections.singletonMap(database, requested), type);
  }

  private void checkSingleSlotTimePartitionsReachMinimumLikeAuto(int minimum) throws Exception {
    for (String database : Arrays.asList(DATABASE, SECOND_DATABASE)) {
      when(schemaManager.getMinRegionGroupNum(database, type)).thenReturn(minimum);
      when(schemaManager.getMaxRegionGroupNum(database, type)).thenReturn(5);
      setPolicy(
          database.equals(DATABASE)
              ? RegionGroupExtensionPolicy.AUTO
              : RegionGroupExtensionPolicy.PROACTIVE);

      for (int request = 0; request < minimum + 2; request++) {
        // Several new time partitions of one existing series slot still add at most one group.
        int firstTime = request * 3;
        Collection<TSeriesPartitionSlot> pending =
            pendingDataSlots(database, firstTime, firstTime + 1, firstTime + 2);
        assertEquals(slots(0), pending);
        if (request >= minimum) {
          clearInvocations(loadManager);
        }
        assertSuccess(extend(database, pending));
        assertEquals(
            Math.min(request + 1, minimum), partitionInfo.getRegionGroupCount(database, type));
        if (request >= minimum) {
          verify(loadManager, never()).allocateRegionGroups(anyMap(), any());
        }
        for (int time = firstTime; time < firstTime + 3; time++) {
          persistSlots(database, type, time, 0);
        }
        assertEquals(
            1, partitionInfo.getSeriesPartitionSlotsCount(database, type, Collections.emptyList()));
        assertEquals(
            Collections.emptyList(),
            unassignedDataSlots(database, firstTime)
                .get(new TSeriesPartitionSlot(0))
                .getTimePartitionSlots());
      }
    }
  }

  private List<TSeriesPartitionSlot> pendingDataSlots(String database, int... times) {
    return unassignedDataSlots(database, times).keySet().stream().collect(Collectors.toList());
  }

  private Map<TSeriesPartitionSlot, TTimeSlotList> unassignedDataSlots(
      String database, int... times) {
    TTimeSlotList timeSlots =
        new TTimeSlotList(
            Arrays.stream(times).mapToObj(TTimePartitionSlot::new).collect(Collectors.toList()),
            false,
            false);
    return partitionInfo
        .filterUnassignedDataPartitionSlots(
            Collections.singletonMap(
                database, Collections.singletonMap(new TSeriesPartitionSlot(0), timeSlots)))
        .get(database);
  }

  private void configureSparseSlotComparison() {
    CONF.setSeriesSlotNum(1000);
    int minimum = type == TConsensusGroupType.SchemaRegion ? 1 : 2;
    for (String database : Arrays.asList(DATABASE, SECOND_DATABASE)) {
      when(schemaManager.getMinRegionGroupNum(database, type)).thenReturn(minimum);
      when(schemaManager.getMaxRegionGroupNum(database, type)).thenReturn(9);
    }
  }

  private void setPolicy(RegionGroupExtensionPolicy policy) {
    if (type == TConsensusGroupType.SchemaRegion) {
      CONF.setSchemaRegionGroupExtensionPolicy(policy);
    } else {
      CONF.setDataRegionGroupExtensionPolicy(policy);
    }
  }

  private TConsensusGroupType otherType() {
    return type == TConsensusGroupType.SchemaRegion
        ? TConsensusGroupType.DataRegion
        : TConsensusGroupType.SchemaRegion;
  }

  private CreateRegionGroupsPlan regionPlan(
      Map<String, Integer> allotments, TConsensusGroupType allocatedType) {
    CreateRegionGroupsPlan plan = new CreateRegionGroupsPlan();
    allotments.forEach(
        (database, count) -> {
          for (int i = 0; i < count; i++) {
            plan.addRegionGroup(
                database,
                new TRegionReplicaSet(
                    new TConsensusGroupId(allocatedType, partitionInfo.generateNextRegionGroupId()),
                    Collections.singletonList(new TDataNodeLocation().setDataNodeId(0))));
          }
        });
    return plan;
  }

  private void persistSlots(
      String database, TConsensusGroupType persistedType, long time, int... slotIds)
      throws Exception {
    TConsensusGroupId regionId = partitionInfo.getAllRegionGroupIds(database, persistedType).get(0);
    if (persistedType == TConsensusGroupType.SchemaRegion) {
      SchemaPartitionTable table = new SchemaPartitionTable();
      for (TSeriesPartitionSlot slot : slots(slotIds)) {
        table.getSchemaPartitionMap().put(slot, regionId);
      }
      CreateSchemaPartitionPlan plan = new CreateSchemaPartitionPlan();
      plan.setAssignedSchemaPartition(Collections.singletonMap(database, table));
      partitionInfo.createSchemaPartition(plan);
    } else {
      DataPartitionTable table = new DataPartitionTable();
      for (TSeriesPartitionSlot slot : slots(slotIds)) {
        SeriesPartitionTable seriesTable = new SeriesPartitionTable();
        seriesTable.putDataPartition(new TTimePartitionSlot(time), regionId);
        table.getDataPartitionMap().put(slot, seriesTable);
      }
      CreateDataPartitionPlan plan = new CreateDataPartitionPlan();
      plan.setAssignedDataPartition(Collections.singletonMap(database, table));
      partitionInfo.createDataPartition(plan);
    }
  }

  private static List<TSeriesPartitionSlot> slots(int... ids) {
    return Arrays.stream(ids).mapToObj(TSeriesPartitionSlot::new).collect(Collectors.toList());
  }

  private static void assertSuccess(TSStatus status) {
    assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
  }
}
