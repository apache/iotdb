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
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.write.database.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.consensus.request.write.region.CreateRegionGroupsPlan;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.ProcedureManager;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.schema.ClusterSchemaManager;
import org.apache.iotdb.confignode.persistence.partition.PartitionInfo;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.rpc.RpcUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class PartitionManagerAutoExtensionTest {

  private static final String DATABASE = "root.auto";
  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> parameters() {
    return Arrays.asList(
        new Object[] {TConsensusGroupType.SchemaRegion},
        new Object[] {TConsensusGroupType.DataRegion});
  }

  private final TConsensusGroupType type;
  private TConsensusGroupId requestedGroup;
  private TConsensusGroupId otherGroup;
  private PartitionManager partitionManager;
  private LoadManager loadManager;
  private ClusterSchemaManager schemaManager;
  private RegionGroupExtensionPolicy originalSchemaPolicy;
  private RegionGroupExtensionPolicy originalDataPolicy;
  private int originalSeriesSlotNum;

  public PartitionManagerAutoExtensionTest(TConsensusGroupType type) {
    this.type = type;
  }

  @Before
  public void setUp() throws Exception {
    originalSchemaPolicy = CONF.getSchemaRegionGroupExtensionPolicy();
    originalDataPolicy = CONF.getDataRegionGroupExtensionPolicy();
    originalSeriesSlotNum = CONF.getSeriesSlotNum();
    CONF.setSchemaRegionGroupExtensionPolicy(RegionGroupExtensionPolicy.AUTO);
    CONF.setDataRegionGroupExtensionPolicy(RegionGroupExtensionPolicy.AUTO);
    CONF.setSeriesSlotNum(100);

    requestedGroup = new TConsensusGroupId(type, 1);
    otherGroup =
        new TConsensusGroupId(
            type == TConsensusGroupType.SchemaRegion
                ? TConsensusGroupType.DataRegion
                : TConsensusGroupType.SchemaRegion,
            2);
    PartitionInfo partitionInfo = new PartitionInfo();
    partitionInfo.createDatabase(
        new DatabaseSchemaPlan(
            ConfigPhysicalPlanType.CreateDatabase, new TDatabaseSchema(DATABASE)));
    CreateRegionGroupsPlan existingGroups = new CreateRegionGroupsPlan();
    for (TConsensusGroupId group : Arrays.asList(requestedGroup, otherGroup)) {
      existingGroups.addRegionGroup(
          DATABASE, new TRegionReplicaSet(group, Collections.emptyList()));
    }
    partitionInfo.createRegionGroups(existingGroups);

    IManager manager = mock(IManager.class);
    loadManager = mock(LoadManager.class);
    schemaManager = mock(ClusterSchemaManager.class);
    ProcedureManager procedureManager = mock(ProcedureManager.class);
    when(manager.getLoadManager()).thenReturn(loadManager);
    when(manager.getClusterSchemaManager()).thenReturn(schemaManager);
    when(manager.getProcedureManager()).thenReturn(procedureManager);
    // Keep per-group status lookups consistent with the filtered status lists in each test.
    when(loadManager.getRegionGroupStatus(requestedGroup)).thenReturn(RegionGroupStatus.Running);
    when(loadManager.getRegionGroupStatus(otherGroup)).thenReturn(RegionGroupStatus.Disabled);
    // Keep the minimum and slot-based growth conditions false, so status drives replacement.
    when(schemaManager.getMinRegionGroupNum(DATABASE, type)).thenReturn(1);
    when(schemaManager.getMaxRegionGroupNum(DATABASE, type)).thenReturn(3);
    when(loadManager.allocateRegionGroups(anyMap(), eq(type)))
        .thenReturn(new CreateRegionGroupsPlan());
    when(procedureManager.createRegionGroups(eq(type), any())).thenReturn(RpcUtils.SUCCESS_STATUS);
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
  public void testDisabledOtherTypeDoesNotTriggerReplacement() throws Exception {
    when(loadManager.filterRegionGroupThroughStatus(RegionGroupStatus.Disabled))
        .thenReturn(Collections.singletonList(otherGroup));

    extend();

    verify(loadManager, never()).allocateRegionGroups(anyMap(), any());
  }

  @Test
  public void testDisabledOtherTypeDoesNotHideReplacement() throws Exception {
    when(loadManager.getRegionGroupStatus(requestedGroup)).thenReturn(RegionGroupStatus.Disabled);
    when(loadManager.filterRegionGroupThroughStatus(RegionGroupStatus.Disabled))
        .thenReturn(Arrays.asList(requestedGroup, otherGroup));

    extend();

    verify(loadManager).allocateRegionGroups(Collections.singletonMap(DATABASE, 1), type);
  }

  @Test
  public void testReplacementRespectsMaximum() throws Exception {
    when(schemaManager.getMaxRegionGroupNum(DATABASE, type)).thenReturn(1);
    when(loadManager.getRegionGroupStatus(requestedGroup)).thenReturn(RegionGroupStatus.Disabled);
    when(loadManager.filterRegionGroupThroughStatus(RegionGroupStatus.Disabled))
        .thenReturn(Arrays.asList(requestedGroup, otherGroup));

    extend();

    verify(loadManager, never()).allocateRegionGroups(anyMap(), any());
  }

  private void extend() {
    assertEquals(
        RpcUtils.SUCCESS_STATUS,
        partitionManager.extendRegionGroupIfNecessary(
            Collections.singletonMap(
                DATABASE, Collections.singletonList(new TSeriesPartitionSlot(0))),
            type));
  }
}
