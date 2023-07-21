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

package org.apache.iotdb.confignode.consensus.request;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TNodeResource;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TSpaceQuota;
import org.apache.iotdb.common.rpc.thrift.TThrottleQuota;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimedQuota;
import org.apache.iotdb.common.rpc.thrift.ThrottleType;
import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.partition.DataPartitionTable;
import org.apache.iotdb.commons.partition.SchemaPartitionTable;
import org.apache.iotdb.commons.partition.SeriesPartitionTable;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.pipe.plugin.meta.PipePluginMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.sync.PipeInfo;
import org.apache.iotdb.commons.sync.PipeMessage;
import org.apache.iotdb.commons.sync.PipeStatus;
import org.apache.iotdb.commons.sync.TsFilePipeInfo;
import org.apache.iotdb.commons.trigger.TriggerInformation;
import org.apache.iotdb.commons.udf.UDFInformation;
import org.apache.iotdb.confignode.consensus.request.auth.AuthorPlan;
import org.apache.iotdb.confignode.consensus.request.read.database.CountDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.read.database.GetDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.read.datanode.GetDataNodeConfigurationPlan;
import org.apache.iotdb.confignode.consensus.request.read.function.GetFunctionTablePlan;
import org.apache.iotdb.confignode.consensus.request.read.function.GetUDFJarPlan;
import org.apache.iotdb.confignode.consensus.request.read.partition.CountTimeSlotListPlan;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetDataPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetNodePathsPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetOrCreateDataPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetOrCreateSchemaPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetSchemaPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetSeriesSlotListPlan;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetTimeSlotListPlan;
import org.apache.iotdb.confignode.consensus.request.read.pipe.plugin.GetPipePluginJarPlan;
import org.apache.iotdb.confignode.consensus.request.read.pipe.plugin.GetPipePluginTablePlan;
import org.apache.iotdb.confignode.consensus.request.read.pipe.task.ShowPipePlanV2;
import org.apache.iotdb.confignode.consensus.request.read.region.GetRegionIdPlan;
import org.apache.iotdb.confignode.consensus.request.read.region.GetRegionInfoListPlan;
import org.apache.iotdb.confignode.consensus.request.read.template.GetAllSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.read.template.GetAllTemplateSetInfoPlan;
import org.apache.iotdb.confignode.consensus.request.read.template.GetPathsSetTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.read.template.GetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.read.trigger.GetTransferringTriggersPlan;
import org.apache.iotdb.confignode.consensus.request.read.trigger.GetTriggerJarPlan;
import org.apache.iotdb.confignode.consensus.request.read.trigger.GetTriggerLocationPlan;
import org.apache.iotdb.confignode.consensus.request.read.trigger.GetTriggerTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.confignode.ApplyConfigNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.confignode.RemoveConfigNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.cq.ActiveCQPlan;
import org.apache.iotdb.confignode.consensus.request.write.cq.AddCQPlan;
import org.apache.iotdb.confignode.consensus.request.write.cq.DropCQPlan;
import org.apache.iotdb.confignode.consensus.request.write.cq.ShowCQPlan;
import org.apache.iotdb.confignode.consensus.request.write.cq.UpdateCQLastExecTimePlan;
import org.apache.iotdb.confignode.consensus.request.write.database.AdjustMaxRegionGroupNumPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DeleteDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetDataReplicationFactorPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetSchemaReplicationFactorPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetTTLPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetTimePartitionIntervalPlan;
import org.apache.iotdb.confignode.consensus.request.write.datanode.RegisterDataNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.datanode.RemoveDataNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.datanode.UpdateDataNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.function.CreateFunctionPlan;
import org.apache.iotdb.confignode.consensus.request.write.function.DropFunctionPlan;
import org.apache.iotdb.confignode.consensus.request.write.partition.CreateDataPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.write.partition.CreateSchemaPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.plugin.CreatePipePluginPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.plugin.DropPipePluginPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.runtime.PipeHandleLeaderChangePlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.runtime.PipeHandleMetaChangePlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.CreatePipePlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.DropPipePlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.SetPipeStatusPlanV2;
import org.apache.iotdb.confignode.consensus.request.write.procedure.DeleteProcedurePlan;
import org.apache.iotdb.confignode.consensus.request.write.procedure.UpdateProcedurePlan;
import org.apache.iotdb.confignode.consensus.request.write.quota.SetSpaceQuotaPlan;
import org.apache.iotdb.confignode.consensus.request.write.quota.SetThrottleQuotaPlan;
import org.apache.iotdb.confignode.consensus.request.write.region.CreateRegionGroupsPlan;
import org.apache.iotdb.confignode.consensus.request.write.region.OfferRegionMaintainTasksPlan;
import org.apache.iotdb.confignode.consensus.request.write.region.PollRegionMaintainTaskPlan;
import org.apache.iotdb.confignode.consensus.request.write.region.PollSpecificRegionMaintainTaskPlan;
import org.apache.iotdb.confignode.consensus.request.write.sync.CreatePipeSinkPlanV1;
import org.apache.iotdb.confignode.consensus.request.write.sync.DropPipeSinkPlanV1;
import org.apache.iotdb.confignode.consensus.request.write.sync.GetPipeSinkPlanV1;
import org.apache.iotdb.confignode.consensus.request.write.sync.PreCreatePipePlanV1;
import org.apache.iotdb.confignode.consensus.request.write.sync.RecordPipeMessagePlan;
import org.apache.iotdb.confignode.consensus.request.write.sync.SetPipeStatusPlanV1;
import org.apache.iotdb.confignode.consensus.request.write.sync.ShowPipePlanV1;
import org.apache.iotdb.confignode.consensus.request.write.template.CreateSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.DropSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.PreUnsetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.RollbackPreUnsetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.SetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.UnsetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.trigger.AddTriggerInTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.trigger.DeleteTriggerInTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.trigger.UpdateTriggerLocationPlan;
import org.apache.iotdb.confignode.consensus.request.write.trigger.UpdateTriggerStateInTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.trigger.UpdateTriggersOnTransferNodesPlan;
import org.apache.iotdb.confignode.persistence.partition.maintainer.RegionCreateTask;
import org.apache.iotdb.confignode.persistence.partition.maintainer.RegionDeleteTask;
import org.apache.iotdb.confignode.procedure.Procedure;
import org.apache.iotdb.confignode.procedure.impl.schema.DeleteDatabaseProcedure;
import org.apache.iotdb.confignode.procedure.impl.statemachine.CreateRegionGroupsProcedure;
import org.apache.iotdb.confignode.rpc.thrift.TCreateCQReq;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.confignode.rpc.thrift.TPipeSinkInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TTimeSlotList;
import org.apache.iotdb.confignode.rpc.thrift.TTriggerState;
import org.apache.iotdb.db.schemaengine.template.Template;
import org.apache.iotdb.trigger.api.enums.FailureStrategy;
import org.apache.iotdb.trigger.api.enums.TriggerEvent;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.Pair;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.iotdb.common.rpc.thrift.TConsensusGroupType.ConfigRegion;
import static org.apache.iotdb.common.rpc.thrift.TConsensusGroupType.DataRegion;
import static org.apache.iotdb.common.rpc.thrift.TConsensusGroupType.SchemaRegion;
import static org.junit.Assert.assertEquals;

public class ConfigPhysicalPlanSerDeTest {

  @Test
  public void RegisterDataNodePlanTest() throws IOException {
    TDataNodeLocation dataNodeLocation = new TDataNodeLocation();
    dataNodeLocation.setDataNodeId(1);
    dataNodeLocation.setClientRpcEndPoint(new TEndPoint("0.0.0.0", 6667));
    dataNodeLocation.setInternalEndPoint(new TEndPoint("0.0.0.0", 10730));
    dataNodeLocation.setMPPDataExchangeEndPoint(new TEndPoint("0.0.0.0", 10740));
    dataNodeLocation.setDataRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 10760));
    dataNodeLocation.setSchemaRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 10750));

    TDataNodeConfiguration dataNodeConfiguration = new TDataNodeConfiguration();
    dataNodeConfiguration.setLocation(dataNodeLocation);
    dataNodeConfiguration.setResource(new TNodeResource(16, 34359738368L));

    RegisterDataNodePlan plan0 = new RegisterDataNodePlan(dataNodeConfiguration);
    RegisterDataNodePlan plan1 =
        (RegisterDataNodePlan) ConfigPhysicalPlan.Factory.create(plan0.serializeToByteBuffer());
    Assert.assertEquals(plan0, plan1);
  }

  @Test
  public void UpdateDataNodePlanTest() throws IOException {
    TDataNodeLocation dataNodeLocation = new TDataNodeLocation();
    dataNodeLocation.setDataNodeId(0);
    dataNodeLocation.setClientRpcEndPoint(new TEndPoint("0.0.0.0", 6667));
    dataNodeLocation.setInternalEndPoint(new TEndPoint("0.0.0.0", 10730));
    dataNodeLocation.setMPPDataExchangeEndPoint(new TEndPoint("0.0.0.0", 10740));
    dataNodeLocation.setDataRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 10760));
    dataNodeLocation.setSchemaRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 10750));

    TNodeResource dataNodeResource = new TNodeResource();
    dataNodeResource.setCpuCoreNum(16);
    dataNodeResource.setMaxMemory(2022213861);

    TDataNodeConfiguration dataNodeConfiguration = new TDataNodeConfiguration();
    dataNodeConfiguration.setLocation(dataNodeLocation);
    dataNodeConfiguration.setResource(dataNodeResource);

    UpdateDataNodePlan plan0 = new UpdateDataNodePlan(dataNodeConfiguration);
    UpdateDataNodePlan plan1 =
        (UpdateDataNodePlan) ConfigPhysicalPlan.Factory.create(plan0.serializeToByteBuffer());
    Assert.assertEquals(plan0, plan1);
  }

  @Test
  public void QueryDataNodeInfoPlanTest() throws IOException {
    GetDataNodeConfigurationPlan plan0 = new GetDataNodeConfigurationPlan(-1);
    GetDataNodeConfigurationPlan plan1 =
        (GetDataNodeConfigurationPlan)
            ConfigPhysicalPlan.Factory.create(plan0.serializeToByteBuffer());
    Assert.assertEquals(plan0, plan1);
  }

  @Test
  public void CreateDatabasePlanTest() throws IOException {
    DatabaseSchemaPlan req0 =
        new DatabaseSchemaPlan(
            ConfigPhysicalPlanType.CreateDatabase,
            new TDatabaseSchema()
                .setName("sg")
                .setTTL(Long.MAX_VALUE)
                .setSchemaReplicationFactor(3)
                .setDataReplicationFactor(3)
                .setTimePartitionInterval(604800));
    DatabaseSchemaPlan req1 =
        (DatabaseSchemaPlan) ConfigPhysicalPlan.Factory.create(req0.serializeToByteBuffer());
    Assert.assertEquals(req0, req1);
  }

  @Test
  public void AlterDatabasePlanTest() throws IOException {
    DatabaseSchemaPlan req0 =
        new DatabaseSchemaPlan(
            ConfigPhysicalPlanType.AlterDatabase,
            new TDatabaseSchema()
                .setName("sg")
                .setTTL(Long.MAX_VALUE)
                .setSchemaReplicationFactor(3)
                .setDataReplicationFactor(3)
                .setTimePartitionInterval(604800)
                .setMinSchemaRegionGroupNum(2)
                .setMaxSchemaRegionGroupNum(5)
                .setMinDataRegionGroupNum(3)
                .setMaxDataRegionGroupNum(8));
    DatabaseSchemaPlan req1 =
        (DatabaseSchemaPlan) ConfigPhysicalPlan.Factory.create(req0.serializeToByteBuffer());
    Assert.assertEquals(req0, req1);
  }

  @Test
  public void DeleteStorageGroupPlanTest() throws IOException {
    // TODO: Add serialize and deserialize test
    DeleteDatabasePlan req0 = new DeleteDatabasePlan("root.sg");
    DeleteDatabasePlan req1 =
        (DeleteDatabasePlan) ConfigPhysicalPlan.Factory.create(req0.serializeToByteBuffer());
    Assert.assertEquals(req0, req1);
  }

  @Test
  public void SetTTLPlanTest() throws IOException {
    SetTTLPlan req0 = new SetTTLPlan(Arrays.asList("root", "sg0"), Long.MAX_VALUE);
    SetTTLPlan req1 = (SetTTLPlan) ConfigPhysicalPlan.Factory.create(req0.serializeToByteBuffer());
    Assert.assertEquals(req0, req1);
  }

  @Test
  public void SetSchemaReplicationFactorPlanTest() throws IOException {
    SetSchemaReplicationFactorPlan req0 = new SetSchemaReplicationFactorPlan("root.sg0", 3);
    SetSchemaReplicationFactorPlan req1 =
        (SetSchemaReplicationFactorPlan)
            ConfigPhysicalPlan.Factory.create(req0.serializeToByteBuffer());
    Assert.assertEquals(req0, req1);
  }

  @Test
  public void SetDataReplicationFactorPlanTest() throws IOException {
    SetDataReplicationFactorPlan req0 = new SetDataReplicationFactorPlan("root.sg0", 3);
    SetDataReplicationFactorPlan req1 =
        (SetDataReplicationFactorPlan)
            ConfigPhysicalPlan.Factory.create(req0.serializeToByteBuffer());
    Assert.assertEquals(req0, req1);
  }

  @Test
  public void SetTimePartitionIntervalPlanTest() throws IOException {
    SetTimePartitionIntervalPlan req0 = new SetTimePartitionIntervalPlan("root.sg0", 6048000L);
    SetTimePartitionIntervalPlan req1 =
        (SetTimePartitionIntervalPlan)
            ConfigPhysicalPlan.Factory.create(req0.serializeToByteBuffer());
    Assert.assertEquals(req0, req1);
  }

  @Test
  public void AdjustMaxRegionGroupCountPlanTest() throws IOException {
    AdjustMaxRegionGroupNumPlan req0 = new AdjustMaxRegionGroupNumPlan();
    for (int i = 0; i < 3; i++) {
      req0.putEntry("root.sg" + i, new Pair<>(i, i));
    }

    AdjustMaxRegionGroupNumPlan req1 =
        (AdjustMaxRegionGroupNumPlan)
            ConfigPhysicalPlan.Factory.create(req0.serializeToByteBuffer());
    Assert.assertEquals(req0, req1);
  }

  @Test
  public void CountStorageGroupPlanTest() throws IOException {
    CountDatabasePlan req0 = new CountDatabasePlan(Arrays.asList("root", "sg"));
    CountDatabasePlan req1 =
        (CountDatabasePlan) ConfigPhysicalPlan.Factory.create(req0.serializeToByteBuffer());
    Assert.assertEquals(req0, req1);
  }

  @Test
  public void GetStorageGroupPlanTest() throws IOException {
    GetDatabasePlan req0 = new GetDatabasePlan(Arrays.asList("root", "sg"));
    CountDatabasePlan req1 =
        (CountDatabasePlan) ConfigPhysicalPlan.Factory.create(req0.serializeToByteBuffer());
    Assert.assertEquals(req0, req1);
  }

  @Test
  public void CreateRegionsPlanTest() throws IOException {
    TDataNodeLocation dataNodeLocation = new TDataNodeLocation();
    dataNodeLocation.setDataNodeId(0);
    dataNodeLocation.setClientRpcEndPoint(new TEndPoint("0.0.0.0", 6667));
    dataNodeLocation.setInternalEndPoint(new TEndPoint("0.0.0.0", 10730));
    dataNodeLocation.setMPPDataExchangeEndPoint(new TEndPoint("0.0.0.0", 10740));
    dataNodeLocation.setDataRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 10760));
    dataNodeLocation.setSchemaRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 10750));

    CreateRegionGroupsPlan req0 = new CreateRegionGroupsPlan();
    TRegionReplicaSet dataRegionSet = new TRegionReplicaSet();
    dataRegionSet.setRegionId(new TConsensusGroupId(TConsensusGroupType.DataRegion, 0));
    dataRegionSet.setDataNodeLocations(Collections.singletonList(dataNodeLocation));
    req0.addRegionGroup("root.sg0", dataRegionSet);

    TRegionReplicaSet schemaRegionSet = new TRegionReplicaSet();
    schemaRegionSet.setRegionId(new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 1));
    schemaRegionSet.setDataNodeLocations(Collections.singletonList(dataNodeLocation));
    req0.addRegionGroup("root.sg1", schemaRegionSet);

    CreateRegionGroupsPlan req1 =
        (CreateRegionGroupsPlan) ConfigPhysicalPlan.Factory.create(req0.serializeToByteBuffer());
    Assert.assertEquals(req0, req1);
  }

  @Test
  public void OfferRegionMaintainTasksPlanTest() throws IOException {
    TDataNodeLocation dataNodeLocation = new TDataNodeLocation();
    dataNodeLocation.setDataNodeId(0);
    dataNodeLocation.setClientRpcEndPoint(new TEndPoint("0.0.0.0", 6667));
    dataNodeLocation.setInternalEndPoint(new TEndPoint("0.0.0.0", 10730));
    dataNodeLocation.setMPPDataExchangeEndPoint(new TEndPoint("0.0.0.0", 10740));
    dataNodeLocation.setDataRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 10760));
    dataNodeLocation.setSchemaRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 10750));

    TRegionReplicaSet regionReplicaSet = new TRegionReplicaSet();
    regionReplicaSet.setRegionId(new TConsensusGroupId(TConsensusGroupType.DataRegion, 0));
    regionReplicaSet.setDataNodeLocations(Collections.singletonList(dataNodeLocation));

    OfferRegionMaintainTasksPlan plan0 = new OfferRegionMaintainTasksPlan();
    plan0.appendRegionMaintainTask(
        new RegionCreateTask(dataNodeLocation, "root.sg", regionReplicaSet));
    plan0.appendRegionMaintainTask(
        new RegionCreateTask(dataNodeLocation, "root.sg", regionReplicaSet).setTTL(86400));
    plan0.appendRegionMaintainTask(
        new RegionDeleteTask(
            dataNodeLocation, new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 2)));

    OfferRegionMaintainTasksPlan plan1 =
        (OfferRegionMaintainTasksPlan)
            ConfigPhysicalPlan.Factory.create(plan0.serializeToByteBuffer());
    Assert.assertEquals(plan0, plan1);
  }

  @Test
  public void PollRegionMaintainTaskPlan() throws IOException {
    PollRegionMaintainTaskPlan plan0 = new PollRegionMaintainTaskPlan();
    PollRegionMaintainTaskPlan plan1 =
        (PollRegionMaintainTaskPlan)
            ConfigPhysicalPlan.Factory.create(plan0.serializeToByteBuffer());
    Assert.assertEquals(plan0, plan1);
  }

  @Test
  public void CreateSchemaPartitionPlanTest() throws IOException {
    TDataNodeLocation dataNodeLocation = new TDataNodeLocation();
    dataNodeLocation.setDataNodeId(0);
    dataNodeLocation.setClientRpcEndPoint(new TEndPoint("0.0.0.0", 6667));
    dataNodeLocation.setInternalEndPoint(new TEndPoint("0.0.0.0", 10730));
    dataNodeLocation.setMPPDataExchangeEndPoint(new TEndPoint("0.0.0.0", 10740));
    dataNodeLocation.setDataRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 10760));
    dataNodeLocation.setSchemaRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 10750));

    String storageGroup = "root.sg0";
    TSeriesPartitionSlot seriesPartitionSlot = new TSeriesPartitionSlot(10);
    TConsensusGroupId consensusGroupId = new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 0);

    Map<String, SchemaPartitionTable> assignedSchemaPartition = new HashMap<>();
    Map<TSeriesPartitionSlot, TConsensusGroupId> schemaPartitionMap = new HashMap<>();
    schemaPartitionMap.put(seriesPartitionSlot, consensusGroupId);
    assignedSchemaPartition.put(storageGroup, new SchemaPartitionTable(schemaPartitionMap));

    CreateSchemaPartitionPlan req0 = new CreateSchemaPartitionPlan();
    req0.setAssignedSchemaPartition(assignedSchemaPartition);
    CreateSchemaPartitionPlan req1 =
        (CreateSchemaPartitionPlan) ConfigPhysicalPlan.Factory.create(req0.serializeToByteBuffer());
    Assert.assertEquals(req0, req1);
  }

  @Test
  public void GetSchemaPartitionPlanTest() throws IOException {
    String storageGroup = "root.sg0";
    TSeriesPartitionSlot seriesPartitionSlot = new TSeriesPartitionSlot(10);

    Map<String, List<TSeriesPartitionSlot>> partitionSlotsMap = new HashMap<>();
    partitionSlotsMap.put(storageGroup, Collections.singletonList(seriesPartitionSlot));

    GetSchemaPartitionPlan req0 = new GetSchemaPartitionPlan(partitionSlotsMap);
    GetSchemaPartitionPlan req1 =
        (GetSchemaPartitionPlan) ConfigPhysicalPlan.Factory.create(req0.serializeToByteBuffer());
    Assert.assertEquals(req0, req1);
  }

  @Test
  public void GetOrCreateSchemaPartitionPlanTest() throws IOException {
    String storageGroup = "root.sg0";
    TSeriesPartitionSlot seriesPartitionSlot = new TSeriesPartitionSlot(10);

    Map<String, List<TSeriesPartitionSlot>> partitionSlotsMap = new HashMap<>();
    partitionSlotsMap.put(storageGroup, Collections.singletonList(seriesPartitionSlot));

    GetOrCreateSchemaPartitionPlan req0 = new GetOrCreateSchemaPartitionPlan(partitionSlotsMap);
    GetOrCreateSchemaPartitionPlan req1 =
        (GetOrCreateSchemaPartitionPlan)
            ConfigPhysicalPlan.Factory.create(req0.serializeToByteBuffer());
    Assert.assertEquals(req0, req1);
  }

  @Test
  public void CreateDataPartitionPlanTest() throws IOException {
    TDataNodeLocation dataNodeLocation = new TDataNodeLocation();
    dataNodeLocation.setDataNodeId(0);
    dataNodeLocation.setClientRpcEndPoint(new TEndPoint("0.0.0.0", 6667));
    dataNodeLocation.setInternalEndPoint(new TEndPoint("0.0.0.0", 10730));
    dataNodeLocation.setMPPDataExchangeEndPoint(new TEndPoint("0.0.0.0", 10740));
    dataNodeLocation.setDataRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 10760));
    dataNodeLocation.setSchemaRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 10750));

    String storageGroup = "root.sg0";
    TSeriesPartitionSlot seriesPartitionSlot = new TSeriesPartitionSlot(10);
    TTimePartitionSlot timePartitionSlot = new TTimePartitionSlot(100);
    TRegionReplicaSet regionReplicaSet = new TRegionReplicaSet();
    regionReplicaSet.setRegionId(new TConsensusGroupId(TConsensusGroupType.DataRegion, 0));
    regionReplicaSet.setDataNodeLocations(Collections.singletonList(dataNodeLocation));

    Map<String, DataPartitionTable> assignedDataPartition = new HashMap<>();
    Map<TSeriesPartitionSlot, SeriesPartitionTable> dataPartitionMap = new HashMap<>();
    Map<TTimePartitionSlot, List<TConsensusGroupId>> seriesPartitionMap = new HashMap<>();

    seriesPartitionMap.put(
        timePartitionSlot,
        Collections.singletonList(new TConsensusGroupId(TConsensusGroupType.DataRegion, 0)));
    dataPartitionMap.put(seriesPartitionSlot, new SeriesPartitionTable(seriesPartitionMap));
    assignedDataPartition.put(storageGroup, new DataPartitionTable(dataPartitionMap));

    CreateDataPartitionPlan req0 = new CreateDataPartitionPlan();
    req0.setAssignedDataPartition(assignedDataPartition);
    CreateDataPartitionPlan req1 =
        (CreateDataPartitionPlan) ConfigPhysicalPlan.Factory.create(req0.serializeToByteBuffer());
    Assert.assertEquals(req0, req1);
  }

  @Test
  public void GetDataPartitionPlanTest() throws IOException {
    String storageGroup = "root.sg0";
    TSeriesPartitionSlot seriesPartitionSlot = new TSeriesPartitionSlot(10);
    TTimePartitionSlot timePartitionSlot = new TTimePartitionSlot(100);

    Map<String, Map<TSeriesPartitionSlot, TTimeSlotList>> partitionSlotsMap = new HashMap<>();
    partitionSlotsMap.put(storageGroup, new HashMap<>());
    partitionSlotsMap
        .get(storageGroup)
        .put(seriesPartitionSlot, new TTimeSlotList().setTimePartitionSlots(new ArrayList<>()));
    partitionSlotsMap
        .get(storageGroup)
        .get(seriesPartitionSlot)
        .getTimePartitionSlots()
        .add(timePartitionSlot);

    GetDataPartitionPlan req0 = new GetDataPartitionPlan(partitionSlotsMap);
    GetDataPartitionPlan req1 =
        (GetDataPartitionPlan) ConfigPhysicalPlan.Factory.create(req0.serializeToByteBuffer());
    Assert.assertEquals(req0, req1);
  }

  @Test
  public void GetOrCreateDataPartitionPlanTest() throws IOException {
    String storageGroup = "root.sg0";
    TSeriesPartitionSlot seriesPartitionSlot = new TSeriesPartitionSlot(10);
    TTimePartitionSlot timePartitionSlot = new TTimePartitionSlot(100);

    Map<String, Map<TSeriesPartitionSlot, TTimeSlotList>> partitionSlotsMap = new HashMap<>();
    partitionSlotsMap.put(storageGroup, new HashMap<>());
    partitionSlotsMap
        .get(storageGroup)
        .put(seriesPartitionSlot, new TTimeSlotList().setTimePartitionSlots(new ArrayList<>()));
    partitionSlotsMap
        .get(storageGroup)
        .get(seriesPartitionSlot)
        .getTimePartitionSlots()
        .add(timePartitionSlot);

    GetOrCreateDataPartitionPlan req0 = new GetOrCreateDataPartitionPlan(partitionSlotsMap);
    GetOrCreateDataPartitionPlan req1 =
        (GetOrCreateDataPartitionPlan)
            ConfigPhysicalPlan.Factory.create(req0.serializeToByteBuffer());
    Assert.assertEquals(req0, req1);
  }

  @Test
  public void AuthorPlanTest() throws IOException, AuthException, IllegalPathException {

    AuthorPlan req0;
    AuthorPlan req1;
    Set<Integer> permissions = new HashSet<>();
    permissions.add(PrivilegeType.GRANT_PRIVILEGE.ordinal());

    // create user
    req0 =
        new AuthorPlan(
            ConfigPhysicalPlanType.CreateUser,
            "thulab",
            "",
            "passwd",
            "",
            new HashSet<>(),
            new ArrayList<>());
    req1 = (AuthorPlan) ConfigPhysicalPlan.Factory.create(req0.serializeToByteBuffer());
    Assert.assertEquals(req0, req1);

    // create role
    req0 =
        new AuthorPlan(
            ConfigPhysicalPlanType.CreateRole,
            "",
            "admin",
            "",
            "",
            new HashSet<>(),
            new ArrayList<>());
    req1 = (AuthorPlan) ConfigPhysicalPlan.Factory.create(req0.serializeToByteBuffer());
    Assert.assertEquals(req0, req1);

    // alter user
    req0 =
        new AuthorPlan(
            ConfigPhysicalPlanType.UpdateUser,
            "tempuser",
            "",
            "",
            "newpwd",
            new HashSet<>(),
            new ArrayList<>());
    req1 = (AuthorPlan) ConfigPhysicalPlan.Factory.create(req0.serializeToByteBuffer());
    Assert.assertEquals(req0, req1);

    // grant user
    List<PartialPath> nodeNameList = new ArrayList<>();
    nodeNameList.add(new PartialPath("root.ln.**"));
    nodeNameList.add(new PartialPath("root.abc.**"));
    req0 =
        new AuthorPlan(
            ConfigPhysicalPlanType.GrantUser, "tempuser", "", "", "", permissions, nodeNameList);
    req1 = (AuthorPlan) ConfigPhysicalPlan.Factory.create(req0.serializeToByteBuffer());
    Assert.assertEquals(req0, req1);

    // grant role
    req0 =
        new AuthorPlan(
            ConfigPhysicalPlanType.GrantRoleToUser,
            "tempuser",
            "temprole",
            "",
            "",
            permissions,
            nodeNameList);
    req1 = (AuthorPlan) ConfigPhysicalPlan.Factory.create(req0.serializeToByteBuffer());
    Assert.assertEquals(req0, req1);

    // grant role to user
    req0 =
        new AuthorPlan(
            ConfigPhysicalPlanType.GrantRole,
            "",
            "temprole",
            "",
            "",
            new HashSet<>(),
            new ArrayList<>());
    req1 = (AuthorPlan) ConfigPhysicalPlan.Factory.create(req0.serializeToByteBuffer());
    Assert.assertEquals(req0, req1);

    // revoke user
    req0 =
        new AuthorPlan(
            ConfigPhysicalPlanType.RevokeUser, "tempuser", "", "", "", permissions, nodeNameList);
    req1 = (AuthorPlan) ConfigPhysicalPlan.Factory.create(req0.serializeToByteBuffer());
    Assert.assertEquals(req0, req1);

    // revoke role
    req0 =
        new AuthorPlan(
            ConfigPhysicalPlanType.RevokeRole, "", "temprole", "", "", permissions, nodeNameList);
    req1 = (AuthorPlan) ConfigPhysicalPlan.Factory.create(req0.serializeToByteBuffer());
    Assert.assertEquals(req0, req1);

    // revoke role from user
    req0 =
        new AuthorPlan(
            ConfigPhysicalPlanType.RevokeRoleFromUser,
            "tempuser",
            "temprole",
            "",
            "",
            new HashSet<>(),
            new ArrayList<>());
    req1 = (AuthorPlan) ConfigPhysicalPlan.Factory.create(req0.serializeToByteBuffer());
    Assert.assertEquals(req0, req1);

    // drop user
    req0 =
        new AuthorPlan(
            ConfigPhysicalPlanType.DropUser,
            "xiaoming",
            "",
            "",
            "",
            new HashSet<>(),
            new ArrayList<>());
    req1 = (AuthorPlan) ConfigPhysicalPlan.Factory.create(req0.serializeToByteBuffer());
    Assert.assertEquals(req0, req1);

    // drop role
    req0 =
        new AuthorPlan(
            ConfigPhysicalPlanType.DropRole,
            "",
            "admin",
            "",
            "",
            new HashSet<>(),
            new ArrayList<>());
    req1 = (AuthorPlan) ConfigPhysicalPlan.Factory.create(req0.serializeToByteBuffer());
    Assert.assertEquals(req0, req1);

    // list user
    req0 =
        new AuthorPlan(
            ConfigPhysicalPlanType.ListUser, "", "", "", "", new HashSet<>(), new ArrayList<>());
    req1 = (AuthorPlan) ConfigPhysicalPlan.Factory.create(req0.serializeToByteBuffer());
    Assert.assertEquals(req0, req1);

    // list role
    req0 =
        new AuthorPlan(
            ConfigPhysicalPlanType.ListRole, "", "", "", "", new HashSet<>(), new ArrayList<>());
    req1 = (AuthorPlan) ConfigPhysicalPlan.Factory.create(req0.serializeToByteBuffer());
    Assert.assertEquals(req0, req1);

    // list privileges user
    req0 =
        new AuthorPlan(
            ConfigPhysicalPlanType.ListUserPrivilege,
            "",
            "",
            "",
            "",
            new HashSet<>(),
            new ArrayList<>());
    req1 = (AuthorPlan) ConfigPhysicalPlan.Factory.create(req0.serializeToByteBuffer());
    Assert.assertEquals(req0, req1);

    // list privileges role
    req0 =
        new AuthorPlan(
            ConfigPhysicalPlanType.ListRolePrivilege,
            "",
            "",
            "",
            "",
            new HashSet<>(),
            new ArrayList<>());
    req1 = (AuthorPlan) ConfigPhysicalPlan.Factory.create(req0.serializeToByteBuffer());
    Assert.assertEquals(req0, req1);

    // list user privileges
    req0 =
        new AuthorPlan(
            ConfigPhysicalPlanType.ListUserPrivilege,
            "",
            "",
            "",
            "",
            new HashSet<>(),
            new ArrayList<>());
    req1 = (AuthorPlan) ConfigPhysicalPlan.Factory.create(req0.serializeToByteBuffer());
    Assert.assertEquals(req0, req1);

    // list role privileges
    req0 =
        new AuthorPlan(
            ConfigPhysicalPlanType.ListRolePrivilege,
            "",
            "",
            "",
            "",
            new HashSet<>(),
            new ArrayList<>());
    req1 = (AuthorPlan) ConfigPhysicalPlan.Factory.create(req0.serializeToByteBuffer());
    Assert.assertEquals(req0, req1);

    // list all role of user
    req0 =
        new AuthorPlan(
            ConfigPhysicalPlanType.ListUserRoles,
            "",
            "",
            "",
            "",
            new HashSet<>(),
            new ArrayList<>());
    req1 = (AuthorPlan) ConfigPhysicalPlan.Factory.create(req0.serializeToByteBuffer());
    Assert.assertEquals(req0, req1);

    // list all user of role
    req0 =
        new AuthorPlan(
            ConfigPhysicalPlanType.ListRoleUsers,
            "",
            "",
            "",
            "",
            new HashSet<>(),
            new ArrayList<>());
    req1 = (AuthorPlan) ConfigPhysicalPlan.Factory.create(req0.serializeToByteBuffer());
    Assert.assertEquals(req0, req1);
  }

  @Test
  public void registerConfigNodePlanTest() throws IOException {
    ApplyConfigNodePlan req0 =
        new ApplyConfigNodePlan(
            new TConfigNodeLocation(
                0, new TEndPoint("0.0.0.0", 22277), new TEndPoint("0.0.0.0", 22278)));
    ApplyConfigNodePlan req1 =
        (ApplyConfigNodePlan) ConfigPhysicalPlan.Factory.create(req0.serializeToByteBuffer());
    Assert.assertEquals(req0, req1);
  }

  @Test
  public void removeConfigNodePlanTest() throws IOException {
    RemoveConfigNodePlan req0 =
        new RemoveConfigNodePlan(
            new TConfigNodeLocation(
                0, new TEndPoint("0.0.0.0", 22277), new TEndPoint("0.0.0.0", 22278)));
    RemoveConfigNodePlan req1 =
        (RemoveConfigNodePlan) ConfigPhysicalPlan.Factory.create(req0.serializeToByteBuffer());
    Assert.assertEquals(req0, req1);
  }

  @Test
  public void updateProcedureTest() throws IOException {
    // test procedure equals DeleteStorageGroupProcedure
    DeleteDatabaseProcedure deleteDatabaseProcedure = new DeleteDatabaseProcedure();
    deleteDatabaseProcedure.setDeleteDatabaseSchema(new TDatabaseSchema("root.sg"));
    UpdateProcedurePlan updateProcedurePlan0 = new UpdateProcedurePlan();
    updateProcedurePlan0.setProcedure(deleteDatabaseProcedure);
    UpdateProcedurePlan updateProcedurePlan1 =
        (UpdateProcedurePlan)
            ConfigPhysicalPlan.Factory.create(updateProcedurePlan0.serializeToByteBuffer());
    Procedure proc = updateProcedurePlan1.getProcedure();
    Assert.assertEquals(proc, deleteDatabaseProcedure);

    // test procedure equals CreateRegionGroupsProcedure
    TDataNodeLocation dataNodeLocation0 = new TDataNodeLocation();
    dataNodeLocation0.setDataNodeId(5);
    dataNodeLocation0.setClientRpcEndPoint(new TEndPoint("0.0.0.0", 6667));
    dataNodeLocation0.setInternalEndPoint(new TEndPoint("0.0.0.0", 10730));
    dataNodeLocation0.setMPPDataExchangeEndPoint(new TEndPoint("0.0.0.0", 10740));
    dataNodeLocation0.setDataRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 10760));
    dataNodeLocation0.setSchemaRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 10750));

    TConsensusGroupId schemaRegionGroupId = new TConsensusGroupId(SchemaRegion, 1);
    TConsensusGroupId dataRegionGroupId = new TConsensusGroupId(DataRegion, 0);
    TRegionReplicaSet schemaRegionSet =
        new TRegionReplicaSet(schemaRegionGroupId, Collections.singletonList(dataNodeLocation0));
    TRegionReplicaSet dataRegionSet =
        new TRegionReplicaSet(dataRegionGroupId, Collections.singletonList(dataNodeLocation0));
    Map<TConsensusGroupId, TRegionReplicaSet> failedRegions = new HashMap<>();
    failedRegions.put(dataRegionGroupId, dataRegionSet);
    failedRegions.put(schemaRegionGroupId, schemaRegionSet);
    CreateRegionGroupsPlan createRegionGroupsPlan = new CreateRegionGroupsPlan();
    createRegionGroupsPlan.addRegionGroup("root.sg0", dataRegionSet);
    createRegionGroupsPlan.addRegionGroup("root.sg1", schemaRegionSet);
    CreateRegionGroupsPlan persistPlan = new CreateRegionGroupsPlan();
    persistPlan.addRegionGroup("root.sg0", dataRegionSet);
    persistPlan.addRegionGroup("root.sg1", schemaRegionSet);
    CreateRegionGroupsProcedure procedure0 =
        new CreateRegionGroupsProcedure(
            TConsensusGroupType.DataRegion, createRegionGroupsPlan, persistPlan, failedRegions);

    updateProcedurePlan0.setProcedure(procedure0);
    updateProcedurePlan1 =
        (UpdateProcedurePlan)
            ConfigPhysicalPlan.Factory.create(updateProcedurePlan0.serializeToByteBuffer());
    assertEquals(updateProcedurePlan0, updateProcedurePlan1);
  }

  @Test
  public void UpdateProcedurePlanTest() throws IOException {
    UpdateProcedurePlan req0 = new UpdateProcedurePlan();
    DeleteDatabaseProcedure deleteDatabaseProcedure = new DeleteDatabaseProcedure();
    TDatabaseSchema tDatabaseSchema = new TDatabaseSchema();
    tDatabaseSchema.setName("root.sg");
    deleteDatabaseProcedure.setDeleteDatabaseSchema(tDatabaseSchema);
    req0.setProcedure(deleteDatabaseProcedure);
    UpdateProcedurePlan req1 =
        (UpdateProcedurePlan) ConfigPhysicalPlan.Factory.create(req0.serializeToByteBuffer());
    Assert.assertEquals(req0, req1);
  }

  @Test
  public void DeleteProcedurePlanTest() throws IOException {
    DeleteProcedurePlan req0 = new DeleteProcedurePlan();
    req0.setProcId(1L);
    DeleteProcedurePlan req1 =
        (DeleteProcedurePlan) ConfigPhysicalPlan.Factory.create(req0.serializeToByteBuffer());
    Assert.assertEquals(req0, req1);
  }

  @Test
  public void GetRegionLocationsPlanTest() throws IOException {
    GetRegionInfoListPlan req0 = new GetRegionInfoListPlan();
    TShowRegionReq showRegionReq = new TShowRegionReq();
    req0.setShowRegionReq(showRegionReq);
    showRegionReq.setConsensusGroupType(TConsensusGroupType.DataRegion);
    GetRegionInfoListPlan req1 =
        (GetRegionInfoListPlan) ConfigPhysicalPlan.Factory.create(req0.serializeToByteBuffer());
    Assert.assertEquals(req0.getType(), req1.getType());
    Assert.assertEquals(req0.getShowRegionReq(), req1.getShowRegionReq());
    final List<String> sgList = Collections.singletonList("root.sg1, root.sg2, root.*");
    showRegionReq.setDatabases(new ArrayList<>(sgList));
    GetRegionInfoListPlan req2 =
        (GetRegionInfoListPlan) ConfigPhysicalPlan.Factory.create(req0.serializeToByteBuffer());
    Assert.assertEquals(req0.getType(), req1.getType());
    Assert.assertEquals(req0.getShowRegionReq(), req2.getShowRegionReq());
  }

  @Test
  public void CreateSchemaTemplatePlanTest() throws IOException, IllegalPathException {
    Template template = newSchemaTemplate("template_name");
    CreateSchemaTemplatePlan createSchemaTemplatePlan0 =
        new CreateSchemaTemplatePlan(template.serialize().array());
    CreateSchemaTemplatePlan createSchemaTemplatePlan1 =
        (CreateSchemaTemplatePlan)
            ConfigPhysicalPlan.Factory.create(createSchemaTemplatePlan0.serializeToByteBuffer());
    Assert.assertEquals(createSchemaTemplatePlan0, createSchemaTemplatePlan1);
  }

  private Template newSchemaTemplate(String name) throws IllegalPathException {
    List<String> measurements = Arrays.asList(name + "_" + "temperature", name + "_" + "status");
    List<TSDataType> dataTypes = Arrays.asList(TSDataType.FLOAT, TSDataType.BOOLEAN);
    List<TSEncoding> encodings = Arrays.asList(TSEncoding.RLE, TSEncoding.PLAIN);
    List<CompressionType> compressors =
        Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY);
    return new Template(name, measurements, dataTypes, encodings, compressors);
  }

  @Test
  public void GetSchemaTemplatePlanTest() throws IOException {
    GetSchemaTemplatePlan getSchemaTemplatePlan = new GetSchemaTemplatePlan("template1");
    GetSchemaTemplatePlan deserializedPlan =
        (GetSchemaTemplatePlan)
            ConfigPhysicalPlan.Factory.create(getSchemaTemplatePlan.serializeToByteBuffer());
    Assert.assertEquals("template1", deserializedPlan.getTemplateName());
  }

  @Test
  public void GetAllSchemaTemplatePlanTest() throws IOException {
    GetAllSchemaTemplatePlan getAllSchemaTemplatePlan0 = new GetAllSchemaTemplatePlan();
    Assert.assertTrue(
        ConfigPhysicalPlan.Factory.create(getAllSchemaTemplatePlan0.serializeToByteBuffer())
            instanceof GetAllSchemaTemplatePlan);
  }

  @Test
  public void GetNodesInSchemaTemplatePlanTest() throws IOException {
    GetSchemaTemplatePlan getSchemaTemplatePlan0 = new GetSchemaTemplatePlan("template_name_test");
    GetSchemaTemplatePlan getSchemaTemplatePlan1 =
        (GetSchemaTemplatePlan)
            ConfigPhysicalPlan.Factory.create(getSchemaTemplatePlan0.serializeToByteBuffer());
    Assert.assertEquals(getSchemaTemplatePlan0, getSchemaTemplatePlan1);
  }

  @Test
  public void GetNodePathsPartitionPlanTest() throws IOException, IllegalPathException {
    GetNodePathsPartitionPlan getNodePathsPartitionPlan0 = new GetNodePathsPartitionPlan();
    getNodePathsPartitionPlan0.setPartialPath(new PartialPath("root.sg1.**"));
    GetNodePathsPartitionPlan getNodePathsPartitionPlan1 =
        (GetNodePathsPartitionPlan)
            ConfigPhysicalPlan.Factory.create(getNodePathsPartitionPlan0.serializeToByteBuffer());
    Assert.assertEquals(getNodePathsPartitionPlan0, getNodePathsPartitionPlan1);
  }

  @Test
  public void GetAllTemplateSetInfoPlanTest() throws IOException {
    GetAllTemplateSetInfoPlan getAllTemplateSetInfoPlan = new GetAllTemplateSetInfoPlan();
    Assert.assertTrue(
        ConfigPhysicalPlan.Factory.create(getAllTemplateSetInfoPlan.serializeToByteBuffer())
            instanceof GetAllTemplateSetInfoPlan);
  }

  @Test
  public void SetSchemaTemplatePlanTest() throws IOException {
    SetSchemaTemplatePlan setSchemaTemplatePlanPlan0 =
        new SetSchemaTemplatePlan("template_name_test", "root.in.sg.dw");
    SetSchemaTemplatePlan setSchemaTemplatePlanPlan1 =
        (SetSchemaTemplatePlan)
            ConfigPhysicalPlan.Factory.create(setSchemaTemplatePlanPlan0.serializeToByteBuffer());
    Assert.assertEquals(
        setSchemaTemplatePlanPlan0.getName().equalsIgnoreCase(setSchemaTemplatePlanPlan1.getName()),
        setSchemaTemplatePlanPlan0.getPath().equals(setSchemaTemplatePlanPlan1.getPath()));
  }

  @Test
  public void ShowPathSetTemplatePlanTest() throws IOException {
    GetPathsSetTemplatePlan getPathsSetTemplatePlan0 =
        new GetPathsSetTemplatePlan("template_name_test");
    GetPathsSetTemplatePlan getPathsSetTemplatePlan1 =
        (GetPathsSetTemplatePlan)
            ConfigPhysicalPlan.Factory.create(getPathsSetTemplatePlan0.serializeToByteBuffer());
    Assert.assertEquals(getPathsSetTemplatePlan0.getName(), getPathsSetTemplatePlan1.getName());
  }

  @Test
  public void DropSchemaTemplateTest() throws IOException {
    DropSchemaTemplatePlan dropSchemaTemplatePlan = new DropSchemaTemplatePlan("template");
    DropSchemaTemplatePlan deserializedPlan =
        (DropSchemaTemplatePlan)
            ConfigPhysicalPlan.Factory.create(dropSchemaTemplatePlan.serializeToByteBuffer());
    Assert.assertEquals(
        dropSchemaTemplatePlan.getTemplateName(), deserializedPlan.getTemplateName());
  }

  @Test
  public void CreatePipeSinkPlanTest() throws IOException {
    Map<String, String> attributes = new HashMap<>();
    attributes.put("ip", "127.0.0.1");
    attributes.put("port", "6667");
    TPipeSinkInfo pipeSinkInfo =
        new TPipeSinkInfo()
            .setPipeSinkName("demo")
            .setPipeSinkType("IoTDB")
            .setAttributes(attributes);
    CreatePipeSinkPlanV1 createPipeSinkPlan = new CreatePipeSinkPlanV1(pipeSinkInfo);
    CreatePipeSinkPlanV1 createPipeSinkPlan1 =
        (CreatePipeSinkPlanV1)
            ConfigPhysicalPlan.Factory.create(createPipeSinkPlan.serializeToByteBuffer());
    Assert.assertEquals(
        createPipeSinkPlan.getPipeSinkInfo(), createPipeSinkPlan1.getPipeSinkInfo());
  }

  @Test
  public void DropPipeSinkPlanTest() throws IOException {
    DropPipeSinkPlanV1 dropPipeSinkPlan = new DropPipeSinkPlanV1("demo");
    DropPipeSinkPlanV1 dropPipeSinkPlan1 =
        (DropPipeSinkPlanV1)
            ConfigPhysicalPlan.Factory.create(dropPipeSinkPlan.serializeToByteBuffer());
    Assert.assertEquals(dropPipeSinkPlan.getPipeSinkName(), dropPipeSinkPlan1.getPipeSinkName());
  }

  @Test
  public void GetPipeSinkPlanTest() throws IOException {
    GetPipeSinkPlanV1 getPipeSinkPlan = new GetPipeSinkPlanV1("demo");
    GetPipeSinkPlanV1 getPipeSinkPlan1 =
        (GetPipeSinkPlanV1)
            ConfigPhysicalPlan.Factory.create(getPipeSinkPlan.serializeToByteBuffer());
    Assert.assertEquals(getPipeSinkPlan.getPipeSinkName(), getPipeSinkPlan1.getPipeSinkName());
    GetPipeSinkPlanV1 getPipeSinkPlanWithNullName = new GetPipeSinkPlanV1();
    GetPipeSinkPlanV1 getPipeSinkPlanWithNullName1 =
        (GetPipeSinkPlanV1)
            ConfigPhysicalPlan.Factory.create(getPipeSinkPlanWithNullName.serializeToByteBuffer());
    Assert.assertEquals(
        getPipeSinkPlanWithNullName.getPipeSinkName(),
        getPipeSinkPlanWithNullName1.getPipeSinkName());
  }

  @Test
  public void PreCreatePipePlanTest() throws IOException {
    PipeInfo pipeInfo =
        new TsFilePipeInfo(
            "name", "demo", PipeStatus.PARTIAL_CREATE, System.currentTimeMillis(), 999, false);
    PreCreatePipePlanV1 PreCreatePipePlan = new PreCreatePipePlanV1(pipeInfo);
    PreCreatePipePlanV1 PreCreatePipePlan1 =
        (PreCreatePipePlanV1)
            ConfigPhysicalPlan.Factory.create(PreCreatePipePlan.serializeToByteBuffer());
    Assert.assertEquals(PreCreatePipePlan.getPipeInfo(), PreCreatePipePlan1.getPipeInfo());
  }

  @Test
  public void RecordPipeMessagePlanTest() throws IOException {
    RecordPipeMessagePlan recordPipeMessagePlan =
        new RecordPipeMessagePlan(
            "testPipe", new PipeMessage(PipeMessage.PipeMessageType.ERROR, "testError"));
    RecordPipeMessagePlan recordPipeMessagePlan1 =
        (RecordPipeMessagePlan)
            ConfigPhysicalPlan.Factory.create(recordPipeMessagePlan.serializeToByteBuffer());
    Assert.assertEquals(recordPipeMessagePlan.getPipeName(), recordPipeMessagePlan1.getPipeName());
    Assert.assertEquals(
        recordPipeMessagePlan.getPipeMessage().getType(),
        recordPipeMessagePlan1.getPipeMessage().getType());
    Assert.assertEquals(
        recordPipeMessagePlan.getPipeMessage().getMessage(),
        recordPipeMessagePlan1.getPipeMessage().getMessage());
  }

  @Test
  public void SetPipeStatusPlanTest() throws IOException {
    SetPipeStatusPlanV1 setPipeStatusPlan =
        new SetPipeStatusPlanV1("pipe", PipeStatus.PARTIAL_CREATE);
    SetPipeStatusPlanV1 setPipeStatusPlan1 =
        (SetPipeStatusPlanV1)
            ConfigPhysicalPlan.Factory.create(setPipeStatusPlan.serializeToByteBuffer());
    Assert.assertEquals(setPipeStatusPlan.getPipeName(), setPipeStatusPlan1.getPipeName());
    Assert.assertEquals(setPipeStatusPlan.getPipeStatus(), setPipeStatusPlan1.getPipeStatus());
  }

  @Test
  public void CreatePipePlanV2Test() throws IOException {
    Map<String, String> extractorAttributes = new HashMap<>();
    Map<String, String> processorAttributes = new HashMap<>();
    Map<String, String> connectorAttributes = new HashMap<>();
    extractorAttributes.put("extractor", "org.apache.iotdb.pipe.extractor.DefaultExtractor");
    processorAttributes.put("processor", "org.apache.iotdb.pipe.processor.SDTFilterProcessor");
    connectorAttributes.put("connector", "org.apache.iotdb.pipe.protocal.ThriftTransporter");
    PipeTaskMeta pipeTaskMeta = new PipeTaskMeta(new MinimumProgressIndex(), 1);
    Map<TConsensusGroupId, PipeTaskMeta> pipeTasks = new HashMap<>();
    pipeTasks.put(new TConsensusGroupId(DataRegion, 1), pipeTaskMeta);
    PipeStaticMeta pipeStaticMeta =
        new PipeStaticMeta(
            "testPipe", 121, extractorAttributes, processorAttributes, connectorAttributes);
    PipeRuntimeMeta pipeRuntimeMeta = new PipeRuntimeMeta(pipeTasks);
    CreatePipePlanV2 createPipePlanV2 = new CreatePipePlanV2(pipeStaticMeta, pipeRuntimeMeta);
    CreatePipePlanV2 createPipePlanV21 =
        (CreatePipePlanV2)
            ConfigPhysicalPlan.Factory.create(createPipePlanV2.serializeToByteBuffer());
    Assert.assertEquals(
        createPipePlanV2.getPipeStaticMeta(), createPipePlanV21.getPipeStaticMeta());
  }

  @Test
  public void SetPipeStatusPlanV2Test() throws IOException {
    SetPipeStatusPlanV2 setPipeStatusPlanV2 =
        new SetPipeStatusPlanV2("pipe", org.apache.iotdb.commons.pipe.task.meta.PipeStatus.RUNNING);
    SetPipeStatusPlanV2 setPipeStatusPlanV21 =
        (SetPipeStatusPlanV2)
            ConfigPhysicalPlan.Factory.create(setPipeStatusPlanV2.serializeToByteBuffer());
    Assert.assertEquals(setPipeStatusPlanV2.getPipeName(), setPipeStatusPlanV21.getPipeName());
    Assert.assertEquals(setPipeStatusPlanV2.getPipeStatus(), setPipeStatusPlanV21.getPipeStatus());
  }

  @Test
  public void DropPipePlanV2Test() throws IOException {
    DropPipePlanV2 dropPipePlanV2 = new DropPipePlanV2("demo");
    DropPipePlanV2 dropPipePlanV21 =
        (DropPipePlanV2) ConfigPhysicalPlan.Factory.create(dropPipePlanV2.serializeToByteBuffer());
    Assert.assertEquals(dropPipePlanV2.getPipeName(), dropPipePlanV21.getPipeName());
  }

  @Test
  public void ShowPipePlanTest() throws IOException {
    ShowPipePlanV1 showPipePlan = new ShowPipePlanV1("demo");
    ShowPipePlanV1 showPipePlan1 =
        (ShowPipePlanV1) ConfigPhysicalPlan.Factory.create(showPipePlan.serializeToByteBuffer());
    Assert.assertEquals(showPipePlan.getPipeName(), showPipePlan1.getPipeName());
    ShowPipePlanV1 showPipePlanWithNullName = new ShowPipePlanV1();
    ShowPipePlanV1 showPipePlanWithNullName1 =
        (ShowPipePlanV1)
            ConfigPhysicalPlan.Factory.create(showPipePlanWithNullName.serializeToByteBuffer());
    Assert.assertEquals(
        showPipePlanWithNullName.getPipeName(), showPipePlanWithNullName1.getPipeName());
  }

  @Test
  public void CreatePipePluginPlanTest() throws IOException {
    CreatePipePluginPlan createPipePluginPlan =
        new CreatePipePluginPlan(
            new PipePluginMeta("testPlugin", "org.apache.iotdb.TestJar", false, "test.jar", "???"),
            new Binary("123"));
    CreatePipePluginPlan createPipePluginPlan1 =
        (CreatePipePluginPlan)
            ConfigPhysicalPlan.Factory.create(createPipePluginPlan.serializeToByteBuffer());
    Assert.assertEquals(
        createPipePluginPlan.getPipePluginMeta(), createPipePluginPlan1.getPipePluginMeta());
    Assert.assertEquals(createPipePluginPlan.getJarFile(), createPipePluginPlan1.getJarFile());
  }

  @Test
  public void DropPipePluginPlanTest() throws IOException {
    DropPipePluginPlan dropPipePluginPlan = new DropPipePluginPlan("testPlugin");
    DropPipePluginPlan dropPipePluginPlan1 =
        (DropPipePluginPlan)
            ConfigPhysicalPlan.Factory.create(dropPipePluginPlan.serializeToByteBuffer());
    Assert.assertEquals(dropPipePluginPlan.getPluginName(), dropPipePluginPlan1.getPluginName());
  }

  @Test
  public void pipeHandleLeaderChangePlanTest() throws IOException {
    Map<TConsensusGroupId, Integer> newLeaderMap = new HashMap<>();
    newLeaderMap.put(new TConsensusGroupId(TConsensusGroupType.DataRegion, 1), 2);
    newLeaderMap.put(new TConsensusGroupId(TConsensusGroupType.DataRegion, 2), 3);
    newLeaderMap.put(new TConsensusGroupId(TConsensusGroupType.DataRegion, 3), 5);

    PipeHandleLeaderChangePlan pipeHandleLeaderChangePlan =
        new PipeHandleLeaderChangePlan(newLeaderMap);
    PipeHandleLeaderChangePlan pipeHandleLeaderChangePlan1 =
        (PipeHandleLeaderChangePlan)
            ConfigPhysicalPlan.Factory.create(pipeHandleLeaderChangePlan.serializeToByteBuffer());
    Assert.assertEquals(
        pipeHandleLeaderChangePlan.getConsensusGroupId2NewDataRegionLeaderIdMap(),
        pipeHandleLeaderChangePlan1.getConsensusGroupId2NewDataRegionLeaderIdMap());
  }

  @Test
  public void pipeHandleMetaChangePlanTest() throws IOException {
    List<PipeMeta> pipeMetaList = new ArrayList<>();
    PipeStaticMeta pipeStaticMeta =
        new PipeStaticMeta(
            "pipeName",
            123L,
            new HashMap() {
              {
                put("extractor-key", "extractor-value");
              }
            },
            new HashMap() {
              {
                put("processor-key-1", "processor-value-1");
                put("processor-key-2", "processor-value-2");
              }
            },
            new HashMap() {});
    PipeRuntimeMeta pipeRuntimeMeta =
        new PipeRuntimeMeta(
            new HashMap() {
              {
                put(
                    new TConsensusGroupId(TConsensusGroupType.DataRegion, 456),
                    new PipeTaskMeta(
                        new MinimumProgressIndex(), 987)); // TODO: replace with IoTConsensus
                put(
                    new TConsensusGroupId(TConsensusGroupType.DataRegion, 123),
                    new PipeTaskMeta(
                        new MinimumProgressIndex(), 789)); // TODO: replace with IoTConsensus
              }
            });
    pipeMetaList.add(new PipeMeta(pipeStaticMeta, pipeRuntimeMeta));
    PipeHandleMetaChangePlan pipeHandleMetaChangePlan1 = new PipeHandleMetaChangePlan(pipeMetaList);
    PipeHandleMetaChangePlan pipeHandleMetaChangePlan2 =
        (PipeHandleMetaChangePlan)
            ConfigPhysicalPlan.Factory.create(pipeHandleMetaChangePlan1.serializeToByteBuffer());
    Assert.assertEquals(
        pipeHandleMetaChangePlan1.getPipeMetaList(), pipeHandleMetaChangePlan2.getPipeMetaList());
  }

  @Test
  public void GetTriggerTablePlanTest() throws IOException {
    GetTriggerTablePlan getTriggerTablePlan0 = new GetTriggerTablePlan(true);
    GetTriggerTablePlan getTriggerTablePlan1 =
        (GetTriggerTablePlan)
            ConfigPhysicalPlan.Factory.create(getTriggerTablePlan0.serializeToByteBuffer());
    Assert.assertEquals(
        getTriggerTablePlan0.isOnlyStateful(), getTriggerTablePlan1.isOnlyStateful());
  }

  @Test
  public void GetTriggerLocationPlanTest() throws IOException {
    GetTriggerLocationPlan getTriggerLocationPlan0 = new GetTriggerLocationPlan("test1");
    GetTriggerLocationPlan getTriggerLocationPlan1 =
        (GetTriggerLocationPlan)
            ConfigPhysicalPlan.Factory.create(getTriggerLocationPlan0.serializeToByteBuffer());
    Assert.assertEquals(
        getTriggerLocationPlan0.getTriggerName(), getTriggerLocationPlan1.getTriggerName());
  }

  @Test
  public void AddTriggerInTablePlanTest() throws IOException, IllegalPathException {
    TriggerInformation triggerInformation =
        new TriggerInformation(
            new PartialPath("root.test.**"),
            "test",
            "test.class",
            true,
            "test.jar",
            null,
            TriggerEvent.AFTER_INSERT,
            TTriggerState.INACTIVE,
            false,
            null,
            FailureStrategy.OPTIMISTIC,
            "testMD5test");
    AddTriggerInTablePlan addTriggerInTablePlan0 =
        new AddTriggerInTablePlan(triggerInformation, new Binary(new byte[] {1, 2, 3}));
    AddTriggerInTablePlan addTriggerInTablePlan1 =
        (AddTriggerInTablePlan)
            ConfigPhysicalPlan.Factory.create(addTriggerInTablePlan0.serializeToByteBuffer());
    Assert.assertEquals(
        addTriggerInTablePlan0.getTriggerInformation(),
        addTriggerInTablePlan1.getTriggerInformation());
    Assert.assertEquals(addTriggerInTablePlan0.getJarFile(), addTriggerInTablePlan1.getJarFile());
  }

  @Test
  public void DeleteTriggerInTablePlanTest() throws IOException {
    DeleteTriggerInTablePlan deleteTriggerInTablePlan0 = new DeleteTriggerInTablePlan("test");
    DeleteTriggerInTablePlan deleteTriggerInTablePlan1 =
        (DeleteTriggerInTablePlan)
            ConfigPhysicalPlan.Factory.create(deleteTriggerInTablePlan0.serializeToByteBuffer());
    Assert.assertEquals(
        deleteTriggerInTablePlan0.getTriggerName(), deleteTriggerInTablePlan1.getTriggerName());
  }

  @Test
  public void UpdateTriggerStateInTablePlanTest() throws IOException {
    UpdateTriggerStateInTablePlan updateTriggerStateInTablePlan0 =
        new UpdateTriggerStateInTablePlan("test", TTriggerState.ACTIVE);
    UpdateTriggerStateInTablePlan updateTriggerStateInTablePlan1 =
        (UpdateTriggerStateInTablePlan)
            ConfigPhysicalPlan.Factory.create(
                updateTriggerStateInTablePlan0.serializeToByteBuffer());
    Assert.assertEquals(
        updateTriggerStateInTablePlan0.getTriggerName(),
        updateTriggerStateInTablePlan1.getTriggerName());
    Assert.assertEquals(
        updateTriggerStateInTablePlan0.getTriggerState(),
        updateTriggerStateInTablePlan1.getTriggerState());
  }

  @Test
  public void ActiveCQPlanTest() throws IOException {
    ActiveCQPlan activeCQPlan0 = new ActiveCQPlan("testCq", "testCq_md5");
    ActiveCQPlan activeCQPlan1 =
        (ActiveCQPlan) ConfigPhysicalPlan.Factory.create(activeCQPlan0.serializeToByteBuffer());

    Assert.assertEquals(activeCQPlan0, activeCQPlan1);
  }

  @Test
  public void AddCQPlanTest() throws IOException {
    long executionTime = System.currentTimeMillis();
    AddCQPlan addCQPlan0 =
        new AddCQPlan(
            new TCreateCQReq(
                "testCq1",
                1000,
                0,
                1000,
                0,
                (byte) 0,
                "select s1 into root.backup.d1.s1 from root.sg.d1",
                "create cq testCq1 BEGIN select s1 into root.backup.d1.s1 from root.sg.d1 END",
                "Asia",
                "root"),
            "testCq1_md5",
            executionTime);
    AddCQPlan addCQPlan1 =
        (AddCQPlan) ConfigPhysicalPlan.Factory.create(addCQPlan0.serializeToByteBuffer());

    Assert.assertEquals(addCQPlan0, addCQPlan1);
  }

  @Test
  public void DropCQPlanTest() throws IOException {
    DropCQPlan dropCQPlan0 = new DropCQPlan("testCq1");
    DropCQPlan dropCQPlan1 =
        (DropCQPlan) ConfigPhysicalPlan.Factory.create(dropCQPlan0.serializeToByteBuffer());
    Assert.assertEquals(dropCQPlan0, dropCQPlan1);

    dropCQPlan0 = new DropCQPlan("testCq1", "testCq1_md5");
    dropCQPlan1 =
        (DropCQPlan) ConfigPhysicalPlan.Factory.create(dropCQPlan0.serializeToByteBuffer());
    Assert.assertEquals(dropCQPlan0, dropCQPlan1);
  }

  @Test
  public void ShowCQPlanTest() throws IOException {
    ShowCQPlan showCQPlan0 = new ShowCQPlan();
    ShowCQPlan showCQPlan1 =
        (ShowCQPlan) ConfigPhysicalPlan.Factory.create(showCQPlan0.serializeToByteBuffer());

    Assert.assertEquals(showCQPlan0, showCQPlan1);
  }

  @Test
  public void UpdateCQLastExecTimePlanTest() throws IOException {
    UpdateCQLastExecTimePlan updateCQLastExecTimePlan0 =
        new UpdateCQLastExecTimePlan("testCq", System.currentTimeMillis(), "testCq_md5");
    UpdateCQLastExecTimePlan updateCQLastExecTimePlan1 =
        (UpdateCQLastExecTimePlan)
            ConfigPhysicalPlan.Factory.create(updateCQLastExecTimePlan0.serializeToByteBuffer());

    Assert.assertEquals(updateCQLastExecTimePlan0, updateCQLastExecTimePlan1);
  }

  @Test
  public void GetTriggerJarPlanTest() throws IOException {
    List<String> jarNames = new ArrayList<>();
    jarNames.add("test1");
    jarNames.add("test2");
    GetTriggerJarPlan getTriggerJarPlan0 = new GetTriggerJarPlan(jarNames);

    GetTriggerJarPlan getTriggerJarPlan1 =
        (GetTriggerJarPlan)
            ConfigPhysicalPlan.Factory.create(getTriggerJarPlan0.serializeToByteBuffer());
    Assert.assertEquals(getTriggerJarPlan0.getJarNames(), getTriggerJarPlan1.getJarNames());
  }

  @Test
  public void GetRegionIdPlanTest() throws IOException {
    GetRegionIdPlan getRegionIdPlan0 = new GetRegionIdPlan(ConfigRegion);
    GetRegionIdPlan getRegionIdPlan1 =
        (GetRegionIdPlan)
            ConfigPhysicalPlan.Factory.create(getRegionIdPlan0.serializeToByteBuffer());
    Assert.assertEquals(getRegionIdPlan0, getRegionIdPlan1);
  }

  @Test
  public void GetTimeSlotListPlanTest() throws IOException {
    GetTimeSlotListPlan getTimeSlotListPlan0 = new GetTimeSlotListPlan(0, Long.MAX_VALUE);
    GetTimeSlotListPlan getTimeSlotListPlan1 =
        (GetTimeSlotListPlan)
            ConfigPhysicalPlan.Factory.create(getTimeSlotListPlan0.serializeToByteBuffer());
    Assert.assertEquals(getTimeSlotListPlan0, getTimeSlotListPlan1);
  }

  @Test
  public void CountTimeSlotListPlanTest() throws IOException {
    CountTimeSlotListPlan countTimeSlotListPlan0 = new CountTimeSlotListPlan(0, Long.MAX_VALUE);
    CountTimeSlotListPlan countTimeSlotListPlan1 =
        (CountTimeSlotListPlan)
            ConfigPhysicalPlan.Factory.create(countTimeSlotListPlan0.serializeToByteBuffer());
    Assert.assertEquals(countTimeSlotListPlan0, countTimeSlotListPlan1);
  }

  @Test
  public void GetSeriesSlotListPlanTest() throws IOException {
    GetSeriesSlotListPlan getSeriesSlotListPlan0 =
        new GetSeriesSlotListPlan("root.test", SchemaRegion);
    GetSeriesSlotListPlan getSeriesSlotListPlan1 =
        (GetSeriesSlotListPlan)
            ConfigPhysicalPlan.Factory.create(getSeriesSlotListPlan0.serializeToByteBuffer());
    Assert.assertEquals(getSeriesSlotListPlan0, getSeriesSlotListPlan1);
  }

  @Test
  public void GetPipePluginJarPlanTest() throws IOException {
    List<String> jarNames = new ArrayList<>();
    jarNames.add("org.apache.testJar");
    jarNames.add("org.apache.testJar2");
    GetPipePluginJarPlan getPipePluginJarPlan0 = new GetPipePluginJarPlan(jarNames);
    GetPipePluginJarPlan getPipePluginJarPlan1 =
        (GetPipePluginJarPlan)
            ConfigPhysicalPlan.Factory.create(getPipePluginJarPlan0.serializeToByteBuffer());
    Assert.assertEquals(getPipePluginJarPlan0, getPipePluginJarPlan1);
  }

  @Test
  public void GetPipePluginTablePlanTest() throws IOException {
    GetPipePluginTablePlan getPipePluginTablePlan0 = new GetPipePluginTablePlan();
    GetPipePluginTablePlan getPipePluginTablePlan1 =
        (GetPipePluginTablePlan)
            ConfigPhysicalPlan.Factory.create(getPipePluginTablePlan0.serializeToByteBuffer());
    Assert.assertEquals(getPipePluginTablePlan0, getPipePluginTablePlan1);
  }

  @Test
  public void ShowPipePlanV2Test() throws IOException {
    ShowPipePlanV2 showPipePlanV2 = new ShowPipePlanV2();
    ShowPipePlanV2 showPipePlanV21 =
        (ShowPipePlanV2) ConfigPhysicalPlan.Factory.create(showPipePlanV2.serializeToByteBuffer());
    Assert.assertEquals(showPipePlanV2, showPipePlanV21);
  }

  @Test
  public void RemoveDataNodePlanTest() throws IOException {
    List<TDataNodeLocation> locations = new ArrayList<>();
    TDataNodeLocation location1 = new TDataNodeLocation();
    location1.setDataNodeId(1);
    location1.setInternalEndPoint(new TEndPoint("192.168.12.1", 6661));
    location1.setClientRpcEndPoint(new TEndPoint("192.168.12.1", 6662));
    location1.setDataRegionConsensusEndPoint(new TEndPoint("192.168.12.1", 6663));
    location1.setSchemaRegionConsensusEndPoint(new TEndPoint("192.168.12.1", 6664));
    location1.setMPPDataExchangeEndPoint(new TEndPoint("192.168.12.1", 6665));
    locations.add(location1);

    TDataNodeLocation location2 = new TDataNodeLocation();
    location2.setDataNodeId(2);
    location2.setInternalEndPoint(new TEndPoint("192.168.12.2", 6661));
    location2.setClientRpcEndPoint(new TEndPoint("192.168.12.2", 6662));
    location2.setDataRegionConsensusEndPoint(new TEndPoint("192.168.12.2", 6663));
    location2.setSchemaRegionConsensusEndPoint(new TEndPoint("192.168.12.2", 6664));
    location2.setMPPDataExchangeEndPoint(new TEndPoint("192.168.12.2", 6665));
    locations.add(location2);

    RemoveDataNodePlan removeDataNodePlan0 = new RemoveDataNodePlan(new ArrayList<>(locations));
    RemoveDataNodePlan removeDataNodePlan1 =
        (RemoveDataNodePlan)
            ConfigPhysicalPlan.Factory.create(removeDataNodePlan0.serializeToByteBuffer());
    Assert.assertEquals(removeDataNodePlan0, removeDataNodePlan1);
  }

  @Test
  public void UpdateTriggersOnTransferNodesPlanTest() throws IOException {
    List<TDataNodeLocation> dataNodeLocations = new ArrayList<>(2);
    dataNodeLocations.add(
        new TDataNodeLocation(
            10000,
            new TEndPoint("127.0.0.1", 6600),
            new TEndPoint("127.0.0.1", 7700),
            new TEndPoint("127.0.0.1", 8800),
            new TEndPoint("127.0.0.1", 9900),
            new TEndPoint("127.0.0.1", 11000)));
    dataNodeLocations.add(
        new TDataNodeLocation(
            20000,
            new TEndPoint("127.0.0.1", 6600),
            new TEndPoint("127.0.0.1", 7700),
            new TEndPoint("127.0.0.1", 8800),
            new TEndPoint("127.0.0.1", 9900),
            new TEndPoint("127.0.0.1", 11000)));

    UpdateTriggersOnTransferNodesPlan plan0 =
        new UpdateTriggersOnTransferNodesPlan(dataNodeLocations);
    UpdateTriggersOnTransferNodesPlan plan1 =
        (UpdateTriggersOnTransferNodesPlan)
            ConfigPhysicalPlan.Factory.create(plan0.serializeToByteBuffer());

    Assert.assertEquals(plan0.getDataNodeLocations(), plan1.getDataNodeLocations());
  }

  @Test
  public void UpdateTriggerLocationPlanTest() throws IOException {
    UpdateTriggerLocationPlan plan0 =
        new UpdateTriggerLocationPlan(
            "test",
            new TDataNodeLocation(
                10000,
                new TEndPoint("127.0.0.1", 6600),
                new TEndPoint("127.0.0.1", 7700),
                new TEndPoint("127.0.0.1", 8800),
                new TEndPoint("127.0.0.1", 9900),
                new TEndPoint("127.0.0.1", 11000)));
    UpdateTriggerLocationPlan plan1 =
        (UpdateTriggerLocationPlan)
            ConfigPhysicalPlan.Factory.create(plan0.serializeToByteBuffer());

    Assert.assertEquals(plan0.getTriggerName(), plan1.getTriggerName());
    Assert.assertEquals(plan0.getDataNodeLocation(), plan1.getDataNodeLocation());
  }

  @Test
  public void GetTransferringTriggersPlanTest() throws IOException {
    GetTransferringTriggersPlan getTransferringTriggerPlan0 = new GetTransferringTriggersPlan();
    Assert.assertTrue(
        ConfigPhysicalPlan.Factory.create(getTransferringTriggerPlan0.serializeToByteBuffer())
            instanceof GetTransferringTriggersPlan);
  }

  @Test
  public void GetUDFTablePlanTest() throws IOException {
    GetFunctionTablePlan getUDFTablePlan0 = new GetFunctionTablePlan();
    Assert.assertTrue(
        ConfigPhysicalPlan.Factory.create(getUDFTablePlan0.serializeToByteBuffer())
            instanceof GetFunctionTablePlan);
  }

  @Test
  public void GetUDFJarPlanTest() throws IOException {
    List<String> jarNames = new ArrayList<>();
    jarNames.add("test1");
    jarNames.add("test2");
    GetUDFJarPlan getUDFJarPlan0 = new GetUDFJarPlan(jarNames);

    GetUDFJarPlan getUDFJarPlan1 =
        (GetUDFJarPlan) ConfigPhysicalPlan.Factory.create(getUDFJarPlan0.serializeToByteBuffer());
    Assert.assertEquals(getUDFJarPlan0.getJarNames(), getUDFJarPlan1.getJarNames());
  }

  @Test
  public void CreateFunctionPlanTest() throws IOException {
    UDFInformation udfInformation =
        new UDFInformation("test1", "test1", false, true, "test1.jar", "12345");
    CreateFunctionPlan createFunctionPlan0 =
        new CreateFunctionPlan(udfInformation, new Binary(new byte[] {1, 2, 3}));
    CreateFunctionPlan createFunctionPlan1 =
        (CreateFunctionPlan)
            ConfigPhysicalPlan.Factory.create(createFunctionPlan0.serializeToByteBuffer());
    Assert.assertEquals(createFunctionPlan0, createFunctionPlan1);
  }

  @Test
  public void DropFunctionPlanTest() throws IOException {
    DropFunctionPlan dropFunctionPlan0 = new DropFunctionPlan("test");
    DropFunctionPlan dropFunctionPlan1 =
        (DropFunctionPlan)
            ConfigPhysicalPlan.Factory.create(dropFunctionPlan0.serializeToByteBuffer());
    Assert.assertEquals(dropFunctionPlan0, dropFunctionPlan1);
  }

  @Test
  public void PreUnsetSchemaTemplatePlanTest() throws IllegalPathException, IOException {
    PreUnsetSchemaTemplatePlan plan = new PreUnsetSchemaTemplatePlan(1, new PartialPath("root.sg"));
    PreUnsetSchemaTemplatePlan deserializedPlan =
        (PreUnsetSchemaTemplatePlan)
            ConfigPhysicalPlan.Factory.create(plan.serializeToByteBuffer());
    Assert.assertEquals(plan.getTemplateId(), deserializedPlan.getTemplateId());
    Assert.assertEquals(plan.getPath(), deserializedPlan.getPath());
  }

  @Test
  public void RollbackPreUnsetSchemaTemplatePlanTest() throws IllegalPathException, IOException {
    RollbackPreUnsetSchemaTemplatePlan plan =
        new RollbackPreUnsetSchemaTemplatePlan(1, new PartialPath("root.sg"));
    RollbackPreUnsetSchemaTemplatePlan deserializedPlan =
        (RollbackPreUnsetSchemaTemplatePlan)
            ConfigPhysicalPlan.Factory.create(plan.serializeToByteBuffer());
    Assert.assertEquals(plan.getTemplateId(), deserializedPlan.getTemplateId());
    Assert.assertEquals(plan.getPath(), deserializedPlan.getPath());
  }

  @Test
  public void UnsetSchemaTemplatePlanTest() throws IllegalPathException, IOException {
    UnsetSchemaTemplatePlan plan = new UnsetSchemaTemplatePlan(1, new PartialPath("root.sg"));
    UnsetSchemaTemplatePlan deserializedPlan =
        (UnsetSchemaTemplatePlan) ConfigPhysicalPlan.Factory.create(plan.serializeToByteBuffer());
    Assert.assertEquals(plan.getTemplateId(), deserializedPlan.getTemplateId());
    Assert.assertEquals(plan.getPath(), deserializedPlan.getPath());
  }

  @Test
  public void PollSpecificRegionMaintainTaskPlanTest() throws IOException {
    Set<TConsensusGroupId> regionIdSet =
        new HashSet<>(
            Arrays.asList(
                new TConsensusGroupId(SchemaRegion, 1),
                new TConsensusGroupId(DataRegion, 2),
                new TConsensusGroupId(DataRegion, 3)));
    PollSpecificRegionMaintainTaskPlan plan = new PollSpecificRegionMaintainTaskPlan(regionIdSet);

    PollSpecificRegionMaintainTaskPlan deserializedPlan =
        (PollSpecificRegionMaintainTaskPlan)
            ConfigPhysicalPlan.Factory.create(plan.serializeToByteBuffer());
    Assert.assertEquals(deserializedPlan.getRegionIdSet(), regionIdSet);
  }

  @Test
  public void setSpaceQuotaPlanTest() throws IOException {
    TSpaceQuota spaceQuota = new TSpaceQuota();
    spaceQuota.setDeviceNum(2);
    spaceQuota.setTimeserieNum(3);
    spaceQuota.setDiskSize(1024);
    SetSpaceQuotaPlan plan =
        new SetSpaceQuotaPlan(Collections.singletonList("root.sg"), spaceQuota);
    SetSpaceQuotaPlan deserializedPlan =
        (SetSpaceQuotaPlan) ConfigPhysicalPlan.Factory.create(plan.serializeToByteBuffer());
    Assert.assertEquals(plan.getPrefixPathList(), deserializedPlan.getPrefixPathList());
    Assert.assertEquals(plan.getSpaceLimit(), deserializedPlan.getSpaceLimit());
  }

  @Test
  public void setThrottleQuotaPlanTest() throws IOException {
    TTimedQuota timedQuota1 = new TTimedQuota(3600, 5);
    TTimedQuota timedQuota2 = new TTimedQuota(3600, 5);
    Map<ThrottleType, TTimedQuota> throttleLimit = new HashMap<>();
    throttleLimit.put(ThrottleType.READ_NUMBER, timedQuota1);
    throttleLimit.put(ThrottleType.READ_SIZE, timedQuota2);
    SetThrottleQuotaPlan plan = new SetThrottleQuotaPlan();
    TThrottleQuota throttleQuota = new TThrottleQuota();
    throttleQuota.setThrottleLimit(throttleLimit);
    throttleQuota.setMemLimit(1000000);
    throttleQuota.setCpuLimit(100);
    plan.setThrottleQuota(throttleQuota);
    plan.setUserName("tempuser");
    SetThrottleQuotaPlan deserializedPlan =
        (SetThrottleQuotaPlan) ConfigPhysicalPlan.Factory.create(plan.serializeToByteBuffer());
    Assert.assertEquals(plan.getUserName(), deserializedPlan.getUserName());
    Assert.assertEquals(plan.getThrottleQuota(), deserializedPlan.getThrottleQuota());
  }
}
