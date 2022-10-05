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
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.partition.DataPartitionTable;
import org.apache.iotdb.commons.partition.SchemaPartitionTable;
import org.apache.iotdb.commons.partition.SeriesPartitionTable;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.sync.pipe.PipeInfo;
import org.apache.iotdb.commons.sync.pipe.PipeStatus;
import org.apache.iotdb.commons.sync.pipe.SyncOperation;
import org.apache.iotdb.commons.sync.pipe.TsFilePipeInfo;
import org.apache.iotdb.commons.trigger.TriggerInformation;
import org.apache.iotdb.confignode.consensus.request.auth.AuthorPlan;
import org.apache.iotdb.confignode.consensus.request.read.CountStorageGroupPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetDataNodeConfigurationPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetDataPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetOrCreateDataPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetOrCreateSchemaPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetRegionInfoListPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetSchemaPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetStorageGroupPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetTriggerTablePlan;
import org.apache.iotdb.confignode.consensus.request.read.template.GetAllSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.read.template.GetAllTemplateSetInfoPlan;
import org.apache.iotdb.confignode.consensus.request.read.template.GetPathsSetTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.read.template.GetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.DeleteProcedurePlan;
import org.apache.iotdb.confignode.consensus.request.write.RegisterDataNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.UpdateProcedurePlan;
import org.apache.iotdb.confignode.consensus.request.write.confignode.ApplyConfigNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.confignode.RemoveConfigNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.partition.CreateDataPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.write.partition.CreateSchemaPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.write.region.CreateRegionGroupsPlan;
import org.apache.iotdb.confignode.consensus.request.write.region.OfferRegionMaintainTasksPlan;
import org.apache.iotdb.confignode.consensus.request.write.region.PollRegionMaintainTaskPlan;
import org.apache.iotdb.confignode.consensus.request.write.storagegroup.AdjustMaxRegionGroupCountPlan;
import org.apache.iotdb.confignode.consensus.request.write.storagegroup.DeleteStorageGroupPlan;
import org.apache.iotdb.confignode.consensus.request.write.storagegroup.SetDataReplicationFactorPlan;
import org.apache.iotdb.confignode.consensus.request.write.storagegroup.SetSchemaReplicationFactorPlan;
import org.apache.iotdb.confignode.consensus.request.write.storagegroup.SetStorageGroupPlan;
import org.apache.iotdb.confignode.consensus.request.write.storagegroup.SetTTLPlan;
import org.apache.iotdb.confignode.consensus.request.write.storagegroup.SetTimePartitionIntervalPlan;
import org.apache.iotdb.confignode.consensus.request.write.sync.CreatePipeSinkPlan;
import org.apache.iotdb.confignode.consensus.request.write.sync.DropPipeSinkPlan;
import org.apache.iotdb.confignode.consensus.request.write.sync.GetPipeSinkPlan;
import org.apache.iotdb.confignode.consensus.request.write.sync.OperatePipePlan;
import org.apache.iotdb.confignode.consensus.request.write.sync.PreCreatePipePlan;
import org.apache.iotdb.confignode.consensus.request.write.sync.ShowPipePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.CreateSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.SetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.trigger.AddTriggerInTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.trigger.DeleteTriggerInTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.trigger.UpdateTriggerStateInTablePlan;
import org.apache.iotdb.confignode.persistence.partition.RegionCreateTask;
import org.apache.iotdb.confignode.persistence.partition.RegionDeleteTask;
import org.apache.iotdb.confignode.procedure.Procedure;
import org.apache.iotdb.confignode.procedure.impl.CreateRegionGroupsProcedure;
import org.apache.iotdb.confignode.procedure.impl.DeleteStorageGroupProcedure;
import org.apache.iotdb.confignode.rpc.thrift.TPipeSinkInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;
import org.apache.iotdb.confignode.rpc.thrift.TTriggerState;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.CreateSchemaTemplateStatement;
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

import static org.apache.iotdb.common.rpc.thrift.TConsensusGroupType.DataRegion;
import static org.apache.iotdb.common.rpc.thrift.TConsensusGroupType.SchemaRegion;
import static org.junit.Assert.assertEquals;

public class ConfigPhysicalPlanSerDeTest {

  @Test
  public void RegisterDataNodePlanTest() throws IOException {
    TDataNodeLocation dataNodeLocation = new TDataNodeLocation();
    dataNodeLocation.setDataNodeId(1);
    dataNodeLocation.setClientRpcEndPoint(new TEndPoint("0.0.0.0", 6667));
    dataNodeLocation.setInternalEndPoint(new TEndPoint("0.0.0.0", 9003));
    dataNodeLocation.setMPPDataExchangeEndPoint(new TEndPoint("0.0.0.0", 8777));
    dataNodeLocation.setDataRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 40010));
    dataNodeLocation.setSchemaRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 50010));

    TDataNodeConfiguration dataNodeConfiguration = new TDataNodeConfiguration();
    dataNodeConfiguration.setLocation(dataNodeLocation);
    dataNodeConfiguration.setResource(new TNodeResource(16, 34359738368L));

    RegisterDataNodePlan plan0 = new RegisterDataNodePlan(dataNodeConfiguration);
    RegisterDataNodePlan plan1 =
        (RegisterDataNodePlan) ConfigPhysicalPlan.Factory.create(plan0.serializeToByteBuffer());
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
  public void SetStorageGroupPlanTest() throws IOException {
    SetStorageGroupPlan req0 =
        new SetStorageGroupPlan(
            new TStorageGroupSchema()
                .setName("sg")
                .setTTL(Long.MAX_VALUE)
                .setSchemaReplicationFactor(3)
                .setDataReplicationFactor(3)
                .setTimePartitionInterval(604800));
    SetStorageGroupPlan req1 =
        (SetStorageGroupPlan) ConfigPhysicalPlan.Factory.create(req0.serializeToByteBuffer());
    Assert.assertEquals(req0, req1);
  }

  @Test
  public void DeleteStorageGroupPlanTest() throws IOException {
    // TODO: Add serialize and deserialize test
    DeleteStorageGroupPlan req0 = new DeleteStorageGroupPlan("root.sg");
    DeleteStorageGroupPlan req1 =
        (DeleteStorageGroupPlan) ConfigPhysicalPlan.Factory.create(req0.serializeToByteBuffer());
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
    AdjustMaxRegionGroupCountPlan req0 = new AdjustMaxRegionGroupCountPlan();
    for (int i = 0; i < 3; i++) {
      req0.putEntry("root.sg" + i, new Pair<>(i, i));
    }

    AdjustMaxRegionGroupCountPlan req1 =
        (AdjustMaxRegionGroupCountPlan)
            ConfigPhysicalPlan.Factory.create(req0.serializeToByteBuffer());
    Assert.assertEquals(req0, req1);
  }

  @Test
  public void CountStorageGroupPlanTest() throws IOException {
    CountStorageGroupPlan req0 = new CountStorageGroupPlan(Arrays.asList("root", "sg"));
    CountStorageGroupPlan req1 =
        (CountStorageGroupPlan) ConfigPhysicalPlan.Factory.create(req0.serializeToByteBuffer());
    Assert.assertEquals(req0, req1);
  }

  @Test
  public void GetStorageGroupPlanTest() throws IOException {
    GetStorageGroupPlan req0 = new GetStorageGroupPlan(Arrays.asList("root", "sg"));
    CountStorageGroupPlan req1 =
        (CountStorageGroupPlan) ConfigPhysicalPlan.Factory.create(req0.serializeToByteBuffer());
    Assert.assertEquals(req0, req1);
  }

  @Test
  public void CreateRegionsPlanTest() throws IOException {
    TDataNodeLocation dataNodeLocation = new TDataNodeLocation();
    dataNodeLocation.setDataNodeId(0);
    dataNodeLocation.setClientRpcEndPoint(new TEndPoint("0.0.0.0", 6667));
    dataNodeLocation.setInternalEndPoint(new TEndPoint("0.0.0.0", 9003));
    dataNodeLocation.setMPPDataExchangeEndPoint(new TEndPoint("0.0.0.0", 8777));
    dataNodeLocation.setDataRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 40010));
    dataNodeLocation.setSchemaRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 50010));

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
    dataNodeLocation.setInternalEndPoint(new TEndPoint("0.0.0.0", 9003));
    dataNodeLocation.setMPPDataExchangeEndPoint(new TEndPoint("0.0.0.0", 8777));
    dataNodeLocation.setDataRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 40010));
    dataNodeLocation.setSchemaRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 50010));

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
    dataNodeLocation.setInternalEndPoint(new TEndPoint("0.0.0.0", 9003));
    dataNodeLocation.setMPPDataExchangeEndPoint(new TEndPoint("0.0.0.0", 8777));
    dataNodeLocation.setDataRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 40010));
    dataNodeLocation.setSchemaRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 50010));

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
    dataNodeLocation.setInternalEndPoint(new TEndPoint("0.0.0.0", 9003));
    dataNodeLocation.setMPPDataExchangeEndPoint(new TEndPoint("0.0.0.0", 8777));
    dataNodeLocation.setDataRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 40010));
    dataNodeLocation.setSchemaRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 50010));

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

    Map<String, Map<TSeriesPartitionSlot, List<TTimePartitionSlot>>> partitionSlotsMap =
        new HashMap<>();
    partitionSlotsMap.put(storageGroup, new HashMap<>());
    partitionSlotsMap.get(storageGroup).put(seriesPartitionSlot, new ArrayList<>());
    partitionSlotsMap.get(storageGroup).get(seriesPartitionSlot).add(timePartitionSlot);

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

    Map<String, Map<TSeriesPartitionSlot, List<TTimePartitionSlot>>> partitionSlotsMap =
        new HashMap<>();
    partitionSlotsMap.put(storageGroup, new HashMap<>());
    partitionSlotsMap.get(storageGroup).put(seriesPartitionSlot, new ArrayList<>());
    partitionSlotsMap.get(storageGroup).get(seriesPartitionSlot).add(timePartitionSlot);

    GetOrCreateDataPartitionPlan req0 = new GetOrCreateDataPartitionPlan(partitionSlotsMap);
    GetOrCreateDataPartitionPlan req1 =
        (GetOrCreateDataPartitionPlan)
            ConfigPhysicalPlan.Factory.create(req0.serializeToByteBuffer());
    Assert.assertEquals(req0, req1);
  }

  @Test
  public void AuthorPlanTest() throws IOException, AuthException {

    AuthorPlan req0;
    AuthorPlan req1;
    Set<Integer> permissions = new HashSet<>();
    permissions.add(PrivilegeType.GRANT_USER_PRIVILEGE.ordinal());
    permissions.add(PrivilegeType.REVOKE_USER_ROLE.ordinal());

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
    List<String> nodeNameList = new ArrayList<>();
    nodeNameList.add("root.ln.**");
    nodeNameList.add("root.abc.**");
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
    DeleteStorageGroupProcedure deleteStorageGroupProcedure = new DeleteStorageGroupProcedure();
    deleteStorageGroupProcedure.setDeleteSgSchema(new TStorageGroupSchema("root.sg"));
    UpdateProcedurePlan updateProcedurePlan0 = new UpdateProcedurePlan();
    updateProcedurePlan0.setProcedure(deleteStorageGroupProcedure);
    UpdateProcedurePlan updateProcedurePlan1 =
        (UpdateProcedurePlan)
            ConfigPhysicalPlan.Factory.create(updateProcedurePlan0.serializeToByteBuffer());
    Procedure proc = updateProcedurePlan1.getProcedure();
    Assert.assertEquals(proc, deleteStorageGroupProcedure);

    // test procedure equals CreateRegionGroupsProcedure
    TDataNodeLocation dataNodeLocation0 = new TDataNodeLocation();
    dataNodeLocation0.setDataNodeId(5);
    dataNodeLocation0.setClientRpcEndPoint(new TEndPoint("0.0.0.0", 6667));
    dataNodeLocation0.setInternalEndPoint(new TEndPoint("0.0.0.0", 9003));
    dataNodeLocation0.setMPPDataExchangeEndPoint(new TEndPoint("0.0.0.0", 8777));
    dataNodeLocation0.setDataRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 40010));
    dataNodeLocation0.setSchemaRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 50010));

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
    CreateRegionGroupsProcedure procedure0 =
        new CreateRegionGroupsProcedure(createRegionGroupsPlan, failedRegions);

    updateProcedurePlan0.setProcedure(procedure0);
    updateProcedurePlan1 =
        (UpdateProcedurePlan)
            ConfigPhysicalPlan.Factory.create(updateProcedurePlan0.serializeToByteBuffer());
    assertEquals(updateProcedurePlan0, updateProcedurePlan1);
  }

  @Test
  public void UpdateProcedurePlanTest() throws IOException {
    UpdateProcedurePlan req0 = new UpdateProcedurePlan();
    DeleteStorageGroupProcedure deleteStorageGroupProcedure = new DeleteStorageGroupProcedure();
    TStorageGroupSchema tStorageGroupSchema = new TStorageGroupSchema();
    tStorageGroupSchema.setName("root.sg");
    deleteStorageGroupProcedure.setDeleteSgSchema(tStorageGroupSchema);
    req0.setProcedure(deleteStorageGroupProcedure);
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
  public void GetRegionLocaltionsPlanTest() throws IOException {
    GetRegionInfoListPlan req0 = new GetRegionInfoListPlan();
    TShowRegionReq showRegionReq = new TShowRegionReq();
    req0.setShowRegionReq(showRegionReq);
    showRegionReq.setConsensusGroupType(TConsensusGroupType.DataRegion);
    GetRegionInfoListPlan req1 =
        (GetRegionInfoListPlan) ConfigPhysicalPlan.Factory.create(req0.serializeToByteBuffer());
    Assert.assertEquals(req0.getType(), req1.getType());
    Assert.assertEquals(req0.getShowRegionReq(), req1.getShowRegionReq());
    final List<String> sgList = Collections.singletonList("root.sg1, root.sg2, root.*");
    showRegionReq.setStorageGroups(new ArrayList<>(sgList));
    GetRegionInfoListPlan req2 =
        (GetRegionInfoListPlan) ConfigPhysicalPlan.Factory.create(req0.serializeToByteBuffer());
    Assert.assertEquals(req0.getType(), req1.getType());
    Assert.assertEquals(req0.getShowRegionReq(), req2.getShowRegionReq());
  }

  @Test
  public void CreateSchemaTemplatePlanTest() throws IOException, IllegalPathException {
    Template template = new Template(newCreateSchemaTemplateStatement("template_name"));
    CreateSchemaTemplatePlan createSchemaTemplatePlan0 =
        new CreateSchemaTemplatePlan(template.serialize().array());
    CreateSchemaTemplatePlan createSchemaTemplatePlan1 =
        (CreateSchemaTemplatePlan)
            ConfigPhysicalPlan.Factory.create(createSchemaTemplatePlan0.serializeToByteBuffer());
    Assert.assertEquals(createSchemaTemplatePlan0, createSchemaTemplatePlan1);
  }

  private CreateSchemaTemplateStatement newCreateSchemaTemplateStatement(String name) {
    List<List<String>> measurements =
        Arrays.asList(
            Collections.singletonList(name + "_" + "temperature"),
            Collections.singletonList(name + "_" + "status"));
    List<List<TSDataType>> dataTypes =
        Arrays.asList(
            Collections.singletonList(TSDataType.FLOAT),
            Collections.singletonList(TSDataType.BOOLEAN));
    List<List<TSEncoding>> encodings =
        Arrays.asList(
            Collections.singletonList(TSEncoding.RLE), Collections.singletonList(TSEncoding.PLAIN));
    List<List<CompressionType>> compressors =
        Arrays.asList(
            Collections.singletonList(CompressionType.SNAPPY),
            Collections.singletonList(CompressionType.SNAPPY));
    return new CreateSchemaTemplateStatement(name, measurements, dataTypes, encodings, compressors);
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
  public void GetAllTemplateSetInfoPlan() throws IOException {
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
  public void CreatePipeSinkPlanTest() throws IOException {
    Map<String, String> attributes = new HashMap<>();
    attributes.put("ip", "127.0.0.1");
    attributes.put("port", "6667");
    TPipeSinkInfo pipeSinkInfo =
        new TPipeSinkInfo()
            .setPipeSinkName("demo")
            .setPipeSinkType("IoTDB")
            .setAttributes(attributes);
    CreatePipeSinkPlan createPipeSinkPlan = new CreatePipeSinkPlan(pipeSinkInfo);
    CreatePipeSinkPlan createPipeSinkPlan1 =
        (CreatePipeSinkPlan)
            ConfigPhysicalPlan.Factory.create(createPipeSinkPlan.serializeToByteBuffer());
    Assert.assertEquals(
        createPipeSinkPlan.getPipeSinkInfo(), createPipeSinkPlan1.getPipeSinkInfo());
  }

  @Test
  public void DropPipeSinkPlanTest() throws IOException {
    DropPipeSinkPlan dropPipeSinkPlan = new DropPipeSinkPlan("demo");
    DropPipeSinkPlan dropPipeSinkPlan1 =
        (DropPipeSinkPlan)
            ConfigPhysicalPlan.Factory.create(dropPipeSinkPlan.serializeToByteBuffer());
    Assert.assertEquals(dropPipeSinkPlan.getPipeSinkName(), dropPipeSinkPlan1.getPipeSinkName());
  }

  @Test
  public void GetPipeSinkPlanTest() throws IOException {
    GetPipeSinkPlan getPipeSinkPlan = new GetPipeSinkPlan("demo");
    GetPipeSinkPlan getPipeSinkPlan1 =
        (GetPipeSinkPlan)
            ConfigPhysicalPlan.Factory.create(getPipeSinkPlan.serializeToByteBuffer());
    Assert.assertEquals(getPipeSinkPlan.getPipeSinkName(), getPipeSinkPlan1.getPipeSinkName());
    GetPipeSinkPlan getPipeSinkPlanWithNullName = new GetPipeSinkPlan();
    GetPipeSinkPlan getPipeSinkPlanWithNullName1 =
        (GetPipeSinkPlan)
            ConfigPhysicalPlan.Factory.create(getPipeSinkPlanWithNullName.serializeToByteBuffer());
    Assert.assertEquals(
        getPipeSinkPlanWithNullName.getPipeSinkName(),
        getPipeSinkPlanWithNullName1.getPipeSinkName());
  }

  @Test
  public void PreCreatePipePlanTest() throws IOException {
    PipeInfo pipeInfo =
        new TsFilePipeInfo(
            "name", "demo", PipeStatus.PREPARE_CREATE, System.currentTimeMillis(), 999, false);
    PreCreatePipePlan PreCreatePipePlan = new PreCreatePipePlan(pipeInfo);
    PreCreatePipePlan PreCreatePipePlan1 =
        (PreCreatePipePlan)
            ConfigPhysicalPlan.Factory.create(PreCreatePipePlan.serializeToByteBuffer());
    Assert.assertEquals(PreCreatePipePlan.getPipeInfo(), PreCreatePipePlan1.getPipeInfo());
  }

  @Test
  public void OperatePipePlanTest() throws IOException {
    OperatePipePlan operatePipePlan = new OperatePipePlan("pipe", SyncOperation.CREATE_PIPE);
    OperatePipePlan operatePipePlan1 =
        (OperatePipePlan)
            ConfigPhysicalPlan.Factory.create(operatePipePlan.serializeToByteBuffer());
    Assert.assertEquals(operatePipePlan.getPipeName(), operatePipePlan1.getPipeName());
    Assert.assertEquals(operatePipePlan.getOperation(), operatePipePlan1.getOperation());
  }

  @Test
  public void ShowPipePlanTest() throws IOException {
    ShowPipePlan showPipePlan = new ShowPipePlan("demo");
    ShowPipePlan showPipePlan1 =
        (ShowPipePlan) ConfigPhysicalPlan.Factory.create(showPipePlan.serializeToByteBuffer());
    Assert.assertEquals(showPipePlan.getPipeName(), showPipePlan1.getPipeName());
    ShowPipePlan showPipePlanWithNullName = new ShowPipePlan();
    ShowPipePlan showPipePlanWithNullName1 =
        (ShowPipePlan)
            ConfigPhysicalPlan.Factory.create(showPipePlanWithNullName.serializeToByteBuffer());
    Assert.assertEquals(
        showPipePlanWithNullName.getPipeName(), showPipePlanWithNullName1.getPipeName());
  }

  @Test
  public void GetTriggerTablePlan() throws IOException {
    GetTriggerTablePlan getTriggerTablePlan0 = new GetTriggerTablePlan();
    Assert.assertTrue(
        ConfigPhysicalPlan.Factory.create(getTriggerTablePlan0.serializeToByteBuffer())
            instanceof GetTriggerTablePlan);
  }

  @Test
  public void AddTriggerInTablePlanTest() throws IOException, IllegalPathException {
    TriggerInformation triggerInformation =
        new TriggerInformation(
            new PartialPath("root.test.**"),
            "test",
            "test.class",
            "test.jar",
            null,
            TriggerEvent.AFTER_INSERT,
            TTriggerState.INACTIVE,
            false,
            null,
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
}
