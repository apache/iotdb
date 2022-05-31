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
import org.apache.iotdb.common.rpc.thrift.TDataNodeInfo;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.confignode.consensus.request.auth.AuthorReq;
import org.apache.iotdb.confignode.consensus.request.read.CountStorageGroupReq;
import org.apache.iotdb.confignode.consensus.request.read.GetDataNodeInfoReq;
import org.apache.iotdb.confignode.consensus.request.read.GetDataPartitionReq;
import org.apache.iotdb.confignode.consensus.request.read.GetOrCreateDataPartitionReq;
import org.apache.iotdb.confignode.consensus.request.read.GetOrCreateSchemaPartitionReq;
import org.apache.iotdb.confignode.consensus.request.read.GetSchemaPartitionReq;
import org.apache.iotdb.confignode.consensus.request.read.GetStorageGroupReq;
import org.apache.iotdb.confignode.consensus.request.write.ApplyConfigNodeReq;
import org.apache.iotdb.confignode.consensus.request.write.CreateDataPartitionReq;
import org.apache.iotdb.confignode.consensus.request.write.CreateRegionsReq;
import org.apache.iotdb.confignode.consensus.request.write.CreateSchemaPartitionReq;
import org.apache.iotdb.confignode.consensus.request.write.DeleteProcedureReq;
import org.apache.iotdb.confignode.consensus.request.write.DeleteRegionsReq;
import org.apache.iotdb.confignode.consensus.request.write.DeleteStorageGroupReq;
import org.apache.iotdb.confignode.consensus.request.write.RegisterDataNodeReq;
import org.apache.iotdb.confignode.consensus.request.write.SetDataReplicationFactorReq;
import org.apache.iotdb.confignode.consensus.request.write.SetSchemaReplicationFactorReq;
import org.apache.iotdb.confignode.consensus.request.write.SetStorageGroupReq;
import org.apache.iotdb.confignode.consensus.request.write.SetTTLReq;
import org.apache.iotdb.confignode.consensus.request.write.SetTimePartitionIntervalReq;
import org.apache.iotdb.confignode.consensus.request.write.UpdateProcedureReq;
import org.apache.iotdb.confignode.procedure.Procedure;
import org.apache.iotdb.confignode.procedure.impl.DeleteStorageGroupProcedure;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ConfigRequestSerDeTest {

  private final ByteBuffer buffer = ByteBuffer.allocate(10240);

  @After
  public void cleanBuffer() {
    buffer.clear();
  }

  @Test
  public void RegisterDataNodeReqTest() throws IOException {
    TDataNodeLocation dataNodeLocation = new TDataNodeLocation();
    dataNodeLocation.setDataNodeId(1);
    dataNodeLocation.setExternalEndPoint(new TEndPoint("0.0.0.0", 6667));
    dataNodeLocation.setInternalEndPoint(new TEndPoint("0.0.0.0", 9003));
    dataNodeLocation.setDataBlockManagerEndPoint(new TEndPoint("0.0.0.0", 8777));
    dataNodeLocation.setDataRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 40010));
    dataNodeLocation.setSchemaRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 50010));

    TDataNodeInfo dataNodeInfo = new TDataNodeInfo();
    dataNodeInfo.setLocation(dataNodeLocation);
    dataNodeInfo.setCpuCoreNum(16);
    dataNodeInfo.setMaxMemory(34359738368L);

    RegisterDataNodeReq req0 = new RegisterDataNodeReq(dataNodeInfo);
    req0.serialize(buffer);
    buffer.flip();
    RegisterDataNodeReq req1 = (RegisterDataNodeReq) ConfigRequest.Factory.create(buffer);
    Assert.assertEquals(req0, req1);
  }

  @Test
  public void QueryDataNodeInfoReqTest() throws IOException {
    GetDataNodeInfoReq req0 = new GetDataNodeInfoReq(-1);
    req0.serialize(buffer);
    buffer.flip();
    GetDataNodeInfoReq req1 = (GetDataNodeInfoReq) ConfigRequest.Factory.create(buffer);
    Assert.assertEquals(req0, req1);
  }

  @Test
  public void SetStorageGroupReqTest() throws IOException {
    SetStorageGroupReq req0 =
        new SetStorageGroupReq(
            new TStorageGroupSchema()
                .setName("sg")
                .setTTL(Long.MAX_VALUE)
                .setSchemaReplicationFactor(3)
                .setDataReplicationFactor(3)
                .setTimePartitionInterval(604800)
                .setSchemaRegionGroupIds(new ArrayList<>())
                .setDataRegionGroupIds(new ArrayList<>()));
    req0.serialize(buffer);
    buffer.flip();
    SetStorageGroupReq req1 = (SetStorageGroupReq) ConfigRequest.Factory.create(buffer);
    Assert.assertEquals(req0, req1);
  }

  @Test
  public void DeleteStorageGroupReqTest() throws IOException {
    TStorageGroupSchema storageGroupSchema = new TStorageGroupSchema();
    storageGroupSchema.setName("root.sg");
    storageGroupSchema.addToSchemaRegionGroupIds(
        new TConsensusGroupId(TConsensusGroupType.DataRegion, 1));
    storageGroupSchema.addToSchemaRegionGroupIds(
        new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 2));
    DeleteStorageGroupReq req0 = new DeleteStorageGroupReq(storageGroupSchema);
    req0.serialize(buffer);
    buffer.flip();
    DeleteStorageGroupReq req1 = (DeleteStorageGroupReq) ConfigRequest.Factory.create(buffer);
    Assert.assertEquals(req0, req1);
  }

  @Test
  public void SetTTLReqTest() throws IOException {
    SetTTLReq req0 = new SetTTLReq("root.sg0", Long.MAX_VALUE);
    req0.serialize(buffer);
    buffer.flip();
    SetTTLReq req1 = (SetTTLReq) ConfigRequest.Factory.create(buffer);
    Assert.assertEquals(req0, req1);
  }

  @Test
  public void SetSchemaReplicationFactorReqTest() throws IOException {
    SetSchemaReplicationFactorReq req0 = new SetSchemaReplicationFactorReq("root.sg0", 3);
    req0.serialize(buffer);
    buffer.flip();
    SetSchemaReplicationFactorReq req1 =
        (SetSchemaReplicationFactorReq) ConfigRequest.Factory.create(buffer);
    Assert.assertEquals(req0, req1);
  }

  @Test
  public void SetDataReplicationFactorReqTest() throws IOException {
    SetDataReplicationFactorReq req0 = new SetDataReplicationFactorReq("root.sg0", 3);
    req0.serialize(buffer);
    buffer.flip();
    SetDataReplicationFactorReq req1 =
        (SetDataReplicationFactorReq) ConfigRequest.Factory.create(buffer);
    Assert.assertEquals(req0, req1);
  }

  @Test
  public void SetTimePartitionIntervalReqTest() throws IOException {
    SetTimePartitionIntervalReq req0 = new SetTimePartitionIntervalReq("root.sg0", 6048000L);
    req0.serialize(buffer);
    buffer.flip();
    SetTimePartitionIntervalReq req1 =
        (SetTimePartitionIntervalReq) ConfigRequest.Factory.create(buffer);
    Assert.assertEquals(req0, req1);
  }

  @Test
  public void CountStorageGroupReqTest() throws IOException {
    CountStorageGroupReq req0 = new CountStorageGroupReq(Arrays.asList("root", "sg"));
    req0.serialize(buffer);
    buffer.flip();
    CountStorageGroupReq req1 = (CountStorageGroupReq) ConfigRequest.Factory.create(buffer);
    Assert.assertEquals(req0, req1);
  }

  @Test
  public void GetStorageGroupReqTest() throws IOException {
    GetStorageGroupReq req0 = new GetStorageGroupReq(Arrays.asList("root", "sg"));
    req0.serialize(buffer);
    buffer.flip();
    CountStorageGroupReq req1 = (CountStorageGroupReq) ConfigRequest.Factory.create(buffer);
    Assert.assertEquals(req0, req1);
  }

  @Test
  public void DeleteStorageGroupPlanTest() {
    // TODO: Add serialize and deserialize test
  }

  @Test
  public void CreateRegionsPlanTest() throws IOException {
    TDataNodeLocation dataNodeLocation = new TDataNodeLocation();
    dataNodeLocation.setDataNodeId(0);
    dataNodeLocation.setExternalEndPoint(new TEndPoint("0.0.0.0", 6667));
    dataNodeLocation.setInternalEndPoint(new TEndPoint("0.0.0.0", 9003));
    dataNodeLocation.setDataBlockManagerEndPoint(new TEndPoint("0.0.0.0", 8777));
    dataNodeLocation.setDataRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 40010));
    dataNodeLocation.setSchemaRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 50010));

    CreateRegionsReq req0 = new CreateRegionsReq();
    TRegionReplicaSet dataRegionSet = new TRegionReplicaSet();
    dataRegionSet.setRegionId(new TConsensusGroupId(TConsensusGroupType.DataRegion, 0));
    dataRegionSet.setDataNodeLocations(Collections.singletonList(dataNodeLocation));
    req0.addRegion("root.sg0", dataRegionSet);

    TRegionReplicaSet schemaRegionSet = new TRegionReplicaSet();
    schemaRegionSet.setRegionId(new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 1));
    schemaRegionSet.setDataNodeLocations(Collections.singletonList(dataNodeLocation));
    req0.addRegion("root.sg1", schemaRegionSet);

    req0.serialize(buffer);
    buffer.flip();
    CreateRegionsReq req1 = (CreateRegionsReq) ConfigRequest.Factory.create(buffer);
    Assert.assertEquals(req0, req1);
  }

  @Test
  public void DeleteRegionsPlanTest() throws IOException {
    DeleteRegionsReq req0 = new DeleteRegionsReq();
    req0.addConsensusGroupId(new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 0));
    req0.addConsensusGroupId(new TConsensusGroupId(TConsensusGroupType.DataRegion, 1));

    req0.serialize(buffer);
    buffer.flip();
    DeleteRegionsReq req1 = (DeleteRegionsReq) ConfigRequest.Factory.create(buffer);
    Assert.assertEquals(req0, req1);
  }

  @Test
  public void CreateSchemaPartitionPlanTest() throws IOException {
    TDataNodeLocation dataNodeLocation = new TDataNodeLocation();
    dataNodeLocation.setDataNodeId(0);
    dataNodeLocation.setExternalEndPoint(new TEndPoint("0.0.0.0", 6667));
    dataNodeLocation.setInternalEndPoint(new TEndPoint("0.0.0.0", 9003));
    dataNodeLocation.setDataBlockManagerEndPoint(new TEndPoint("0.0.0.0", 8777));
    dataNodeLocation.setDataRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 40010));
    dataNodeLocation.setSchemaRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 50010));

    String storageGroup = "root.sg0";
    TSeriesPartitionSlot seriesPartitionSlot = new TSeriesPartitionSlot(10);
    TRegionReplicaSet regionReplicaSet = new TRegionReplicaSet();
    regionReplicaSet.setRegionId(new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 0));
    regionReplicaSet.setDataNodeLocations(Collections.singletonList(dataNodeLocation));

    Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> assignedSchemaPartition =
        new HashMap<>();
    assignedSchemaPartition.put(storageGroup, new HashMap<>());
    assignedSchemaPartition.get(storageGroup).put(seriesPartitionSlot, regionReplicaSet);

    CreateSchemaPartitionReq req0 = new CreateSchemaPartitionReq();
    req0.setAssignedSchemaPartition(assignedSchemaPartition);
    req0.serialize(buffer);
    buffer.flip();
    CreateSchemaPartitionReq req1 = (CreateSchemaPartitionReq) ConfigRequest.Factory.create(buffer);
    Assert.assertEquals(req0, req1);
  }

  @Test
  public void GetSchemaPartitionPlanTest() throws IOException {
    String storageGroup = "root.sg0";
    TSeriesPartitionSlot seriesPartitionSlot = new TSeriesPartitionSlot(10);

    Map<String, List<TSeriesPartitionSlot>> partitionSlotsMap = new HashMap<>();
    partitionSlotsMap.put(storageGroup, Collections.singletonList(seriesPartitionSlot));

    GetSchemaPartitionReq req0 = new GetSchemaPartitionReq();
    req0.setPartitionSlotsMap(partitionSlotsMap);
    req0.serialize(buffer);
    buffer.flip();
    GetSchemaPartitionReq req1 = (GetSchemaPartitionReq) ConfigRequest.Factory.create(buffer);
    Assert.assertEquals(req0, req1);
  }

  @Test
  public void GetOrCreateSchemaPartitionPlanTest() throws IOException {
    String storageGroup = "root.sg0";
    TSeriesPartitionSlot seriesPartitionSlot = new TSeriesPartitionSlot(10);

    Map<String, List<TSeriesPartitionSlot>> partitionSlotsMap = new HashMap<>();
    partitionSlotsMap.put(storageGroup, Collections.singletonList(seriesPartitionSlot));

    GetOrCreateSchemaPartitionReq req0 = new GetOrCreateSchemaPartitionReq();
    req0.setPartitionSlotsMap(partitionSlotsMap);
    req0.serialize(buffer);
    buffer.flip();
    GetOrCreateSchemaPartitionReq req1 =
        (GetOrCreateSchemaPartitionReq) ConfigRequest.Factory.create(buffer);
    Assert.assertEquals(req0, req1);
  }

  @Test
  public void CreateDataPartitionPlanTest() throws IOException {
    TDataNodeLocation dataNodeLocation = new TDataNodeLocation();
    dataNodeLocation.setDataNodeId(0);
    dataNodeLocation.setExternalEndPoint(new TEndPoint("0.0.0.0", 6667));
    dataNodeLocation.setInternalEndPoint(new TEndPoint("0.0.0.0", 9003));
    dataNodeLocation.setDataBlockManagerEndPoint(new TEndPoint("0.0.0.0", 8777));
    dataNodeLocation.setDataRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 40010));
    dataNodeLocation.setSchemaRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 50010));

    String storageGroup = "root.sg0";
    TSeriesPartitionSlot seriesPartitionSlot = new TSeriesPartitionSlot(10);
    TTimePartitionSlot timePartitionSlot = new TTimePartitionSlot(100);
    TRegionReplicaSet regionReplicaSet = new TRegionReplicaSet();
    regionReplicaSet.setRegionId(new TConsensusGroupId(TConsensusGroupType.DataRegion, 0));
    regionReplicaSet.setDataNodeLocations(Collections.singletonList(dataNodeLocation));

    Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
        assignedDataPartition = new HashMap<>();
    assignedDataPartition.put(storageGroup, new HashMap<>());
    assignedDataPartition.get(storageGroup).put(seriesPartitionSlot, new HashMap<>());
    assignedDataPartition
        .get(storageGroup)
        .get(seriesPartitionSlot)
        .put(timePartitionSlot, new ArrayList<>());
    assignedDataPartition
        .get(storageGroup)
        .get(seriesPartitionSlot)
        .get(timePartitionSlot)
        .add(regionReplicaSet);

    CreateDataPartitionReq req0 = new CreateDataPartitionReq();
    req0.setAssignedDataPartition(assignedDataPartition);
    req0.serialize(buffer);
    buffer.flip();
    CreateDataPartitionReq req1 = (CreateDataPartitionReq) ConfigRequest.Factory.create(buffer);
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

    GetDataPartitionReq req0 = new GetDataPartitionReq();
    req0.setPartitionSlotsMap(partitionSlotsMap);
    req0.serialize(buffer);
    buffer.flip();
    GetDataPartitionReq req1 = (GetDataPartitionReq) ConfigRequest.Factory.create(buffer);
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

    GetOrCreateDataPartitionReq req0 = new GetOrCreateDataPartitionReq();
    req0.setPartitionSlotsMap(partitionSlotsMap);
    req0.serialize(buffer);
    buffer.flip();
    GetOrCreateDataPartitionReq req1 =
        (GetOrCreateDataPartitionReq) ConfigRequest.Factory.create(buffer);
    Assert.assertEquals(req0, req1);
  }

  @Test
  public void AuthorReqTest() throws IOException, AuthException {

    AuthorReq req0;
    AuthorReq req1;
    Set<Integer> permissions = new HashSet<>();
    permissions.add(PrivilegeType.GRANT_USER_PRIVILEGE.ordinal());
    permissions.add(PrivilegeType.REVOKE_USER_ROLE.ordinal());

    // create user
    req0 =
        new AuthorReq(
            ConfigRequestType.CreateUser, "thulab", "", "passwd", "", new HashSet<>(), "");
    req0.serialize(buffer);
    buffer.flip();
    req1 = (AuthorReq) ConfigRequest.Factory.create(buffer);
    Assert.assertEquals(req0, req1);
    cleanBuffer();

    // create role
    req0 = new AuthorReq(ConfigRequestType.CreateRole, "", "admin", "", "", new HashSet<>(), "");
    req0.serialize(buffer);
    buffer.flip();
    req1 = (AuthorReq) ConfigRequest.Factory.create(buffer);
    Assert.assertEquals(req0, req1);
    cleanBuffer();

    // alter user
    req0 =
        new AuthorReq(
            ConfigRequestType.UpdateUser, "tempuser", "", "", "newpwd", new HashSet<>(), "");
    req0.serialize(buffer);
    buffer.flip();
    req1 = (AuthorReq) ConfigRequest.Factory.create(buffer);
    Assert.assertEquals(req0, req1);
    cleanBuffer();

    // grant user
    req0 =
        new AuthorReq(ConfigRequestType.GrantUser, "tempuser", "", "", "", permissions, "root.ln");
    req0.serialize(buffer);
    buffer.flip();
    req1 = (AuthorReq) ConfigRequest.Factory.create(buffer);
    Assert.assertEquals(req0, req1);
    cleanBuffer();

    // grant role
    req0 =
        new AuthorReq(
            ConfigRequestType.GrantRoleToUser,
            "tempuser",
            "temprole",
            "",
            "",
            permissions,
            "root.ln");
    req0.serialize(buffer);
    buffer.flip();
    req1 = (AuthorReq) ConfigRequest.Factory.create(buffer);
    Assert.assertEquals(req0, req1);
    cleanBuffer();

    // grant role to user
    req0 = new AuthorReq(ConfigRequestType.GrantRole, "", "temprole", "", "", new HashSet<>(), "");
    req0.serialize(buffer);
    buffer.flip();
    req1 = (AuthorReq) ConfigRequest.Factory.create(buffer);
    Assert.assertEquals(req0, req1);
    cleanBuffer();

    // revoke user
    req0 =
        new AuthorReq(ConfigRequestType.RevokeUser, "tempuser", "", "", "", permissions, "root.ln");
    req0.serialize(buffer);
    buffer.flip();
    req1 = (AuthorReq) ConfigRequest.Factory.create(buffer);
    Assert.assertEquals(req0, req1);
    cleanBuffer();

    // revoke role
    req0 =
        new AuthorReq(ConfigRequestType.RevokeRole, "", "temprole", "", "", permissions, "root.ln");
    req0.serialize(buffer);
    buffer.flip();
    req1 = (AuthorReq) ConfigRequest.Factory.create(buffer);
    Assert.assertEquals(req0, req1);
    cleanBuffer();

    // revoke role from user
    req0 =
        new AuthorReq(
            ConfigRequestType.RevokeRoleFromUser,
            "tempuser",
            "temprole",
            "",
            "",
            new HashSet<>(),
            "");
    req0.serialize(buffer);
    buffer.flip();
    req1 = (AuthorReq) ConfigRequest.Factory.create(buffer);
    Assert.assertEquals(req0, req1);
    cleanBuffer();

    // drop user
    req0 = new AuthorReq(ConfigRequestType.DropUser, "xiaoming", "", "", "", new HashSet<>(), "");
    req0.serialize(buffer);
    buffer.flip();
    req1 = (AuthorReq) ConfigRequest.Factory.create(buffer);
    Assert.assertEquals(req0, req1);
    cleanBuffer();

    // drop role
    req0 = new AuthorReq(ConfigRequestType.DropRole, "", "admin", "", "", new HashSet<>(), "");
    req0.serialize(buffer);
    buffer.flip();
    req1 = (AuthorReq) ConfigRequest.Factory.create(buffer);
    Assert.assertEquals(req0, req1);
    cleanBuffer();

    // list user
    req0 = new AuthorReq(ConfigRequestType.ListUser, "", "", "", "", new HashSet<>(), "");
    req0.serialize(buffer);
    buffer.flip();
    req1 = (AuthorReq) ConfigRequest.Factory.create(buffer);
    Assert.assertEquals(req0, req1);
    cleanBuffer();

    // list role
    req0 = new AuthorReq(ConfigRequestType.ListRole, "", "", "", "", new HashSet<>(), "");
    req0.serialize(buffer);
    buffer.flip();
    req1 = (AuthorReq) ConfigRequest.Factory.create(buffer);
    Assert.assertEquals(req0, req1);
    cleanBuffer();

    // list privileges user
    req0 = new AuthorReq(ConfigRequestType.ListUserPrivilege, "", "", "", "", new HashSet<>(), "");
    req0.serialize(buffer);
    buffer.flip();
    req1 = (AuthorReq) ConfigRequest.Factory.create(buffer);
    Assert.assertEquals(req0, req1);
    cleanBuffer();

    // list privileges role
    req0 = new AuthorReq(ConfigRequestType.ListRolePrivilege, "", "", "", "", new HashSet<>(), "");
    req0.serialize(buffer);
    buffer.flip();
    req1 = (AuthorReq) ConfigRequest.Factory.create(buffer);
    Assert.assertEquals(req0, req1);
    cleanBuffer();

    // list user privileges
    req0 = new AuthorReq(ConfigRequestType.ListUserPrivilege, "", "", "", "", new HashSet<>(), "");
    req0.serialize(buffer);
    buffer.flip();
    req1 = (AuthorReq) ConfigRequest.Factory.create(buffer);
    Assert.assertEquals(req0, req1);
    cleanBuffer();

    // list role privileges
    req0 = new AuthorReq(ConfigRequestType.ListRolePrivilege, "", "", "", "", new HashSet<>(), "");
    req0.serialize(buffer);
    buffer.flip();
    req1 = (AuthorReq) ConfigRequest.Factory.create(buffer);
    Assert.assertEquals(req0, req1);
    cleanBuffer();

    // list all role of user
    req0 = new AuthorReq(ConfigRequestType.ListUserRoles, "", "", "", "", new HashSet<>(), "");
    req0.serialize(buffer);
    buffer.flip();
    req1 = (AuthorReq) ConfigRequest.Factory.create(buffer);
    Assert.assertEquals(req0, req1);
    cleanBuffer();

    // list all user of role
    req0 = new AuthorReq(ConfigRequestType.ListRoleUsers, "", "", "", "", new HashSet<>(), "");
    req0.serialize(buffer);
    buffer.flip();
    req1 = (AuthorReq) ConfigRequest.Factory.create(buffer);
    Assert.assertEquals(req0, req1);
    cleanBuffer();
  }

  @Test
  public void registerConfigNodeReqTest() throws IOException {
    ApplyConfigNodeReq req0 =
        new ApplyConfigNodeReq(
            new TConfigNodeLocation(
                new TEndPoint("0.0.0.0", 22277), new TEndPoint("0.0.0.0", 22278)));
    req0.serialize(buffer);
    buffer.flip();
    ApplyConfigNodeReq req1 = (ApplyConfigNodeReq) ConfigRequest.Factory.create(buffer);
    Assert.assertEquals(req0, req1);
  }

  @Test
  public void updateProcedureTest() throws IOException {
    DeleteStorageGroupProcedure procedure = new DeleteStorageGroupProcedure();
    TStorageGroupSchema storageGroupSchema = new TStorageGroupSchema();
    storageGroupSchema.setName("root.sg");
    storageGroupSchema.setSchemaRegionGroupIds(
        Collections.singletonList(new TConsensusGroupId(TConsensusGroupType.DataRegion, 0)));
    storageGroupSchema.setDataRegionGroupIds(
        Collections.singletonList(new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 1)));
    procedure.setDeleteSgSchema(storageGroupSchema);
    UpdateProcedureReq updateProcedureReq = new UpdateProcedureReq();
    updateProcedureReq.setProcedure(procedure);
    updateProcedureReq.serialize(buffer);
    buffer.flip();
    UpdateProcedureReq reqNew = (UpdateProcedureReq) ConfigRequest.Factory.create(buffer);
    Procedure proc = reqNew.getProcedure();
    Assert.assertEquals(proc, procedure);
  }

  @Test
  public void UpdateProcedureReqTest() throws IOException {
    UpdateProcedureReq req0 = new UpdateProcedureReq();
    DeleteStorageGroupProcedure deleteStorageGroupProcedure = new DeleteStorageGroupProcedure();
    TStorageGroupSchema tStorageGroupSchema = new TStorageGroupSchema();
    tStorageGroupSchema.setName("root.sg");
    List<TConsensusGroupId> dataRegionIds = new ArrayList<>();
    List<TConsensusGroupId> schemaRegionIds = new ArrayList<>();
    TConsensusGroupId dataRegionId = new TConsensusGroupId(TConsensusGroupType.DataRegion, 1);
    dataRegionIds.add(dataRegionId);
    TConsensusGroupId schemaRegionId = new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 2);
    schemaRegionIds.add(schemaRegionId);
    tStorageGroupSchema.setDataRegionGroupIds(dataRegionIds);
    tStorageGroupSchema.setSchemaRegionGroupIds(schemaRegionIds);
    deleteStorageGroupProcedure.setDeleteSgSchema(tStorageGroupSchema);
    req0.setProcedure(deleteStorageGroupProcedure);
    req0.serialize(buffer);
    buffer.flip();
    UpdateProcedureReq req1 = (UpdateProcedureReq) ConfigRequest.Factory.create(buffer);
    Assert.assertEquals(req0, req1);
  }

  @Test
  public void DeleteProcedureReqTest() throws IOException {
    DeleteProcedureReq req0 = new DeleteProcedureReq();
    req0.setProcId(1L);
    req0.serialize(buffer);
    buffer.flip();
    DeleteProcedureReq req1 = (DeleteProcedureReq) ConfigRequest.Factory.create(buffer);
    Assert.assertEquals(req0, req1);
  }
}
