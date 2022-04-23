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
package org.apache.iotdb.confignode.physical;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.confignode.physical.crud.CreateDataPartitionPlan;
import org.apache.iotdb.confignode.physical.crud.CreateRegionsPlan;
import org.apache.iotdb.confignode.physical.crud.CreateSchemaPartitionPlan;
import org.apache.iotdb.confignode.physical.crud.GetOrCreateDataPartitionPlan;
import org.apache.iotdb.confignode.physical.crud.GetOrCreateSchemaPartitionPlan;
import org.apache.iotdb.confignode.physical.sys.AuthorPlan;
import org.apache.iotdb.confignode.physical.sys.QueryDataNodeInfoPlan;
import org.apache.iotdb.confignode.physical.sys.RegisterDataNodePlan;
import org.apache.iotdb.confignode.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.entity.PrivilegeType;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PhysicalPlanSerDeTest {

  private final ByteBuffer buffer = ByteBuffer.allocate(10240);

  @After
  public void cleanBuffer() {
    buffer.clear();
  }

  @Test
  public void RegisterDataNodePlanTest() throws IOException {
    TDataNodeLocation dataNodeLocation = new TDataNodeLocation();
    dataNodeLocation.setDataNodeId(1);
    dataNodeLocation.setExternalEndPoint(new TEndPoint("0.0.0.0", 6667));
    dataNodeLocation.setInternalEndPoint(new TEndPoint("0.0.0.0", 9003));
    dataNodeLocation.setDataBlockManagerEndPoint(new TEndPoint("0.0.0.0", 8777));
    dataNodeLocation.setConsensusEndPoint(new TEndPoint("0.0.0.0", 7777));
    RegisterDataNodePlan plan0 = new RegisterDataNodePlan(dataNodeLocation);
    plan0.serialize(buffer);
    buffer.flip();
    RegisterDataNodePlan plan1 = (RegisterDataNodePlan) PhysicalPlan.Factory.create(buffer);
    Assert.assertEquals(plan0, plan1);
  }

  @Test
  public void QueryDataNodeInfoPlanTest() throws IOException {
    QueryDataNodeInfoPlan plan0 = new QueryDataNodeInfoPlan(-1);
    plan0.serialize(buffer);
    buffer.flip();
    QueryDataNodeInfoPlan plan1 = (QueryDataNodeInfoPlan) PhysicalPlan.Factory.create(buffer);
    Assert.assertEquals(plan0, plan1);
  }

  @Test
  public void SetStorageGroupPlanTest() throws IOException {
    SetStorageGroupPlan plan0 =
        new SetStorageGroupPlan(
            new TStorageGroupSchema()
                .setName("sg")
                .setTTL(Long.MAX_VALUE)
                .setSchemaReplicationFactor(3)
                .setDataReplicationFactor(3)
                .setTimePartitionInterval(604800)
                .setSchemaRegionGroupIds(new ArrayList<>())
                .setDataRegionGroupIds(new ArrayList<>()));
    plan0.serialize(buffer);
    buffer.flip();
    SetStorageGroupPlan plan1 = (SetStorageGroupPlan) PhysicalPlan.Factory.create(buffer);
    Assert.assertEquals(plan0, plan1);
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
    dataNodeLocation.setConsensusEndPoint(new TEndPoint("0.0.0.0", 40010));

    CreateRegionsPlan plan0 = new CreateRegionsPlan();
    plan0.setStorageGroup("sg");
    TRegionReplicaSet dataRegionSet = new TRegionReplicaSet();
    dataRegionSet.setRegionId(new TConsensusGroupId(TConsensusGroupType.DataRegion, 0));
    dataRegionSet.setDataNodeLocations(Collections.singletonList(dataNodeLocation));
    plan0.addRegion(dataRegionSet);

    TRegionReplicaSet schemaRegionSet = new TRegionReplicaSet();
    schemaRegionSet.setRegionId(new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 1));
    schemaRegionSet.setDataNodeLocations(Collections.singletonList(dataNodeLocation));
    plan0.addRegion(schemaRegionSet);

    plan0.serialize(buffer);
    buffer.flip();
    CreateRegionsPlan plan1 = (CreateRegionsPlan) PhysicalPlan.Factory.create(buffer);
    Assert.assertEquals(plan0, plan1);
  }

  @Test
  public void CreateSchemaPartitionPlanTest() throws IOException {
    TDataNodeLocation dataNodeLocation = new TDataNodeLocation();
    dataNodeLocation.setDataNodeId(0);
    dataNodeLocation.setExternalEndPoint(new TEndPoint("0.0.0.0", 6667));
    dataNodeLocation.setInternalEndPoint(new TEndPoint("0.0.0.0", 9003));
    dataNodeLocation.setDataBlockManagerEndPoint(new TEndPoint("0.0.0.0", 8777));
    dataNodeLocation.setConsensusEndPoint(new TEndPoint("0.0.0.0", 40010));

    String storageGroup = "root.sg0";
    TSeriesPartitionSlot seriesPartitionSlot = new TSeriesPartitionSlot(10);
    TRegionReplicaSet regionReplicaSet = new TRegionReplicaSet();
    regionReplicaSet.setRegionId(new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 0));
    regionReplicaSet.setDataNodeLocations(Collections.singletonList(dataNodeLocation));

    Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> assignedSchemaPartition =
        new HashMap<>();
    assignedSchemaPartition.put(storageGroup, new HashMap<>());
    assignedSchemaPartition.get(storageGroup).put(seriesPartitionSlot, regionReplicaSet);

    CreateSchemaPartitionPlan plan0 = new CreateSchemaPartitionPlan();
    plan0.setAssignedSchemaPartition(assignedSchemaPartition);
    plan0.serialize(buffer);
    buffer.flip();
    CreateSchemaPartitionPlan plan1 =
        (CreateSchemaPartitionPlan) PhysicalPlan.Factory.create(buffer);
    Assert.assertEquals(plan0, plan1);
  }

  @Test
  public void GetOrCreateSchemaPartitionPlanTest() throws IOException {
    String storageGroup = "root.sg0";
    TSeriesPartitionSlot seriesPartitionSlot = new TSeriesPartitionSlot(10);

    Map<String, List<TSeriesPartitionSlot>> partitionSlotsMap = new HashMap<>();
    partitionSlotsMap.put(storageGroup, Collections.singletonList(seriesPartitionSlot));

    GetOrCreateSchemaPartitionPlan plan0 =
        new GetOrCreateSchemaPartitionPlan(PhysicalPlanType.GetOrCreateSchemaPartition);
    plan0.setPartitionSlotsMap(partitionSlotsMap);
    plan0.serialize(buffer);
    buffer.flip();
    GetOrCreateSchemaPartitionPlan plan1 =
        (GetOrCreateSchemaPartitionPlan) PhysicalPlan.Factory.create(buffer);
    Assert.assertEquals(plan0, plan1);
  }

  @Test
  public void CreateDataPartitionPlanTest() throws IOException {
    TDataNodeLocation dataNodeLocation = new TDataNodeLocation();
    dataNodeLocation.setDataNodeId(0);
    dataNodeLocation.setExternalEndPoint(new TEndPoint("0.0.0.0", 6667));
    dataNodeLocation.setInternalEndPoint(new TEndPoint("0.0.0.0", 9003));
    dataNodeLocation.setDataBlockManagerEndPoint(new TEndPoint("0.0.0.0", 8777));
    dataNodeLocation.setConsensusEndPoint(new TEndPoint("0.0.0.0", 40010));

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

    CreateDataPartitionPlan plan0 = new CreateDataPartitionPlan();
    plan0.setAssignedDataPartition(assignedDataPartition);
    plan0.serialize(buffer);
    buffer.flip();
    CreateDataPartitionPlan plan1 = (CreateDataPartitionPlan) PhysicalPlan.Factory.create(buffer);
    Assert.assertEquals(plan0, plan1);
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

    GetOrCreateDataPartitionPlan plan0 =
        new GetOrCreateDataPartitionPlan(PhysicalPlanType.GetDataPartition);
    plan0.setPartitionSlotsMap(partitionSlotsMap);
    plan0.serialize(buffer);
    buffer.flip();
    GetOrCreateDataPartitionPlan plan1 =
        (GetOrCreateDataPartitionPlan) PhysicalPlan.Factory.create(buffer);
    Assert.assertEquals(plan0, plan1);
  }

  @Test
  public void AuthorPlanTest() throws IOException, AuthException {

    AuthorPlan plan0 = null;
    AuthorPlan plan1 = null;
    Set<Integer> permissions = new HashSet<>();
    permissions.add(PrivilegeType.GRANT_USER_PRIVILEGE.ordinal());
    permissions.add(PrivilegeType.REVOKE_USER_ROLE.ordinal());

    // create user
    plan0 =
        new AuthorPlan(
            PhysicalPlanType.CREATE_USER, "thulab", "", "passwd", "", new HashSet<>(), "");
    plan0.serialize(buffer);
    buffer.flip();
    plan1 = (AuthorPlan) PhysicalPlan.Factory.create(buffer);
    Assert.assertEquals(plan0, plan1);
    cleanBuffer();

    // create role
    plan0 = new AuthorPlan(PhysicalPlanType.CREATE_ROLE, "", "admin", "", "", new HashSet<>(), "");
    plan0.serialize(buffer);
    buffer.flip();
    plan1 = (AuthorPlan) PhysicalPlan.Factory.create(buffer);
    Assert.assertEquals(plan0, plan1);
    cleanBuffer();

    // alter user
    plan0 =
        new AuthorPlan(
            PhysicalPlanType.UPDATE_USER, "tempuser", "", "", "newpwd", new HashSet<>(), "");
    plan0.serialize(buffer);
    buffer.flip();
    plan1 = (AuthorPlan) PhysicalPlan.Factory.create(buffer);
    Assert.assertEquals(plan0, plan1);
    cleanBuffer();

    // grant user
    plan0 =
        new AuthorPlan(PhysicalPlanType.GRANT_USER, "tempuser", "", "", "", permissions, "root.ln");
    plan0.serialize(buffer);
    buffer.flip();
    plan1 = (AuthorPlan) PhysicalPlan.Factory.create(buffer);
    Assert.assertEquals(plan0, plan1);
    cleanBuffer();

    // grant role
    plan0 =
        new AuthorPlan(
            PhysicalPlanType.GRANT_ROLE_TO_USER,
            "tempuser",
            "temprole",
            "",
            "",
            permissions,
            "root.ln");
    plan0.serialize(buffer);
    buffer.flip();
    plan1 = (AuthorPlan) PhysicalPlan.Factory.create(buffer);
    Assert.assertEquals(plan0, plan1);
    cleanBuffer();

    // grant role to user
    plan0 =
        new AuthorPlan(PhysicalPlanType.GRANT_ROLE, "", "temprole", "", "", new HashSet<>(), "");
    plan0.serialize(buffer);
    buffer.flip();
    plan1 = (AuthorPlan) PhysicalPlan.Factory.create(buffer);
    Assert.assertEquals(plan0, plan1);
    cleanBuffer();

    // revoke user
    plan0 =
        new AuthorPlan(
            PhysicalPlanType.REVOKE_USER, "tempuser", "", "", "", permissions, "root.ln");
    plan0.serialize(buffer);
    buffer.flip();
    plan1 = (AuthorPlan) PhysicalPlan.Factory.create(buffer);
    Assert.assertEquals(plan0, plan1);
    cleanBuffer();

    // revoke role
    plan0 =
        new AuthorPlan(
            PhysicalPlanType.REVOKE_ROLE, "", "temprole", "", "", permissions, "root.ln");
    plan0.serialize(buffer);
    buffer.flip();
    plan1 = (AuthorPlan) PhysicalPlan.Factory.create(buffer);
    Assert.assertEquals(plan0, plan1);
    cleanBuffer();

    // revoke role from user
    plan0 =
        new AuthorPlan(
            PhysicalPlanType.REVOKE_ROLE_FROM_USER,
            "tempuser",
            "temprole",
            "",
            "",
            new HashSet<>(),
            "");
    plan0.serialize(buffer);
    buffer.flip();
    plan1 = (AuthorPlan) PhysicalPlan.Factory.create(buffer);
    Assert.assertEquals(plan0, plan1);
    cleanBuffer();

    // drop user
    plan0 = new AuthorPlan(PhysicalPlanType.DROP_USER, "xiaoming", "", "", "", new HashSet<>(), "");
    plan0.serialize(buffer);
    buffer.flip();
    plan1 = (AuthorPlan) PhysicalPlan.Factory.create(buffer);
    Assert.assertEquals(plan0, plan1);
    cleanBuffer();

    // drop role
    plan0 = new AuthorPlan(PhysicalPlanType.DROP_ROLE, "", "admin", "", "", new HashSet<>(), "");
    plan0.serialize(buffer);
    buffer.flip();
    plan1 = (AuthorPlan) PhysicalPlan.Factory.create(buffer);
    Assert.assertEquals(plan0, plan1);
    cleanBuffer();

    // list user
    plan0 = new AuthorPlan(PhysicalPlanType.LIST_USER, "", "", "", "", new HashSet<>(), "");
    plan0.serialize(buffer);
    buffer.flip();
    plan1 = (AuthorPlan) PhysicalPlan.Factory.create(buffer);
    Assert.assertEquals(plan0, plan1);
    cleanBuffer();

    // list role
    plan0 = new AuthorPlan(PhysicalPlanType.LIST_ROLE, "", "", "", "", new HashSet<>(), "");
    plan0.serialize(buffer);
    buffer.flip();
    plan1 = (AuthorPlan) PhysicalPlan.Factory.create(buffer);
    Assert.assertEquals(plan0, plan1);
    cleanBuffer();

    // list privileges user
    plan0 =
        new AuthorPlan(PhysicalPlanType.LIST_USER_PRIVILEGE, "", "", "", "", new HashSet<>(), "");
    plan0.serialize(buffer);
    buffer.flip();
    plan1 = (AuthorPlan) PhysicalPlan.Factory.create(buffer);
    Assert.assertEquals(plan0, plan1);
    cleanBuffer();

    // list privileges role
    plan0 =
        new AuthorPlan(PhysicalPlanType.LIST_ROLE_PRIVILEGE, "", "", "", "", new HashSet<>(), "");
    plan0.serialize(buffer);
    buffer.flip();
    plan1 = (AuthorPlan) PhysicalPlan.Factory.create(buffer);
    Assert.assertEquals(plan0, plan1);
    cleanBuffer();

    // list user privileges
    plan0 =
        new AuthorPlan(PhysicalPlanType.LIST_USER_PRIVILEGE, "", "", "", "", new HashSet<>(), "");
    plan0.serialize(buffer);
    buffer.flip();
    plan1 = (AuthorPlan) PhysicalPlan.Factory.create(buffer);
    Assert.assertEquals(plan0, plan1);
    cleanBuffer();

    // list role privileges
    plan0 =
        new AuthorPlan(PhysicalPlanType.LIST_ROLE_PRIVILEGE, "", "", "", "", new HashSet<>(), "");
    plan0.serialize(buffer);
    buffer.flip();
    plan1 = (AuthorPlan) PhysicalPlan.Factory.create(buffer);
    Assert.assertEquals(plan0, plan1);
    cleanBuffer();

    // list all role of user
    plan0 = new AuthorPlan(PhysicalPlanType.LIST_USER_ROLES, "", "", "", "", new HashSet<>(), "");
    plan0.serialize(buffer);
    buffer.flip();
    plan1 = (AuthorPlan) PhysicalPlan.Factory.create(buffer);
    Assert.assertEquals(plan0, plan1);
    cleanBuffer();

    // list all user of role
    plan0 = new AuthorPlan(PhysicalPlanType.LIST_ROLE_USERS, "", "", "", "", new HashSet<>(), "");
    plan0.serialize(buffer);
    buffer.flip();
    plan1 = (AuthorPlan) PhysicalPlan.Factory.create(buffer);
    Assert.assertEquals(plan0, plan1);
    cleanBuffer();
  }
}
