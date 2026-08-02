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

package org.apache.iotdb.confignode.procedure.impl;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.consensus.request.write.region.CreateRegionGroupsPlan;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.procedure.Procedure;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.impl.region.CreateRegionGroupsProcedure;
import org.apache.iotdb.confignode.procedure.impl.region.RemoveRegionGroupProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.DeleteDatabaseProcedure;
import org.apache.iotdb.confignode.procedure.scheduler.ProcedureScheduler;
import org.apache.iotdb.confignode.procedure.state.CreateRegionGroupsState;
import org.apache.iotdb.confignode.procedure.state.ProcedureLockState;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.PublicBAOS;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.common.rpc.thrift.TConsensusGroupType.DataRegion;
import static org.apache.iotdb.common.rpc.thrift.TConsensusGroupType.SchemaRegion;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class CreateRegionGroupsProcedureTest {

  private static class TestCreateRegionGroupsProcedure extends CreateRegionGroupsProcedure {

    private final List<Procedure<ConfigNodeProcedureEnv>> childProcedures = new ArrayList<>();

    private TestCreateRegionGroupsProcedure() {
      super();
    }

    private TestCreateRegionGroupsProcedure(
        final TConsensusGroupType consensusGroupType,
        final CreateRegionGroupsPlan createRegionGroupsPlan,
        final CreateRegionGroupsPlan persistPlan,
        final Map<TConsensusGroupId, TRegionReplicaSet> failedRegionReplicaSets) {
      super(consensusGroupType, createRegionGroupsPlan, persistPlan, failedRegionReplicaSets);
    }

    private void executeShunt(final ConfigNodeProcedureEnv env) {
      executeFromState(env, CreateRegionGroupsState.SHUNT_REGION_REPLICAS);
    }

    private void executeCreate(final ConfigNodeProcedureEnv env) {
      executeFromState(env, CreateRegionGroupsState.CREATE_REGION_GROUPS);
    }

    private void executePostPersist(final ConfigNodeProcedureEnv env) {
      executeFromState(env, CreateRegionGroupsState.REBALANCE_DATA_PARTITION_POLICY);
    }

    private void executeFinish(final ConfigNodeProcedureEnv env) {
      executeFromState(env, CreateRegionGroupsState.CREATE_REGION_GROUPS_FINISH);
    }

    private ProcedureLockState acquireDatabaseLock(final ConfigNodeProcedureEnv env) {
      return acquireLock(env);
    }

    private void releaseDatabaseLock(final ConfigNodeProcedureEnv env) {
      releaseLock(env);
    }

    @Override
    protected void addChildProcedure(final Procedure<ConfigNodeProcedureEnv> childProcedure) {
      super.addChildProcedure(childProcedure);
      childProcedures.add(childProcedure);
    }

    private List<Procedure<ConfigNodeProcedureEnv>> getChildProcedures() {
      return childProcedures;
    }
  }

  private static class TestDeleteDatabaseProcedure extends DeleteDatabaseProcedure {

    private TestDeleteDatabaseProcedure(final TDatabaseSchema databaseSchema) {
      super(databaseSchema, false);
    }

    private ProcedureLockState acquireDatabaseLock(final ConfigNodeProcedureEnv env) {
      return acquireLock(env);
    }

    private void releaseDatabaseLock(final ConfigNodeProcedureEnv env) {
      releaseLock(env);
    }
  }

  @Test
  public void serializeDeserializeTest() {
    TDataNodeLocation dataNodeLocation0 = new TDataNodeLocation();
    dataNodeLocation0.setDataNodeId(5);
    dataNodeLocation0.setClientRpcEndPoint(new TEndPoint("0.0.0.0", 6667));
    dataNodeLocation0.setInternalEndPoint(new TEndPoint("0.0.0.0", 10730));
    dataNodeLocation0.setMPPDataExchangeEndPoint(new TEndPoint("0.0.0.0", 10740));
    dataNodeLocation0.setDataRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 10760));
    dataNodeLocation0.setSchemaRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 10750));

    TDataNodeLocation dataNodeLocation1 = new TDataNodeLocation();
    dataNodeLocation1.setDataNodeId(6);
    dataNodeLocation1.setClientRpcEndPoint(new TEndPoint("0.0.0.1", 6667));
    dataNodeLocation1.setInternalEndPoint(new TEndPoint("0.0.0.1", 10730));
    dataNodeLocation1.setMPPDataExchangeEndPoint(new TEndPoint("0.0.0.1", 10740));
    dataNodeLocation1.setDataRegionConsensusEndPoint(new TEndPoint("0.0.0.1", 10760));
    dataNodeLocation1.setSchemaRegionConsensusEndPoint(new TEndPoint("0.0.0.1", 10750));

    TConsensusGroupId schemaRegionGroupId = new TConsensusGroupId(SchemaRegion, 1);
    TConsensusGroupId dataRegionGroupId = new TConsensusGroupId(DataRegion, 0);

    TRegionReplicaSet schemaRegionSet =
        new TRegionReplicaSet(schemaRegionGroupId, Collections.singletonList(dataNodeLocation0));
    TRegionReplicaSet dataRegionSet =
        new TRegionReplicaSet(dataRegionGroupId, Collections.singletonList(dataNodeLocation1));

    // to test the equals method of Map<TConsensusGroupId, TRegionReplicaSet>
    Map<TConsensusGroupId, TRegionReplicaSet> failedRegions0 =
        new HashMap<TConsensusGroupId, TRegionReplicaSet>() {
          {
            put(dataRegionGroupId, dataRegionSet);
            put(schemaRegionGroupId, schemaRegionSet);
          }
        };
    Map<TConsensusGroupId, TRegionReplicaSet> failedRegions1 =
        new HashMap<TConsensusGroupId, TRegionReplicaSet>() {
          {
            put(schemaRegionGroupId, schemaRegionSet);
            put(dataRegionGroupId, dataRegionSet);
          }
        };
    assertEquals(failedRegions0, failedRegions1);

    CreateRegionGroupsPlan createRegionGroupsPlan = new CreateRegionGroupsPlan();
    createRegionGroupsPlan.addRegionGroup("root.sg0", dataRegionSet);
    createRegionGroupsPlan.addRegionGroup("root.sg1", schemaRegionSet);

    CreateRegionGroupsPlan persistPlan = new CreateRegionGroupsPlan();
    persistPlan.addRegionGroup("root.sg0", dataRegionSet);
    persistPlan.addRegionGroup("root.sg1", schemaRegionSet);

    CreateRegionGroupsProcedure procedure0 =
        new CreateRegionGroupsProcedure(
            TConsensusGroupType.DataRegion, createRegionGroupsPlan, persistPlan, failedRegions0);
    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);

    try {
      procedure0.serialize(outputStream);
      CreateRegionGroupsProcedure procedure1 = new CreateRegionGroupsProcedure();
      ByteBuffer buffer =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
      Assert.assertEquals(ProcedureType.CREATE_REGION_GROUPS.getTypeCode(), buffer.getShort());
      procedure1.deserialize(buffer);
      Assert.assertFalse(buffer.hasRemaining());
      assertEquals(procedure0, procedure1);
      assertEquals(procedure0.hashCode(), procedure1.hashCode());

      CreateRegionGroupsProcedure procedure2 =
          (CreateRegionGroupsProcedure)
              ProcedureFactory.getInstance()
                  .create(ByteBuffer.wrap(byteArrayOutputStream.getBuf()));
      assertEquals(procedure0, procedure2);
      assertEquals(procedure0.hashCode(), procedure2.hashCode());
    } catch (IOException e) {
      fail();
    }
  }

  @Test
  public void testPersistFailureRetriesWithoutReleasingOwnership() {
    final TDataNodeLocation createdDataNode =
        new TDataNodeLocation().setDataNodeId(1).setInternalEndPoint(new TEndPoint("0.0.0.1", 1));
    final TDataNodeLocation failedDataNode =
        new TDataNodeLocation().setDataNodeId(2).setInternalEndPoint(new TEndPoint("0.0.0.2", 2));
    final TDataNodeLocation otherFailedDataNode =
        new TDataNodeLocation().setDataNodeId(3).setInternalEndPoint(new TEndPoint("0.0.0.3", 3));
    final TConsensusGroupId regionId = new TConsensusGroupId(DataRegion, 10);
    final TConsensusGroupId otherRegionId = new TConsensusGroupId(DataRegion, 11);
    final TRegionReplicaSet allocatedReplicaSet =
        new TRegionReplicaSet(regionId, List.of(createdDataNode, failedDataNode));
    final TRegionReplicaSet failedReplicaSet =
        new TRegionReplicaSet(regionId, Collections.singletonList(failedDataNode));
    final TRegionReplicaSet otherAllocatedReplicaSet =
        new TRegionReplicaSet(otherRegionId, Collections.singletonList(otherFailedDataNode));

    final CreateRegionGroupsPlan createPlan = new CreateRegionGroupsPlan();
    createPlan.addRegionGroup("root.sg", allocatedReplicaSet);
    createPlan.addRegionGroup("root.sg", otherAllocatedReplicaSet);
    final Map<TConsensusGroupId, TRegionReplicaSet> failedReplicaSets = new HashMap<>();
    failedReplicaSets.put(regionId, failedReplicaSet);
    failedReplicaSets.put(otherRegionId, otherAllocatedReplicaSet);
    final TestCreateRegionGroupsProcedure procedure =
        new TestCreateRegionGroupsProcedure(
            DataRegion, createPlan, new CreateRegionGroupsPlan(), failedReplicaSets);

    final ConfigNodeProcedureEnv env = Mockito.mock(ConfigNodeProcedureEnv.class);
    Mockito.when(env.validateCreateRegionGroups(createPlan))
        .thenReturn(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    Mockito.when(env.persistRegionGroup(Mockito.any()))
        .thenReturn(new TSStatus(TSStatusCode.DATABASE_NOT_EXIST.getStatusCode()));

    procedure.executeShunt(env);

    Assert.assertTrue(procedure.getChildProcedures().isEmpty());
    Assert.assertFalse(procedure.isFailed());
    Mockito.verify(env).persistRegionGroup(Mockito.any());
  }

  @Test
  public void testRegionCreateTaskPersistenceRetriesIdempotently() throws Exception {
    final TDataNodeLocation dataNode0 = new TDataNodeLocation().setDataNodeId(1);
    final TDataNodeLocation dataNode1 = new TDataNodeLocation().setDataNodeId(2);
    final TDataNodeLocation failedDataNode = new TDataNodeLocation().setDataNodeId(3);
    final TConsensusGroupId regionId = new TConsensusGroupId(DataRegion, 10);
    final TRegionReplicaSet allocatedReplicaSet =
        new TRegionReplicaSet(regionId, List.of(dataNode0, dataNode1, failedDataNode));
    final TRegionReplicaSet failedReplicaSet =
        new TRegionReplicaSet(regionId, Collections.singletonList(failedDataNode));
    final CreateRegionGroupsPlan createPlan = new CreateRegionGroupsPlan();
    createPlan.addRegionGroup("root.sg", allocatedReplicaSet);
    final Map<TConsensusGroupId, TRegionReplicaSet> failedReplicaSets = new HashMap<>();
    failedReplicaSets.put(regionId, failedReplicaSet);
    final TestCreateRegionGroupsProcedure procedure =
        new TestCreateRegionGroupsProcedure(
            DataRegion, createPlan, new CreateRegionGroupsPlan(), failedReplicaSets);

    final ConfigNodeProcedureEnv env = Mockito.mock(ConfigNodeProcedureEnv.class);
    final ConfigManager configManager = Mockito.mock(ConfigManager.class);
    final ConsensusManager consensusManager = Mockito.mock(ConsensusManager.class);
    final TSStatus success = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    final TSStatus failure = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
    Mockito.when(env.validateCreateRegionGroups(createPlan)).thenReturn(success);
    Mockito.when(env.persistRegionGroup(Mockito.any())).thenReturn(success);
    Mockito.when(env.getConfigManager()).thenReturn(configManager);
    Mockito.when(configManager.getConsensusManager()).thenReturn(consensusManager);
    Mockito.when(consensusManager.write(Mockito.any())).thenReturn(failure, success);

    procedure.executeShunt(env);
    procedure.executeShunt(env);

    Assert.assertFalse(procedure.isFailed());
    Mockito.verify(env, Mockito.times(2)).persistRegionGroup(Mockito.any());
    Mockito.verify(consensusManager, Mockito.times(2)).write(Mockito.any());
  }

  @Test
  public void testFencedBeforeCreateRpcDoesNotSubmitCleanup() {
    final String database = "root.sg";
    final CreateRegionGroupsPlan createPlan = new CreateRegionGroupsPlan();
    createPlan.addRegionGroup(
        database,
        new TRegionReplicaSet(new TConsensusGroupId(DataRegion, 10), Collections.emptyList()));
    final TestCreateRegionGroupsProcedure procedure =
        new TestCreateRegionGroupsProcedure(
            DataRegion, createPlan, new CreateRegionGroupsPlan(), Collections.emptyMap());

    final ConfigNodeProcedureEnv env = Mockito.mock(ConfigNodeProcedureEnv.class);
    Mockito.when(env.validateCreateRegionGroups(createPlan))
        .thenReturn(new TSStatus(TSStatusCode.DATABASE_CONFIG_ERROR.getStatusCode()));
    procedure.executeCreate(env);

    Mockito.verify(env, Mockito.never())
        .doRegionCreation(Mockito.any(), Mockito.any(CreateRegionGroupsPlan.class));
    Mockito.verify(env, Mockito.never()).getConfigManager();
    Assert.assertTrue(procedure.isFailed());
  }

  @Test
  public void testFencedAfterCreateRpcCleansEveryPlannedRegionReplica() {
    final TRegionReplicaSet replicaSet =
        new TRegionReplicaSet(
            new TConsensusGroupId(DataRegion, 10),
            Collections.singletonList(new TDataNodeLocation().setDataNodeId(1)));
    final CreateRegionGroupsPlan createPlan = new CreateRegionGroupsPlan();
    createPlan.addRegionGroup("root.sg", replicaSet);
    final TestCreateRegionGroupsProcedure procedure =
        new TestCreateRegionGroupsProcedure(
            DataRegion, createPlan, new CreateRegionGroupsPlan(), Collections.emptyMap());

    final ConfigNodeProcedureEnv env = Mockito.mock(ConfigNodeProcedureEnv.class);
    Mockito.when(env.validateCreateRegionGroups(createPlan))
        .thenReturn(new TSStatus(TSStatusCode.DATABASE_NOT_EXIST.getStatusCode()));

    procedure.executeShunt(env);

    Assert.assertEquals(
        Collections.singletonList(new RemoveRegionGroupProcedure(replicaSet)),
        procedure.getChildProcedures());
    Mockito.verify(env, Mockito.never()).persistRegionGroup(Mockito.any());
    Assert.assertFalse(procedure.isFailed());
    procedure.executeFinish(env);
    Assert.assertTrue(procedure.isFailed());
  }

  @Test
  public void testFencedAfterPersistenceDoesNotSubmitCleanup() {
    final CreateRegionGroupsPlan createPlan = new CreateRegionGroupsPlan();
    createPlan.addRegionGroup(
        "root.sg",
        new TRegionReplicaSet(new TConsensusGroupId(DataRegion, 10), Collections.emptyList()));
    final TestCreateRegionGroupsProcedure procedure =
        new TestCreateRegionGroupsProcedure(
            DataRegion, createPlan, createPlan, Collections.emptyMap());

    final ConfigNodeProcedureEnv env = Mockito.mock(ConfigNodeProcedureEnv.class);
    Mockito.when(env.validateCreateRegionGroups(createPlan))
        .thenReturn(new TSStatus(TSStatusCode.DATABASE_NOT_EXIST.getStatusCode()));

    procedure.executePostPersist(env);

    Mockito.verify(env, Mockito.never()).getConfigManager();
    Assert.assertTrue(procedure.isFailed());
  }

  @Test
  public void testCreateAndDeleteDatabaseLifecycleAreMutuallyExclusive() {
    final String database = "root.sg";
    final CreateRegionGroupsPlan createPlan = new CreateRegionGroupsPlan();
    createPlan.addRegionGroup(
        database,
        new TRegionReplicaSet(new TConsensusGroupId(DataRegion, 1), Collections.emptyList()));
    final TestCreateRegionGroupsProcedure createProcedure =
        new TestCreateRegionGroupsProcedure(
            DataRegion, createPlan, new CreateRegionGroupsPlan(), Collections.emptyMap());
    createProcedure.setProcId(1);
    final TestDeleteDatabaseProcedure deleteProcedure =
        new TestDeleteDatabaseProcedure(new TDatabaseSchema(database));
    deleteProcedure.setProcId(2);

    final ConfigNodeProcedureEnv env =
        new ConfigNodeProcedureEnv(
            Mockito.mock(ConfigManager.class), Mockito.mock(ProcedureScheduler.class));
    Assert.assertEquals(ProcedureLockState.LOCK_ACQUIRED, createProcedure.acquireDatabaseLock(env));
    Assert.assertEquals(
        ProcedureLockState.LOCK_EVENT_WAIT, deleteProcedure.acquireDatabaseLock(env));

    createProcedure.releaseDatabaseLock(env);
    Assert.assertEquals(ProcedureLockState.LOCK_ACQUIRED, deleteProcedure.acquireDatabaseLock(env));
    deleteProcedure.releaseDatabaseLock(env);
  }
}
