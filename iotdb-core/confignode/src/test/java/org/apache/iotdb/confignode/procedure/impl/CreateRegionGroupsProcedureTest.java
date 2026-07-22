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
import org.apache.iotdb.confignode.manager.ProcedureManager;
import org.apache.iotdb.confignode.procedure.Procedure;
import org.apache.iotdb.confignode.procedure.ProcedureExecutor;
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
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
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

    private ProcedureLockState acquireDatabaseLock(final ConfigNodeProcedureEnv env) {
      return acquireLock(env);
    }

    private void releaseDatabaseLock(final ConfigNodeProcedureEnv env) {
      releaseLock(env);
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
    createRegionGroupsPlan.setDatabaseGeneration("root.sg0", 11);
    createRegionGroupsPlan.setDatabaseGeneration("root.sg1", 12);
    createRegionGroupsPlan.addRegionGroup("root.sg0", dataRegionSet);
    createRegionGroupsPlan.addRegionGroup("root.sg1", schemaRegionSet);

    CreateRegionGroupsPlan persistPlan = new CreateRegionGroupsPlan();
    persistPlan.setDatabaseGeneration("root.sg0", 11);
    persistPlan.setDatabaseGeneration("root.sg1", 12);
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
  public void testPersistRejectionCleansCreatedRegionReplicas() {
    final TDataNodeLocation createdDataNode =
        new TDataNodeLocation().setDataNodeId(1).setInternalEndPoint(new TEndPoint("0.0.0.1", 1));
    final TDataNodeLocation failedDataNode =
        new TDataNodeLocation().setDataNodeId(2).setInternalEndPoint(new TEndPoint("0.0.0.2", 2));
    final TConsensusGroupId regionId = new TConsensusGroupId(DataRegion, 10);
    final TRegionReplicaSet allocatedReplicaSet =
        new TRegionReplicaSet(regionId, List.of(createdDataNode, failedDataNode));
    final TRegionReplicaSet failedReplicaSet =
        new TRegionReplicaSet(regionId, Collections.singletonList(failedDataNode));

    final CreateRegionGroupsPlan createPlan = new CreateRegionGroupsPlan();
    createPlan.setDatabaseGeneration("root.sg", 1);
    createPlan.addRegionGroup("root.sg", allocatedReplicaSet);
    final Map<TConsensusGroupId, TRegionReplicaSet> failedReplicaSets = new HashMap<>();
    failedReplicaSets.put(regionId, failedReplicaSet);
    final TestCreateRegionGroupsProcedure procedure =
        new TestCreateRegionGroupsProcedure(
            DataRegion, createPlan, new CreateRegionGroupsPlan(), failedReplicaSets);

    final ConfigNodeProcedureEnv env = Mockito.mock(ConfigNodeProcedureEnv.class);
    final ConfigManager configManager = Mockito.mock(ConfigManager.class);
    final ProcedureManager procedureManager = Mockito.mock(ProcedureManager.class);
    @SuppressWarnings("unchecked")
    final ProcedureExecutor<ConfigNodeProcedureEnv> executor =
        Mockito.mock(ProcedureExecutor.class);
    Mockito.when(env.persistRegionGroup(Mockito.any()))
        .thenReturn(new TSStatus(TSStatusCode.DATABASE_NOT_EXIST.getStatusCode()));
    Mockito.when(env.getConfigManager()).thenReturn(configManager);
    Mockito.when(configManager.getProcedureManager()).thenReturn(procedureManager);
    Mockito.when(procedureManager.getExecutor()).thenReturn(executor);

    procedure.executeShunt(env);

    final ArgumentCaptor<Procedure<ConfigNodeProcedureEnv>> cleanupCaptor =
        ArgumentCaptor.forClass(Procedure.class);
    Mockito.verify(executor).submitProcedure(cleanupCaptor.capture());
    Assert.assertEquals(
        new RemoveRegionGroupProcedure(
            new TRegionReplicaSet(regionId, Collections.singletonList(createdDataNode))),
        cleanupCaptor.getValue());
  }

  @Test
  public void testLegacyProcedureCannotBindToRecreatedDatabase() throws IOException {
    final String database = "root.sg";
    final CreateRegionGroupsPlan legacyCreatePlan = new CreateRegionGroupsPlan();
    legacyCreatePlan.addRegionGroup(
        database,
        new TRegionReplicaSet(new TConsensusGroupId(DataRegion, 10), Collections.emptyList()));
    final CreateRegionGroupsProcedure sourceProcedure =
        new CreateRegionGroupsProcedure(DataRegion, legacyCreatePlan);

    final PublicBAOS byteArrayOutputStream = new PublicBAOS();
    sourceProcedure.serialize(new DataOutputStream(byteArrayOutputStream));
    final ByteBuffer legacyProcedureBuffer =
        ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size() - 8);
    Assert.assertEquals(
        ProcedureType.CREATE_REGION_GROUPS.getTypeCode(), legacyProcedureBuffer.getShort());
    final TestCreateRegionGroupsProcedure restoredProcedure = new TestCreateRegionGroupsProcedure();
    restoredProcedure.deserialize(legacyProcedureBuffer);

    final ConfigNodeProcedureEnv env = Mockito.mock(ConfigNodeProcedureEnv.class);
    Mockito.when(env.validateCreateRegionGroups(Mockito.any()))
        .thenReturn(new TSStatus(TSStatusCode.DATABASE_CONFIG_ERROR.getStatusCode()));
    restoredProcedure.executeCreate(env);

    final ArgumentCaptor<CreateRegionGroupsPlan> validationPlanCaptor =
        ArgumentCaptor.forClass(CreateRegionGroupsPlan.class);
    Mockito.verify(env).validateCreateRegionGroups(validationPlanCaptor.capture());
    Assert.assertTrue(validationPlanCaptor.getValue().isDatabaseGenerationSet(database));
    Assert.assertEquals(
        CreateRegionGroupsPlan.DATABASE_GENERATION_NOT_SET,
        validationPlanCaptor.getValue().getDatabaseGeneration(database));
    Mockito.verify(env, Mockito.never())
        .doRegionCreation(Mockito.any(), Mockito.any(CreateRegionGroupsPlan.class));
    Assert.assertTrue(restoredProcedure.isFailed());
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
