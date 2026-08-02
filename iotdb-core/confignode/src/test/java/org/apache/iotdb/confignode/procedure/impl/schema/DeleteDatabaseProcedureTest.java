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

package org.apache.iotdb.confignode.procedure.impl.schema;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.consensus.request.write.database.PreDeleteDatabasePlan;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.procedure.Procedure;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.impl.region.RemoveRegionGroupProcedure;
import org.apache.iotdb.confignode.procedure.state.schema.DeleteDatabaseState;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.PublicBAOS;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class DeleteDatabaseProcedureTest {

  private static class TestDeleteDatabaseProcedure extends DeleteDatabaseProcedure {
    private final List<Procedure<ConfigNodeProcedureEnv>> childProcedures = new ArrayList<>();

    private TestDeleteDatabaseProcedure(final TDatabaseSchema databaseSchema) {
      super(databaseSchema, false);
    }

    @Override
    protected void addChildProcedure(final Procedure<ConfigNodeProcedureEnv> childProcedure) {
      super.addChildProcedure(childProcedure);
      childProcedures.add(childProcedure);
    }

    private void executeRegionGroupDeletion(final ConfigNodeProcedureEnv env)
        throws InterruptedException {
      executeFromState(env, DeleteDatabaseState.DELETE_DATABASE_SCHEMA);
    }

    private void executeOneStep(final ConfigNodeProcedureEnv env) throws InterruptedException {
      execute(env);
    }

    private DeleteDatabaseState currentState() {
      return getCurrentState();
    }
  }

  @Test
  public void serializeDeserializeTest() {

    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);
    DeleteDatabaseProcedure p1 = new DeleteDatabaseProcedure(new TDatabaseSchema("root.sg"), false);

    try {
      p1.serialize(outputStream);
      ByteBuffer buffer =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());

      DeleteDatabaseProcedure p2 =
          (DeleteDatabaseProcedure) ProcedureFactory.getInstance().create(buffer);
      assertFalse(buffer.hasRemaining());
      assertEquals(p1, p2);

    } catch (Exception e) {
      fail();
    }
  }

  @Test
  public void testRegionGroupsAreDeletedByChildProcedures() throws InterruptedException {
    final TRegionReplicaSet regionReplicaSet =
        new TRegionReplicaSet(
            new TConsensusGroupId(TConsensusGroupType.DataRegion, 1),
            Collections.singletonList(new TDataNodeLocation().setDataNodeId(1)));
    final TestDeleteDatabaseProcedure procedure =
        new TestDeleteDatabaseProcedure(new TDatabaseSchema("root.sg"));
    final ConfigNodeProcedureEnv env = Mockito.mock(ConfigNodeProcedureEnv.class);
    final ConfigManager configManager = Mockito.mock(ConfigManager.class);
    Mockito.when(env.getConfigManager()).thenReturn(configManager);
    Mockito.when(configManager.getLoadManager()).thenReturn(Mockito.mock(LoadManager.class));
    Mockito.when(env.getAllReplicaSets("root.sg"))
        .thenReturn(Collections.singletonList(regionReplicaSet));

    procedure.executeRegionGroupDeletion(env);

    assertEquals(
        Collections.singletonList(new RemoveRegionGroupProcedure(regionReplicaSet)),
        procedure.childProcedures);
  }

  @Test
  public void testTransientConsensusFailuresRetryTheSameState() throws Exception {
    final TestDeleteDatabaseProcedure procedure =
        new TestDeleteDatabaseProcedure(new TDatabaseSchema("root.sg"));
    final ConfigNodeProcedureEnv env = Mockito.mock(ConfigNodeProcedureEnv.class);
    final TSStatus success = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    final TSStatus failure = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
    Mockito.when(env.preDeleteDatabase(PreDeleteDatabasePlan.PreDeleteType.EXECUTE, "root.sg"))
        .thenReturn(failure, success);
    Mockito.when(env.invalidateCache("root.sg")).thenReturn(true);
    Mockito.when(env.batchRemoveRegionCreateTasks("root.sg")).thenReturn(failure, success);

    procedure.executeOneStep(env);
    assertEquals(DeleteDatabaseState.PRE_DELETE_DATABASE, procedure.currentState());
    procedure.executeOneStep(env);
    assertEquals(DeleteDatabaseState.INVALIDATE_CACHE, procedure.currentState());
    procedure.executeOneStep(env);
    assertEquals(DeleteDatabaseState.INVALIDATE_CACHE, procedure.currentState());
    procedure.executeOneStep(env);
    assertEquals(DeleteDatabaseState.DELETE_DATABASE_SCHEMA, procedure.currentState());
  }
}
