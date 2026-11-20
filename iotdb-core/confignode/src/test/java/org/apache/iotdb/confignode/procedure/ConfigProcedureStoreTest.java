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

package org.apache.iotdb.confignode.procedure;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.confignode.consensus.request.write.procedure.UpdateProcedurePlan;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.persistence.ProcedureInfo;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.impl.schema.DeleteDatabaseProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.DeleteTimeSeriesProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.DropTableColumnProcedure;
import org.apache.iotdb.confignode.procedure.state.ProcedureState;
import org.apache.iotdb.confignode.procedure.store.ConfigProcedureStore;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class ConfigProcedureStoreTest {

  @Test
  public void testRejectedTreeDeletionUpdatePreservesRecoveryState() throws Exception {
    assertRejectedUpdatePreservesRecovery(newTreeDeletion(), false);
  }

  @Test
  public void testRejectedColumnDeletionUpdatePreservesRecoveryState() throws Exception {
    assertRejectedUpdatePreservesRecovery(
        new DropTableColumnProcedure("test", "t", "query", "s", false), false);
  }

  @Test
  public void testRejectedDatabaseDeletionUpdatePreservesRecoveryState() throws Exception {
    assertRejectedUpdatePreservesRecovery(
        new DeleteDatabaseProcedure(new TDatabaseSchema("root.async"), false), false);
  }

  @Test
  public void testRejectedLegacyDatabaseDeletionUpdatePreservesRecoveryState() throws Exception {
    assertRejectedUpdatePreservesRecovery(
        new DeleteDatabaseProcedure(new TDatabaseSchema("root.async"), false), true);
  }

  @Test
  public void testRejectedLegacyTreeDeletionUpdatePreservesRecoveryState() throws Exception {
    assertRejectedUpdatePreservesRecovery(newTreeDeletion(), true);
  }

  @Test
  public void testRejectedLegacyColumnDeletionUpdatePreservesRecoveryState() throws Exception {
    assertRejectedUpdatePreservesRecovery(
        new DropTableColumnProcedure("test", "t", "query", "s", false), true);
  }

  private DeleteTimeSeriesProcedure newTreeDeletion() throws Exception {
    final PathPatternTree tree = new PathPatternTree();
    tree.appendPathPattern(new PartialPath("root.async.d.s"));
    tree.constructTree();
    return new DeleteTimeSeriesProcedure("query", tree, false, false);
  }

  private void assertRejectedUpdatePreservesRecovery(
      Procedure<ConfigNodeProcedureEnv> procedure, boolean legacyStore) throws Exception {
    final ConfigManager configManager = mock(ConfigManager.class);
    final ConsensusManager consensus = mock(ConsensusManager.class);
    final ProcedureInfo procedureInfo = spy(new ProcedureInfo(configManager));
    if (legacyStore) {
      // oldLoad also inserts the objects it returns into ProcedureInfo's in-memory map.
      // Reproduce that boundary without relying on or changing the shared legacy WAL directory.
      doAnswer(invocation -> procedureInfo.getProcedures()).when(procedureInfo).oldLoad();
    }
    when(configManager.getConsensusManager()).thenReturn(consensus);
    final AtomicBoolean rejectUpdates = new AtomicBoolean();
    when(consensus.write(any(UpdateProcedurePlan.class)))
        .thenAnswer(
            invocation ->
                rejectUpdates.get()
                    ? new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
                    : procedureInfo.updateProcedure(invocation.getArgument(0)));
    final ConfigProcedureStore store = new ConfigProcedureStore(configManager, procedureInfo);
    procedure.setProcId(1L);
    procedure.setProcRunnable();
    store.update(procedure);

    // A failed terminal write must not change the last successful consensus record, including
    // when the leader applies the original UpdateProcedurePlan without deserializing it.
    procedure.setState(ProcedureState.SUCCESS);
    rejectUpdates.set(true);
    assertRejectedUpdate(store, procedure);
    final Procedure<ConfigNodeProcedureEnv> recovered =
        (legacyStore ? store.load() : store.getProcedures()).get(0);
    assertEquals(ProcedureState.RUNNABLE, recovered.getState());

    // Execution of a recovered object must be isolated as well: another failed terminal write
    // must leave the same resumable state available for the following recovery.
    recovered.setState(ProcedureState.SUCCESS);
    assertRejectedUpdate(store, recovered);
    assertEquals(
        ProcedureState.RUNNABLE,
        (legacyStore ? store.load() : store.getProcedures()).get(0).getState());
    assertEquals(ProcedureState.RUNNABLE, procedureInfo.getProcedures().get(0).getState());

    rejectUpdates.set(false);
    store.update(recovered);
    assertEquals(
        ProcedureState.SUCCESS,
        (legacyStore ? store.load() : store.getProcedures()).get(0).getState());
  }

  private void assertRejectedUpdate(
      ConfigProcedureStore store, Procedure<ConfigNodeProcedureEnv> procedure) throws Exception {
    try {
      store.update(procedure);
      fail("A rejected consensus write must fail persistence");
    } catch (IoTDBException e) {
      assertEquals(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode(), e.getErrorCode());
    }
  }
}
