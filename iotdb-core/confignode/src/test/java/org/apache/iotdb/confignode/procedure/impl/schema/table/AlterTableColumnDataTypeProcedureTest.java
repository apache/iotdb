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

package org.apache.iotdb.confignode.procedure.impl.schema.table;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.manager.schema.ClusterSchemaManager;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.state.schema.AlterTableColumnDataTypeState;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.TSDataType;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

public class AlterTableColumnDataTypeProcedureTest {

  @Test
  public void commitReleaseIsNeverRolledBack() throws Exception {
    final AlterTableColumnDataTypeProcedure procedure =
        new AlterTableColumnDataTypeProcedure("database", "table", "query", "value", null, false);
    procedure.table = new TsTable("table");

    // The commit state may be present on the rollback stack when an abort races with cache
    // notification. It must not issue a rollback plan or touch DataNode caches.
    procedure.rollbackState(null, AlterTableColumnDataTypeState.COMMIT_RELEASE);
  }

  @Test
  public void rollbackClearsCnMarkerAfterProcedureRestartBeforeTableSnapshot() throws Exception {
    final ConsensusManager consensusManager = Mockito.mock(ConsensusManager.class);
    final ClusterSchemaManager schemaManager = Mockito.mock(ClusterSchemaManager.class);
    final ConfigManager configManager = Mockito.mock(ConfigManager.class);
    final ConfigNodeProcedureEnv env = Mockito.mock(ConfigNodeProcedureEnv.class);
    Mockito.when(env.getConfigManager()).thenReturn(configManager);
    Mockito.when(configManager.getConsensusManager()).thenReturn(consensusManager);
    Mockito.when(configManager.getClusterSchemaManager()).thenReturn(schemaManager);
    Mockito.when(consensusManager.write(Mockito.any()))
        .thenReturn(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    Mockito.when(
            schemaManager.isColumnAlterCommitted("database", "table", "value", TSDataType.INT64))
        .thenReturn(false);

    final AlterTableColumnDataTypeProcedure procedure =
        new AlterTableColumnDataTypeProcedure(
            "database", "table", "query", "value", TSDataType.INT64, false);

    // The table snapshot is intentionally null, as it can be after a restart that happened after
    // the pre-alter consensus entry was applied but before the procedure persisted its snapshot.
    procedure.rollbackState(env, AlterTableColumnDataTypeState.CHECK_AND_INVALIDATE_COLUMN);

    Mockito.verify(consensusManager).write(Mockito.any());
  }

  @Test
  public void sameTypePreAlterRollbackStillCleansDataNodeCache() throws Exception {
    final ConsensusManager consensusManager = Mockito.mock(ConsensusManager.class);
    final ClusterSchemaManager schemaManager = Mockito.mock(ClusterSchemaManager.class);
    final ConfigManager configManager = Mockito.mock(ConfigManager.class);
    final ConfigNodeProcedureEnv env = Mockito.mock(ConfigNodeProcedureEnv.class);
    Mockito.when(env.getConfigManager()).thenReturn(configManager);
    Mockito.when(configManager.getConsensusManager()).thenReturn(consensusManager);
    Mockito.when(configManager.getClusterSchemaManager()).thenReturn(schemaManager);

    final AtomicBoolean markerCleared = new AtomicBoolean(false);
    Mockito.when(schemaManager.getPreAlteredColumnType("database", "table", "value"))
        .thenAnswer(
            invocation -> markerCleared.get() ? Optional.empty() : Optional.of(TSDataType.INT64));
    Mockito.when(
            schemaManager.isColumnAlterCommitted("database", "table", "value", TSDataType.INT64))
        .thenAnswer(invocation -> markerCleared.get());
    Mockito.when(consensusManager.write(Mockito.any()))
        .thenAnswer(
            invocation -> {
              markerCleared.set(true);
              return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
            });

    final TestAlterTableColumnDataTypeProcedure procedure =
        new TestAlterTableColumnDataTypeProcedure();
    procedure.table = new TsTable("table");
    procedure.rollbackState(env, AlterTableColumnDataTypeState.PRE_RELEASE);

    Assert.assertTrue(procedure.dataNodeRollbackCalled);
  }

  @Test
  public void staleRollbackDoesNotClearNewerPreAlter() throws Exception {
    final ConsensusManager consensusManager = Mockito.mock(ConsensusManager.class);
    final ClusterSchemaManager schemaManager = Mockito.mock(ClusterSchemaManager.class);
    final ConfigManager configManager = Mockito.mock(ConfigManager.class);
    final ConfigNodeProcedureEnv env = Mockito.mock(ConfigNodeProcedureEnv.class);
    Mockito.when(env.getConfigManager()).thenReturn(configManager);
    Mockito.when(configManager.getConsensusManager()).thenReturn(consensusManager);
    Mockito.when(configManager.getClusterSchemaManager()).thenReturn(schemaManager);
    Mockito.when(schemaManager.getPreAlteredColumnType("database", "table", "value"))
        .thenReturn(Optional.of(TSDataType.FLOAT));

    final TestAlterTableColumnDataTypeProcedure procedure =
        new TestAlterTableColumnDataTypeProcedure();
    procedure.table = new TsTable("table");
    procedure.rollbackState(env, AlterTableColumnDataTypeState.PRE_RELEASE);

    Mockito.verify(consensusManager, Mockito.never()).write(Mockito.any());
    Assert.assertFalse(procedure.dataNodeRollbackCalled);
  }

  @Test
  public void consensusFailureAfterCommitContinuesToCommitRelease() throws Exception {
    final ClusterSchemaManager schemaManager = Mockito.mock(ClusterSchemaManager.class);
    final ConfigManager configManager = Mockito.mock(ConfigManager.class);
    final ConfigNodeProcedureEnv env = Mockito.mock(ConfigNodeProcedureEnv.class);
    Mockito.when(env.getConfigManager()).thenReturn(configManager);
    Mockito.when(configManager.getClusterSchemaManager()).thenReturn(schemaManager);

    Mockito.when(schemaManager.executePlan(Mockito.any(), Mockito.eq(false)))
        .thenReturn(new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()));
    Mockito.when(
            schemaManager.isColumnAlterCommitted("database", "table", "value", TSDataType.INT64))
        .thenReturn(true);

    final AlterTableColumnDataTypeProcedure procedure =
        new AlterTableColumnDataTypeProcedure(
            "database", "table", "query", "value", TSDataType.INT64, false);
    final Method alterColumnDataType =
        AlterTableColumnDataTypeProcedure.class.getDeclaredMethod(
            "alterColumnDataType", ConfigNodeProcedureEnv.class);
    alterColumnDataType.setAccessible(true);
    alterColumnDataType.invoke(procedure, env);

    Assert.assertFalse(procedure.isFailed());
  }

  private static class TestAlterTableColumnDataTypeProcedure
      extends AlterTableColumnDataTypeProcedure {
    private boolean dataNodeRollbackCalled;

    private TestAlterTableColumnDataTypeProcedure() {
      super("database", "table", "query", "value", TSDataType.INT64, false);
    }

    @Override
    protected void rollbackPreRelease(final ConfigNodeProcedureEnv env) {
      dataNodeRollbackCalled = true;
    }
  }
}
