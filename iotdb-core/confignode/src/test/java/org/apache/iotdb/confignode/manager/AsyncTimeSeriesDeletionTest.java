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

package org.apache.iotdb.confignode.manager;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.table.TableNodeStatus;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.FieldColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TagColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TimeColumnSchema;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.manager.schema.ClusterSchemaManager;
import org.apache.iotdb.confignode.persistence.ProcedureInfo;
import org.apache.iotdb.confignode.procedure.Procedure;
import org.apache.iotdb.confignode.procedure.ProcedureExecutor;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.schema.DeleteTimeSeriesProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.DropTableColumnProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.view.DropViewColumnProcedure;
import org.apache.iotdb.confignode.procedure.scheduler.DeleteTimeSeriesProcedureScheduler;
import org.apache.iotdb.confignode.procedure.state.ProcedureState;
import org.apache.iotdb.confignode.procedure.store.ConfigProcedureStore;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.confignode.rpc.thrift.TAlterOrDropTableReq;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.confignode.rpc.thrift.TDropTableProcedureInfo;
import org.apache.iotdb.mpp.rpc.thrift.TDropTableModProgress;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AsyncTimeSeriesDeletionTest {
  private ProcedureManager manager;
  private ProcedureExecutor<ConfigNodeProcedureEnv> executor;
  private ConcurrentHashMap<Long, Procedure<ConfigNodeProcedureEnv>> running;
  private List<Procedure<ConfigNodeProcedureEnv>> completed;
  private ClusterSchemaManager schemaManager;

  @Before
  public void setUp() throws Exception {
    executor = mock(ProcedureExecutor.class);
    running = new ConcurrentHashMap<>();
    completed = new ArrayList<>();
    final DeleteTimeSeriesProcedureScheduler scheduler =
        new DeleteTimeSeriesProcedureScheduler(1, 1000);
    when(executor.isRunning()).thenReturn(true);
    when(executor.getProcedures()).thenReturn(running);
    when(executor.getCompletedProcedures()).thenReturn(completed);
    when(executor.getDeleteTimeSeriesScheduler()).thenReturn(scheduler);
    final AtomicLong ids = new AtomicLong();
    doAnswer(
            invocation -> {
              final Procedure<ConfigNodeProcedureEnv> procedure = invocation.getArgument(0);
              procedure.setProcId(ids.incrementAndGet());
              procedure.setProcRunnable();
              running.put(procedure.getProcId(), procedure);
              scheduler.addBack(procedure);
              return procedure.getProcId();
            })
        .when(executor)
        .submitProcedureAndPersist(any());
    final ConfigManager configManager = mock(ConfigManager.class);
    schemaManager = mock(ClusterSchemaManager.class);
    when(configManager.getClusterSchemaManager()).thenReturn(schemaManager);
    manager =
        new ProcedureManager(configManager, mock(ProcedureInfo.class)) {
          @Override
          protected TSStatus waitingProcedureFinished(Procedure<?> procedure, long timeout) {
            throw new AssertionError("SQL submission must not wait for deletion");
          }
        };
    manager.setExecutor(executor);
  }

  private PathPatternTree pattern(String path) throws Exception {
    final PathPatternTree tree = new PathPatternTree();
    tree.appendPathPattern(new PartialPath(path));
    tree.constructTree();
    return tree;
  }

  private TAlterOrDropTableReq dropColumnRequest(String queryId, String column) throws IOException {
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    ReadWriteIOUtils.write(column, new DataOutputStream(bytes));
    return new TAlterOrDropTableReq()
        .setDatabase("test")
        .setTableName("t")
        .setQueryId(queryId)
        .setUpdateInfo(ByteBuffer.wrap(bytes.toByteArray()));
  }

  @Test
  public void testConsensusRejectionDoesNotEnqueueDeletion() throws Exception {
    final ConfigManager configManager = mock(ConfigManager.class);
    final ConsensusManager consensus = mock(ConsensusManager.class);
    when(configManager.getConsensusManager()).thenReturn(consensus);
    when(consensus.write(any()))
        .thenReturn(new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()));
    final ConfigProcedureStore store =
        new ConfigProcedureStore(configManager, mock(ProcedureInfo.class));
    final ProcedureExecutor<ConfigNodeProcedureEnv> realExecutor =
        new ProcedureExecutor<>(null, store);
    try {
      realExecutor.submitProcedureAndPersist(
          new DeleteTimeSeriesProcedure("query", pattern("root.async.d.s"), false, false));
      fail("A rejected consensus write must not acknowledge or enqueue deletion");
    } catch (IoTDBException e) {
      assertEquals(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode(), e.getErrorCode());
    }
    assertTrue(realExecutor.getProcedures().isEmpty());
  }

  @Test
  public void testFinalizingTreeDeletionStillRejectsOverlap() throws Exception {
    final DeleteTimeSeriesProcedure procedure = mock(DeleteTimeSeriesProcedure.class);
    when(procedure.getPatternTree()).thenReturn(pattern("root.async.**"));
    when(procedure.getQueryId()).thenReturn("first");
    when(procedure.isFinished()).thenReturn(true);
    running.put(1L, procedure);
    assertEquals(
        TSStatusCode.OVERLAP_WITH_EXISTING_TASK.getStatusCode(),
        manager.deleteTimeSeries("another", pattern("root.async.d.s"), false, false).getCode());
    verify(executor, times(0)).submitProcedureAndPersist(any());
  }

  @Test
  public void testFullDatabaseDeletionReservesTableAndDatabaseChanges() throws Exception {
    manager.deleteTimeSeries("query", pattern("root.async.**"), false, false);
    assertTrue(manager.hasPendingFullDatabaseTimeSeriesDeletion("root.async"));
    assertTrue(
        manager
            .checkDuplicateTableTask(
                "root.async", null, "t", null, "table-query", ProcedureType.CREATE_TABLE_PROCEDURE)
            .getRight());
    assertTrue(
        manager
            .checkDuplicateTableTask(
                "root.async", null, null, null, null, ProcedureType.DELETE_DATABASE_PROCEDURE)
            .getRight());
    assertTrue(
        !manager
            .checkDuplicateTableTask(
                "root.other", null, "t", null, "other-query", ProcedureType.CREATE_TABLE_PROCEDURE)
            .getRight());
    assertTrue(
        !manager
            .checkDuplicateTableTask(
                "root.async", null, null, null, "query", ProcedureType.DELETE_DATABASE_PROCEDURE)
            .getRight());
    assertEquals(
        TSStatusCode.OVERLAP_WITH_EXISTING_TASK.getStatusCode(),
        manager
            .deleteDatabases(
                Collections.singletonList(new TDatabaseSchema("root.async")), false, 100)
            .getCode());

    completed.add(running.remove(1L));
    assertTrue(!manager.hasPendingFullDatabaseTimeSeriesDeletion("root.async"));
  }

  @Test
  public void testTreeDeletionDoesNotReserveTableModelDatabase() throws Exception {
    manager.deleteTimeSeries("query", pattern("root.async.**"), false, false);
    assertTrue(!manager.hasPendingFullDatabaseTimeSeriesDeletion("async"));
    assertTrue(
        !manager
            .checkDuplicateTableTask(
                "async", null, "t", null, "table-query", ProcedureType.CREATE_TABLE_PROCEDURE)
            .getRight());
    assertTrue(manager.hasPendingFullDatabaseTimeSeriesDeletion("root.async"));

    running.clear();
    manager.deleteTimeSeries("all", pattern("root.**"), false, false);
    assertTrue(!manager.hasPendingFullDatabaseTimeSeriesDeletion("other_table_database"));
    assertTrue(manager.hasPendingFullDatabaseTimeSeriesDeletion("root.other_tree_database"));
  }

  @Test
  public void testFinalizingColumnDeletionStillRejectsTableChanges() {
    final DropTableColumnProcedure procedure = mock(DropTableColumnProcedure.class);
    when(procedure.getDatabase()).thenReturn("test");
    when(procedure.getTableName()).thenReturn("t");
    when(procedure.isFinished()).thenReturn(true);
    running.put(1L, procedure);
    assertTrue(
        manager
            .checkDuplicateTableTask(
                "test", null, "t", null, "another", ProcedureType.ADD_TABLE_COLUMN_PROCEDURE)
            .getRight());
  }

  @Test
  public void testColumnDeletionIsDurableAsynchronousAndIdempotent() throws Exception {
    final TsTable table = new TsTable("t");
    table.addColumnSchema(new FieldColumnSchema("s", TSDataType.INT32));
    when(schemaManager.getTableAndStatusIfExists("test", "t"))
        .thenReturn(Optional.of(new Pair<>(table, TableNodeStatus.USING)));
    assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        manager.alterTableDropColumn(dropColumnRequest("query", "s")).getCode());
    assertTrue(
        manager
            .alterTableDropColumn(dropColumnRequest("query", "s"))
            .getMessage()
            .contains("asynchronous"));
    assertEquals(
        TSStatusCode.OVERLAP_WITH_EXISTING_TASK.getStatusCode(),
        manager.alterTableDropColumn(dropColumnRequest("another", "s")).getCode());
    completed.add(running.remove(1L));
    assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        manager.alterTableDropColumn(dropColumnRequest("query", "s")).getCode());
    verify(executor, times(1)).submitProcedureAndPersist(any());
  }

  @Test
  public void testInvalidColumnDeletionIsRejectedBeforeSubmission() throws Exception {
    final TsTable table = new TsTable("t");
    table.addColumnSchema(new TimeColumnSchema("time", TSDataType.TIMESTAMP));
    table.addColumnSchema(new TagColumnSchema("tag", TSDataType.STRING));
    when(schemaManager.getTableAndStatusIfExists("test", "t"))
        .thenReturn(Optional.of(new Pair<>(table, TableNodeStatus.USING)));
    assertEquals(
        TSStatusCode.COLUMN_NOT_EXISTS.getStatusCode(),
        manager.alterTableDropColumn(dropColumnRequest("missing", "s")).getCode());
    assertEquals(
        TSStatusCode.SEMANTIC_ERROR.getStatusCode(),
        manager.alterTableDropColumn(dropColumnRequest("time", "time")).getCode());
    assertEquals(
        TSStatusCode.SEMANTIC_ERROR.getStatusCode(),
        manager.alterTableDropColumn(dropColumnRequest("tag", "tag")).getCode());
    verify(executor, times(0)).submitProcedureAndPersist(any());
  }

  @Test
  public void testCompletionDuringSnapshotDoesNotLoseTaskOrResubmit() throws Exception {
    final PathPatternTree tree = pattern("root.async.d.s");
    manager.deleteTimeSeries("query", tree, false, false);
    completeDuringSnapshot();
    assertEquals(1, manager.getDeleteTimeSeriesProcedures().size());
    // Repeat the race for submission-time deduplication.
    running.put(1L, completed.remove(0));
    completeDuringSnapshot();
    assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        manager.deleteTimeSeries("query", tree, false, false).getCode());
    verify(executor, times(1)).submitProcedureAndPersist(any());
  }

  @Test
  public void testProgressWaitsForDurableCompletion() throws Exception {
    final DeleteTimeSeriesProcedure procedure = mock(DeleteTimeSeriesProcedure.class);
    when(procedure.getProcId()).thenReturn(1L);
    when(procedure.getQueryId()).thenReturn("query");
    when(procedure.getPatternTree()).thenReturn(pattern("root.async.d.s"));
    when(procedure.isFinished()).thenReturn(true);
    when(procedure.getState()).thenReturn(ProcedureState.SUCCESS);
    when(procedure.getDeleteProgress()).thenReturn("SUCCESS");
    when(executor.isCompleted(1L)).thenAnswer(invocation -> completed.contains(procedure));
    running.put(1L, procedure);
    assertEquals("RUNNING", manager.getDeleteTimeSeriesProcedures().get(0).getState());
    assertEquals("FINALIZING", manager.getDeleteTimeSeriesProcedures().get(0).getProgress());
    completed.add(running.remove(1L));
    assertEquals("SUCCESS", manager.getDeleteTimeSeriesProcedures().get(0).getState());
    assertEquals("SUCCESS", manager.getDeleteTimeSeriesProcedures().get(0).getProgress());
  }

  @Test
  public void testColumnCompletionDuringSnapshotDoesNotResubmit() throws Exception {
    final TsTable table = new TsTable("t");
    table.addColumnSchema(new FieldColumnSchema("s", TSDataType.INT32));
    when(schemaManager.getTableAndStatusIfExists("test", "t"))
        .thenReturn(Optional.of(new Pair<>(table, TableNodeStatus.USING)));
    manager.alterTableDropColumn(dropColumnRequest("query", "s"));
    completeDuringSnapshot();
    assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        manager.alterTableDropColumn(dropColumnRequest("query", "s")).getCode());
    verify(executor, times(1)).submitProcedureAndPersist(any());
  }

  private void completeDuringSnapshot() {
    when(executor.getCompletedProcedures())
        .thenAnswer(
            invocation -> {
              final List<Procedure<ConfigNodeProcedureEnv>> snapshot = new ArrayList<>(completed);
              completed.add(running.remove(1L));
              return snapshot;
            })
        .thenReturn(completed);
  }

  @Test
  public void testPipeDoesNotAcknowledgeUnpersistedCompletionDuringShutdown() throws Exception {
    final PathPatternTree tree = pattern("root.async.d.s");
    final DeleteTimeSeriesProcedure procedure =
        new SuccessfulDeleteTimeSeriesProcedure("pipe-query", tree);
    procedure.setProcId(1L);
    running.put(1L, procedure);
    when(executor.isRunning()).thenReturn(true, false);
    when(executor.isFinished(1L)).thenAnswer(invocation -> !running.containsKey(1L));
    final ProcedureManager pipeManager =
        new ProcedureManager(mock(ConfigManager.class), mock(ProcedureInfo.class));
    pipeManager.setExecutor(executor);
    assertEquals(
        TSStatusCode.INTERNAL_REQUEST_TIME_OUT.getStatusCode(),
        pipeManager.deleteTimeSeries("pipe-query", tree, true, false).getCode());
    completed.add(running.remove(1L));
    when(executor.isRunning()).thenReturn(true);
    assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        pipeManager.deleteTimeSeries("pipe-query", tree, true, false).getCode());
    verify(executor, times(0)).submitProcedureAndPersist(any());
  }

  private static class SuccessfulDeleteTimeSeriesProcedure extends DeleteTimeSeriesProcedure {
    private SuccessfulDeleteTimeSeriesProcedure(String queryId, PathPatternTree tree) {
      super(queryId, tree, true, false);
      setState(ProcedureState.SUCCESS);
    }
  }

  @Test
  public void testAcknowledgementRequiresPersistenceAndRetriesAreIdempotent() throws Exception {
    final PathPatternTree tree = pattern("root.async.d.s");
    assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        manager.deleteTimeSeries("query", tree, false, false).getCode());
    assertTrue(
        manager
            .deleteTimeSeries("query", tree, false, false)
            .getMessage()
            .contains("asynchronous"));
    completed.add(running.remove(1L));
    assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        manager.deleteTimeSeries("query", tree, false, false).getCode());
    verify(executor, times(1)).submitProcedureAndPersist(any());
    assertEquals(1, manager.getDeleteTimeSeriesProcedures().size());
  }

  @Test
  public void testPersistenceFailureDoesNotAcknowledgeOrConsumeQueueCapacity() throws Exception {
    doThrow(new IOException("store unavailable")).when(executor).submitProcedureAndPersist(any());
    assertEquals(
        TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(),
        manager.deleteTimeSeries("query", pattern("root.async.d.s"), false, false).getCode());
    assertTrue(running.isEmpty());
    assertEquals(0, executor.getDeleteTimeSeriesScheduler().getWaitingTaskCount());
  }

  @Test
  public void testQueueCapacityAndOverlapAreCheckedBeforePersistence() throws Exception {
    final ConfigNodeConfig config = ConfigNodeDescriptor.getInstance().getConf();
    final int capacity = config.getTimeseriesDeletionQueueCapacity();
    config.setTimeseriesDeletionQueueCapacity(1);
    try {
      manager.deleteTimeSeries("first", pattern("root.async.d.s1"), false, false);
      assertEquals(
          TSStatusCode.OVERLAP_WITH_EXISTING_TASK.getStatusCode(),
          manager.deleteTimeSeries("overlap", pattern("root.async.**"), false, false).getCode());
      assertEquals(
          TSStatusCode.WRITE_PROCESS_REJECT.getStatusCode(),
          manager.deleteTimeSeries("full", pattern("root.async.d.s2"), false, false).getCode());
      // Repeating an accepted request succeeds even when its queue is full.
      assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          manager.deleteTimeSeries("first", pattern("root.async.d.s1"), false, false).getCode());
      verify(executor, times(1)).submitProcedureAndPersist(any());
    } finally {
      config.setTimeseriesDeletionQueueCapacity(capacity);
    }
  }

  @Test
  public void testColumnProgressIncludesRunningAndRetainedTasks() {
    final DropTableColumnProcedure active = mock(DropTableColumnProcedure.class);
    when(active.getProcId()).thenReturn(1L);
    when(active.getDatabase()).thenReturn("test");
    when(active.getTableName()).thenReturn("t");
    when(active.getColumnName()).thenReturn("reading");
    when(active.getState()).thenReturn(ProcedureState.RUNNABLE);
    when(active.getDropProgress()).thenReturn("EXECUTE_ON_REGIONS");
    when(active.getModFileProgress()).thenReturn(new TDropTableModProgress(2));
    running.put(1L, active);

    final DropTableColumnProcedure failed = mock(DropTableColumnProcedure.class);
    when(failed.getProcId()).thenReturn(2L);
    when(failed.getDatabase()).thenReturn("test");
    when(failed.getTableName()).thenReturn("t");
    when(failed.getColumnName()).thenReturn("other");
    when(failed.isFinished()).thenReturn(true);
    when(failed.getState()).thenReturn(ProcedureState.ROLLEDBACK);
    when(failed.getDropProgress()).thenReturn("ROLLEDBACK");
    when(failed.hasException()).thenReturn(true);
    when(failed.getException()).thenReturn(new ProcedureException("deletion failed"));
    completed.add(failed);
    // A task observed in both executor snapshots is still listed once.
    completed.add(active);
    running.put(3L, mock(DropViewColumnProcedure.class));

    final List<TDropTableProcedureInfo> infos = manager.getDropTableColumnProcedures();
    assertEquals(2, infos.size());
    final TDropTableProcedureInfo runningInfo =
        infos.stream().filter(info -> info.getProcedureId() == 1).findFirst().get();
    assertEquals("test", runningInfo.getDatabase());
    assertEquals("t", runningInfo.getTableName());
    assertEquals("reading", runningInfo.getColumnName());
    assertEquals("EXECUTE_ON_REGIONS", runningInfo.getProgress());
    assertEquals(2, runningInfo.getWrittenModFileCount());
    assertFalse(runningInfo.isSetRemainingModFileCount());
    final TDropTableProcedureInfo failedInfo =
        infos.stream().filter(info -> info.getProcedureId() == 2).findFirst().get();
    assertEquals("deletion failed", failedInfo.getErrorMessage());
    assertFalse(failedInfo.isSetWrittenModFileCount());
    verify(active).updateModFileProgress();
    verify(failed, times(0)).updateModFileProgress();
  }
}
