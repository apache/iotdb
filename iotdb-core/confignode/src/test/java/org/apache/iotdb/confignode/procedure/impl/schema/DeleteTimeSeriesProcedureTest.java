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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.ProcedureManager;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.partition.PartitionManager;
import org.apache.iotdb.confignode.manager.schema.ClusterSchemaManager;
import org.apache.iotdb.confignode.persistence.ProcedureInfo;
import org.apache.iotdb.confignode.procedure.NoopProcedureStore;
import org.apache.iotdb.confignode.procedure.Procedure;
import org.apache.iotdb.confignode.procedure.ProcedureExecutor;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.state.ProcedureState;
import org.apache.iotdb.confignode.procedure.state.schema.DeleteTimeSeriesState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.confignode.procedure.util.ProcedureTestUtil;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DeleteTimeSeriesProcedureTest {

  @Test
  public void testLeaderReelectionDiscardsUnpersistedDeletionState() throws Exception {
    final AtomicBoolean allowCompletion = new AtomicBoolean();
    final CountDownLatch completionRetried = new CountDownLatch(1);
    final AtomicReference<List<Procedure>> recovered =
        new AtomicReference<>(Collections.emptyList());
    final NoopProcedureStore store =
        new NoopProcedureStore() {
          @Override
          public List<Procedure> load() {
            return recovered.get();
          }

          @Override
          public void update(Procedure procedure) {
            if (procedure.isSuccess() && !allowCompletion.get()) {
              completionRetried.countDown();
              throw new IllegalStateException("completion store unavailable");
            }
          }
        };
    final PathPatternTree tree = new PathPatternTree();
    tree.appendPathPattern(new PartialPath("root.reelection.d.s"));
    tree.constructTree();
    final DeleteTimeSeriesProcedure procedure =
        new DeleteTimeSeriesProcedure("reelection", tree, false, false) {
          @Override
          protected Flow executeFromState(ConfigNodeProcedureEnv env, DeleteTimeSeriesState state) {
            return Flow.NO_MORE_STATE;
          }
        };
    final ProcedureExecutor<ConfigNodeProcedureEnv> executor = new ProcedureExecutor<>(null, store);
    store.start();
    executor.init(1);
    executor.startWorkers();
    try {
      executor.submitProcedureAndPersist(procedure);
      Assert.assertTrue(completionRetried.await(10, TimeUnit.SECONDS));
      executor.stop();
      executor.join();
      Assert.assertTrue(executor.getProcedures().containsKey(procedure.getProcId()));
      Assert.assertFalse(executor.isCompleted(procedure.getProcId()));

      // A different leader completes the task before this same executor becomes leader again.
      final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      procedure.serialize(new DataOutputStream(bytes));
      final ByteBuffer buffer = ByteBuffer.wrap(bytes.toByteArray());
      buffer.getShort();
      final DeleteTimeSeriesProcedure finished = new DeleteTimeSeriesProcedure(false);
      finished.deserialize(buffer);
      Assert.assertTrue(finished.isSuccess());
      recovered.set(Collections.singletonList(finished));
      allowCompletion.set(true);
      executor.init(1);
      executor.startWorkers();
      Assert.assertTrue(executor.isFinished(procedure.getProcId()));
      Assert.assertTrue(executor.getProcedures().isEmpty());
      Assert.assertEquals(Collections.singletonList(finished), executor.getCompletedProcedures());
      Assert.assertEquals(0, executor.getDeleteTimeSeriesScheduler().getWaitingTaskCount());
      Assert.assertNull(executor.getRollbackStack(procedure.getProcId()));
    } finally {
      allowCompletion.set(true);
      executor.stop();
      executor.join();
      store.stop();
    }
  }

  @Test
  public void testStatePersistenceFailureBlocksNextStepAndCompletion() throws Exception {
    final AtomicBoolean allowTransition = new AtomicBoolean();
    final AtomicBoolean allowCompletion = new AtomicBoolean();
    final AtomicInteger transitionAttempts = new AtomicInteger();
    final AtomicInteger completionAttempts = new AtomicInteger();
    final CountDownLatch transitionRetried = new CountDownLatch(1);
    final CountDownLatch completionRetried = new CountDownLatch(1);
    final CountDownLatch recreated = new CountDownLatch(1);
    final NoopProcedureStore store =
        new NoopProcedureStore() {
          @Override
          public void update(Procedure procedure) {
            final DeleteTimeSeriesProcedure deletion = (DeleteTimeSeriesProcedure) procedure;
            if ("RECREATE_DATABASES".equals(deletion.getDeleteProgress())
                && !allowTransition.get()) {
              if (transitionAttempts.incrementAndGet() >= 2) {
                transitionRetried.countDown();
              }
              throw new IllegalStateException("transition store unavailable");
            }
            if (deletion.isSuccess() && !allowCompletion.get()) {
              if (completionAttempts.incrementAndGet() >= 2) {
                completionRetried.countDown();
              }
              throw new IllegalStateException("completion store unavailable");
            }
          }
        };
    final DeleteTimeSeriesProcedure procedure =
        new DeleteTimeSeriesProcedure(false) {
          @Override
          protected Flow executeFromState(ConfigNodeProcedureEnv env, DeleteTimeSeriesState state) {
            if (state == DeleteTimeSeriesState.PREPARE_DELETE) {
              setNextState(DeleteTimeSeriesState.RECREATE_DATABASES);
              return Flow.HAS_MORE_STATE;
            }
            recreated.countDown();
            return Flow.NO_MORE_STATE;
          }
        };
    final ProcedureExecutor<ConfigNodeProcedureEnv> executor = new ProcedureExecutor<>(null, store);
    store.start();
    executor.init(1);
    executor.startWorkers();
    try {
      executor.submitProcedureAndPersist(procedure);
      Assert.assertTrue(transitionRetried.await(10, TimeUnit.SECONDS));
      Assert.assertEquals(1, recreated.getCount());
      Assert.assertFalse(executor.isCompleted(procedure.getProcId()));
      allowTransition.set(true);
      Assert.assertTrue(completionRetried.await(10, TimeUnit.SECONDS));
      Assert.assertEquals(0, recreated.getCount());
      Assert.assertFalse(executor.isCompleted(procedure.getProcId()));
      Assert.assertTrue(executor.getProcedures().containsKey(procedure.getProcId()));
      allowCompletion.set(true);
      ProcedureTestUtil.waitForProcedure(executor, procedure.getProcId());
      Assert.assertTrue(executor.isCompleted(procedure.getProcId()));
    } finally {
      executor.stop();
      executor.join();
      store.stop();
    }
  }

  @Test
  public void testRollbackPersistenceFailureDoesNotCompleteDuringShutdown() throws Exception {
    final AtomicInteger attempts = new AtomicInteger();
    final CountDownLatch rollbackRetried = new CountDownLatch(1);
    final NoopProcedureStore store =
        new NoopProcedureStore() {
          @Override
          public void update(Procedure procedure) {
            if (procedure.getState() == ProcedureState.ROLLEDBACK) {
              if (attempts.incrementAndGet() >= 2) {
                rollbackRetried.countDown();
              }
              throw new IllegalStateException("rollback store unavailable");
            }
          }
        };
    final ProcedureExecutor<ConfigNodeProcedureEnv> executor = new ProcedureExecutor<>(null, store);
    final DeleteTimeSeriesProcedure procedure =
        new DeleteTimeSeriesProcedure(false) {
          @Override
          protected Flow executeFromState(ConfigNodeProcedureEnv env, DeleteTimeSeriesState state) {
            setFailure(new ProcedureException("deletion failed"));
            return Flow.NO_MORE_STATE;
          }
        };
    store.start();
    executor.init(1);
    executor.startWorkers();
    try {
      executor.submitProcedureAndPersist(procedure);
      Assert.assertTrue(rollbackRetried.await(10, TimeUnit.SECONDS));
      Assert.assertFalse(executor.isCompleted(procedure.getProcId()));
      executor.stop();
      executor.join();
      Assert.assertFalse(executor.isCompleted(procedure.getProcId()));
      Assert.assertTrue(executor.getProcedures().containsKey(procedure.getProcId()));
    } finally {
      executor.stop();
      executor.join();
      store.stop();
    }
  }

  @Test
  public void testDeletionDoesNotOccupyRegularWorkerAndRetainsConcurrencySlot() throws Exception {
    final NoopProcedureStore store = new NoopProcedureStore();
    final ProcedureExecutor<ConfigNodeProcedureEnv> executor = new ProcedureExecutor<>(null, store);
    final CountDownLatch firstStarted = new CountDownLatch(1);
    final CountDownLatch releaseFirst = new CountDownLatch(1);
    final CountDownLatch secondStarted = new CountDownLatch(1);
    final CountDownLatch regularCompleted = new CountDownLatch(1);
    store.start();
    executor.init(1);
    executor.startWorkers();
    try {
      executor.submitProcedureAndPersist(
          new DeleteTimeSeriesProcedure(false) {
            @Override
            protected Flow executeFromState(ConfigNodeProcedureEnv env, DeleteTimeSeriesState state)
                throws InterruptedException {
              firstStarted.countDown();
              releaseFirst.await();
              return Flow.NO_MORE_STATE;
            }
          });
      Assert.assertTrue(firstStarted.await(10, TimeUnit.SECONDS));
      executor.submitProcedureAndPersist(
          new DeleteTimeSeriesProcedure(false) {
            @Override
            protected Flow executeFromState(
                ConfigNodeProcedureEnv env, DeleteTimeSeriesState state) {
              secondStarted.countDown();
              return Flow.NO_MORE_STATE;
            }
          });
      executor.submitProcedure(
          new Procedure<ConfigNodeProcedureEnv>() {
            @Override
            protected Procedure<ConfigNodeProcedureEnv>[] execute(ConfigNodeProcedureEnv env) {
              regularCompleted.countDown();
              return null;
            }

            @Override
            protected void rollback(ConfigNodeProcedureEnv env) {}
          });
      Assert.assertTrue(regularCompleted.await(10, TimeUnit.SECONDS));
      Assert.assertEquals(1, secondStarted.getCount());
      Assert.assertEquals(1, executor.getDeleteTimeSeriesScheduler().getWaitingTaskCount());
      releaseFirst.countDown();
      Assert.assertTrue(secondStarted.await(10, TimeUnit.SECONDS));
    } finally {
      releaseFirst.countDown();
      executor.stop();
      executor.join();
      store.stop();
    }
  }

  @Test
  public void testDatabaseRecreationRecoveryDoesNotDeleteAgain() throws Exception {
    assertDatabaseRecreationRecovery(false);
  }

  @Test
  public void testDatabaseDeletionFailureSurvivesRecoveryAndStillRecreates() throws Exception {
    assertDatabaseRecreationRecovery(true);
  }

  @Test
  public void testDatabaseRecreationWaitsForDurableDatabaseDeletion() throws Exception {
    final AtomicBoolean allowCompletion = new AtomicBoolean();
    final AtomicInteger terminalAttempts = new AtomicInteger();
    final CountDownLatch completionRetried = new CountDownLatch(1);
    final AtomicReference<DeleteDatabaseProcedure> databaseDeletion = new AtomicReference<>();
    final AtomicReference<Thread> databaseWorker = new AtomicReference<>();
    final NoopProcedureStore store =
        new NoopProcedureStore() {
          @Override
          public void update(Procedure procedure) {
            if (procedure instanceof DeleteDatabaseProcedure
                && procedure.isSuccess()
                && !allowCompletion.get()) {
              databaseDeletion.set((DeleteDatabaseProcedure) procedure);
              databaseWorker.set(Thread.currentThread());
              if (terminalAttempts.incrementAndGet() >= 2) {
                completionRetried.countDown();
              }
              throw new IllegalStateException("database completion store unavailable");
            }
          }
        };
    final ConfigNodeProcedureEnv env = mock(ConfigNodeProcedureEnv.class);
    final ConfigManager manager = mock(ConfigManager.class);
    final ClusterSchemaManager schemaManager = mock(ClusterSchemaManager.class);
    final PartitionManager partitionManager = mock(PartitionManager.class);
    final TDatabaseSchema database = new TDatabaseSchema("root.durable_database_deletion");
    when(env.getConfigManager()).thenReturn(manager);
    when(env.invalidateCache(database.getName())).thenReturn(true);
    when(env.getAllReplicaSets(database.getName())).thenReturn(Collections.emptyList());
    when(env.deleteDatabaseConfig(database.getName(), false))
        .thenReturn(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    when(manager.getClusterSchemaManager()).thenReturn(schemaManager);
    when(manager.getLoadManager()).thenReturn(mock(LoadManager.class));
    when(manager.getPartitionManager()).thenReturn(partitionManager);
    when(partitionManager.getRegionMaintainer()).thenReturn(mock(ScheduledExecutorService.class));
    when(schemaManager.getMatchedDatabaseSchemasByPrefix(new PartialPath(database.getName())))
        .thenReturn(new HashMap<>(Collections.singletonMap(database.getName(), database)));
    when(manager.getRelatedSchemaRegionGroup(any())).thenReturn(Collections.emptyMap());
    when(schemaManager.setDatabase(any(), anyBoolean()))
        .thenReturn(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    final ProcedureExecutor<ConfigNodeProcedureEnv> executor = new ProcedureExecutor<>(env, store);
    final ProcedureManager procedureManager =
        new ProcedureManager(manager, mock(ProcedureInfo.class));
    procedureManager.setExecutor(executor);
    when(manager.getProcedureManager()).thenReturn(procedureManager);
    final PathPatternTree tree = new PathPatternTree();
    tree.appendPathPattern(new PartialPath(database.getName() + ".**"));
    tree.constructTree();
    final DeleteTimeSeriesProcedure procedure =
        new DeleteTimeSeriesProcedure("durable-database-deletion", tree, false, false);
    store.start();
    executor.init(1);
    executor.startWorkers();
    try {
      executor.submitProcedureAndPersist(procedure);
      Assert.assertTrue(completionRetried.await(10, TimeUnit.SECONDS));
      Assert.assertTrue(terminalAttempts.get() >= 2);
      Assert.assertEquals("DELETE_DATABASES", procedure.getDeleteProgress());
      verify(schemaManager, never()).setDatabase(any(), anyBoolean());
      // An interrupted unlock retry must not turn an unpersisted child into a completed root.
      final Thread worker = databaseWorker.get();
      worker.interrupt();
      worker.join(TimeUnit.SECONDS.toMillis(10));
      Assert.assertFalse(worker.isAlive());
      Assert.assertFalse(executor.isCompleted(databaseDeletion.get().getProcId()));
      Assert.assertTrue(executor.getProcedures().containsKey(databaseDeletion.get().getProcId()));
      executor.stop();
      executor.join();
      verify(schemaManager, never()).setDatabase(any(), anyBoolean());
    } finally {
      allowCompletion.set(true);
      executor.stop();
      executor.join();
      store.stop();
    }
  }

  private void assertDatabaseRecreationRecovery(boolean deleteFails) throws Exception {
    final TDatabaseSchema database = new TDatabaseSchema("root.async_full");
    final PathPatternTree tree = new PathPatternTree();
    tree.appendPathPattern(new PartialPath(database.getName() + ".**"));
    tree.constructTree();
    final SteppableDeletion procedure = new SteppableDeletion("recovery", tree);
    final ConfigNodeProcedureEnv env = mock(ConfigNodeProcedureEnv.class);
    final ConfigManager manager = mock(ConfigManager.class);
    final ClusterSchemaManager schemaManager = mock(ClusterSchemaManager.class);
    final ProcedureManager procedureManager = mock(ProcedureManager.class);
    when(env.getConfigManager()).thenReturn(manager);
    when(manager.getClusterSchemaManager()).thenReturn(schemaManager);
    when(manager.getProcedureManager()).thenReturn(procedureManager);
    when(manager.getLoadManager()).thenReturn(mock(LoadManager.class));
    when(schemaManager.getMatchedDatabaseSchemasByPrefix(new PartialPath(database.getName())))
        .thenReturn(new HashMap<>(Collections.singletonMap(database.getName(), database)));
    when(manager.getRelatedSchemaRegionGroup(any())).thenReturn(Collections.emptyMap());
    when(procedureManager.deleteDatabases(
            Collections.singletonList(database), false, Long.MAX_VALUE, "recovery"))
        .thenReturn(
            new TSStatus(
                    deleteFails
                        ? TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()
                        : TSStatusCode.SUCCESS_STATUS.getStatusCode())
                .setMessage(deleteFails ? "database deletion failed" : null));
    procedure.step(env);
    procedure.step(env);
    Assert.assertEquals("RECREATE_DATABASES", procedure.getDeleteProgress());
    verify(schemaManager, never()).setDatabase(any(), anyBoolean());

    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    procedure.serialize(new DataOutputStream(bytes));
    final ByteBuffer buffer = ByteBuffer.wrap(bytes.toByteArray());
    buffer.getShort();
    final SteppableDeletion restored = new SteppableDeletion();
    restored.deserialize(buffer);
    Assert.assertEquals("RECREATE_DATABASES", restored.getDeleteProgress());
    // Successful recreation can also have happened just before a restart.
    when(schemaManager.setDatabase(any(), anyBoolean()))
        .thenReturn(
            new TSStatus(
                deleteFails
                    ? TSStatusCode.SUCCESS_STATUS.getStatusCode()
                    : TSStatusCode.DATABASE_ALREADY_EXISTS.getStatusCode()));
    when(schemaManager.getDatabaseSchemaByName(database.getName())).thenReturn(database);
    restored.step(env);
    Assert.assertEquals(deleteFails, restored.hasException());
    if (deleteFails) {
      Assert.assertTrue(restored.getException().getMessage().contains("database deletion failed"));
    }
    verify(procedureManager, times(1))
        .deleteDatabases(Collections.singletonList(database), false, Long.MAX_VALUE, "recovery");
    verify(schemaManager, times(1)).setDatabase(any(), anyBoolean());
  }

  private static class SteppableDeletion extends DeleteTimeSeriesProcedure {
    SteppableDeletion() {
      super(false);
    }

    SteppableDeletion(String queryId, PathPatternTree tree) {
      super(queryId, tree, false, false);
    }

    void step(ConfigNodeProcedureEnv env) throws InterruptedException {
      super.execute(env);
    }
  }

  @Test
  public void testPreparedDatabaseDeletionSurvivesSerialization() throws Exception {
    final PathPatternTree tree = new PathPatternTree();
    tree.appendPathPattern(new PartialPath("root.async_full.**"));
    tree.appendPathPattern(new PartialPath("root.async_other.d.s"));
    tree.constructTree();
    final DeleteTimeSeriesProcedure procedure =
        new DeleteTimeSeriesProcedure("prepared", tree, false, false);
    final ConfigNodeProcedureEnv env = mock(ConfigNodeProcedureEnv.class);
    final ConfigManager manager = mock(ConfigManager.class);
    final ClusterSchemaManager schemaManager = mock(ClusterSchemaManager.class);
    when(env.getConfigManager()).thenReturn(manager);
    when(manager.getClusterSchemaManager()).thenReturn(schemaManager);
    when(manager.getLoadManager()).thenReturn(mock(LoadManager.class));
    when(schemaManager.getMatchedDatabaseSchemasByPrefix(new PartialPath("root.async_full")))
        .thenReturn(
            new HashMap<>(
                Collections.singletonMap(
                    "root.async_full", new TDatabaseSchema("root.async_full"))));
    when(manager.getRelatedSchemaRegionGroup(any())).thenReturn(Collections.emptyMap());
    procedure.executeFromState(env, DeleteTimeSeriesState.PREPARE_DELETE);
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    procedure.serialize(new DataOutputStream(bytes));
    final ByteBuffer buffer = ByteBuffer.wrap(bytes.toByteArray());
    buffer.getShort();
    final DeleteTimeSeriesProcedure restored = new DeleteTimeSeriesProcedure(false);
    restored.deserialize(buffer);
    Assert.assertEquals(procedure, restored);
    Assert.assertEquals(tree, restored.getPatternTree());
  }

  @Test
  public void testLegacyProcedureWithoutAsyncSuffixCanBeRecovered() throws Exception {
    final PathPatternTree tree = new PathPatternTree();
    tree.appendPathPattern(new PartialPath("root.legacy.d.s"));
    tree.constructTree();
    final DeleteTimeSeriesProcedure procedure =
        new DeleteTimeSeriesProcedure("legacy", tree, false, false);
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    procedure.serialize(new DataOutputStream(bytes));
    // Remove the original-tree flag, empty database-schema count and null deletion-failure string.
    final ByteBuffer buffer = ByteBuffer.wrap(bytes.toByteArray(), 0, bytes.size() - 9);
    buffer.getShort();
    final DeleteTimeSeriesProcedure restored = new DeleteTimeSeriesProcedure(false);
    restored.deserialize(buffer);
    Assert.assertEquals(tree, restored.getPatternTree());
    Assert.assertEquals("legacy", restored.getQueryId());
  }

  @Test
  public void serializeDeserializeTest() throws IllegalPathException, IOException {
    String queryId = "1";
    PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendPathPattern(new PartialPath("root.sg1.**"));
    patternTree.appendPathPattern(new PartialPath("root.sg2.*.s1"));
    patternTree.constructTree();
    DeleteTimeSeriesProcedure deleteTimeSeriesProcedure =
        new DeleteTimeSeriesProcedure(queryId, patternTree, false, false);

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    deleteTimeSeriesProcedure.serialize(dataOutputStream);

    ByteBuffer byteBuffer = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());

    Assert.assertEquals(
        ProcedureType.DELETE_TIMESERIES_PROCEDURE.getTypeCode(), byteBuffer.getShort());

    DeleteTimeSeriesProcedure deserializedProcedure = new DeleteTimeSeriesProcedure(false);
    deserializedProcedure.deserialize(byteBuffer);

    Assert.assertEquals(queryId, deserializedProcedure.getQueryId());

    List<PartialPath> pathList = deserializedProcedure.getPatternTree().getAllPathPatterns();
    pathList.sort(PartialPath::compareTo);
    Assert.assertEquals("root.sg1.**", pathList.get(0).getFullPath());
    Assert.assertEquals("root.sg2.*.s1", pathList.get(1).getFullPath());
  }
}
