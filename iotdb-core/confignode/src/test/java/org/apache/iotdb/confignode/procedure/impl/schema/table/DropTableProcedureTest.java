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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.confignode.procedure.NoopProcedureStore;
import org.apache.iotdb.confignode.procedure.Procedure;
import org.apache.iotdb.confignode.procedure.ProcedureExecutor;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.impl.schema.table.view.DropViewProcedure;
import org.apache.iotdb.confignode.procedure.state.schema.DropTableState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.confignode.procedure.util.ProcedureTestUtil;
import org.apache.iotdb.mpp.rpc.thrift.TDropTableModProgress;
import org.apache.iotdb.mpp.rpc.thrift.TGetDropTableModProgressResp;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class DropTableProcedureTest {
  @Test
  public void dropTableDoesNotOccupyRegularProcedureWorker() throws Exception {
    final NoopProcedureStore store = new NoopProcedureStore();
    final ProcedureExecutor<ConfigNodeProcedureEnv> executor = new ProcedureExecutor<>(null, store);
    final CountDownLatch dropStarted = new CountDownLatch(1);
    final CountDownLatch releaseDrop = new CountDownLatch(1);
    final CountDownLatch regularCompleted = new CountDownLatch(1);
    Assert.assertEquals(0, executor.getDropTableWorkerThreadCount());
    store.start();
    executor.init(1);
    Assert.assertEquals(0, executor.getDropTableWorkerThreadCount());
    executor.startWorkers();
    try {
      Assert.assertSame(
          executor.getScheduler(), executor.getScheduler(new DropViewProcedure(false)));
      Assert.assertEquals(0, executor.getDropTableWorkerThreadCount());
      final DropTableProcedure dropProcedure =
          new DropTableProcedure("db", "table", "drop", false) {
            @Override
            protected Flow executeFromState(
                final ConfigNodeProcedureEnv env, final DropTableState state)
                throws InterruptedException {
              dropStarted.countDown();
              releaseDrop.await();
              return Flow.NO_MORE_STATE;
            }
          };
      executor.submitProcedureAndPersist(dropProcedure);
      Assert.assertTrue(dropStarted.await(10, TimeUnit.SECONDS));
      Assert.assertEquals(1, executor.getDropTableWorkerThreadCount());
      Assert.assertEquals(
          DropTableState.CHECK_AND_INVALIDATE_TABLE.name(), dropProcedure.getDropProgress());

      executor.submitProcedure(
          new Procedure<ConfigNodeProcedureEnv>() {
            @Override
            protected Procedure<ConfigNodeProcedureEnv>[] execute(
                final ConfigNodeProcedureEnv env) {
              regularCompleted.countDown();
              return null;
            }

            @Override
            protected void rollback(final ConfigNodeProcedureEnv env) {}
          });
      Assert.assertTrue(regularCompleted.await(10, TimeUnit.SECONDS));
    } finally {
      releaseDrop.countDown();
      executor.stop();
      executor.join();
      store.stop();
    }
    Assert.assertEquals(0, executor.getDropTableWorkerThreadCount());
  }

  @Test
  public void recoveredDropStartsWorkerAndCompletedDropDoesNot() throws Exception {
    final CountDownLatch dropStarted = new CountDownLatch(1);
    final CountDownLatch releaseDrop = new CountDownLatch(1);
    final DropTableProcedure dropProcedure =
        new DropTableProcedure("db", "table", "drop", false) {
          @Override
          protected Flow executeFromState(
              final ConfigNodeProcedureEnv env, final DropTableState state)
              throws InterruptedException {
            dropStarted.countDown();
            releaseDrop.await();
            return Flow.NO_MORE_STATE;
          }
        };
    dropProcedure.setProcId(0);
    dropProcedure.setProcRunnable();
    final NoopProcedureStore store =
        new NoopProcedureStore() {
          @Override
          public List<Procedure> load() {
            return Collections.singletonList(dropProcedure);
          }
        };
    final ProcedureExecutor<ConfigNodeProcedureEnv> executor = new ProcedureExecutor<>(null, store);
    store.start();
    executor.init(1);
    try {
      Assert.assertEquals(1, executor.getDropTableWorkerThreadCount());
      Assert.assertEquals(1, dropStarted.getCount());
      executor.startWorkers();
      Assert.assertTrue(dropStarted.await(10, TimeUnit.SECONDS));
      releaseDrop.countDown();
      ProcedureTestUtil.waitForProcedure(executor, dropProcedure.getProcId());
      Assert.assertTrue(dropProcedure.isSuccess());
      executor.stop();
      executor.join();

      executor.init(1);
      executor.startWorkers();
      Assert.assertEquals(0, executor.getDropTableWorkerThreadCount());
      Assert.assertEquals(
          Collections.singletonList(dropProcedure), executor.getCompletedProcedures());
    } finally {
      releaseDrop.countDown();
      executor.stop();
      executor.join();
      store.stop();
    }
  }

  @Test
  public void concurrentFirstDropsShareOneWorker() throws Exception {
    final NoopProcedureStore store = new NoopProcedureStore();
    final ProcedureExecutor<ConfigNodeProcedureEnv> executor = new ProcedureExecutor<>(null, store);
    final ExecutorService submitters = Executors.newFixedThreadPool(4);
    final CountDownLatch submittersReady = new CountDownLatch(4);
    final CountDownLatch startSubmissions = new CountDownLatch(1);
    final CountDownLatch dropStarted = new CountDownLatch(1);
    final CountDownLatch releaseDrops = new CountDownLatch(1);
    store.start();
    executor.init(1);
    executor.startWorkers();
    try {
      final List<Future<Long>> submissions = new ArrayList<>();
      for (int i = 0; i < 4; i++) {
        final DropTableProcedure dropProcedure =
            new DropTableProcedure("db", "table" + i, "drop" + i, false) {
              @Override
              protected Flow executeFromState(
                  final ConfigNodeProcedureEnv env, final DropTableState state)
                  throws InterruptedException {
                dropStarted.countDown();
                releaseDrops.await();
                return Flow.NO_MORE_STATE;
              }
            };
        submissions.add(
            submitters.submit(
                () -> {
                  submittersReady.countDown();
                  startSubmissions.await();
                  return executor.submitProcedureAndPersist(dropProcedure);
                }));
      }
      Assert.assertTrue(submittersReady.await(10, TimeUnit.SECONDS));
      startSubmissions.countDown();
      for (final Future<Long> submission : submissions) {
        submission.get(10, TimeUnit.SECONDS);
      }
      Assert.assertTrue(dropStarted.await(10, TimeUnit.SECONDS));
      Assert.assertEquals(1, executor.getDropTableWorkerThreadCount());
      releaseDrops.countDown();
      for (final Future<Long> submission : submissions) {
        ProcedureTestUtil.waitForProcedure(executor, submission.get());
      }
      Assert.assertEquals(4, executor.getCompletedProcedures().size());
    } finally {
      startSubmissions.countDown();
      releaseDrops.countDown();
      submitters.shutdownNow();
      executor.stop();
      executor.join();
      store.stop();
    }
  }

  @Test
  public void serializeDeserializeTest() throws IllegalPathException, IOException {
    final DropTableProcedure dropTableProcedure =
        new DropTableProcedure("database1", "table1", "0", false);

    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    final DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    dropTableProcedure.serialize(dataOutputStream);

    final ByteBuffer byteBuffer = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());

    Assert.assertEquals(ProcedureType.DROP_TABLE_PROCEDURE.getTypeCode(), byteBuffer.getShort());

    final DropTableProcedure deserializedProcedure = new DropTableProcedure(false);
    deserializedProcedure.deserialize(byteBuffer);

    Assert.assertEquals(dropTableProcedure, deserializedProcedure);
    Assert.assertNull(deserializedProcedure.getModFileProgress());
    Assert.assertFalse(byteBuffer.hasRemaining());
  }

  @Test
  public void aggregateModProgressCountsEachRegionOnItsExecutingReplica() {
    final TConsensusGroupId firstRegion = new TConsensusGroupId(TConsensusGroupType.DataRegion, 1);
    final TConsensusGroupId secondRegion = new TConsensusGroupId(TConsensusGroupType.DataRegion, 2);
    final Map<TConsensusGroupId, TDataNodeLocation> targets =
        Map.of(
            firstRegion, new TDataNodeLocation().setDataNodeId(1),
            secondRegion, new TDataNodeLocation().setDataNodeId(2));
    final Map<Integer, TGetDropTableModProgressResp> responses =
        Map.of(
            1,
                new TGetDropTableModProgressResp(StatusUtils.OK)
                    .setRegionProgress(
                        Map.of(1, new TDropTableModProgress(2).setRemainingModFileCount(3))),
            2,
                new TGetDropTableModProgressResp(StatusUtils.OK)
                    .setRegionProgress(
                        Map.of(
                            1, new TDropTableModProgress(100).setRemainingModFileCount(100),
                            2, new TDropTableModProgress(5).setRemainingModFileCount(7))));

    Assert.assertEquals(
        new TDropTableModProgress(7).setRemainingModFileCount(10),
        DropTableProcedure.aggregateModFileProgress(targets, responses));
  }

  @Test
  public void aggregateModProgressBeforeAllRegionsStart() {
    final Map<TConsensusGroupId, TDataNodeLocation> targets =
        Map.of(
            new TConsensusGroupId(TConsensusGroupType.DataRegion, 1),
                new TDataNodeLocation().setDataNodeId(1),
            new TConsensusGroupId(TConsensusGroupType.DataRegion, 2),
                new TDataNodeLocation().setDataNodeId(1));
    final Map<Integer, TGetDropTableModProgressResp> responses =
        Map.of(
            1,
            new TGetDropTableModProgressResp(StatusUtils.OK)
                .setRegionProgress(
                    Map.of(
                        1,
                        new TDropTableModProgress(2).setRemainingModFileCount(3),
                        2,
                        new TDropTableModProgress(0))));

    final TDropTableModProgress progress =
        DropTableProcedure.aggregateModFileProgress(targets, responses);
    Assert.assertEquals(2, progress.getWrittenModFileCount());
    Assert.assertFalse(progress.isSetRemainingModFileCount());
  }

  @Test
  public void aggregateUnavailableModProgressIsUnknown() {
    final Map<TConsensusGroupId, TDataNodeLocation> targets =
        Map.of(
            new TConsensusGroupId(TConsensusGroupType.DataRegion, 1),
            new TDataNodeLocation().setDataNodeId(1));
    Assert.assertNull(DropTableProcedure.aggregateModFileProgress(targets, Collections.emptyMap()));
    Assert.assertNull(
        DropTableProcedure.aggregateModFileProgress(
            targets, Map.of(1, new TGetDropTableModProgressResp(StatusUtils.OK))));
    Assert.assertNull(
        DropTableProcedure.aggregateModFileProgress(
            targets,
            Map.of(
                1,
                new TGetDropTableModProgressResp(StatusUtils.OK)
                    .setRegionProgress(Collections.emptyMap()))));
  }

  @Test
  public void concurrentModProgressQueriesWaitForInFlightRefresh() throws Exception {
    final DropTableProcedure procedure =
        new DropTableProcedure("database", "table", "query", false);
    final Field futureField = DropTableProcedure.class.getDeclaredField("modProgressRefreshFuture");
    futureField.setAccessible(true);
    final CountDownLatch waitingQueryStarted = new CountDownLatch(1);
    final CompletableFuture<Void> refreshFuture =
        new CompletableFuture<Void>() {
          @Override
          public Void join() {
            waitingQueryStarted.countDown();
            return super.join();
          }
        };
    futureField.set(procedure, refreshFuture);
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      final Future<?> waitingQuery = executor.submit(procedure::updateModFileProgress);
      Assert.assertTrue(waitingQueryStarted.await(10, TimeUnit.SECONDS));
      Assert.assertFalse(waitingQuery.isDone());
      refreshFuture.complete(null);
      waitingQuery.get(10, TimeUnit.SECONDS);
    } finally {
      refreshFuture.complete(null);
      futureField.set(procedure, null);
      executor.shutdownNow();
      Assert.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  @Test
  public void modProgressSurvivesSerialization() throws Exception {
    for (final boolean isGeneratedByPipe : new boolean[] {false, true}) {
      for (final TDropTableModProgress progress :
          new TDropTableModProgress[] {
            new TDropTableModProgress(4),
            new TDropTableModProgress(4).setRemainingModFileCount(6),
            new TDropTableModProgress(10).setRemainingModFileCount(0)
          }) {
        final DropTableProcedure procedure =
            new DropTableProcedure("database", "table", "query", isGeneratedByPipe);
        final Field progressField = DropTableProcedure.class.getDeclaredField("modFileProgress");
        progressField.setAccessible(true);
        progressField.set(procedure, progress);
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        procedure.serialize(new DataOutputStream(bytes));
        final ByteBuffer buffer = ByteBuffer.wrap(bytes.toByteArray());
        buffer.getShort();
        final DropTableProcedure recovered = new DropTableProcedure(isGeneratedByPipe);
        recovered.deserialize(buffer);
        Assert.assertEquals(progress, recovered.getModFileProgress());
        Assert.assertFalse(buffer.hasRemaining());
        // Callers cannot mutate the stored snapshot.
        recovered.getModFileProgress().setWrittenModFileCount(0);
        Assert.assertEquals(progress, recovered.getModFileProgress());
      }
    }
  }

  @Test
  public void deserializeProcedureWithoutModProgress() throws Exception {
    final DropTableProcedure procedure =
        new DropTableProcedure("database", "table", "query", false);
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    // The previous procedure format ended immediately after the superclass fields.
    procedure.innerSerialize(new DataOutputStream(bytes));
    final ByteBuffer buffer = ByteBuffer.wrap(bytes.toByteArray());
    final DropTableProcedure recovered = new DropTableProcedure(false);
    recovered.deserialize(buffer);
    Assert.assertEquals(procedure, recovered);
    Assert.assertNull(recovered.getModFileProgress());
    Assert.assertFalse(buffer.hasRemaining());
  }
}
