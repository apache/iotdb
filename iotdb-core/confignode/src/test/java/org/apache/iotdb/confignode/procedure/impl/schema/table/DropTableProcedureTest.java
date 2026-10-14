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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.confignode.procedure.NoopProcedureStore;
import org.apache.iotdb.confignode.procedure.Procedure;
import org.apache.iotdb.confignode.procedure.ProcedureExecutor;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.impl.schema.table.view.DropViewProcedure;
import org.apache.iotdb.confignode.procedure.state.schema.DropTableState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.confignode.procedure.util.ProcedureTestUtil;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
  }
}
