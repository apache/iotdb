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
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.state.ProcedureState;
import org.apache.iotdb.confignode.procedure.state.schema.DropTableColumnState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.confignode.procedure.util.ProcedureTestUtil;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class DropTableColumnProcedureTest {
  @Test
  public void testColumnFailurePersistenceDoesNotBlockShutdown() throws Exception {
    final AtomicBoolean allowCompletion = new AtomicBoolean();
    final CountDownLatch completionRetried = new CountDownLatch(1);
    final AtomicInteger attempts = new AtomicInteger();
    final NoopProcedureStore store =
        new NoopProcedureStore() {
          @Override
          public void update(Procedure procedure) {
            if (procedure.getState() == ProcedureState.ROLLEDBACK && !allowCompletion.get()) {
              if (attempts.incrementAndGet() >= 2) {
                completionRetried.countDown();
              }
              throw new IllegalStateException("column failure store unavailable");
            }
          }
        };
    final DropTableColumnProcedure procedure =
        new DropTableColumnProcedure(false) {
          @Override
          protected Flow executeFromState(ConfigNodeProcedureEnv env, DropTableColumnState state) {
            setFailure(new ProcedureException("column deletion failed"));
            return Flow.NO_MORE_STATE;
          }
        };
    final ProcedureExecutor<ConfigNodeProcedureEnv> executor = new ProcedureExecutor<>(null, store);
    final ExecutorService joiner = Executors.newSingleThreadExecutor();
    store.start();
    executor.init(1);
    executor.startWorkers();
    try {
      executor.submitProcedureAndPersist(procedure);
      Assert.assertTrue(completionRetried.await(10, TimeUnit.SECONDS));
      executor.stop();
      final Future<?> stopped = joiner.submit(executor::join);
      stopped.get(5, TimeUnit.SECONDS);
      Assert.assertFalse(executor.isCompleted(procedure.getProcId()));
      Assert.assertTrue(executor.getProcedures().containsKey(procedure.getProcId()));
    } finally {
      allowCompletion.set(true);
      executor.stop();
      executor.join();
      joiner.shutdownNow();
      store.stop();
    }
  }

  @Test
  public void testColumnCompletionWaitsForPersistence() throws Exception {
    final AtomicBoolean allowCompletion = new AtomicBoolean();
    final AtomicInteger attempts = new AtomicInteger();
    final CountDownLatch completionRetried = new CountDownLatch(1);
    final NoopProcedureStore store =
        new NoopProcedureStore() {
          @Override
          public void update(Procedure procedure) {
            if (procedure.isSuccess() && !allowCompletion.get()) {
              if (attempts.incrementAndGet() >= 2) {
                completionRetried.countDown();
              }
              throw new IllegalStateException("column completion store unavailable");
            }
          }
        };
    final DropTableColumnProcedure procedure =
        new DropTableColumnProcedure(false) {
          @Override
          protected Flow executeFromState(ConfigNodeProcedureEnv env, DropTableColumnState state) {
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
      Assert.assertFalse(executor.isCompleted(procedure.getProcId()));
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
  public void serializeDeserializeTest() throws IllegalPathException, IOException {
    final DropTableColumnProcedure dropTableColumnProcedure =
        new DropTableColumnProcedure("database1", "table1", "0", "columnName", false);

    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    final DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    dropTableColumnProcedure.serialize(dataOutputStream);

    final ByteBuffer byteBuffer = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());

    Assert.assertEquals(
        ProcedureType.DROP_TABLE_COLUMN_PROCEDURE.getTypeCode(), byteBuffer.getShort());

    final DropTableColumnProcedure deserializedProcedure = new DropTableColumnProcedure(false);
    deserializedProcedure.deserialize(byteBuffer);

    Assert.assertEquals(dropTableColumnProcedure, deserializedProcedure);
  }
}
