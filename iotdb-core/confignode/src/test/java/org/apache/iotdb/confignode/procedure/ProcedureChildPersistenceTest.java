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

import org.apache.iotdb.confignode.procedure.env.TestProcEnv;
import org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.state.ProcedureState;
import org.apache.iotdb.confignode.procedure.util.ProcedureTestUtil;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ProcedureChildPersistenceTest {
  @Test
  public void restartAfterPersistingOneChildDoesNotResubmit() throws Exception {
    restartAfterPersistingChildren(1);
  }

  @Test
  public void restartAfterPersistingMultipleChildrenDoesNotResubmit() throws Exception {
    restartAfterPersistingChildren(3);
  }

  private void restartAfterPersistingChildren(int childCount) throws Exception {
    ChildTestEnv env = new ChildTestEnv(childCount);
    RecordingStore store = new RecordingStore();
    ProcedureExecutor<ChildTestEnv> executor = startExecutor(env, store);
    CountDownLatch persisted = new CountDownLatch(1);
    store.afterBatch =
        () -> {
          // Simulate a crash at the persistence boundary, before children are scheduled.
          executor.stop();
          persisted.countDown();
        };
    long parentId;
    try {
      parentId = executor.submitProcedure(new ParentProcedure());
      assertTrue(persisted.await(10, TimeUnit.SECONDS));
    } finally {
      executor.stop();
      executor.join();
    }
    Procedure<?> savedParent = store.records.get(parentId);
    assertEquals(ProcedureState.WAITING, savedParent.getState());
    assertEquals(childCount + 1, store.records.size());
    assertEquals(0, env.childrenExecuted.get());

    store.afterBatch = null;
    ProcedureExecutor<ChildTestEnv> recovered = startExecutor(env, store);
    try {
      ProcedureTestUtil.waitForProcedure(recovered, parentId);
      assertTrue(recovered.isFinished(parentId));
      assertEquals(1, env.submissions.get());
      assertEquals(childCount, env.childrenExecuted.get());
      assertEquals(1, env.parentCompleted.get());
      assertTrue(
          recovered.getCompletedProcedures().stream()
              .anyMatch(p -> p.getProcId() == parentId && p.isSuccess()));
    } finally {
      recovered.stop();
      recovered.join();
      store.stop();
    }
  }

  @Test
  public void failedBatchWriteIsRetriedBeforeChildrenRun() throws Exception {
    ChildTestEnv env = new ChildTestEnv(3);
    RecordingStore store = new RecordingStore();
    store.failFirstBatch = true;
    ProcedureExecutor<ChildTestEnv> executor = startExecutor(env, store);
    try {
      long parentId = executor.submitProcedure(new ParentProcedure());
      ProcedureTestUtil.waitForProcedure(executor, parentId);
      assertTrue(executor.isFinished(parentId));
      assertEquals(2, store.batchAttempts.get());
      assertEquals(1, env.submissions.get());
      assertEquals(3, env.childrenExecuted.get());
      assertEquals(1, env.parentCompleted.get());
      assertTrue(
          executor.getCompletedProcedures().stream()
              .anyMatch(p -> p.getProcId() == parentId && p.isSuccess()));
    } finally {
      executor.stop();
      executor.join();
      store.stop();
    }
  }

  @Test
  public void stoppingDuringFailedBatchWriteDoesNotStartChildren() throws Exception {
    ChildTestEnv env = new ChildTestEnv(1);
    RecordingStore store = new RecordingStore();
    store.failEveryBatch = true;
    ProcedureExecutor<ChildTestEnv> executor = startExecutor(env, store);
    try {
      executor.submitProcedure(new ParentProcedure());
      assertTrue(store.batchAttempted.await(10, TimeUnit.SECONDS));
    } finally {
      executor.stop();
      executor.join();
      store.stop();
    }
    assertEquals(0, env.childrenExecuted.get());
    assertEquals(0, env.parentCompleted.get());
    assertEquals(1, store.records.size());
    assertEquals(ProcedureState.RUNNABLE, store.records.values().iterator().next().getState());
  }

  private ProcedureExecutor<ChildTestEnv> startExecutor(ChildTestEnv env, RecordingStore store) {
    env.store = store;
    store.start();
    ProcedureExecutor<ChildTestEnv> executor = new ProcedureExecutor<>(env, store);
    executor.init(2);
    executor.startWorkers();
    return executor;
  }

  private static class ChildTestEnv extends TestProcEnv {
    private final int childCount;
    private final AtomicInteger submissions = new AtomicInteger();
    private final AtomicInteger childrenExecuted = new AtomicInteger();
    private final AtomicInteger parentCompleted = new AtomicInteger();
    private RecordingStore store;

    private ChildTestEnv(int childCount) {
      this.childCount = childCount;
    }
  }

  private static class ParentProcedure extends StateMachineProcedure<ChildTestEnv, Integer> {
    @Override
    protected Flow executeFromState(ChildTestEnv env, Integer state) {
      if (state == 0) {
        env.submissions.incrementAndGet();
        for (int i = 0; i < env.childCount; i++) {
          addChildProcedure(new ChildProcedure());
        }
        setNextState(1);
        return Flow.HAS_MORE_STATE;
      }
      assertEquals(env.childCount, env.childrenExecuted.get());
      env.parentCompleted.incrementAndGet();
      return Flow.NO_MORE_STATE;
    }

    @Override
    protected void rollbackState(ChildTestEnv env, Integer state) {}

    @Override
    protected Integer getState(int stateId) {
      return stateId;
    }

    @Override
    protected int getStateId(Integer state) {
      return state;
    }

    @Override
    protected Integer getInitialState() {
      return 0;
    }
  }

  private static class ChildProcedure extends Procedure<ChildTestEnv> {
    @Override
    protected Procedure<ChildTestEnv>[] execute(ChildTestEnv env) {
      assertTrue(env.store.batchCommitted);
      env.childrenExecuted.incrementAndGet();
      return null;
    }

    @Override
    protected void rollback(ChildTestEnv env) {}
  }

  private static class RecordingStore extends NoopProcedureStore {
    private final Map<Long, Procedure> records = new HashMap<>();
    private final AtomicInteger batchAttempts = new AtomicInteger();
    private volatile boolean batchCommitted;
    private boolean failFirstBatch;
    private boolean failEveryBatch;
    private final CountDownLatch batchAttempted = new CountDownLatch(1);
    private Runnable afterBatch;

    @Override
    public synchronized void update(Procedure procedure) {
      records.put(procedure.getProcId(), copy(procedure));
    }

    @Override
    public synchronized void update(Procedure[] procedures) {
      int attempt = batchAttempts.incrementAndGet();
      batchAttempted.countDown();
      if (failEveryBatch || (attempt == 1 && failFirstBatch)) {
        throw new IllegalStateException("Injected failure before committing the batch");
      }
      for (Procedure procedure : procedures) {
        update(procedure);
      }
      batchCommitted = true;
      if (afterBatch != null) {
        afterBatch.run();
      }
    }

    @Override
    public synchronized List<Procedure> load() {
      List<Procedure> loaded = new ArrayList<>();
      records.values().forEach(procedure -> loaded.add(copy(procedure)));
      return loaded;
    }

    @Override
    public synchronized void delete(long[] ids) {
      for (long id : ids) {
        records.remove(id);
      }
    }

    private Procedure copy(Procedure procedure) {
      try {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        procedure.serialize(new DataOutputStream(bytes));
        Procedure restored =
            procedure instanceof ParentProcedure ? new ParentProcedure() : new ChildProcedure();
        restored.deserialize(ByteBuffer.wrap(bytes.toByteArray()));
        return restored;
      } catch (IOException e) {
        throw new AssertionError(e);
      }
    }
  }
}
