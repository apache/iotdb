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

import org.apache.iotdb.confignode.procedure.entity.IncProcedure;
import org.apache.iotdb.confignode.procedure.entity.NoopProcedure;
import org.apache.iotdb.confignode.procedure.entity.StuckProcedure;
import org.apache.iotdb.confignode.procedure.env.TestProcEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.state.ProcedureState;
import org.apache.iotdb.confignode.procedure.util.ProcedureTestUtil;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TestProcedureExecutor extends TestProcedureBase {

  @Override
  protected void initExecutor() {
    this.env = new TestProcEnv();
    this.procStore = new NoopProcedureStore();
    this.procExecutor = new ProcedureExecutor<>(env, procStore);
    this.env.setScheduler(this.procExecutor.getScheduler());
    this.procExecutor.init(2);
  }

  @Test
  public void testSubmitProcedure() {
    IncProcedure incProcedure = new IncProcedure();
    long procId = this.procExecutor.submitProcedure(incProcedure);
    ProcedureTestUtil.waitForProcedure(this.procExecutor, procId);
    TestProcEnv env = this.getEnv();
    AtomicInteger acc = env.getAcc();
    Assert.assertEquals(acc.get(), 1);
  }

  @Test
  public void testProcedureFailedDuringSubmissionIsRolledBack() throws InterruptedException {
    TestProcEnv localEnv = new TestProcEnv();
    FailOnFirstUpdateProcedureStore localStore = new FailOnFirstUpdateProcedureStore();
    ProcedureExecutor<TestProcEnv> localExecutor = new ProcedureExecutor<>(localEnv, localStore);
    localEnv.setScheduler(localExecutor.getScheduler());
    localStore.start();
    localExecutor.init(1);

    try {
      FailedDuringSubmissionProcedure procedure = new FailedDuringSubmissionProcedure();
      long procId = localExecutor.submitProcedure(procedure);

      boolean rollbackStackPersisted = localStore.isRollbackStackPersisted();
      localExecutor.startWorkers();
      Assert.assertTrue(rollbackStackPersisted);
      Assert.assertTrue(procedure.awaitRollback(10, TimeUnit.SECONDS));
      ProcedureTestUtil.waitForProcedure(localExecutor, procId);
      Assert.assertTrue(localExecutor.isFinished(procId));
      Assert.assertEquals(ProcedureState.ROLLEDBACK, procedure.getState());
      Assert.assertEquals(0, procedure.getExecutionCount());
      Assert.assertEquals(1, procedure.getRollbackCount());
    } finally {
      localStore.stop();
      localExecutor.stop();
      localExecutor.join();
      localStore.cleanup();
    }
  }

  @Test
  public void testFailedProcedureWithoutRollbackStackCanBeRecovered() throws InterruptedException {
    FailedDuringSubmissionProcedure procedure = new FailedDuringSubmissionProcedure();
    procedure.setProcId(0);
    procedure.setProcRunnable();
    procedure.setFailure(new ProcedureException("Failed to deserialize procedure"));

    TestProcEnv localEnv = new TestProcEnv();
    FailedProcedureStore localStore = new FailedProcedureStore(procedure);
    ProcedureExecutor<TestProcEnv> localExecutor = new ProcedureExecutor<>(localEnv, localStore);
    localEnv.setScheduler(localExecutor.getScheduler());
    localStore.start();
    localExecutor.init(1);
    localExecutor.startWorkers();

    try {
      Assert.assertTrue(procedure.awaitRollback(10, TimeUnit.SECONDS));
      ProcedureTestUtil.waitForProcedure(localExecutor, procedure.getProcId());
      Assert.assertTrue(localExecutor.isFinished(procedure.getProcId()));
      Assert.assertEquals(ProcedureState.ROLLEDBACK, procedure.getState());
      Assert.assertEquals(0, procedure.getExecutionCount());
      Assert.assertEquals(1, procedure.getRollbackCount());
    } finally {
      localStore.stop();
      localExecutor.stop();
      localExecutor.join();
      localStore.cleanup();
    }
  }

  @Test
  public void testWorkerThreadStuck() throws InterruptedException {
    Semaphore latch1 = new Semaphore(2);
    latch1.acquire(2);
    StuckProcedure busyProc1 = new StuckProcedure(latch1);

    Semaphore latch2 = new Semaphore(2);
    latch2.acquire(2);
    StuckProcedure busyProc2 = new StuckProcedure(latch2);

    long busyProcId1 = procExecutor.submitProcedure(busyProc1);
    long busyProcId2 = procExecutor.submitProcedure(busyProc2);
    long otherProcId = procExecutor.submitProcedure(new NoopProcedure());

    // wait until a new worker is being created
    int threads1 = waitThreadCount(3);
    LOG.info("new threads got created: " + (threads1 - 2));
    Assert.assertEquals(3, threads1);

    ProcedureTestUtil.waitForProcedure(procExecutor, otherProcId);
    Assert.assertEquals(true, procExecutor.isFinished(otherProcId));
    Assert.assertEquals(true, procExecutor.isRunning());
    Assert.assertEquals(false, procExecutor.isFinished(busyProcId1));
    Assert.assertEquals(false, procExecutor.isFinished(busyProcId2));

    // terminate the busy procedures
    latch1.release();
    latch2.release();

    LOG.info("set keep alive and wait threads being removed");
    int threads2 = waitThreadCount(2);
    LOG.info("threads got removed: " + (threads1 - threads2));
    Assert.assertEquals(2, threads2);

    // terminate the busy procedures
    latch1.release();
    latch2.release();

    // wait for all procs to complete
    ProcedureTestUtil.waitForProcedure(procExecutor, busyProcId1);
    ProcedureTestUtil.waitForProcedure(procExecutor, busyProcId2);
  }

  @Test
  public void testDuplicatedSchedulingDoesNotExecuteConcurrently() throws InterruptedException {
    BlockingProcedure blockingProcedure = new BlockingProcedure();
    long procId = procExecutor.submitProcedure(blockingProcedure);

    Assert.assertTrue(blockingProcedure.awaitExecution(30, TimeUnit.SECONDS));

    procExecutor.getScheduler().addFront(blockingProcedure);
    boolean duplicated = blockingProcedure.awaitExecution(3, TimeUnit.SECONDS);

    blockingProcedure.releaseExecutions(duplicated ? 2 : 1);
    ProcedureTestUtil.waitForProcedure(procExecutor, procId);

    Assert.assertFalse(duplicated);
    Assert.assertEquals(1, blockingProcedure.getExecutionCount());
  }

  @Test
  public void testInternalProcedureCanBeDeduplicatedAndRemoved() throws InterruptedException {
    CompletingInternalProcedure internalProcedure = new CompletingInternalProcedure();

    procExecutor.addInternalProcedure(internalProcedure);
    procExecutor.addInternalProcedure(internalProcedure);

    Assert.assertTrue(internalProcedure.awaitExecution(30, TimeUnit.SECONDS));
    Assert.assertFalse(internalProcedure.awaitExecution(300, TimeUnit.MILLISECONDS));
    Assert.assertEquals(1, internalProcedure.getExecutionCount());

    procExecutor.addInternalProcedure(internalProcedure);
    Assert.assertFalse(internalProcedure.awaitExecution(300, TimeUnit.MILLISECONDS));
    Assert.assertEquals(1, internalProcedure.getExecutionCount());

    Assert.assertTrue(procExecutor.removeInternalProcedure(internalProcedure));

    procExecutor.addInternalProcedure(internalProcedure);
    Assert.assertTrue(internalProcedure.awaitExecution(30, TimeUnit.SECONDS));
    Assert.assertEquals(2, internalProcedure.getExecutionCount());

    Assert.assertTrue(procExecutor.removeInternalProcedure(internalProcedure));
  }

  @Test
  public void testRestartCompletedCleanerAppliesNewEvictTtl() {
    procExecutor.startCompletedCleaner(30, 60);
    CompletedProcedureRecycler<TestProcEnv> first = procExecutor.getCompletedProcedureRecycler();
    Assert.assertNotNull(first);
    Assert.assertEquals(TimeUnit.SECONDS.toMillis(60), first.getEvictTTLInMs());

    // Hot reload with a different interval / TTL replaces the recycler with a fresh instance.
    procExecutor.restartCompletedCleaner(15, 120);
    CompletedProcedureRecycler<TestProcEnv> second = procExecutor.getCompletedProcedureRecycler();
    Assert.assertNotSame(first, second);
    Assert.assertEquals(TimeUnit.SECONDS.toMillis(120), second.getEvictTTLInMs());
  }

  private int waitThreadCount(final int expectedThreads) {
    long startTime = System.currentTimeMillis();
    while (procExecutor.isRunning()
        && TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - startTime) <= 180) {
      if (procExecutor.getWorkerThreadCount() == expectedThreads) {
        break;
      }
      ProcedureTestUtil.sleepWithoutInterrupt(250);
    }
    return procExecutor.getWorkerThreadCount();
  }

  private static class BlockingProcedure extends Procedure<TestProcEnv> {

    private final Semaphore entered = new Semaphore(0);
    private final Semaphore finish = new Semaphore(0);
    private final AtomicInteger executionCount = new AtomicInteger();

    @Override
    protected Procedure<TestProcEnv>[] execute(TestProcEnv env) throws InterruptedException {
      executionCount.incrementAndGet();
      entered.release();
      finish.acquire();
      return null;
    }

    @Override
    protected void rollback(TestProcEnv env)
        throws IOException, InterruptedException, ProcedureException {
      // No state to roll back.
    }

    private boolean awaitExecution(long timeout, TimeUnit unit) throws InterruptedException {
      return entered.tryAcquire(timeout, unit);
    }

    private void releaseExecutions(int permits) {
      finish.release(permits);
    }

    private int getExecutionCount() {
      return executionCount.get();
    }
  }

  private static class CompletingInternalProcedure extends InternalProcedure<TestProcEnv> {

    private final Semaphore entered = new Semaphore(0);
    private final AtomicInteger executionCount = new AtomicInteger();

    private CompletingInternalProcedure() {
      super(0);
    }

    @Override
    protected void periodicExecute(TestProcEnv env) {
      executionCount.incrementAndGet();
      entered.release();
      setState(ProcedureState.SUCCESS);
    }

    private boolean awaitExecution(long timeout, TimeUnit unit) throws InterruptedException {
      return entered.tryAcquire(timeout, unit);
    }

    private int getExecutionCount() {
      return executionCount.get();
    }
  }

  private static class FailOnFirstUpdateProcedureStore extends NoopProcedureStore {

    private final AtomicBoolean firstUpdate = new AtomicBoolean(true);
    private final AtomicBoolean rollbackStackPersisted = new AtomicBoolean(false);

    @Override
    public void update(Procedure procedure) {
      if (firstUpdate.compareAndSet(true, false)) {
        procedure.setFailure(new ProcedureException("Failed to deserialize procedure"));
      } else if (procedure.isFailed() && procedure.wasExecuted()) {
        rollbackStackPersisted.set(true);
      }
    }

    private boolean isRollbackStackPersisted() {
      return rollbackStackPersisted.get();
    }
  }

  private static class FailedProcedureStore extends NoopProcedureStore {

    private final Procedure procedure;

    private FailedProcedureStore(Procedure procedure) {
      this.procedure = procedure;
    }

    @Override
    public List<Procedure> load() {
      return Collections.singletonList(procedure);
    }
  }

  private static class FailedDuringSubmissionProcedure extends Procedure<TestProcEnv> {

    private final Semaphore rolledBack = new Semaphore(0);
    private final AtomicInteger executionCount = new AtomicInteger();
    private final AtomicInteger rollbackCount = new AtomicInteger();

    @Override
    protected Procedure<TestProcEnv>[] execute(TestProcEnv env) {
      executionCount.incrementAndGet();
      return null;
    }

    @Override
    protected void rollback(TestProcEnv env) {
      rollbackCount.incrementAndGet();
      rolledBack.release();
    }

    private boolean awaitRollback(long timeout, TimeUnit unit) throws InterruptedException {
      return rolledBack.tryAcquire(timeout, unit);
    }

    private int getExecutionCount() {
      return executionCount.get();
    }

    private int getRollbackCount() {
      return rollbackCount.get();
    }
  }
}
