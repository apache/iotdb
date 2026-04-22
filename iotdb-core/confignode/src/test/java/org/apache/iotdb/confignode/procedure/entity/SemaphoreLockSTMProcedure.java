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

package org.apache.iotdb.confignode.procedure.entity;

import org.apache.iotdb.confignode.procedure.Procedure;
import org.apache.iotdb.confignode.procedure.env.TestProcEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.scheduler.LockQueue;
import org.apache.iotdb.confignode.procedure.state.ProcedureLockState;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A multi-step StateMachineProcedure that uses a LockQueue semaphore to limit concurrency. This
 * simulates the real RegionMigrateProcedure pattern: multiple states with child procedures that
 * take time to complete.
 *
 * <p>The procedure tracks concurrent execution across its ENTIRE lifecycle (from first state to
 * completion), not just within a single step. This exposes the bug where holdLock() returning false
 * causes the semaphore to be released between state transitions.
 *
 * @see SemaphoreLockProcedure for the single-step variant that cannot detect this bug
 */
public class SemaphoreLockSTMProcedure
    extends StateMachineProcedure<TestProcEnv, SemaphoreLockSTMProcedure.MigrateState> {

  private final LockQueue semaphore;
  private final AtomicInteger concurrentCount;
  private final AtomicInteger maxConcurrent;
  private final long childSleepMs;
  private final boolean shouldHoldLock;

  public enum MigrateState {
    PREPARE,
    ADD_PEER,
    CHANGE_LEADER,
    REMOVE_PEER,
    DONE
  }

  /**
   * @param semaphore the shared semaphore controlling concurrency
   * @param concurrentCount tracks the number of procedures currently executing
   * @param maxConcurrent records the peak concurrent count observed
   * @param childSleepMs how long each child procedure sleeps (simulating real work)
   * @param shouldHoldLock whether to override holdLock() to return true
   */
  public SemaphoreLockSTMProcedure(
      LockQueue semaphore,
      AtomicInteger concurrentCount,
      AtomicInteger maxConcurrent,
      long childSleepMs,
      boolean shouldHoldLock) {
    this.semaphore = semaphore;
    this.concurrentCount = concurrentCount;
    this.maxConcurrent = maxConcurrent;
    this.childSleepMs = childSleepMs;
    this.shouldHoldLock = shouldHoldLock;
  }

  @Override
  protected Flow executeFromState(TestProcEnv env, MigrateState state) throws InterruptedException {
    switch (state) {
      case PREPARE:
        // Track that this procedure is active (entered first state)
        int current = concurrentCount.incrementAndGet();
        maxConcurrent.updateAndGet(prev -> Math.max(prev, current));
        // Spawn a child procedure that simulates work
        addChildProcedure(new SleepChildProcedure(childSleepMs));
        setNextState(MigrateState.ADD_PEER);
        break;
      case ADD_PEER:
        // Re-check concurrent count after being resumed from child completion
        current = concurrentCount.get();
        maxConcurrent.updateAndGet(prev -> Math.max(prev, current));
        addChildProcedure(new SleepChildProcedure(childSleepMs));
        setNextState(MigrateState.CHANGE_LEADER);
        break;
      case CHANGE_LEADER:
        current = concurrentCount.get();
        maxConcurrent.updateAndGet(prev -> Math.max(prev, current));
        addChildProcedure(new SleepChildProcedure(childSleepMs));
        setNextState(MigrateState.REMOVE_PEER);
        break;
      case REMOVE_PEER:
        current = concurrentCount.get();
        maxConcurrent.updateAndGet(prev -> Math.max(prev, current));
        addChildProcedure(new SleepChildProcedure(childSleepMs));
        setNextState(MigrateState.DONE);
        break;
      case DONE:
        // Procedure is completing, decrement the counter
        concurrentCount.decrementAndGet();
        env.getAcc().incrementAndGet();
        return Flow.NO_MORE_STATE;
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected ProcedureLockState acquireLock(TestProcEnv env) {
    if (semaphore.tryLock(this)) {
      return ProcedureLockState.LOCK_ACQUIRED;
    }
    return ProcedureLockState.LOCK_EVENT_WAIT;
  }

  @Override
  protected void releaseLock(TestProcEnv env) {
    if (semaphore.releaseLock(this)) {
      semaphore.wakeWaitingProcedures(env.getScheduler());
    }
  }

  @Override
  protected void onLockEventWait(TestProcEnv env) {
    semaphore.waitProcedure(this);
  }

  @Override
  protected boolean holdLock(TestProcEnv env) {
    return shouldHoldLock;
  }

  @Override
  protected void rollbackState(TestProcEnv env, MigrateState state)
      throws IOException, InterruptedException, ProcedureException {}

  @Override
  protected boolean isRollbackSupported(MigrateState state) {
    return false;
  }

  @Override
  protected MigrateState getState(int stateId) {
    return MigrateState.values()[stateId];
  }

  @Override
  protected int getStateId(MigrateState state) {
    return state.ordinal();
  }

  @Override
  protected MigrateState getInitialState() {
    return MigrateState.PREPARE;
  }

  /**
   * A simple child procedure that sleeps for a specified duration to simulate real work (e.g.,
   * AddRegionPeer, RemoveRegionPeer RPC calls).
   */
  public static class SleepChildProcedure extends Procedure<TestProcEnv> {
    private final long sleepMs;

    public SleepChildProcedure(long sleepMs) {
      this.sleepMs = sleepMs;
    }

    @Override
    protected Procedure<TestProcEnv>[] execute(TestProcEnv env) throws InterruptedException {
      Thread.sleep(sleepMs);
      return null;
    }

    @Override
    protected void rollback(TestProcEnv env) throws IOException, InterruptedException {}
  }
}
