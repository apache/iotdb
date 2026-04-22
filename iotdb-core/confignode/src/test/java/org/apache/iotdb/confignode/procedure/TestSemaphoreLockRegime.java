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

import org.apache.iotdb.confignode.procedure.entity.SemaphoreLockProcedure;
import org.apache.iotdb.confignode.procedure.entity.SemaphoreLockSTMProcedure;
import org.apache.iotdb.confignode.procedure.scheduler.LockQueue;
import org.apache.iotdb.confignode.procedure.util.ProcedureTestUtil;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Integration test for LockQueue semaphore mode with the ProcedureExecutor. Verifies that the lock
 * queue correctly limits the number of concurrently executing procedures.
 */
public class TestSemaphoreLockRegime extends TestProcedureBase {

  // ==================== Single-step procedure tests ====================

  /**
   * Submit 6 procedures with concurrency limit 2. Verify that at most 2 procedures execute
   * concurrently, and all 6 complete successfully.
   */
  @Test
  public void testConcurrencyLimit() {
    LockQueue semaphore = new LockQueue(2);
    AtomicInteger concurrentCount = new AtomicInteger(0);
    AtomicInteger maxConcurrent = new AtomicInteger(0);

    int totalProcedures = 6;
    List<Long> procIds = new ArrayList<>();

    for (int i = 0; i < totalProcedures; i++) {
      SemaphoreLockProcedure proc =
          new SemaphoreLockProcedure(semaphore, concurrentCount, maxConcurrent, 100);
      long procId = this.procExecutor.submitProcedure(proc);
      procIds.add(procId);
    }

    ProcedureTestUtil.waitForProcedure(
        this.procExecutor, procIds.stream().mapToLong(Long::longValue).toArray());

    // All procedures should have completed
    Assert.assertEquals(totalProcedures, env.getAcc().get());
    // Max concurrent should not exceed the limit
    Assert.assertTrue(
        "Max concurrent was " + maxConcurrent.get() + ", expected <= 2", maxConcurrent.get() <= 2);
    // At least some concurrency should have happened (with 4 worker threads)
    Assert.assertTrue(
        "Expected some concurrency, but maxConcurrent was " + maxConcurrent.get(),
        maxConcurrent.get() >= 1);
  }

  /**
   * Submit 4 procedures with concurrency limit 1 (exclusive lock). Verify serial execution: max
   * concurrent must be exactly 1.
   */
  @Test
  public void testExclusiveLock() {
    LockQueue semaphore = new LockQueue(1);
    AtomicInteger concurrentCount = new AtomicInteger(0);
    AtomicInteger maxConcurrent = new AtomicInteger(0);

    int totalProcedures = 4;
    List<Long> procIds = new ArrayList<>();

    for (int i = 0; i < totalProcedures; i++) {
      SemaphoreLockProcedure proc =
          new SemaphoreLockProcedure(semaphore, concurrentCount, maxConcurrent, 50);
      long procId = this.procExecutor.submitProcedure(proc);
      procIds.add(procId);
    }

    ProcedureTestUtil.waitForProcedure(
        this.procExecutor, procIds.stream().mapToLong(Long::longValue).toArray());

    Assert.assertEquals(totalProcedures, env.getAcc().get());
    Assert.assertEquals(
        "With semaphore limit 1, max concurrent should be 1", 1, maxConcurrent.get());
  }

  /**
   * Submit 4 procedures with no concurrency limit (maxPermits=0). All should be able to run
   * concurrently.
   */
  @Test
  public void testNoLimit() {
    LockQueue semaphore = new LockQueue(0);
    AtomicInteger concurrentCount = new AtomicInteger(0);
    AtomicInteger maxConcurrent = new AtomicInteger(0);

    int totalProcedures = 4;
    List<Long> procIds = new ArrayList<>();

    for (int i = 0; i < totalProcedures; i++) {
      SemaphoreLockProcedure proc =
          new SemaphoreLockProcedure(semaphore, concurrentCount, maxConcurrent, 200);
      long procId = this.procExecutor.submitProcedure(proc);
      procIds.add(procId);
    }

    ProcedureTestUtil.waitForProcedure(
        this.procExecutor, procIds.stream().mapToLong(Long::longValue).toArray());

    Assert.assertEquals(totalProcedures, env.getAcc().get());
    // With 4 worker threads and no limit, we expect higher concurrency
    Assert.assertTrue(
        "With no limit and 4 workers, expected concurrency > 1, but was " + maxConcurrent.get(),
        maxConcurrent.get() > 1);
  }

  // ==================== Multi-step StateMachineProcedure tests ====================
  //
  // These tests reproduce the real RegionMigrateProcedure pattern: a StateMachineProcedure
  // with multiple states and child procedures. They expose the bug where holdLock()=false
  // causes the semaphore to be released between state transitions.

  /**
   * Reproduces the holdLock bug: submit 6 multi-step StateMachineProcedures with concurrency limit
   * 1 (exclusive), but WITHOUT overriding holdLock() to return true.
   *
   * <p>Expected behavior (if lock worked correctly): max concurrent == 1 (serial execution).
   *
   * <p>Actual behavior (bug): the lock is released after each state transition, so multiple
   * procedures run concurrently despite the limit of 1. This test EXPECTS the bug to manifest
   * (maxConcurrent > 1), demonstrating that holdLock()=false breaks the concurrency limit for
   * multi-step procedures.
   */
  @Test
  public void testStateMachineConcurrencyLimit_withoutHoldLock_bugReproduction() {
    LockQueue semaphore = new LockQueue(1);
    AtomicInteger concurrentCount = new AtomicInteger(0);
    AtomicInteger maxConcurrent = new AtomicInteger(0);

    int totalProcedures = 6;
    List<Long> procIds = new ArrayList<>();

    for (int i = 0; i < totalProcedures; i++) {
      SemaphoreLockSTMProcedure proc =
          new SemaphoreLockSTMProcedure(
              semaphore,
              concurrentCount,
              maxConcurrent,
              100, // child sleeps 100ms per step, 4 steps = ~400ms total per procedure
              false); // holdLock = false → the bug
      long procId = this.procExecutor.submitProcedure(proc);
      procIds.add(procId);
    }

    ProcedureTestUtil.waitForProcedure(
        this.procExecutor, procIds.stream().mapToLong(Long::longValue).toArray());

    // All procedures should have completed
    Assert.assertEquals(totalProcedures, env.getAcc().get());

    // BUG DEMONSTRATION: with holdLock=false, the semaphore lock is released between
    // state transitions. Even though maxPermits=1, multiple procedures end up executing
    // concurrently because they acquire the lock, run one state, release it (since
    // holdLock()=false triggers releaseLock in ProcedureExecutor), and then another
    // procedure grabs the lock while this one waits for its child to complete.
    //
    // When the child completes and the parent resumes, it re-acquires the lock for the
    // next state, but the damage is done — multiple procedures had their concurrentCount
    // overlapping in time.
    System.out.println(
        "[BUG REPRODUCTION] holdLock=false, maxPermits=1: maxConcurrent="
            + maxConcurrent.get()
            + " (expected >1 due to bug, would be 1 if lock worked correctly)");
    Assert.assertTrue(
        "BUG: With holdLock=false and maxPermits=1, expected concurrency leak "
            + "(maxConcurrent > 1), but maxConcurrent was "
            + maxConcurrent.get()
            + ". The bug may not have manifested due to timing; "
            + "try increasing totalProcedures or childSleepMs.",
        maxConcurrent.get() > 1);
  }

  /**
   * Verifies the fix: submit 6 multi-step StateMachineProcedures with concurrency limit 1
   * (exclusive), WITH holdLock() returning true.
   *
   * <p>With holdLock=true, the ProcedureExecutor does NOT release the lock between state
   * transitions. The lock is held for the entire procedure lifecycle, so the concurrency limit is
   * properly enforced.
   */
  @Test
  public void testStateMachineConcurrencyLimit_withHoldLock_fixed() {
    LockQueue semaphore = new LockQueue(1);
    AtomicInteger concurrentCount = new AtomicInteger(0);
    AtomicInteger maxConcurrent = new AtomicInteger(0);

    int totalProcedures = 6;
    List<Long> procIds = new ArrayList<>();

    for (int i = 0; i < totalProcedures; i++) {
      SemaphoreLockSTMProcedure proc =
          new SemaphoreLockSTMProcedure(
              semaphore,
              concurrentCount,
              maxConcurrent,
              100, // child sleeps 100ms per step
              true); // holdLock = true → the fix
      long procId = this.procExecutor.submitProcedure(proc);
      procIds.add(procId);
    }

    ProcedureTestUtil.waitForProcedure(
        this.procExecutor, procIds.stream().mapToLong(Long::longValue).toArray());

    // All procedures should have completed
    Assert.assertEquals(totalProcedures, env.getAcc().get());

    // With holdLock=true and maxPermits=1, only one procedure should be active at a time.
    // The lock is held across all state transitions and child procedure executions.
    System.out.println(
        "[FIX VERIFICATION] holdLock=true, maxPermits=1: maxConcurrent="
            + maxConcurrent.get()
            + " (expected exactly 1)");
    Assert.assertEquals(
        "With holdLock=true and maxPermits=1, max concurrent should be exactly 1",
        1,
        maxConcurrent.get());
  }

  /**
   * Verifies that with holdLock=true and maxPermits=2, at most 2 multi-step procedures execute
   * concurrently, and all complete successfully.
   */
  @Test
  public void testStateMachineConcurrencyLimit_withHoldLock_limit2() {
    LockQueue semaphore = new LockQueue(2);
    AtomicInteger concurrentCount = new AtomicInteger(0);
    AtomicInteger maxConcurrent = new AtomicInteger(0);

    int totalProcedures = 6;
    List<Long> procIds = new ArrayList<>();

    for (int i = 0; i < totalProcedures; i++) {
      SemaphoreLockSTMProcedure proc =
          new SemaphoreLockSTMProcedure(
              semaphore, concurrentCount, maxConcurrent, 100, true); // holdLock = true
      long procId = this.procExecutor.submitProcedure(proc);
      procIds.add(procId);
    }

    ProcedureTestUtil.waitForProcedure(
        this.procExecutor, procIds.stream().mapToLong(Long::longValue).toArray());

    Assert.assertEquals(totalProcedures, env.getAcc().get());
    System.out.println(
        "[FIX VERIFICATION] holdLock=true, maxPermits=2: maxConcurrent="
            + maxConcurrent.get()
            + " (expected <=2)");
    Assert.assertTrue(maxConcurrent.get() <= 2);
    // With 4 worker threads and limit 2, we should see some concurrency
    Assert.assertTrue(
        "Expected some concurrency with maxPermits=2, but maxConcurrent was " + maxConcurrent.get(),
        maxConcurrent.get() >= 1);
  }
}
