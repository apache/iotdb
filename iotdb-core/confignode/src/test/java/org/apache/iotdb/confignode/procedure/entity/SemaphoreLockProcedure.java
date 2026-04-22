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
import org.apache.iotdb.confignode.procedure.scheduler.LockQueue;
import org.apache.iotdb.confignode.procedure.state.ProcedureLockState;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A test procedure that uses a LockQueue to limit concurrency. It tracks the maximum concurrent
 * executions observed during the test.
 */
public class SemaphoreLockProcedure extends Procedure<TestProcEnv> {

  private final LockQueue semaphore;
  private final AtomicInteger concurrentCount;
  private final AtomicInteger maxConcurrent;
  private final long sleepMs;

  public SemaphoreLockProcedure(
      LockQueue semaphore,
      AtomicInteger concurrentCount,
      AtomicInteger maxConcurrent,
      long sleepMs) {
    this.semaphore = semaphore;
    this.concurrentCount = concurrentCount;
    this.maxConcurrent = maxConcurrent;
    this.sleepMs = sleepMs;
  }

  @Override
  protected Procedure<TestProcEnv>[] execute(TestProcEnv testProcEnv) throws InterruptedException {
    int current = concurrentCount.incrementAndGet();
    // Track the maximum number of concurrent executions
    maxConcurrent.updateAndGet(prev -> Math.max(prev, current));

    // Simulate some work
    Thread.sleep(sleepMs);

    concurrentCount.decrementAndGet();
    testProcEnv.getAcc().incrementAndGet();
    return null;
  }

  @Override
  protected void rollback(TestProcEnv testProcEnv) throws IOException, InterruptedException {}

  @Override
  protected ProcedureLockState acquireLock(TestProcEnv testProcEnv) {
    if (semaphore.tryLock(this)) {
      return ProcedureLockState.LOCK_ACQUIRED;
    }
    return ProcedureLockState.LOCK_EVENT_WAIT;
  }

  @Override
  protected void releaseLock(TestProcEnv testProcEnv) {
    if (semaphore.releaseLock(this)) {
      semaphore.wakeWaitingProcedures(testProcEnv.getScheduler());
    }
  }

  @Override
  protected void onLockEventWait(TestProcEnv testProcEnv) {
    semaphore.waitProcedure(this);
  }
}
