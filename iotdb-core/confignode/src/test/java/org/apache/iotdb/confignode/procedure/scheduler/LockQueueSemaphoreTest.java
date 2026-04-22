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

package org.apache.iotdb.confignode.procedure.scheduler;

import org.apache.iotdb.confignode.procedure.Procedure;
import org.apache.iotdb.confignode.procedure.entity.IncProcedure;
import org.apache.iotdb.confignode.procedure.env.TestProcEnv;

import org.junit.Assert;
import org.junit.Test;

/** Tests for {@link LockQueue} semaphore mode (maxPermits > 1). */
public class LockQueueSemaphoreTest {

  private Procedure<TestProcEnv> createProcedureWithId(long procId) {
    IncProcedure proc = new IncProcedure();
    proc.setProcId(procId);
    return proc;
  }

  @Test
  public void testExclusiveModeDefault() {
    // Default constructor: maxPermits = 1
    LockQueue queue = new LockQueue();
    Procedure<TestProcEnv> proc1 = createProcedureWithId(1);
    Procedure<TestProcEnv> proc2 = createProcedureWithId(2);

    Assert.assertTrue(queue.tryLock(proc1));
    // Second should fail in exclusive mode
    Assert.assertFalse(queue.tryLock(proc2));

    queue.releaseLock(proc1);
    Assert.assertTrue(queue.tryLock(proc2));
  }

  @Test
  public void testSemaphoreModeBasic() {
    LockQueue queue = new LockQueue(2);
    Procedure<TestProcEnv> proc1 = createProcedureWithId(1);
    Procedure<TestProcEnv> proc2 = createProcedureWithId(2);
    Procedure<TestProcEnv> proc3 = createProcedureWithId(3);

    Assert.assertTrue(queue.tryLock(proc1));
    Assert.assertTrue(queue.tryLock(proc2));
    // Third should fail
    Assert.assertFalse(queue.tryLock(proc3));

    // Release one, then third should succeed
    queue.releaseLock(proc1);
    Assert.assertTrue(queue.tryLock(proc3));
  }

  @Test
  public void testReentrant() {
    LockQueue queue = new LockQueue(2);
    Procedure<TestProcEnv> proc1 = createProcedureWithId(1);

    Assert.assertTrue(queue.tryLock(proc1));
    // Same procedure should re-acquire without consuming extra permits
    Assert.assertTrue(queue.tryLock(proc1));

    // Only one permit consumed, so second procedure should still succeed
    Procedure<TestProcEnv> proc2 = createProcedureWithId(2);
    Assert.assertTrue(queue.tryLock(proc2));
  }

  @Test
  public void testReleaseNonHolder() {
    LockQueue queue = new LockQueue(2);
    Procedure<TestProcEnv> proc1 = createProcedureWithId(1);
    Procedure<TestProcEnv> proc2 = createProcedureWithId(2);

    queue.tryLock(proc1);
    Assert.assertFalse(queue.releaseLock(proc2));
  }

  @Test
  public void testNoLimitMode() {
    LockQueue queue = new LockQueue(0);

    for (int i = 0; i < 100; i++) {
      Assert.assertTrue(queue.tryLock(createProcedureWithId(i)));
    }
  }

  @Test
  public void testNegativeLimitMeansNoLimit() {
    LockQueue queue = new LockQueue(-1);

    for (int i = 0; i < 50; i++) {
      Assert.assertTrue(queue.tryLock(createProcedureWithId(i)));
    }
  }

  @Test
  public void testWakeWaitingProcedures() {
    LockQueue queue = new LockQueue(1);
    SimpleProcedureScheduler scheduler = new SimpleProcedureScheduler();

    Procedure<TestProcEnv> proc1 = createProcedureWithId(1);
    Procedure<TestProcEnv> proc2 = createProcedureWithId(2);

    queue.waitProcedure(proc1);
    queue.waitProcedure(proc2);

    int woken = queue.wakeWaitingProcedures(scheduler);
    Assert.assertEquals(2, woken);
    Assert.assertEquals(2, scheduler.queueSize());
  }

  @Test
  public void testDynamicPermitAdjustment() {
    LockQueue queue = new LockQueue(2);
    Procedure<TestProcEnv> proc1 = createProcedureWithId(1);
    Procedure<TestProcEnv> proc2 = createProcedureWithId(2);
    Procedure<TestProcEnv> proc3 = createProcedureWithId(3);

    queue.tryLock(proc1);
    queue.tryLock(proc2);
    Assert.assertFalse(queue.tryLock(proc3));

    // Increase limit to 3
    queue.setMaxPermits(3);
    Assert.assertTrue(queue.tryLock(proc3));
  }
}
