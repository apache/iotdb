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

package org.apache.iotdb.confignode.manager.pipe.coordinator.task;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class PipeTaskCoordinatorLockTest {

  @Test
  public void testRepeatedUnlockDoesNotIncreaseCapacity() throws Exception {
    PipeTaskCoordinatorLock lock = new PipeTaskCoordinatorLock();

    lock.lock();
    lock.unlock();
    lock.unlock();
    Assert.assertTrue(lock.tryLock());

    AtomicBoolean acquired = new AtomicBoolean(false);
    Thread waiter =
        new Thread(
            () -> {
              lock.lock();
              acquired.set(true);
            });
    waiter.start();
    waitUntilState(waiter, Thread.State.WAITING);
    Assert.assertFalse(acquired.get());

    lock.unlock();
    waiter.join(TimeUnit.SECONDS.toMillis(5));
    Assert.assertFalse(waiter.isAlive());
    Assert.assertTrue(acquired.get());
    lock.unlock();
  }

  @Test
  public void testInterruptedLockDoesNotReturnBeforeAcquired() throws Exception {
    PipeTaskCoordinatorLock lock = new PipeTaskCoordinatorLock();
    AtomicBoolean acquired = new AtomicBoolean(false);
    AtomicBoolean interruptedAfterLock = new AtomicBoolean(false);

    lock.lock();
    Thread waiter =
        new Thread(
            () -> {
              Thread.currentThread().interrupt();
              lock.lock();
              acquired.set(true);
              interruptedAfterLock.set(Thread.currentThread().isInterrupted());
            });
    waiter.start();
    waitUntilState(waiter, Thread.State.WAITING);
    Assert.assertFalse(acquired.get());

    lock.unlock();
    waiter.join(TimeUnit.SECONDS.toMillis(5));
    Assert.assertFalse(waiter.isAlive());
    Assert.assertTrue(acquired.get());
    Assert.assertTrue(interruptedAfterLock.get());
    lock.unlock();
  }

  @Test
  public void testInterruptedTryLockDoesNotAcquire() {
    PipeTaskCoordinatorLock lock = new PipeTaskCoordinatorLock();

    Thread.currentThread().interrupt();
    try {
      Assert.assertFalse(lock.tryLock());
      Assert.assertFalse(lock.isLocked());
      Assert.assertTrue(Thread.currentThread().isInterrupted());
    } finally {
      Thread.interrupted();
    }
  }

  private void waitUntilState(Thread thread, Thread.State expectedState) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (System.nanoTime() < deadline) {
      if (thread.getState() == expectedState) {
        return;
      }
      Thread.sleep(10);
    }
    Assert.assertEquals(expectedState, thread.getState());
  }
}
