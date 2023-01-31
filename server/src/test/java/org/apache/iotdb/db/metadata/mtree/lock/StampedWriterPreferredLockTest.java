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
package org.apache.iotdb.db.metadata.mtree.lock;

import org.apache.iotdb.db.metadata.mtree.store.StampedWriterPreferredLock;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class StampedWriterPreferredLockTest {

  @Test
  public void testStampReadLock() {
    StampedWriterPreferredLock lock = new StampedWriterPreferredLock();
    Semaphore semaphore = new Semaphore(0);
    AtomicInteger counter = new AtomicInteger();
    AtomicLong stamp = new AtomicLong();
    // Read lock can be locked by one thread but unlocked by another thread.
    new Thread(
            () -> {
              stamp.set(lock.stampedReadLock());
              semaphore.release(2);
              counter.incrementAndGet();
            })
        .start();
    new Thread(
            () -> {
              try {
                semaphore.acquire();
                lock.stampedReadUnlock(stamp.get());
                counter.incrementAndGet();
              } catch (InterruptedException e) {
                Assert.fail(e.getMessage());
              }
            })
        .start();
    new Thread(
            () -> {
              try {
                semaphore.acquire();
                lock.writeLock();
                counter.incrementAndGet();
                lock.unlockWrite();
              } catch (InterruptedException e) {
                Assert.fail(e.getMessage());
              }
            })
        .start();
    Awaitility.await().atMost(2, TimeUnit.SECONDS).until(() -> counter.get() == 3);
    Assert.assertEquals(3, counter.get());
  }

  @Test
  public void testThreadReadLock() {
    StampedWriterPreferredLock lock = new StampedWriterPreferredLock();
    Semaphore semaphore = new Semaphore(0);
    AtomicInteger counter = new AtomicInteger();
    // Thread-bound read lock must be locked and unlocked by the same thread.
    new Thread(
            () -> {
              lock.threadReadLock();
              semaphore.release(1);
              counter.incrementAndGet();
              lock.threadReadUnlock();
            })
        .start();
    new Thread(
            () -> {
              try {
                semaphore.acquire();
                lock.writeLock();
                counter.incrementAndGet();
                lock.unlockWrite();
              } catch (InterruptedException e) {
                Assert.fail(e.getMessage());
              }
            })
        .start();

    Awaitility.await().atMost(2, TimeUnit.SECONDS).until(() -> counter.get() == 2);
    Assert.assertEquals(2, counter.get());
    counter.set(0);
    // Thread-bound read lock cannot be unlocked by the another thread
    new Thread(
            () -> {
              lock.threadReadLock();
              semaphore.release(2);
              counter.incrementAndGet();
            })
        .start();
    new Thread(
            () -> {
              try {
                semaphore.acquire();
                lock.threadReadUnlock();
                counter.incrementAndGet();
              } catch (InterruptedException e) {
                Assert.fail(e.getMessage());
              }
            })
        .start();
    new Thread(
            () -> {
              try {
                semaphore.acquire();
                lock.writeLock();
                counter.incrementAndGet();
                lock.unlockWrite();
              } catch (InterruptedException e) {
                Assert.fail(e.getMessage());
              }
            })
        .start();
    try {
      Awaitility.await().atMost(2, TimeUnit.SECONDS).until(() -> counter.get() == 3);
      Assert.fail();
    } catch (ConditionTimeoutException e) {
      Assert.assertNotEquals(3, counter.get());
    }
  }

  @Test
  public void testAcquireReadLockWhileWriting() {
    StampedWriterPreferredLock lock = new StampedWriterPreferredLock();
    lock.writeLock();
    AtomicInteger counter = new AtomicInteger();
    new Thread(
            () -> {
              lock.threadReadLock();
              counter.incrementAndGet();
              lock.threadReadUnlock();
            })
        .start();
    // block reader util writer release write lock
    Assert.assertEquals(0, counter.get());
    lock.unlockWrite();
    Awaitility.await().atMost(2, TimeUnit.SECONDS).until(() -> counter.get() == 1);
    Assert.assertEquals(1, counter.get());
  }

  @Test
  public void testWriterPreferred() {
    StampedWriterPreferredLock lock = new StampedWriterPreferredLock();
    AtomicInteger counter = new AtomicInteger();
    // main thread get read lock by stamp
    long stamp = lock.stampedReadLock();
    new Thread(
            () -> {
              // writer thread will be blocked util main thread release read lock.
              lock.writeLock();
              counter.incrementAndGet();
              lock.unlockWrite();
            })
        .start();
    try {
      Awaitility.await().atMost(1, TimeUnit.SECONDS).until(() -> counter.get() == 1);
      Assert.fail();
    } catch (ConditionTimeoutException e) {
      Assert.assertEquals(0, counter.get());
    }
    // start other reader
    new Thread(
            () -> {
              // it will be blocked because of writer preferred
              lock.threadReadLock();
              counter.incrementAndGet();
              lock.threadReadUnlock();
            })
        .start();
    new Thread(
            () -> {
              // it will be blocked because of writer preferred
              long stamp1 = lock.stampedReadLock();
              counter.incrementAndGet();
              lock.stampedReadUnlock(stamp1);
            })
        .start();
    try {
      Awaitility.await().atMost(1, TimeUnit.SECONDS).until(() -> counter.get() == 3);
      Assert.fail();
    } catch (ConditionTimeoutException e) {
      Assert.assertEquals(0, counter.get());
    }
    // release main read lock
    lock.stampedReadUnlock(stamp);
    Awaitility.await().atMost(1, TimeUnit.SECONDS).until(() -> counter.get() == 3);
    Assert.assertEquals(3, counter.get());
  }
}
