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

import org.apache.iotdb.db.metadata.mtree.store.CachedMTreeReadWriteLock;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class CachedMTreeReadWriteLockTest {

  @Test
  public void testStampReadLock() {
    CachedMTreeReadWriteLock lock = new CachedMTreeReadWriteLock();
    Semaphore semaphore = new Semaphore(0);
    AtomicInteger counter = new AtomicInteger();
    AtomicLong stamp = new AtomicLong();
    // Read lock can be locked by one thread but unlocked by another thread.
    new Thread(
            () -> {
              stamp.set(lock.readLock(CachedMTreeReadWriteLock.ALLOCATE_STAMP));
              semaphore.release(2);
              counter.incrementAndGet();
            })
        .start();
    new Thread(
            () -> {
              try {
                semaphore.acquire();
                lock.unlockRead(stamp.get());
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
    CachedMTreeReadWriteLock lock = new CachedMTreeReadWriteLock();
    Semaphore semaphore = new Semaphore(0);
    AtomicInteger counter = new AtomicInteger();
    // Thread-bound read lock must be locked and unlocked by the same thread.
    new Thread(
            () -> {
              lock.readLock(CachedMTreeReadWriteLock.NON_STAMP);
              semaphore.release(1);
              counter.incrementAndGet();
              lock.unlockRead(CachedMTreeReadWriteLock.NON_STAMP);
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
              lock.readLock(CachedMTreeReadWriteLock.NON_STAMP);
              semaphore.release(2);
              counter.incrementAndGet();
            })
        .start();
    new Thread(
            () -> {
              try {
                semaphore.acquire();
                lock.unlockRead(CachedMTreeReadWriteLock.NON_STAMP);
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
    CachedMTreeReadWriteLock lock = new CachedMTreeReadWriteLock();
    lock.writeLock();
    AtomicInteger counter = new AtomicInteger();
    new Thread(
            () -> {
              lock.readLock(CachedMTreeReadWriteLock.NON_STAMP);
              counter.incrementAndGet();
              lock.unlockRead(CachedMTreeReadWriteLock.NON_STAMP);
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
    CachedMTreeReadWriteLock lock = new CachedMTreeReadWriteLock();
    AtomicInteger counter = new AtomicInteger();
    // main thread get read lock by stamp
    long stamp = lock.readLock(CachedMTreeReadWriteLock.ALLOCATE_STAMP);
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
              // it can be reentry by stamp
              lock.readLock(stamp);
              counter.incrementAndGet();
              lock.unlockRead(stamp);
            })
        .start();
    new Thread(
            () -> {
              // it will be blocked because of writer preferred
              lock.readLock(CachedMTreeReadWriteLock.NON_STAMP);
              counter.incrementAndGet();
              lock.unlockRead(CachedMTreeReadWriteLock.NON_STAMP);
            })
        .start();
    new Thread(
            () -> {
              // it will be blocked because of writer preferred
              long stamp1 = lock.readLock(CachedMTreeReadWriteLock.ALLOCATE_STAMP);
              counter.incrementAndGet();
              lock.unlockRead(stamp1);
            })
        .start();
    try {
      Awaitility.await().atMost(1, TimeUnit.SECONDS).until(() -> counter.get() == 4);
      Assert.fail();
    } catch (ConditionTimeoutException e) {
      Assert.assertEquals(1, counter.get());
    }
    // release main read lock
    lock.unlockRead(stamp);
    Awaitility.await().atMost(1, TimeUnit.SECONDS).until(() -> counter.get() == 4);
    Assert.assertEquals(4, counter.get());
  }
}
