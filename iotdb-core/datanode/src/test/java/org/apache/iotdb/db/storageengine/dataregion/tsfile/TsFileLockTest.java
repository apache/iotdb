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
package org.apache.iotdb.db.storageengine.dataregion.tsfile;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TsFileLockTest {
  @Test
  public void interruptedWriterStillWaitsForReaders() throws Exception {
    interruptedWaiter(true);
  }

  @Test
  public void interruptedReaderStillWaitsForWriter() throws Exception {
    interruptedWaiter(false);
  }

  private void interruptedWaiter(boolean writer) throws Exception {
    TsFileLock lock = new TsFileLock();
    ExecutorService worker = Executors.newSingleThreadExecutor();
    AtomicReference<Thread> waiter = new AtomicReference<>();
    CountDownLatch started = new CountDownLatch(1);
    if (writer) {
      lock.readLock();
    } else {
      lock.writeLock();
    }
    boolean held = true;
    try {
      Future<Boolean> acquired =
          worker.submit(
              () -> {
                waiter.set(Thread.currentThread());
                started.countDown();
                if (writer) {
                  lock.writeLock();
                } else {
                  lock.readLock();
                }
                try {
                  return Thread.currentThread().isInterrupted();
                } finally {
                  if (writer) {
                    lock.writeUnlock();
                  } else {
                    lock.readUnlock();
                  }
                }
              });
      assertTrue(started.await(10, TimeUnit.SECONDS));
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      while (waiter.get().getState() != Thread.State.WAITING
          && waiter.get().getState() != Thread.State.TIMED_WAITING) {
        if (System.nanoTime() > deadline) {
          fail("waiter never reached the resource lock");
        }
        Thread.sleep(1);
      }
      waiter.get().interrupt();
      assertThrows(TimeoutException.class, () -> acquired.get(100, TimeUnit.MILLISECONDS));
      if (writer) {
        lock.readUnlock();
      } else {
        lock.writeUnlock();
      }
      held = false;
      assertTrue(acquired.get(10, TimeUnit.SECONDS));
    } finally {
      if (held) {
        if (writer) {
          lock.readUnlock();
        } else {
          lock.writeUnlock();
        }
      }
      worker.shutdownNow();
      assertTrue(worker.awaitTermination(10, TimeUnit.SECONDS));
    }
  }
}
