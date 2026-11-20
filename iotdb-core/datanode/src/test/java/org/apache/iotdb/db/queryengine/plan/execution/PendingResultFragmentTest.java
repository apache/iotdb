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

package org.apache.iotdb.db.queryengine.plan.execution;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class PendingResultFragmentTest {

  @Test
  public void testReserveWhenBlockBecomesPendingAndReleaseOnClear() {
    RecordingAccount account = new RecordingAccount();
    PendingResultFragment fragment = new PendingResultFragment(account);
    ByteBuffer block = ByteBuffer.allocate(100);

    fragment.reset(block, 30);
    assertEquals(List.of(100L), account.reserved);
    assertEquals(0, account.releases);

    // continuing the same block must not reserve again
    fragment.reset(block, 60);
    assertEquals(List.of(100L), account.reserved);
    assertSame(block, fragment.getBlock());
    assertEquals(60, fragment.getNextOffset());

    fragment.clear();
    assertEquals(1, account.releases);
    assertNull(fragment.getBlock());
  }

  @Test
  public void testNewBlockReplacesPendingOne() {
    RecordingAccount account = new RecordingAccount();
    PendingResultFragment fragment = new PendingResultFragment(account);

    fragment.reset(ByteBuffer.allocate(100), 30);
    fragment.reset(ByteBuffer.allocate(50), 10);

    // the previous reservation is released before the new one is taken
    assertEquals(List.of(100L, 50L), account.reserved);
    assertEquals(1, account.releases);
    assertEquals(50L, account.accounted);
  }

  @Test
  public void testNullAccountDisablesAccounting() {
    PendingResultFragment fragment = new PendingResultFragment(null);
    fragment.reset(ByteBuffer.allocate(100), 30);
    fragment.clear();
    assertNull(fragment.getBlock());
  }

  @Test(timeout = 30_000)
  public void testClearCancelsWaitingReservation() throws Exception {
    WaitingAccount account = new WaitingAccount();
    PendingResultFragment fragment = new PendingResultFragment(account);
    CountDownLatch resetReturned = new CountDownLatch(1);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    Thread fetcher =
        new Thread(
            () -> {
              try {
                fragment.reset(ByteBuffer.allocate(100), 30);
              } catch (IllegalStateException expected) {
                // the reservation was cancelled by the cleanup
                failure.set(expected);
              } finally {
                resetReturned.countDown();
              }
            });
    fetcher.start();
    assertTrue("reset() must wait for memory", account.waiting.await(10, TimeUnit.SECONDS));

    // cleanup must not block behind the waiting reservation
    fragment.clear();
    assertTrue(
        "clear() must cancel the waiting reservation",
        account.released.await(10, TimeUnit.SECONDS));
    assertTrue("the fetcher must be woken up", resetReturned.await(10, TimeUnit.SECONDS));
    assertTrue(failure.get() instanceof IllegalStateException);
    assertNull(fragment.getBlock());
    assertEquals(0L, account.accounted);
  }

  private static final class RecordingAccount implements PendingResultFragment.Account {

    private final List<Long> reserved = new ArrayList<>();
    private long accounted;
    private int releases;

    @Override
    public void reserve(long bytes) {
      reserved.add(bytes);
      accounted += bytes;
    }

    @Override
    public void release() {
      releases++;
      accounted = 0;
    }
  }

  /** A reservation that waits until it is released, as when the pool is exhausted. */
  private static final class WaitingAccount implements PendingResultFragment.Account {

    private final CountDownLatch waiting = new CountDownLatch(1);
    private final CountDownLatch released = new CountDownLatch(1);
    private long accounted;

    @Override
    public void reserve(long bytes) {
      waiting.countDown();
      try {
        released.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      throw new IllegalStateException("aborted by release");
    }

    @Override
    public void release() {
      accounted = 0;
      released.countDown();
    }
  }
}
