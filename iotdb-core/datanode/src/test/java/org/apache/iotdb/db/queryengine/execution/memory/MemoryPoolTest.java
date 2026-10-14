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

package org.apache.iotdb.db.queryengine.execution.memory;

import org.apache.iotdb.commons.memory.MemoryManager;
import org.apache.iotdb.db.exception.runtime.MemoryLeakException;
import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool.MemoryReservationResult;

import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.awaitility.Awaitility.await;

public class MemoryPoolTest {

  MemoryPool pool;

  private final String QUERY_ID = "q0";

  private final String FRAGMENT_INSTANCE_ID = "f0";
  private final String PLAN_NODE_ID = "p0";

  @Before
  public void before() {
    MemoryManager memoryManager = Mockito.spy(new MemoryManager(1024L));
    pool = new MemoryPool("test", memoryManager, 512L);
    pool.registerPlanNodeIdToQueryMemoryMap(
        QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, MemorySide.INPUT);
  }

  @Test
  public void testDeregisterDoesNotRemoveReregisteredQueryMemory() throws Exception {
    Field reservationsField = MemoryPool.class.getDeclaredField("queryMemoryReservations");
    reservationsField.setAccessible(true);
    Object reservationsLock = reservationsField.get(pool);
    long lockOwnerId = Thread.currentThread().getId();
    FutureTask<Void> deregistration =
        new FutureTask<>(
            () -> {
              // An empty fragment may finish without registering any memory.
              pool.deRegisterFragmentInstanceFromQueryMemoryMap(QUERY_ID, "empty", true);
              return null;
            });
    Thread cleanupThread = new Thread(deregistration, "memory-pool-stale-deregister");
    try {
      synchronized (reservationsLock) {
        cleanupThread.start();
        // Hold the registration lock until the empty fragment cleanup reaches it.
        await()
            .atMost(10, TimeUnit.SECONDS)
            .until(
                () -> {
                  ThreadInfo info =
                      ManagementFactory.getThreadMXBean().getThreadInfo(cleanupThread.getId());
                  return info != null
                      && info.getThreadState() == Thread.State.BLOCKED
                      && info.getLockOwnerId() == lockOwnerId
                      && info.getLockInfo().getIdentityHashCode()
                          == System.identityHashCode(reservationsLock);
                });

        // Replace the query map while the delayed cleanup still refers to the old one.
        pool.deRegisterFragmentInstanceFromQueryMemoryMap(QUERY_ID, FRAGMENT_INSTANCE_ID, true);
        Assert.assertEquals(0, pool.getQueryMemoryReservationSize());
        pool.registerPlanNodeIdToQueryMemoryMap(QUERY_ID, "result", PLAN_NODE_ID, MemorySide.INPUT);
        Assert.assertTrue(
            pool.tryReserveForTest(QUERY_ID, "result", PLAN_NODE_ID, 256L, Long.MAX_VALUE));
      }
      deregistration.get(10, TimeUnit.SECONDS);

      Assert.assertEquals(1, pool.getQueryMemoryReservationSize());
      Assert.assertEquals(256L, pool.getQueryMemoryReservedBytes(QUERY_ID));
      Assert.assertEquals(256L, pool.getReservedBytes());
      pool.free(QUERY_ID, "result", PLAN_NODE_ID, 256L);
      pool.deRegisterFragmentInstanceFromQueryMemoryMap(QUERY_ID, "result", true);
      Assert.assertEquals(0, pool.getQueryMemoryReservationSize());
      Assert.assertEquals(0L, pool.getReservedBytes());
    } finally {
      cleanupThread.join(TimeUnit.SECONDS.toMillis(10));
      Assert.assertFalse(cleanupThread.isAlive());
    }
  }

  @Test
  public void testDeregisterMissingFragmentPreservesReservedMemory() {
    Assert.assertTrue(
        pool.tryReserveForTest(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 256L, Long.MAX_VALUE));

    pool.deRegisterFragmentInstanceFromQueryMemoryMap(QUERY_ID, "empty", true);

    Assert.assertEquals(1, pool.getQueryMemoryReservationSize());
    Assert.assertEquals(256L, pool.getQueryMemoryReservedBytes(QUERY_ID));
    pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 256L);
    pool.deRegisterFragmentInstanceFromQueryMemoryMap(QUERY_ID, FRAGMENT_INSTANCE_ID, true);
    Assert.assertEquals(0, pool.getQueryMemoryReservationSize());
    Assert.assertEquals(0L, pool.getReservedBytes());
  }

  @Test
  public void testDeregisterPreservesOtherFragments() {
    pool.registerPlanNodeIdToQueryMemoryMap(QUERY_ID, "other", PLAN_NODE_ID, MemorySide.INPUT);
    Assert.assertTrue(
        pool.tryReserveForTest(QUERY_ID, "other", PLAN_NODE_ID, 256L, Long.MAX_VALUE));

    pool.deRegisterFragmentInstanceFromQueryMemoryMap(QUERY_ID, FRAGMENT_INSTANCE_ID, true);

    Assert.assertEquals(1, pool.getQueryMemoryReservationSize());
    Assert.assertEquals(256L, pool.getQueryMemoryReservedBytes(QUERY_ID));
    pool.free(QUERY_ID, "other", PLAN_NODE_ID, 256L);
    pool.deRegisterFragmentInstanceFromQueryMemoryMap(QUERY_ID, "other", true);
    Assert.assertEquals(0, pool.getQueryMemoryReservationSize());
    Assert.assertEquals(0L, pool.getReservedBytes());
  }

  @Test
  public void testDeregisterWaitsForMemoryRelease() {
    Assert.assertTrue(
        pool.tryReserveForTest(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 256L, Long.MAX_VALUE));

    pool.deRegisterFragmentInstanceFromQueryMemoryMap(QUERY_ID, FRAGMENT_INSTANCE_ID, false);

    Assert.assertEquals(1, pool.getQueryMemoryReservationSize());
    Assert.assertEquals(256L, pool.getQueryMemoryReservedBytes(QUERY_ID));
    pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 256L);
    pool.deRegisterFragmentInstanceFromQueryMemoryMap(QUERY_ID, FRAGMENT_INSTANCE_ID, false);
    Assert.assertEquals(0, pool.getQueryMemoryReservationSize());
    Assert.assertEquals(0L, pool.getReservedBytes());

    // Repeated cleanup after the query has been removed remains a no-op.
    pool.deRegisterFragmentInstanceFromQueryMemoryMap(QUERY_ID, FRAGMENT_INSTANCE_ID, true);
    Assert.assertEquals(0, pool.getQueryMemoryReservationSize());
    Assert.assertEquals(0L, pool.getReservedBytes());
  }

  @Test
  public void testForceDeregisterReportsMemoryLeak() {
    Assert.assertTrue(
        pool.tryReserveForTest(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 256L, Long.MAX_VALUE));

    Assert.assertThrows(
        MemoryLeakException.class,
        () ->
            pool.deRegisterFragmentInstanceFromQueryMemoryMap(
                QUERY_ID, FRAGMENT_INSTANCE_ID, true));

    Assert.assertEquals(0, pool.getQueryMemoryReservationSize());
    Assert.assertEquals(256L, pool.getReservedBytes());
  }

  @Test
  public void testTryReserve() {

    Assert.assertTrue(
        pool.tryReserveForTest(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 256L, Long.MAX_VALUE));
    Assert.assertEquals(256L, pool.getQueryMemoryReservedBytes(QUERY_ID));
    Assert.assertEquals(256L, pool.getReservedBytes());
  }

  @Test
  public void testTryReserveZero() {

    try {
      pool.tryReserveForTest(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 0L, Long.MAX_VALUE);
      Assert.fail("Expect IllegalArgumentException");
    } catch (IllegalArgumentException ignore) {
    }
  }

  @Test
  public void testTryReserveNegative() {

    try {
      pool.tryReserveForTest(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, -1L, Long.MAX_VALUE);
      Assert.fail("Expect IllegalArgumentException");
    } catch (IllegalArgumentException ignore) {
    }
  }

  @Test
  public void testTryReserveAll() {

    Assert.assertTrue(
        pool.tryReserveForTest(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 512L, Long.MAX_VALUE));
    Assert.assertEquals(512L, pool.getQueryMemoryReservedBytes(QUERY_ID));
    Assert.assertEquals(512L, pool.getReservedBytes());
  }

  @Test
  public void testOverTryReserve() {

    Assert.assertTrue(
        pool.tryReserveForTest(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 256L, 512L));
    Assert.assertEquals(256L, pool.getQueryMemoryReservedBytes(QUERY_ID));
    Assert.assertEquals(256L, pool.getReservedBytes());
    Assert.assertFalse(
        pool.tryReserveForTest(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 512L, 511L));
    Assert.assertEquals(256L, pool.getQueryMemoryReservedBytes(QUERY_ID));
    Assert.assertEquals(256L, pool.getReservedBytes());
  }

  @Test
  public void testReserve() {

    ListenableFuture<Void> future =
        pool.reserveWithPriority(
                QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 256L, Long.MAX_VALUE, false)
            .getFuture();
    Assert.assertTrue(future.isDone());
    Assert.assertEquals(256L, pool.getQueryMemoryReservedBytes(QUERY_ID));
    Assert.assertEquals(256L, pool.getReservedBytes());
  }

  @Test
  public void tesReserveZero() {

    try {
      pool.reserveWithPriority(
          QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 0L, Long.MAX_VALUE, false);
      Assert.fail("Expect IllegalArgumentException");
    } catch (IllegalArgumentException ignore) {
    }
  }

  @Test
  public void testReserveNegative() {

    try {
      pool.reserveWithPriority(
          QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, -1L, Long.MAX_VALUE, false);
      Assert.fail("Expect IllegalArgumentException");
    } catch (IllegalArgumentException ignore) {
    }
  }

  @Test
  public void testReserveAll() {

    ListenableFuture<Void> future =
        pool.reserveWithPriority(
                QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 512L, Long.MAX_VALUE, false)
            .getFuture();
    Assert.assertTrue(future.isDone());
    Assert.assertEquals(512L, pool.getQueryMemoryReservedBytes(QUERY_ID));
    Assert.assertEquals(512L, pool.getReservedBytes());
  }

  @Test
  public void testOverReserve() {

    ListenableFuture<Void> future =
        pool.reserveWithPriority(
                QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 256L, Long.MAX_VALUE, false)
            .getFuture();
    Assert.assertTrue(future.isDone());
    Assert.assertEquals(256L, pool.getQueryMemoryReservedBytes(QUERY_ID));
    Assert.assertEquals(256L, pool.getReservedBytes());
    future =
        pool.reserveWithPriority(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 512L, 513L, false)
            .getFuture();
    Assert.assertFalse(future.isDone());
    Assert.assertEquals(256L, pool.getQueryMemoryReservedBytes(QUERY_ID));
    Assert.assertEquals(256L, pool.getReservedBytes());
  }

  @Test
  public void testReserveAndFree() {

    Assert.assertTrue(
        pool.reserveWithPriority(
                QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 512L, Long.MAX_VALUE, false)
            .getFuture()
            .isDone());
    ListenableFuture<Void> future =
        pool.reserveWithPriority(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 512L, 513L, false)
            .getFuture();
    Assert.assertFalse(future.isDone());
    Assert.assertEquals(512L, pool.getQueryMemoryReservedBytes(QUERY_ID));
    Assert.assertEquals(512L, pool.getReservedBytes());
    pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 512);
    Assert.assertEquals(512L, pool.getQueryMemoryReservedBytes(QUERY_ID));
    Assert.assertEquals(512L, pool.getReservedBytes());
    Assert.assertTrue(future.isDone());
  }

  @Test
  public void testMultiReserveAndFree() {

    Assert.assertTrue(
        pool.reserveWithPriority(
                QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 256L, Long.MAX_VALUE, false)
            .getFuture()
            .isDone());
    Assert.assertEquals(256L, pool.getQueryMemoryReservedBytes(QUERY_ID));
    Assert.assertEquals(256L, pool.getReservedBytes());

    ListenableFuture<Void> future1 =
        pool.reserveWithPriority(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 512L, 513L, false)
            .getFuture();
    ListenableFuture<Void> future2 =
        pool.reserveWithPriority(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 512L, 513L, false)
            .getFuture();
    ListenableFuture<Void> future3 =
        pool.reserveWithPriority(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 512L, 513L, false)
            .getFuture();
    Assert.assertFalse(future1.isDone());
    Assert.assertFalse(future2.isDone());
    Assert.assertFalse(future3.isDone());

    pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 256L);
    Assert.assertTrue(future1.isDone());
    Assert.assertFalse(future2.isDone());
    Assert.assertFalse(future3.isDone());
    Assert.assertEquals(512L, pool.getQueryMemoryReservedBytes(QUERY_ID));
    Assert.assertEquals(512L, pool.getReservedBytes());

    pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 512L);
    Assert.assertTrue(future2.isDone());
    Assert.assertFalse(future3.isDone());
    Assert.assertEquals(512L, pool.getQueryMemoryReservedBytes(QUERY_ID));
    Assert.assertEquals(512L, pool.getReservedBytes());

    pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 512L);
    Assert.assertTrue(future3.isDone());
    Assert.assertEquals(512L, pool.getQueryMemoryReservedBytes(QUERY_ID));
    Assert.assertEquals(512L, pool.getReservedBytes());

    pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 512L);
    Assert.assertEquals(0L, pool.getQueryMemoryReservedBytes(QUERY_ID));
    Assert.assertEquals(0L, pool.getReservedBytes());
  }

  @Test
  public void testFree() {

    Assert.assertTrue(
        pool.tryReserveForTest(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 512L, Long.MAX_VALUE));
    Assert.assertEquals(512L, pool.getQueryMemoryReservedBytes(QUERY_ID));
    Assert.assertEquals(512L, pool.getReservedBytes());

    pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 256L);
    Assert.assertEquals(256L, pool.getQueryMemoryReservedBytes(QUERY_ID));
    Assert.assertEquals(256L, pool.getReservedBytes());
  }

  @Test
  public void testFreeAll() {

    Assert.assertTrue(
        pool.tryReserveForTest(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 512L, Long.MAX_VALUE));
    Assert.assertEquals(512L, pool.getQueryMemoryReservedBytes(QUERY_ID));
    Assert.assertEquals(512L, pool.getReservedBytes());

    pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 512L);
    Assert.assertEquals(0L, pool.getQueryMemoryReservedBytes(QUERY_ID));
    Assert.assertEquals(0L, pool.getReservedBytes());
  }

  @Test
  public void testFreeZero() {

    Assert.assertTrue(
        pool.tryReserveForTest(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 512L, Long.MAX_VALUE));
    Assert.assertEquals(512L, pool.getQueryMemoryReservedBytes(QUERY_ID));
    Assert.assertEquals(512L, pool.getReservedBytes());

    try {
      pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 0L);
      Assert.fail("Expect IllegalArgumentException");
    } catch (IllegalArgumentException ignore) {
    }
  }

  @Test
  public void testFreeNegative() {

    Assert.assertTrue(
        pool.tryReserveForTest(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 512L, Long.MAX_VALUE));
    Assert.assertEquals(512L, pool.getQueryMemoryReservedBytes(QUERY_ID));
    Assert.assertEquals(512L, pool.getReservedBytes());

    try {
      pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, -1L);
      Assert.fail("Expect IllegalArgumentException");
    } catch (IllegalArgumentException ignore) {
    }
  }

  @Test
  public void testOverFree() {

    Assert.assertTrue(
        pool.tryReserveForTest(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 512L, Long.MAX_VALUE));
    Assert.assertEquals(512L, pool.getQueryMemoryReservedBytes(QUERY_ID));
    Assert.assertEquals(512L, pool.getReservedBytes());

    try {
      pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 513L);
      Assert.fail("Expect IllegalArgumentException");
    } catch (IllegalArgumentException ignore) {
    }
  }

  @Test
  public void testTryCancelBlockedReservation() {

    // Run out of memory.
    Assert.assertTrue(
        pool.tryReserveForTest(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 512L, Long.MAX_VALUE));

    ListenableFuture<Void> f =
        pool.reserveWithPriority(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 256L, 512L, false)
            .getFuture();
    Assert.assertFalse(f.isDone());
    // Cancel the reservation.
    Assert.assertEquals(256L, pool.tryCancel(f));
    Assert.assertTrue(f.isDone());
    Assert.assertTrue(f.isCancelled());
  }

  @Test
  public void testTryCancelCompletedReservation() {

    ListenableFuture<Void> f =
        pool.reserveWithPriority(
                QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 256L, Long.MAX_VALUE, false)
            .getFuture();
    Assert.assertTrue(f.isDone());
    // Cancel the reservation.
    Assert.assertEquals(0L, pool.tryCancel(f));
    Assert.assertTrue(f.isDone());
    Assert.assertFalse(f.isCancelled());
  }

  /**
   * Normal query: requested bytes exceed what the pool can still provide — reserve fails (blocked
   * future, not immediate success).
   */
  @Test
  public void testReserveWithPriorityNormalQueryExceedsAvailable() {
    MemoryReservationResult r1 =
        pool.reserveWithPriority(
            QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 512L, Long.MAX_VALUE, false);
    Assert.assertTrue(r1.isReserveSuccess());
    Assert.assertEquals(512L, r1.getReservedBytes());
    Assert.assertTrue(r1.getFuture().isDone());

    MemoryReservationResult r2 =
        pool.reserveWithPriority(
            QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 512L, Long.MAX_VALUE, false);
    Assert.assertTrue(r2.isReserveSuccess());
    Assert.assertEquals(512L, r2.getReservedBytes());
    Assert.assertEquals(1024L, pool.getReservedBytes());

    MemoryReservationResult r3 =
        pool.reserveWithPriority(
            QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 256L, Long.MAX_VALUE, false);
    Assert.assertFalse(r3.isReserveSuccess());
    Assert.assertEquals(256L, r3.getReservedBytes());
    Assert.assertFalse(r3.getFuture().isDone());
    Assert.assertEquals(1024L, pool.getReservedBytes());
  }

  /** SHOW QUERIES path: exceeds pool capacity — treated as success with zero bytes from pool. */
  @Test
  public void testReserveWithPriorityShowQueriesExceedsAvailable() {
    Assert.assertTrue(
        pool.reserveWithPriority(
                QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 512L, Long.MAX_VALUE, false)
            .isReserveSuccess());
    Assert.assertTrue(
        pool.reserveWithPriority(
                QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 512L, Long.MAX_VALUE, false)
            .isReserveSuccess());
    Assert.assertEquals(1024L, pool.getReservedBytes());

    MemoryReservationResult r =
        pool.reserveWithPriority(
            QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 256L, Long.MAX_VALUE, true);
    Assert.assertTrue(r.isReserveSuccess());
    Assert.assertEquals(0L, r.getReservedBytes());
    Assert.assertTrue(r.getFuture().isDone());
    Assert.assertEquals(1024L, pool.getReservedBytes());
  }

  /** SHOW QUERIES path: pool has room — same as normal successful reserve. */
  @Test
  public void testReserveWithPriorityShowQueriesWithinAvailable() {
    MemoryReservationResult r =
        pool.reserveWithPriority(
            QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 256L, Long.MAX_VALUE, true);
    Assert.assertTrue(r.isReserveSuccess());
    Assert.assertEquals(256L, r.getReservedBytes());
    Assert.assertTrue(r.getFuture().isDone());
    Assert.assertEquals(256L, pool.getReservedBytes());
  }

  @Test
  public void testSharedInputBudgetAndIndependentOutput() {
    pool.registerPlanNodeIdToQueryMemoryMap(
        QUERY_ID, FRAGMENT_INSTANCE_ID, "input2", MemorySide.INPUT);
    pool.registerPlanNodeIdToQueryMemoryMap(
        QUERY_ID, FRAGMENT_INSTANCE_ID, "output", MemorySide.OUTPUT);
    Assert.assertTrue(
        pool.reserveWithPriority(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 200, 256, false)
            .isReserveSuccess());
    MemoryReservationResult pending =
        pool.reserveWithPriority(QUERY_ID, FRAGMENT_INSTANCE_ID, "input2", 100, 256, false);
    Assert.assertFalse(pending.isReserveSuccess());
    Assert.assertFalse(pending.getFuture().isDone());
    Assert.assertEquals(
        200, pool.getFragmentMemoryReservedBytes(QUERY_ID, FRAGMENT_INSTANCE_ID, MemorySide.INPUT));
    Assert.assertEquals(
        0, pool.getPlanNodeMemoryReservedBytes(QUERY_ID, FRAGMENT_INSTANCE_ID, "input2"));
    Assert.assertTrue(
        pool.reserveWithPriority(QUERY_ID, FRAGMENT_INSTANCE_ID, "output", 256, 256, false)
            .isReserveSuccess());
    Assert.assertEquals(456, pool.getReservedBytes());
    pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 200);
    Assert.assertTrue(pending.getFuture().isDone());
    Assert.assertEquals(
        100, pool.getFragmentMemoryReservedBytes(QUERY_ID, FRAGMENT_INSTANCE_ID, MemorySide.INPUT));
    Assert.assertEquals(
        256,
        pool.getFragmentMemoryReservedBytes(QUERY_ID, FRAGMENT_INSTANCE_ID, MemorySide.OUTPUT));
    pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, "input2", 100);
    pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, "output", 256);
    Assert.assertEquals(0, pool.getReservedBytes());
    Assert.assertEquals(0, pool.getQueryMemoryReservedBytes(QUERY_ID));
  }

  @Test
  public void testSideRegistrationCannotChange() {
    pool.registerPlanNodeIdToQueryMemoryMap(
        QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, MemorySide.INPUT);
    try {
      pool.registerPlanNodeIdToQueryMemoryMap(
          QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, MemorySide.OUTPUT);
      Assert.fail("conflicting side must be rejected");
    } catch (IllegalArgumentException expected) {
      Assert.assertEquals(0, pool.getReservedBytes());
    }
  }

  @Test
  public void testOddBudgetAndSingleRequestBoundary() {
    Assert.assertEquals(256, MemorySide.INPUT.getMaxBytes(513));
    Assert.assertEquals(257, MemorySide.OUTPUT.getMaxBytes(513));
    try {
      pool.reserveWithPriority(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 257, 256, false);
      Assert.fail("request exceeds the input budget");
    } catch (IllegalArgumentException expected) {
      Assert.assertEquals(0, pool.getReservedBytes());
      Assert.assertEquals(0, pool.getQueryMemoryReservedBytes(QUERY_ID));
    }
  }

  @Test
  public void testConcurrentPlanNodesShareOneSideCounter() throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(16);
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch attempted = new CountDownLatch(16);
    CountDownLatch release = new CountDownLatch(1);
    List<Future<?>> tasks = new ArrayList<>();
    try {
      for (int i = 0; i < 16; i++) {
        String id = "pipeline-" + i;
        pool.registerPlanNodeIdToQueryMemoryMap(
            QUERY_ID, FRAGMENT_INSTANCE_ID, id, MemorySide.INPUT);
        tasks.add(
            executor.submit(
                () -> {
                  start.await();
                  boolean reserved =
                      pool.tryReserveForTest(QUERY_ID, FRAGMENT_INSTANCE_ID, id, 32, 128);
                  attempted.countDown();
                  release.await();
                  if (reserved) pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, id, 32);
                  return null;
                }));
      }
      start.countDown();
      Assert.assertTrue(attempted.await(10, TimeUnit.SECONDS));
      long details = 0;
      for (int i = 0; i < 16; i++) {
        details +=
            pool.getPlanNodeMemoryReservedBytes(QUERY_ID, FRAGMENT_INSTANCE_ID, "pipeline-" + i);
      }
      Assert.assertTrue(details > 0 && details <= 128);
      Assert.assertEquals(
          details,
          pool.getFragmentMemoryReservedBytes(QUERY_ID, FRAGMENT_INSTANCE_ID, MemorySide.INPUT));
      Assert.assertEquals(details, pool.getReservedBytes());
      release.countDown();
      for (Future<?> task : tasks) task.get(10, TimeUnit.SECONDS);
      Assert.assertEquals(0, pool.getReservedBytes());
      Assert.assertEquals(0, pool.getQueryMemoryReservedBytes(QUERY_ID));
    } finally {
      start.countDown();
      release.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  public void testReleaseBeforePendingReservationIsPublished() throws Exception {
    pool = Mockito.spy(pool);
    pool.registerPlanNodeIdToQueryMemoryMap(
        QUERY_ID, FRAGMENT_INSTANCE_ID, "input2", MemorySide.INPUT);
    Assert.assertTrue(
        pool.reserveWithPriority(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 256, 256, false)
            .isReserveSuccess());
    CountDownLatch failedAttempt = new CountDownLatch(1);
    CountDownLatch released = new CountDownLatch(1);
    AtomicBoolean firstAttempt = new AtomicBoolean(true);
    Mockito.doAnswer(
            invocation -> {
              boolean granted = (boolean) invocation.callRealMethod();
              if (!granted && firstAttempt.compareAndSet(true, false)) {
                failedAttempt.countDown();
                Assert.assertTrue(released.await(10, TimeUnit.SECONDS));
              }
              return granted;
            })
        .when(pool)
        .tryReserve(QUERY_ID, FRAGMENT_INSTANCE_ID, "input2", 256, 256);
    FutureTask<MemoryReservationResult> reservation =
        new FutureTask<>(
            () ->
                pool.reserveWithPriority(
                    QUERY_ID, FRAGMENT_INSTANCE_ID, "input2", 256, 256, false));
    Thread reserver = new Thread(reservation, "reserve-shared-input");
    reserver.start();
    try {
      Assert.assertTrue(failedAttempt.await(10, TimeUnit.SECONDS));
      pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 256);
      released.countDown();
      MemoryReservationResult result = reservation.get(10, TimeUnit.SECONDS);
      Assert.assertTrue(
          "Releasing the last owner must wake the pending request", result.getFuture().isDone());
      result.getFuture().get(10, TimeUnit.SECONDS);
      Assert.assertEquals(256, pool.getReservedBytes());
      Assert.assertEquals(
          256,
          pool.getFragmentMemoryReservedBytes(QUERY_ID, FRAGMENT_INSTANCE_ID, MemorySide.INPUT));
      Assert.assertEquals(
          256, pool.getPlanNodeMemoryReservedBytes(QUERY_ID, FRAGMENT_INSTANCE_ID, "input2"));
      pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, "input2", 256);
      Assert.assertEquals(0, pool.getReservedBytes());
    } finally {
      released.countDown();
      reserver.join(10000);
      Assert.assertFalse(reserver.isAlive());
    }
  }

  @Test
  public void testCancelledInputDoesNotConsumeReleasedSharedBudget() throws Exception {
    pool.registerPlanNodeIdToQueryMemoryMap(
        QUERY_ID, FRAGMENT_INSTANCE_ID, "cancelled", MemorySide.INPUT);
    pool.registerPlanNodeIdToQueryMemoryMap(
        QUERY_ID, FRAGMENT_INSTANCE_ID, "waiting", MemorySide.INPUT);
    pool.registerPlanNodeIdToQueryMemoryMap(
        QUERY_ID, FRAGMENT_INSTANCE_ID, "output", MemorySide.OUTPUT);
    pool.reserveWithPriority(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 256, 256, false);
    MemoryReservationResult cancelled =
        pool.reserveWithPriority(QUERY_ID, FRAGMENT_INSTANCE_ID, "cancelled", 128, 256, false);
    MemoryReservationResult waiting =
        pool.reserveWithPriority(QUERY_ID, FRAGMENT_INSTANCE_ID, "waiting", 256, 256, false);
    Assert.assertFalse(cancelled.getFuture().isDone());
    Assert.assertFalse(waiting.getFuture().isDone());
    Assert.assertTrue(
        pool.reserveWithPriority(QUERY_ID, FRAGMENT_INSTANCE_ID, "output", 128, 256, false)
            .isReserveSuccess());
    Assert.assertEquals(128, pool.tryCancel(cancelled.getFuture()));
    pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 256);
    waiting.getFuture().get(10, TimeUnit.SECONDS);
    Assert.assertTrue(cancelled.getFuture().isCancelled());
    Assert.assertEquals(
        0, pool.getPlanNodeMemoryReservedBytes(QUERY_ID, FRAGMENT_INSTANCE_ID, "cancelled"));
    Assert.assertEquals(
        256, pool.getFragmentMemoryReservedBytes(QUERY_ID, FRAGMENT_INSTANCE_ID, MemorySide.INPUT));
    Assert.assertEquals(
        128,
        pool.getFragmentMemoryReservedBytes(QUERY_ID, FRAGMENT_INSTANCE_ID, MemorySide.OUTPUT));
    Assert.assertEquals(384, pool.getReservedBytes());
    Assert.assertEquals(0, pool.getMemoryReservationSize());
    pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, "waiting", 256);
    pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, "output", 128);
    Assert.assertEquals(0, pool.getQueryMemoryReservedBytes(QUERY_ID));
    Assert.assertEquals(0, pool.getReservedBytes());
  }

  @Test
  public void testConcurrentPendingReservationsCompleteAndReleaseAllAccounting() throws Exception {
    int count = 16;
    ExecutorService executor = Executors.newFixedThreadPool(count);
    CountDownLatch attempted = new CountDownLatch(count);
    List<Future<?>> tasks = new ArrayList<>();
    pool.reserveWithPriority(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 128, 128, false);
    try {
      for (int i = 0; i < count; i++) {
        String id = "waiting-" + i;
        pool.registerPlanNodeIdToQueryMemoryMap(
            QUERY_ID, FRAGMENT_INSTANCE_ID, id, MemorySide.INPUT);
        tasks.add(
            executor.submit(
                () -> {
                  for (int round = 0; round < 50; round++) {
                    MemoryReservationResult result =
                        pool.reserveWithPriority(
                            QUERY_ID, FRAGMENT_INSTANCE_ID, id, 32, 128, false);
                    if (round == 0) attempted.countDown();
                    result.getFuture().get(10, TimeUnit.SECONDS);
                    pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, id, result.getReservedBytes());
                  }
                  return null;
                }));
      }
      Assert.assertTrue(attempted.await(10, TimeUnit.SECONDS));
      Assert.assertEquals(count, pool.getMemoryReservationSize());
      pool.free(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID, 128);
      for (Future<?> task : tasks) task.get(30, TimeUnit.SECONDS);
      for (int i = 0; i < count; i++) {
        Assert.assertEquals(
            0, pool.getPlanNodeMemoryReservedBytes(QUERY_ID, FRAGMENT_INSTANCE_ID, "waiting-" + i));
      }
      Assert.assertEquals(
          0, pool.getFragmentMemoryReservedBytes(QUERY_ID, FRAGMENT_INSTANCE_ID, MemorySide.INPUT));
      Assert.assertEquals(0, pool.getReservedBytes());
      Assert.assertEquals(0, pool.getQueryMemoryReservedBytes(QUERY_ID));
      Assert.assertEquals(0, pool.getMemoryReservationSize());
    } finally {
      executor.shutdownNow();
      Assert.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }
}
