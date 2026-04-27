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

import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool.MemoryReservationResult;

import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MemoryPoolTest {

  MemoryPool pool;

  private final String QUERY_ID = "q0";

  private final String FRAGMENT_INSTANCE_ID = "f0";
  private final String PLAN_NODE_ID = "p0";

  @Before
  public void before() {
    pool = new MemoryPool("test", 1024L, 512L);
    pool.registerPlanNodeIdToQueryMemoryMap(QUERY_ID, FRAGMENT_INSTANCE_ID, PLAN_NODE_ID);
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
}
