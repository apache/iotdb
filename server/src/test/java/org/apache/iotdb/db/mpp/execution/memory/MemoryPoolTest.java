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

package org.apache.iotdb.db.mpp.execution.memory;

import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MemoryPoolTest {

  MemoryPool pool;

  @Before
  public void before() {
    pool = new MemoryPool("test", 1024L, 512L);
  }

  @Test
  public void testTryReserve() {
    String queryId = "q0";
    Assert.assertTrue(pool.tryReserve(queryId, 256L));
    Assert.assertEquals(256L, pool.getQueryMemoryReservedBytes(queryId));
    Assert.assertEquals(256L, pool.getReservedBytes());
  }

  @Test
  public void testTryReserveZero() {
    String queryId = "q0";
    try {
      pool.tryReserve(queryId, 0L);
      Assert.fail("Expect IllegalArgumentException");
    } catch (IllegalArgumentException ignore) {
    }
  }

  @Test
  public void testTryReserveNegative() {
    String queryId = "q0";
    try {
      pool.tryReserve(queryId, -1L);
      Assert.fail("Expect IllegalArgumentException");
    } catch (IllegalArgumentException ignore) {
    }
  }

  @Test
  public void testTryReserveAll() {
    String queryId = "q0";
    Assert.assertTrue(pool.tryReserve(queryId, 512L));
    Assert.assertEquals(512L, pool.getQueryMemoryReservedBytes(queryId));
    Assert.assertEquals(512L, pool.getReservedBytes());
  }

  @Test
  public void testOverTryReserve() {
    String queryId = "q0";
    Assert.assertTrue(pool.tryReserve(queryId, 256L));
    Assert.assertEquals(256L, pool.getQueryMemoryReservedBytes(queryId));
    Assert.assertEquals(256L, pool.getReservedBytes());
    Assert.assertFalse(pool.tryReserve(queryId, 512L));
    Assert.assertEquals(256L, pool.getQueryMemoryReservedBytes(queryId));
    Assert.assertEquals(256L, pool.getReservedBytes());
  }

  @Test
  public void testReserve() {
    String queryId = "q0";
    ListenableFuture<Void> future = pool.reserve(queryId, 256L).left;
    Assert.assertTrue(future.isDone());
    Assert.assertEquals(256L, pool.getQueryMemoryReservedBytes(queryId));
    Assert.assertEquals(256L, pool.getReservedBytes());
  }

  @Test
  public void tesReserveZero() {
    String queryId = "q0";
    try {
      pool.reserve(queryId, 0L);
      Assert.fail("Expect IllegalArgumentException");
    } catch (IllegalArgumentException ignore) {
    }
  }

  @Test
  public void testReserveNegative() {
    String queryId = "q0";
    try {
      pool.reserve(queryId, -1L);
      Assert.fail("Expect IllegalArgumentException");
    } catch (IllegalArgumentException ignore) {
    }
  }

  @Test
  public void testReserveAll() {
    String queryId = "q0";
    ListenableFuture<Void> future = pool.reserve(queryId, 512L).left;
    Assert.assertTrue(future.isDone());
    Assert.assertEquals(512L, pool.getQueryMemoryReservedBytes(queryId));
    Assert.assertEquals(512L, pool.getReservedBytes());
  }

  @Test
  public void testOverReserve() {
    String queryId = "q0";
    ListenableFuture<Void> future = pool.reserve(queryId, 256L).left;
    Assert.assertTrue(future.isDone());
    Assert.assertEquals(256L, pool.getQueryMemoryReservedBytes(queryId));
    Assert.assertEquals(256L, pool.getReservedBytes());
    future = pool.reserve(queryId, 512L).left;
    Assert.assertFalse(future.isDone());
    Assert.assertEquals(256L, pool.getQueryMemoryReservedBytes(queryId));
    Assert.assertEquals(256L, pool.getReservedBytes());
  }

  @Test
  public void testReserveAndFree() {
    String queryId = "q0";
    Assert.assertTrue(pool.reserve(queryId, 512L).left.isDone());
    ListenableFuture<Void> future = pool.reserve(queryId, 512L).left;
    Assert.assertFalse(future.isDone());
    Assert.assertEquals(512L, pool.getQueryMemoryReservedBytes(queryId));
    Assert.assertEquals(512L, pool.getReservedBytes());
    pool.free(queryId, 512);
    Assert.assertEquals(512L, pool.getQueryMemoryReservedBytes(queryId));
    Assert.assertEquals(512L, pool.getReservedBytes());
    Assert.assertTrue(future.isDone());
  }

  @Test
  public void testMultiReserveAndFree() {
    String queryId = "q0";
    Assert.assertTrue(pool.reserve(queryId, 256L).left.isDone());
    Assert.assertEquals(256L, pool.getQueryMemoryReservedBytes(queryId));
    Assert.assertEquals(256L, pool.getReservedBytes());

    ListenableFuture<Void> future1 = pool.reserve(queryId, 512L).left;
    ListenableFuture<Void> future2 = pool.reserve(queryId, 512L).left;
    ListenableFuture<Void> future3 = pool.reserve(queryId, 512L).left;
    Assert.assertFalse(future1.isDone());
    Assert.assertFalse(future2.isDone());
    Assert.assertFalse(future3.isDone());

    pool.free(queryId, 256L);
    Assert.assertTrue(future1.isDone());
    Assert.assertFalse(future2.isDone());
    Assert.assertFalse(future3.isDone());
    Assert.assertEquals(512L, pool.getQueryMemoryReservedBytes(queryId));
    Assert.assertEquals(512L, pool.getReservedBytes());

    pool.free(queryId, 512L);
    Assert.assertTrue(future2.isDone());
    Assert.assertFalse(future3.isDone());
    Assert.assertEquals(512L, pool.getQueryMemoryReservedBytes(queryId));
    Assert.assertEquals(512L, pool.getReservedBytes());

    pool.free(queryId, 512L);
    Assert.assertTrue(future3.isDone());
    Assert.assertEquals(512L, pool.getQueryMemoryReservedBytes(queryId));
    Assert.assertEquals(512L, pool.getReservedBytes());

    pool.free(queryId, 512L);
    Assert.assertEquals(0L, pool.getQueryMemoryReservedBytes(queryId));
    Assert.assertEquals(0L, pool.getReservedBytes());
  }

  @Test
  public void testFree() {
    String queryId = "q0";
    Assert.assertTrue(pool.tryReserve(queryId, 512L));
    Assert.assertEquals(512L, pool.getQueryMemoryReservedBytes(queryId));
    Assert.assertEquals(512L, pool.getReservedBytes());

    pool.free(queryId, 256L);
    Assert.assertEquals(256L, pool.getQueryMemoryReservedBytes(queryId));
    Assert.assertEquals(256L, pool.getReservedBytes());
  }

  @Test
  public void testFreeAll() {
    String queryId = "q0";
    Assert.assertTrue(pool.tryReserve(queryId, 512L));
    Assert.assertEquals(512L, pool.getQueryMemoryReservedBytes(queryId));
    Assert.assertEquals(512L, pool.getReservedBytes());

    pool.free(queryId, 512L);
    Assert.assertEquals(0L, pool.getQueryMemoryReservedBytes(queryId));
    Assert.assertEquals(0L, pool.getReservedBytes());
  }

  @Test
  public void testFreeZero() {
    String queryId = "q0";
    Assert.assertTrue(pool.tryReserve(queryId, 512L));
    Assert.assertEquals(512L, pool.getQueryMemoryReservedBytes(queryId));
    Assert.assertEquals(512L, pool.getReservedBytes());

    try {
      pool.free(queryId, 0L);
      Assert.fail("Expect IllegalArgumentException");
    } catch (IllegalArgumentException ignore) {
    }
  }

  @Test
  public void testFreeNegative() {
    String queryId = "q0";
    Assert.assertTrue(pool.tryReserve(queryId, 512L));
    Assert.assertEquals(512L, pool.getQueryMemoryReservedBytes(queryId));
    Assert.assertEquals(512L, pool.getReservedBytes());

    try {
      pool.free(queryId, -1L);
      Assert.fail("Expect IllegalArgumentException");
    } catch (IllegalArgumentException ignore) {
    }
  }

  @Test
  public void testOverFree() {
    String queryId = "q0";
    Assert.assertTrue(pool.tryReserve(queryId, 512L));
    Assert.assertEquals(512L, pool.getQueryMemoryReservedBytes(queryId));
    Assert.assertEquals(512L, pool.getReservedBytes());

    try {
      pool.free(queryId, 513L);
      Assert.fail("Expect IllegalArgumentException");
    } catch (IllegalArgumentException ignore) {
    }
  }

  @Test
  public void testTryCancelBlockedReservation() {
    String queryId = "q0";
    // Run out of memory.
    Assert.assertTrue(pool.tryReserve(queryId, 512L));

    ListenableFuture<Void> f = pool.reserve(queryId, 256L).left;
    Assert.assertFalse(f.isDone());
    // Cancel the reservation.
    Assert.assertEquals(256L, pool.tryCancel(f));
    Assert.assertTrue(f.isDone());
    Assert.assertTrue(f.isCancelled());
  }

  @Test
  public void testTryCancelCompletedReservation() {
    String queryId = "q0";
    ListenableFuture<Void> f = pool.reserve(queryId, 256L).left;
    Assert.assertTrue(f.isDone());
    // Cancel the reservation.
    Assert.assertEquals(0L, pool.tryCancel(f));
    Assert.assertTrue(f.isDone());
    Assert.assertFalse(f.isCancelled());
  }
}
