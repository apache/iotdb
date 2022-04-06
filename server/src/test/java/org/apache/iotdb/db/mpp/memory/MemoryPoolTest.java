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

package org.apache.iotdb.db.mpp.memory;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MemoryPoolTest {

  MemoryPool pool;

  @Before
  public void before() {
    pool = new MemoryPool("test", 1024L);
  }

  @Test
  public void testReserve() {
    String queryId = "q0";
    Assert.assertTrue(pool.tryReserve(queryId, 512L));
    Assert.assertEquals(512L, pool.getQueryMemoryReservedBytes(queryId));
    Assert.assertEquals(512L, pool.getReservedBytes());
    Assert.assertEquals(1024L, pool.getMaxBytes());
  }

  @Test
  public void testReserveZero() {
    String queryId = "q0";
    try {
      pool.tryReserve(queryId, 0L);
      Assert.fail("Expect IllegalArgumentException");
    } catch (IllegalArgumentException ignore) {
    }
  }

  @Test
  public void testReserveNegative() {
    String queryId = "q0";
    try {
      pool.tryReserve(queryId, -1L);
      Assert.fail("Expect IllegalArgumentException");
    } catch (IllegalArgumentException ignore) {
    }
  }

  @Test
  public void testReserveAll() {
    String queryId = "q0";
    Assert.assertTrue(pool.tryReserve(queryId, 1024L));
    Assert.assertEquals(1024L, pool.getQueryMemoryReservedBytes(queryId));
    Assert.assertEquals(1024L, pool.getReservedBytes());
    Assert.assertEquals(1024L, pool.getMaxBytes());
  }

  @Test
  public void testOverReserve() {
    String queryId = "q0";
    Assert.assertFalse(pool.tryReserve(queryId, 1025L));
    Assert.assertEquals(0L, pool.getQueryMemoryReservedBytes(queryId));
    Assert.assertEquals(0L, pool.getReservedBytes());
    Assert.assertEquals(1024L, pool.getMaxBytes());
  }

  @Test
  public void testFree() {
    String queryId = "q0";
    Assert.assertTrue(pool.tryReserve(queryId, 512L));
    Assert.assertEquals(512L, pool.getQueryMemoryReservedBytes(queryId));
    Assert.assertEquals(512L, pool.getReservedBytes());
    Assert.assertEquals(1024L, pool.getMaxBytes());

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
    Assert.assertEquals(1024L, pool.getMaxBytes());

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
    Assert.assertEquals(1024L, pool.getMaxBytes());

    pool.free(queryId, 256L);
    Assert.assertEquals(256L, pool.getQueryMemoryReservedBytes(queryId));
    Assert.assertEquals(256L, pool.getReservedBytes());
  }

  @Test
  public void testFreeNegative() {
    String queryId = "q0";
    Assert.assertTrue(pool.tryReserve(queryId, 512L));
    Assert.assertEquals(512L, pool.getQueryMemoryReservedBytes(queryId));
    Assert.assertEquals(512L, pool.getReservedBytes());
    Assert.assertEquals(1024L, pool.getMaxBytes());

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
    Assert.assertEquals(1024L, pool.getMaxBytes());

    try {
      pool.free(queryId, 513L);
      Assert.fail("Expect IllegalArgumentException");
    } catch (IllegalArgumentException ignore) {
    }
  }
}
