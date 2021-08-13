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

package org.apache.iotdb.db.query.udf.datastructure;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LRUCacheTest {

  private static final int CACHE_SIZE = 3;
  private static final int DATA_SIZE = CACHE_SIZE << 3;

  private LRUCache cache;

  @Before
  public void setUp() {
    cache = new LRUCache(CACHE_SIZE);
  }

  @Test
  public void testPutAndGetRandomly() {
    cache.set(0, 0);
    Assert.assertEquals(0, cache.get(0));
    cache.set(4, 4);
    Assert.assertEquals(4, cache.get(4));
    cache.set(10, 10);
    Assert.assertEquals(10, cache.get(10));

    Assert.assertEquals(0, cache.get(0));
    Assert.assertEquals(4, cache.get(4));
    Assert.assertEquals(10, cache.get(10));

    cache.set(DATA_SIZE - 1, DATA_SIZE - 1);
    Assert.assertEquals(DATA_SIZE - 1, cache.get(DATA_SIZE - 1));
    cache.set(DATA_SIZE - CACHE_SIZE, DATA_SIZE - CACHE_SIZE);
    Assert.assertEquals(DATA_SIZE - CACHE_SIZE, cache.get(DATA_SIZE - CACHE_SIZE));
    cache.set(DATA_SIZE - 2 * CACHE_SIZE, DATA_SIZE - 2 * CACHE_SIZE);
    Assert.assertEquals(DATA_SIZE - 2 * CACHE_SIZE, cache.get(DATA_SIZE - 2 * CACHE_SIZE));

    Assert.assertEquals(DATA_SIZE - 1, cache.get(DATA_SIZE - 1));
    Assert.assertEquals(DATA_SIZE - CACHE_SIZE, cache.get(DATA_SIZE - CACHE_SIZE));
    Assert.assertEquals(DATA_SIZE - 2 * CACHE_SIZE, cache.get(DATA_SIZE - 2 * CACHE_SIZE));

    Assert.assertEquals(0, cache.get(0));
    Assert.assertEquals(4, cache.get(4));
    Assert.assertEquals(10, cache.get(10));

    cache.set(1, 1);
    Assert.assertEquals(1, cache.get(1));
    cache.set(3, 3);
    Assert.assertEquals(3, cache.get(3));

    Assert.assertEquals(DATA_SIZE - 1, cache.get(DATA_SIZE - 1));
    Assert.assertEquals(DATA_SIZE - CACHE_SIZE, cache.get(DATA_SIZE - CACHE_SIZE));
    Assert.assertEquals(DATA_SIZE - 2 * CACHE_SIZE, cache.get(DATA_SIZE - 2 * CACHE_SIZE));

    Assert.assertEquals(0, cache.get(0));
    Assert.assertEquals(4, cache.get(4));
    Assert.assertEquals(10, cache.get(10));

    Assert.assertEquals(1, cache.get(1));
    Assert.assertEquals(3, cache.get(3));
  }

  @Test
  public void testPutAndGetOrderly() {
    for (int i = 0; i < DATA_SIZE; ++i) {
      cache.set(i, i);
    }
    for (int i = 0; i < DATA_SIZE; ++i) {
      Assert.assertEquals(i, cache.get(i));
    }

    for (int i = 0; i < DATA_SIZE; ++i) {
      cache.set(i, DATA_SIZE - i);
    }
    for (int i = 0; i < DATA_SIZE; ++i) {
      Assert.assertEquals(DATA_SIZE - i, cache.get(i));
    }

    for (int i = DATA_SIZE - 1; 0 <= i; --i) {
      cache.set(i, i);
    }
    for (int i = 0; i < DATA_SIZE; ++i) {
      cache.set(i, i);
    }

    for (int i = DATA_SIZE - 1; 0 <= i; --i) {
      cache.set(i, i);
      cache.set(i, i);
      cache.set(i, i);
    }
    for (int i = 0; i < DATA_SIZE; ++i) {
      cache.set(i, i);
    }
  }
}
