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
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class LRUCacheTest {

  private static final Logger logger = LoggerFactory.getLogger(LRUCacheTest.class);

  private static final int CACHE_SIZE = 3;
  private static final int DATA_SIZE = CACHE_SIZE << 3;

  private LRUCache cache;

  private int PERFORMANCE_CACHE_SIZE = 60;
  private LRUCache performance_cache;
  private int LOOP_COUNT = 50;
  private int PER_LOOP_NUM = 10000000;
  private Random r = new Random();

  @Before
  public void setUp() {
    cache = new LRUCache(CACHE_SIZE);
    performance_cache = new LRUCache(PERFORMANCE_CACHE_SIZE);
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

  private double loopsForRandom(int data_size) {
    long begin = 0, end;
    double sum = 0;
    int[] random_data = new int[data_size];
    for (int i = 0; i < data_size; i++) {
      random_data[i] = r.nextInt(data_size);
    }
    for (int i = 0; i < data_size; i++) {
      performance_cache.set(i, i);
    }
    for (int i = 0; i < LOOP_COUNT; i++) {
      for (int j = 0; j < PER_LOOP_NUM; j++) {
        if (j == 1000) {
          begin = System.currentTimeMillis();
        }
        int n = random_data[j % data_size];
        int value = performance_cache.get(n);
        performance_cache.set(n, value + 1);
      }
      end = System.currentTimeMillis();
      sum += (end - begin) * 1.0 / 1000;
    }
    return sum;
  }

  @Test
  @Ignore
  public void testRandomPerformance() {
    double[] rate = new double[] {0.07, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9};
    for (int i = 0; i < rate.length; i++) {
      double sum = loopsForRandom((int) (PERFORMANCE_CACHE_SIZE / rate[i]));
      logger.info("Random LRUCache " + rate[i] + " " + (sum / LOOP_COUNT));
    }
  }

  private double loopsForScan(int data_size) {
    long begin = 0, end;
    double sum = 0;
    for (int i = 0; i < data_size; i++) {
      performance_cache.set(i, i);
    }
    for (int i = 0; i < LOOP_COUNT; i++) {
      for (int j = 0; j < PER_LOOP_NUM; j++) {
        if (j == 1000) {
          begin = System.currentTimeMillis();
        }
        int value = performance_cache.get(j % data_size);
        performance_cache.set(j % data_size, value + 1);
      }
      end = System.currentTimeMillis();
      sum += (end - begin) * 1.0 / 1000;
    }
    return sum;
  }

  @Test
  @Ignore
  public void testScanPerformance() {
    double[] rate = new double[] {0.07, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9};
    for (int i = 0; i < rate.length; i++) {
      double sum = loopsForScan((int) (PERFORMANCE_CACHE_SIZE / rate[i]));
      logger.info("Scan LRUCache " + rate[i] + " " + (sum / LOOP_COUNT));
    }
  }
}
