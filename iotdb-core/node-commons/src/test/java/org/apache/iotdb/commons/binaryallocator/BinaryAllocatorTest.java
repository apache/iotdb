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

package org.apache.iotdb.commons.binaryallocator;

import org.apache.iotdb.commons.binaryallocator.config.AllocatorConfig;
import org.apache.iotdb.commons.binaryallocator.utils.SizeClasses;

import org.apache.tsfile.utils.PooledBinary;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class BinaryAllocatorTest {
  @Test
  public void testAllocateBinary() {
    AllocatorConfig config = new AllocatorConfig();
    config.arenaNum = 1;
    BinaryAllocator binaryAllocator = new BinaryAllocator(config);
    binaryAllocator.resetArenaBinding();

    PooledBinary binary = binaryAllocator.allocateBinary(255);
    assertNotNull(binary);
    assertEquals(binary.getArenaIndex(), -1);
    assertEquals(binary.getLength(), 255);
    binaryAllocator.deallocateBinary(binary);

    binary = binaryAllocator.allocateBinary(65536);
    assertNotNull(binary);
    assertEquals(binary.getArenaIndex(), 0);
    assertEquals(binary.getLength(), 65536);
    binaryAllocator.deallocateBinary(binary);

    binary = binaryAllocator.allocateBinary(65535);
    assertNotNull(binary);
    assertEquals(binary.getArenaIndex(), 0);
    assertEquals(binary.getLength(), 65535);
    assertEquals(binary.getValues().length, 65536);
    binaryAllocator.deallocateBinary(binary);
  }

  @Test
  public void testStrategy() throws InterruptedException {
    BinaryAllocator binaryAllocator = new BinaryAllocator(AllocatorConfig.DEFAULT_CONFIG);
    binaryAllocator.resetArenaBinding();

    PooledBinary binary1 = binaryAllocator.allocateBinary(4096);
    PooledBinary binary2 = binaryAllocator.allocateBinary(4096);
    assertEquals(binary1.getArenaIndex(), binary2.getArenaIndex());
    binaryAllocator.deallocateBinary(binary1);
    binaryAllocator.deallocateBinary(binary2);

    int threadCount = 4;
    CountDownLatch latch = new CountDownLatch(threadCount);
    Map<Integer, Integer> arenaUsageCount = new ConcurrentHashMap<>();
    for (int i = 0; i < threadCount; i++) {
      Thread thread =
          new Thread(
              () -> {
                try {
                  PooledBinary firstBinary = binaryAllocator.allocateBinary(2048);
                  int arenaId = firstBinary.getArenaIndex();
                  arenaUsageCount.merge(arenaId, 1, Integer::sum);
                  binaryAllocator.deallocateBinary(firstBinary);
                } finally {
                  latch.countDown();
                }
              });
      thread.start();
    }

    latch.await();
    int maxUsage = Collections.max(arenaUsageCount.values());
    int minUsage = Collections.min(arenaUsageCount.values());
    assertEquals(maxUsage, minUsage);
  }

  @Test
  public void testEviction() throws InterruptedException {
    AllocatorConfig config = new AllocatorConfig();
    config.arenaNum = 1;
    config.minAllocateSize = config.maxAllocateSize = 4096;
    config.setTimeBetweenEvictorRunsMillis(1);
    BinaryAllocator binaryAllocator = new BinaryAllocator(config);
    binaryAllocator.resetArenaBinding();

    PooledBinary binary = binaryAllocator.allocateBinary(4096);
    binaryAllocator.deallocateBinary(binary);
    assertEquals(binaryAllocator.getTotalUsedMemory(), 4096);
    Thread.sleep(200);
    assertEquals(binaryAllocator.getTotalUsedMemory(), 0);
  }

  @Test
  public void testSizeMapping() {
    AllocatorConfig config = new AllocatorConfig();
    config.minAllocateSize = 4096;
    config.maxAllocateSize = 65536;
    SizeClasses sizeClasses = new SizeClasses(config);

    assertEquals(sizeClasses.getSizeClassNum(), 33);
    int[] testSizes = {4607, 8191, 16383, 32767, 65535};

    for (int size : testSizes) {
      int sizeIdx = sizeClasses.size2SizeIdx(size);
      int mappedSize = sizeClasses.sizeIdx2size(sizeIdx);

      assertEquals("Mapped size should be >= original size", mappedSize, size + 1);

      if (sizeIdx > 0) {
        int previousSize = sizeClasses.sizeIdx2size(sizeIdx - 1);
        assertTrue("Previous size should be < original size", previousSize < size);
      }
    }
  }
}
