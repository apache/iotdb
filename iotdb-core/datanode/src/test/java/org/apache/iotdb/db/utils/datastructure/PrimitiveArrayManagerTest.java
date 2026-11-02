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

package org.apache.iotdb.db.utils.datastructure;

import org.apache.iotdb.db.conf.DataNodeMemoryConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.rescon.memory.PrimitiveArrayManager;
import org.apache.iotdb.db.utils.TSDataTypeTestUtils;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.db.storageengine.rescon.memory.PrimitiveArrayManager.ARRAY_SIZE;

public class PrimitiveArrayManagerTest {
  private DataNodeMemoryConfig dataNodeMemoryConfig =
      IoTDBDescriptor.getInstance().getMemoryConfig();

  @Test
  public void testGetArrayRowCount() {

    Assert.assertEquals(1224827, PrimitiveArrayManager.getArrayRowCount(1224826 * ARRAY_SIZE + 1));

    Assert.assertEquals(1224826, PrimitiveArrayManager.getArrayRowCount(1224826 * ARRAY_SIZE));

    Assert.assertEquals(1, PrimitiveArrayManager.getArrayRowCount(ARRAY_SIZE));

    Assert.assertEquals(1, PrimitiveArrayManager.getArrayRowCount(ARRAY_SIZE - 1));

    Assert.assertEquals(2, PrimitiveArrayManager.getArrayRowCount(ARRAY_SIZE + 1));
  }

  @Test
  public void testUpdateLimits() {
    double AMPLIFICATION_FACTOR = 1.5;

    /** threshold total size of arrays for all data types */
    double POOLED_ARRAYS_MEMORY_THRESHOLD =
        dataNodeMemoryConfig.getBufferedArraysMemoryManager().getTotalMemorySizeInBytes()
            / AMPLIFICATION_FACTOR;
    // LIMITS should be updated if (TOTAL_ALLOCATION_REQUEST_COUNT.get() > limitUpdateThreshold)
    int totalDataTypeSize = 0;
    for (TSDataType dataType : TSDataTypeTestUtils.getSupportedTypes()) {
      totalDataTypeSize += dataType.getDataTypeSize();
    }

    double limit = POOLED_ARRAYS_MEMORY_THRESHOLD / ARRAY_SIZE / totalDataTypeSize;
    for (int i = 0; i < limit + 1; i++) {
      for (TSDataType type : TSDataTypeTestUtils.getSupportedTypes()) {
        Object o = PrimitiveArrayManager.allocate(type);
        switch (type) {
          case BOOLEAN:
            Assert.assertTrue(o instanceof boolean[]);
            Assert.assertEquals(ARRAY_SIZE, ((boolean[]) o).length);
            break;
          case INT32:
          case DATE:
            Assert.assertTrue(o instanceof int[]);
            Assert.assertEquals(ARRAY_SIZE, ((int[]) o).length);
            break;
          case INT64:
          case TIMESTAMP:
            Assert.assertTrue(o instanceof long[]);
            Assert.assertEquals(ARRAY_SIZE, ((long[]) o).length);
            break;
          case FLOAT:
            Assert.assertTrue(o instanceof float[]);
            Assert.assertEquals(ARRAY_SIZE, ((float[]) o).length);
            break;
          case DOUBLE:
            Assert.assertTrue(o instanceof double[]);
            Assert.assertEquals(ARRAY_SIZE, ((double[]) o).length);
            break;
          case TEXT:
          case BLOB:
          case OBJECT:
          case STRING:
            Assert.assertTrue(o instanceof Binary[]);
            Assert.assertEquals(ARRAY_SIZE, ((Binary[]) o).length);
            break;
        }
      }
    }
  }

  @Test
  public void countsActualLengthsAndClearsOwnership() {
    PrimitiveArrayManager.close();
    try {
      int[] ints = new int[3];
      long[] longs = new long[0];
      Binary[] binaries = new Binary[97];
      binaries[0] = new Binary(new byte[32]);
      PrimitiveArrayManager.release(ints);
      PrimitiveArrayManager.release(longs);
      PrimitiveArrayManager.release(binaries);
      Assert.assertNull(binaries[0]);
      long bytes =
          RamUsageEstimator.shallowSizeOf(ints)
              + RamUsageEstimator.shallowSizeOf(longs)
              + RamUsageEstimator.shallowSizeOf(binaries);
      Assert.assertEquals(bytes, PrimitiveArrayManager.getMemoryBlock().getUsedMemoryInBytes());
      Assert.assertSame(ints, PrimitiveArrayManager.allocate(TSDataType.INT32));
      Assert.assertEquals(
          bytes - RamUsageEstimator.shallowSizeOf(ints),
          PrimitiveArrayManager.getMemoryBlock().getUsedMemoryInBytes());
      PrimitiveArrayManager.close();
      Assert.assertEquals(0, PrimitiveArrayManager.getMemoryBlock().getUsedMemoryInBytes());
    } finally {
      PrimitiveArrayManager.close();
    }
  }

  @Test
  public void cachedCalculatorsCoverAllSupportedTypes() {
    PrimitiveArrayManager.close();
    try {
      for (TSDataType type : TSDataTypeTestUtils.getSupportedTypes()) {
        Object array = PrimitiveArrayManager.allocate(type);
        long bytes = RamUsageEstimator.shallowSizeOf(array);
        PrimitiveArrayManager.release(array);
        Assert.assertEquals(bytes, PrimitiveArrayManager.getMemoryBlock().getUsedMemoryInBytes());
        // Alias types return to their primitive pool (e.g. DATE -> INT32, STRING -> TEXT).
        TSDataType poolType;
        switch (type) {
          case DATE:
            poolType = TSDataType.INT32;
            break;
          case TIMESTAMP:
            poolType = TSDataType.INT64;
            break;
          case STRING:
          case BLOB:
          case OBJECT:
            poolType = TSDataType.TEXT;
            break;
          default:
            poolType = type;
        }
        Assert.assertSame(array, PrimitiveArrayManager.allocate(poolType));
        Assert.assertEquals(0, PrimitiveArrayManager.getMemoryBlock().getUsedMemoryInBytes());
      }
    } finally {
      PrimitiveArrayManager.close();
    }
  }

  @Test
  public void concurrentBorrowReturnAndSnapshotsKeepExactTotal() throws Exception {
    PrimitiveArrayManager.close();
    TSDataType[] types = {TSDataType.INT64, TSDataType.INT32, TSDataType.BOOLEAN, TSDataType.TEXT};
    long expected = 0;
    for (TSDataType type : types) {
      Object array = PrimitiveArrayManager.allocate(type);
      expected += RamUsageEstimator.shallowSizeOf(array);
      PrimitiveArrayManager.release(array);
    }
    final long upperBound = expected;
    ExecutorService executor = Executors.newFixedThreadPool(5);
    CountDownLatch start = new CountDownLatch(1);
    List<Future<?>> futures = new ArrayList<>();
    try {
      for (TSDataType type : types) {
        futures.add(
            executor.submit(
                () -> {
                  start.await();
                  for (int i = 0; i < 2000; i++) {
                    Object array = PrimitiveArrayManager.allocate(type);
                    PrimitiveArrayManager.release(array);
                  }
                  return null;
                }));
      }
      futures.add(
          executor.submit(
              () -> {
                start.await();
                for (int i = 0; i < 8000; i++) {
                  long actual = PrimitiveArrayManager.getMemoryBlock().getUsedMemoryInBytes();
                  Assert.assertTrue(actual >= 0 && actual <= upperBound);
                }
                return null;
              }));
      start.countDown();
      for (Future<?> future : futures) {
        future.get(30, TimeUnit.SECONDS);
      }
      Assert.assertEquals(expected, PrimitiveArrayManager.getMemoryBlock().getUsedMemoryInBytes());
    } finally {
      start.countDown();
      executor.shutdownNow();
      Assert.assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
      PrimitiveArrayManager.close();
    }
    Assert.assertEquals(0, PrimitiveArrayManager.getMemoryBlock().getUsedMemoryInBytes());
  }
}
