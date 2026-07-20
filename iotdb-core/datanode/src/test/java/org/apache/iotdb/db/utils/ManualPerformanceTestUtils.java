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

package org.apache.iotdb.db.utils;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class ManualPerformanceTestUtils {

  private static final ThreadMXBean THREAD_MX_BEAN = ManagementFactory.getThreadMXBean();
  private static final com.sun.management.ThreadMXBean ALLOCATION_MX_BEAN =
      THREAD_MX_BEAN instanceof com.sun.management.ThreadMXBean
          ? (com.sun.management.ThreadMXBean) THREAD_MX_BEAN
          : null;
  private static final Runnable NO_OP = () -> {};

  private ManualPerformanceTestUtils() {}

  public static boolean enableThreadMetrics() {
    if (!THREAD_MX_BEAN.isCurrentThreadCpuTimeSupported()
        || ALLOCATION_MX_BEAN == null
        || !ALLOCATION_MX_BEAN.isThreadAllocatedMemorySupported()) {
      return false;
    }
    try {
      if (!THREAD_MX_BEAN.isThreadCpuTimeEnabled()) {
        THREAD_MX_BEAN.setThreadCpuTimeEnabled(true);
      }
      if (!ALLOCATION_MX_BEAN.isThreadAllocatedMemoryEnabled()) {
        ALLOCATION_MX_BEAN.setThreadAllocatedMemoryEnabled(true);
      }
      final long threadId = Thread.currentThread().getId();
      return THREAD_MX_BEAN.getCurrentThreadCpuTime() >= 0
          && ALLOCATION_MX_BEAN.getThreadAllocatedBytes(threadId) >= 0;
    } catch (final UnsupportedOperationException | SecurityException ignored) {
      return false;
    }
  }

  public static Measurement measure(final int iterations, final Runnable operation) {
    return measure(iterations, NO_OP, operation);
  }

  public static Measurement measure(
      final int iterations, final Runnable beforeEachIteration, final Runnable operation) {
    final List<MemoryPoolMXBean> heapPools = getHeapMemoryPools();
    System.gc();
    System.runFinalization();
    heapPools.forEach(MemoryPoolMXBean::resetPeakUsage);
    final long baselineHeapBytes = getUsedHeapBytes(heapPools);

    final long threadId = Thread.currentThread().getId();
    final long allocatedBytesBefore = ALLOCATION_MX_BEAN.getThreadAllocatedBytes(threadId);
    long cpuNanos = 0;
    for (int i = 0; i < iterations; ++i) {
      beforeEachIteration.run();
      final long cpuNanosBefore = THREAD_MX_BEAN.getCurrentThreadCpuTime();
      operation.run();
      cpuNanos += THREAD_MX_BEAN.getCurrentThreadCpuTime() - cpuNanosBefore;
    }
    final long allocatedBytes =
        ALLOCATION_MX_BEAN.getThreadAllocatedBytes(threadId) - allocatedBytesBefore;
    final long peakHeapDeltaBytes = Math.max(0L, getPeakHeapBytes(heapPools) - baselineHeapBytes);
    return new Measurement(cpuNanos, allocatedBytes, peakHeapDeltaBytes);
  }

  public static Summary summarize(final Measurement[] measurements, final int iterations) {
    final long[] cpuNanos = new long[measurements.length];
    final long[] allocatedBytes = new long[measurements.length];
    final long[] peakHeapDeltaBytes = new long[measurements.length];
    for (int i = 0; i < measurements.length; ++i) {
      cpuNanos[i] = measurements[i].cpuNanos;
      allocatedBytes[i] = measurements[i].allocatedBytes;
      peakHeapDeltaBytes[i] = measurements[i].peakHeapDeltaBytes;
    }
    return new Summary(
        median(cpuNanos) / iterations,
        median(allocatedBytes) / iterations,
        median(peakHeapDeltaBytes));
  }

  private static List<MemoryPoolMXBean> getHeapMemoryPools() {
    final List<MemoryPoolMXBean> heapPools = new ArrayList<>();
    for (final MemoryPoolMXBean memoryPool : ManagementFactory.getMemoryPoolMXBeans()) {
      if (memoryPool.getType() == MemoryType.HEAP && memoryPool.isValid()) {
        heapPools.add(memoryPool);
      }
    }
    return heapPools;
  }

  private static long getUsedHeapBytes(final List<MemoryPoolMXBean> heapPools) {
    long usedHeapBytes = 0;
    for (final MemoryPoolMXBean heapPool : heapPools) {
      final MemoryUsage usage = heapPool.getUsage();
      if (usage != null) {
        usedHeapBytes += usage.getUsed();
      }
    }
    return usedHeapBytes;
  }

  private static long getPeakHeapBytes(final List<MemoryPoolMXBean> heapPools) {
    long peakHeapBytes = 0;
    for (final MemoryPoolMXBean heapPool : heapPools) {
      final MemoryUsage peakUsage = heapPool.getPeakUsage();
      if (peakUsage != null) {
        peakHeapBytes += peakUsage.getUsed();
      }
    }
    return peakHeapBytes;
  }

  private static double median(final long[] values) {
    Arrays.sort(values);
    final int middle = values.length / 2;
    return (values.length & 1) == 1
        ? values[middle]
        : values[middle - 1] + (values[middle] - values[middle - 1]) / 2.0;
  }

  public static final class Measurement {

    private final long cpuNanos;
    private final long allocatedBytes;
    private final long peakHeapDeltaBytes;

    private Measurement(
        final long cpuNanos, final long allocatedBytes, final long peakHeapDeltaBytes) {
      this.cpuNanos = cpuNanos;
      this.allocatedBytes = allocatedBytes;
      this.peakHeapDeltaBytes = peakHeapDeltaBytes;
    }
  }

  public static final class Summary {

    private final double cpuNanosPerOperation;
    private final double allocatedBytesPerOperation;
    private final double peakHeapDeltaBytes;

    private Summary(
        final double cpuNanosPerOperation,
        final double allocatedBytesPerOperation,
        final double peakHeapDeltaBytes) {
      this.cpuNanosPerOperation = cpuNanosPerOperation;
      this.allocatedBytesPerOperation = allocatedBytesPerOperation;
      this.peakHeapDeltaBytes = peakHeapDeltaBytes;
    }

    public double getCpuNanosPerOperation() {
      return cpuNanosPerOperation;
    }

    public double getAllocatedBytesPerOperation() {
      return allocatedBytesPerOperation;
    }

    public double getPeakHeapDeltaBytes() {
      return peakHeapDeltaBytes;
    }
  }
}
