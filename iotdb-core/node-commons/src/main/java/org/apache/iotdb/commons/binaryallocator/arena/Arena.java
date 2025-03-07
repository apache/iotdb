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

package org.apache.iotdb.commons.binaryallocator.arena;

import org.apache.iotdb.commons.binaryallocator.BinaryAllocator;
import org.apache.iotdb.commons.binaryallocator.PooledBinaryPhantomReference;
import org.apache.iotdb.commons.binaryallocator.config.AllocatorConfig;
import org.apache.iotdb.commons.binaryallocator.ema.AdaptiveWeightedAverage;
import org.apache.iotdb.commons.binaryallocator.utils.SizeClasses;

import org.apache.tsfile.utils.PooledBinary;

import java.lang.ref.ReferenceQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class Arena {

  private static final int EVICT_SAMPLE_COUNT = 100;

  private final BinaryAllocator binaryAllocator;
  private final SizeClasses sizeClasses;
  private final int arenaID;
  private final AtomicInteger numRegisteredThread;
  private final SlabRegion[] regions;

  private int sampleCount;

  private final ReferenceQueue<PooledBinary> referenceQueue;
  private final Set<PooledBinaryPhantomReference> phantomRefs;

  public Arena(
      BinaryAllocator allocator, SizeClasses sizeClasses, int id, AllocatorConfig allocatorConfig) {
    this.binaryAllocator = allocator;
    this.sizeClasses = sizeClasses;
    this.arenaID = id;
    this.numRegisteredThread = new AtomicInteger(0);
    regions = new SlabRegion[sizeClasses.getSizeClassNum()];

    for (int i = 0; i < regions.length; i++) {
      regions[i] = new SlabRegion(sizeClasses.sizeIdx2size(i), allocatorConfig);
    }

    sampleCount = 0;
    referenceQueue = binaryAllocator.referenceQueue;
    phantomRefs = binaryAllocator.phantomRefs;
  }

  public int getArenaID() {
    return arenaID;
  }

  public PooledBinary allocate(int reqCapacity, boolean autoRelease) {
    final int sizeIdx = sizeClasses.size2SizeIdx(reqCapacity);
    byte[] data = regions[sizeIdx].allocate();
    if (autoRelease) {
      PooledBinary binary = new PooledBinary(data, reqCapacity, -1);
      PooledBinaryPhantomReference ref =
          new PooledBinaryPhantomReference(binary, referenceQueue, data, regions[sizeIdx]);
      phantomRefs.add(ref);
      return binary;
    } else {
      return new PooledBinary(data, reqCapacity, arenaID);
    }
  }

  public void deallocate(PooledBinary binary) {
    final int sizeIdx = sizeClasses.size2SizeIdx(binary.getLength());
    regions[sizeIdx].deallocate(binary.getValues());
  }

  public long evict(double ratio) {
    long evictedSize = 0;
    for (SlabRegion region : regions) {
      evictedSize += region.evict(ratio);
    }
    return evictedSize;
  }

  public void close() {
    sampleCount = 0;
    for (SlabRegion region : regions) {
      region.close();
    }
  }

  public long getTotalUsedMemory() {
    long totalUsedMemory = 0;
    for (SlabRegion region : regions) {
      totalUsedMemory += region.getTotalUsedMemory();
    }
    return totalUsedMemory;
  }

  public long getActiveMemory() {
    long totalActiveMemory = 0;
    for (SlabRegion region : regions) {
      totalActiveMemory += region.getActiveUsedMemory();
    }
    return totalActiveMemory;
  }

  public int getNumRegisteredThread() {
    return numRegisteredThread.get();
  }

  public void incRegisteredThread() {
    this.numRegisteredThread.incrementAndGet();
  }

  public void decRegisteredThread() {
    this.numRegisteredThread.decrementAndGet();
  }

  public long runSampleEviction() {
    // update metric
    long allocateFromSlabDelta = 0;
    long allocateFromJVMDelta = 0;
    for (SlabRegion region : regions) {
      allocateFromSlabDelta +=
          (long) region.byteArraySize
              * (region.allocationsFromAllocator.get() - region.prevAllocations);
      region.prevAllocations = region.allocationsFromAllocator.get();
      allocateFromJVMDelta +=
          (long) region.byteArraySize
              * (region.allocationsFromJVM.get() - region.prevAllocationsFromJVM);
      region.prevAllocationsFromJVM = region.allocationsFromJVM.get();
    }
    binaryAllocator
        .getMetrics()
        .updateAllocationCounter(allocateFromSlabDelta, allocateFromJVMDelta);

    // Start sampling
    for (SlabRegion region : regions) {
      region.updateSample();
    }

    sampleCount++;
    long evictedSize = 0;
    if (sampleCount == EVICT_SAMPLE_COUNT) {
      // Evict
      for (SlabRegion region : regions) {
        evictedSize += region.resize();
      }
      sampleCount = 0;
    }
    return evictedSize;
  }

  public static class SlabRegion {
    private final int byteArraySize;

    // Current implementation uses ConcurrentLinkedQueue for simplicity
    // TODO: Can be optimized with more efficient lock-free approaches:
    // 1. No need for strict FIFO, it's just an object pool
    // 2. Use segmented arrays/queues with per-segment counters to reduce contention
    private final ConcurrentLinkedQueue<byte[]> queue;

    private final AtomicInteger allocationsFromAllocator;
    private final AtomicInteger allocationsFromJVM;
    private final AtomicInteger deAllocationsToAllocator;
    private final AtomicInteger evictions;

    public int prevAllocations;
    public int prevAllocationsFromJVM;
    AdaptiveWeightedAverage average;

    SlabRegion(int byteArraySize, AllocatorConfig allocatorConfig) {
      this.byteArraySize = byteArraySize;
      this.average = new AdaptiveWeightedAverage(allocatorConfig.arenaPredictionWeight);
      queue = new ConcurrentLinkedQueue<>();
      allocationsFromAllocator = new AtomicInteger(0);
      allocationsFromJVM = new AtomicInteger(0);
      deAllocationsToAllocator = new AtomicInteger(0);
      evictions = new AtomicInteger(0);
      prevAllocations = 0;
      prevAllocationsFromJVM = 0;
    }

    public final byte[] allocate() {
      byte[] bytes = queue.poll();
      if (bytes == null) {
        allocationsFromJVM.incrementAndGet();
        return new byte[this.byteArraySize];
      }
      allocationsFromAllocator.incrementAndGet();
      return bytes;
    }

    public void deallocate(byte[] bytes) {
      deAllocationsToAllocator.incrementAndGet();
      queue.add(bytes);
    }

    private void updateSample() {
      average.sample(getActiveSize());
    }

    private long resize() {
      average.update();
      int needRemain = (int) Math.ceil(average.average()) - getActiveSize();
      return evict(getQueueSize() - needRemain);
    }

    private long evict(double ratio) {
      return evict((int) (getQueueSize() * ratio));
    }

    private long evict(int num) {
      long evicted = 0;
      while (num > 0 && !queue.isEmpty()) {
        queue.poll();
        evictions.incrementAndGet();
        num--;
        evicted += byteArraySize;
      }
      return evicted;
    }

    private long getTotalUsedMemory() {
      return (long) byteArraySize * getQueueSize();
    }

    private long getActiveUsedMemory() {
      return (long) byteArraySize * getActiveSize();
    }

    // ConcurrentLinkedQueue::size() is O(n)
    private int getQueueSize() {
      return deAllocationsToAllocator.get() - allocationsFromAllocator.get() - evictions.get();
    }

    private int getActiveSize() {
      return allocationsFromAllocator.get()
          + allocationsFromJVM.get()
          - deAllocationsToAllocator.get();
    }

    private void close() {
      queue.clear();
      allocationsFromAllocator.set(0);
      allocationsFromJVM.set(0);
      deAllocationsToAllocator.set(0);
      evictions.set(0);
      prevAllocations = 0;
      prevAllocationsFromJVM = 0;
      average.clear();
    }
  }
}
