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

package org.apache.iotdb.db.utils.binaryallocator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class Arena {
  private static final Logger LOGGER = LoggerFactory.getLogger(Arena.class);
  private final int arenaID;
  private SlabRegion[] regions;
  private final SizeClasses sizeClasses;
  private Evictor sampleEvictor;
  private BinaryAllocator binaryAllocator;
  public AtomicInteger numRegisterThread = new AtomicInteger(0);

  private int sampleCount;
  private final int EVICT_SAMPLE_COUNT = 100;

  private final Duration evictorShutdownTimeoutDuration;
  private final Duration durationBetweenEvictionRuns;

  public Arena(
      BinaryAllocator allocator, SizeClasses sizeClasses, int id, AllocatorConfig allocatorConfig) {
    this.sizeClasses = sizeClasses;
    this.arenaID = id;
    this.evictorShutdownTimeoutDuration = allocatorConfig.getDurationEvictorShutdownTimeout();
    this.durationBetweenEvictionRuns = allocatorConfig.getDurationBetweenEvictorRuns();
    this.binaryAllocator = allocator;
    regions = new SlabRegion[sizeClasses.getSizeClassNum()];

    for (int i = 0; i < regions.length; i++) {
      regions[i] = new SlabRegion(sizeClasses.sizeIdx2size(i), allocatorConfig);
    }

    sampleCount = 0;

    restart();
  }

  public int getArenaID() {
    return arenaID;
  }

  public byte[] allocate(int reqCapacity) {
    final int sizeIdx = sizeClasses.size2SizeIdx(reqCapacity);

    SlabRegion region = regions[sizeIdx];
    return region.allocate();
  }

  public void deallocate(byte[] bytes) {
    final int sizeIdx = sizeClasses.size2SizeIdx(bytes.length);

    SlabRegion region = regions[sizeIdx];
    region.deallocate(bytes);
  }

  public void evict(double ratio) {
    for (SlabRegion region : regions) {
      region.evict(ratio);
    }
  }

  public void close() {
    evict(1.0);
    sampleEvictor.stopEvictor();
  }

  public void restart() {
    sampleEvictor =
        new SampleEvictor("arena-" + arenaID + "-sample-evictor", evictorShutdownTimeoutDuration);
    sampleEvictor.startEvictor(durationBetweenEvictionRuns);
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
      totalActiveMemory += region.size * (region.allocations.get() - region.deallocations.get());
    }
    return totalActiveMemory;
  }

  static boolean isPositive(final Duration delay) {
    return delay != null && !delay.isNegative() && !delay.isZero();
  }

  public class SampleEvictor extends Evictor {

    public SampleEvictor(String name, Duration evictorShutdownTimeoutDuration) {
      super(name, evictorShutdownTimeoutDuration);
    }

    @Override
    public void run() {
      LOGGER.debug("Arena-{} running evictor", arenaID);

      // update metric
      int allocateFromSlabDelta = 0, allocateFromJVMDelta = 0;
      for (SlabRegion region : regions) {
        allocateFromSlabDelta += region.size * (region.allocations.get() - region.prevAllocations);
        region.prevAllocations = region.allocations.get();
        allocateFromJVMDelta +=
            region.size * (region.allocationsFromJVM.get() - region.prevAllocationsFromJVM);
        region.prevAllocationsFromJVM = region.allocationsFromJVM.get();
      }
      binaryAllocator.getMetrics().updateCounter(allocateFromSlabDelta, allocateFromJVMDelta);

      // Start sampling
      for (SlabRegion region : regions) {
        region.updateSample();
      }

      sampleCount++;
      if (sampleCount == EVICT_SAMPLE_COUNT) {
        // Evict
        for (SlabRegion region : regions) {
          region.resize();
        }
        sampleCount = 0;
      }
    }
  }

  private static class SlabRegion {
    private final int size;
    private final Queue<byte[]> queue;

    private final AtomicInteger allocations;
    private final AtomicInteger allocationsFromJVM;
    private final AtomicInteger deallocations;
    private final AtomicInteger evictions;

    public int prevAllocations;
    public int prevAllocationsFromJVM;
    AdaptiveWeightedAverage average;

    SlabRegion(int size, AllocatorConfig allocatorConfig) {
      this.size = size;
      this.average = new AdaptiveWeightedAverage(allocatorConfig.arenaPredictionWeight);
      queue = new ConcurrentLinkedQueue<>();
      allocations = new AtomicInteger(0);
      allocationsFromJVM = new AtomicInteger(0);
      deallocations = new AtomicInteger(0);
      evictions = new AtomicInteger(0);
      prevAllocations = 0;
      prevAllocationsFromJVM = 0;
    }

    public final byte[] allocate() {
      byte[] bytes = queue.poll();
      if (bytes == null) {
        allocationsFromJVM.incrementAndGet();
        return new byte[this.size];
      }
      allocations.incrementAndGet();
      return bytes;
    }

    public void deallocate(byte[] bytes) {
      deallocations.incrementAndGet();
      queue.add(bytes);
    }

    public void updateSample() {
      average.sample(getActiveSize());
    }

    public void resize() {
      int needRemain = (int) Math.ceil(average.average()) - getActiveSize();
      int evictNum = getQueueSize() - needRemain;
      while (evictNum > 0 && !queue.isEmpty()) {
        queue.poll();
        evictions.incrementAndGet();
        evictNum--;
      }
    }

    public void evict(double ratio) {
      int remain = getQueueSize();
      remain = (int) (remain * ratio);
      while (remain > 0 && !queue.isEmpty()) {
        queue.poll();
        evictions.incrementAndGet();
        remain--;
      }
    }

    public long getTotalUsedMemory() {
      return (long) size * getQueueSize();
    }

    // ConcurrentLinkedQueue::size() is O(n)
    private int getQueueSize() {
      return deallocations.get() - allocations.get() - evictions.get();
    }

    private int getActiveSize() {
      return allocations.get() + allocationsFromJVM.get() - deallocations.get();
    }
  }
}
