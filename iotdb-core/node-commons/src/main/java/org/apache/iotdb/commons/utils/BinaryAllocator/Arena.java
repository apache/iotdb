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

package org.apache.iotdb.commons.utils.BinaryAllocator;

import org.apache.iotdb.commons.service.metric.JvmGcMonitorMetrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;

public class Arena {
  private static final Logger LOGGER = LoggerFactory.getLogger(Arena.class);
  private final int arenaID;
  private SlabRegion[] regions;
  private final SizeClasses sizeClasses;
  private Evictor evictor;
  private BinaryAllocator binaryAllocator;
  public AtomicInteger numRegisterThread = new AtomicInteger(0);

  private int sampleCount;
  private final int EVICT_SAMPLE_COUNT = 100;
  private static final long GC_TIME_EVICTOR_PERCENTAGE = 30L;

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
    startEvictor(durationBetweenEvictionRuns);
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
    stopEvictor();
  }

  public void restart() {
    startEvictor(durationBetweenEvictionRuns);
  }

  /**
   * Starts the evictor with the given delay. If there is an evictor running when this method is
   * called, it is stopped and replaced with a new evictor with the specified delay.
   *
   * @param delay time in milliseconds before start and between eviction runs
   */
  final void startEvictor(final Duration delay) {
    LOGGER.info("Starting evictor with delay {}", delay);
    final boolean isPositiveDelay = isPositive(delay);
    if (evictor == null) { // Starting evictor for the first time or after a cancel
      if (isPositiveDelay) { // Starting new evictor
        evictor = new Evictor();
        EvictionTimer.schedule(evictor, delay, delay, "arena-evictor-" + arenaID);
      }
    } else if (isPositiveDelay) { // Restart
      EvictionTimer.cancel(evictor, evictorShutdownTimeoutDuration, true);
      evictor = new Evictor();
      EvictionTimer.schedule(evictor, delay, delay, "arena-evictor-" + arenaID);
    } else { // Stopping evictor
      EvictionTimer.cancel(evictor, evictorShutdownTimeoutDuration, false);
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
      totalActiveMemory += region.size * (region.allocations.get() - region.deallocations.get());
    }
    return totalActiveMemory;
  }

  void stopEvictor() {
    startEvictor(Duration.ofMillis(-1L));
  }

  static boolean isPositive(final Duration delay) {
    return delay != null && !delay.isNegative() && !delay.isZero();
  }

  public class Evictor implements Runnable {
    private ScheduledFuture<?> scheduledFuture;

    /** Cancels the scheduled future. */
    void cancel() {
      scheduledFuture.cancel(false);
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

      if (JvmGcMonitorMetrics.getInstance().getGcData().getGcTimePercentage()
          > GC_TIME_EVICTOR_PERCENTAGE) {
        for (SlabRegion region : regions) {
          region.evict(1.0);
        }
        binaryAllocator.close();
        return;
      } else if (JvmGcMonitorMetrics.getInstance().getGcData().getGcTimePercentage() > 20) {
        for (SlabRegion region : regions) {
          region.evict(0.5);
        }
      } else if (JvmGcMonitorMetrics.getInstance().getGcData().getGcTimePercentage() > 10) {
        for (SlabRegion region : regions) {
          region.evict(0.2);
        }
      }

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

    void setScheduledFuture(final ScheduledFuture<?> scheduledFuture) {
      this.scheduledFuture = scheduledFuture;
    }

    @Override
    public String toString() {
      return getClass().getName() + " [scheduledFuture=" + scheduledFuture + "]";
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
