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

import org.apache.iotdb.commons.binaryallocator.arena.Arena;
import org.apache.iotdb.commons.binaryallocator.arena.ArenaStrategy;
import org.apache.iotdb.commons.binaryallocator.autoreleaser.Releaser;
import org.apache.iotdb.commons.binaryallocator.config.AllocatorConfig;
import org.apache.iotdb.commons.binaryallocator.evictor.Evictor;
import org.apache.iotdb.commons.binaryallocator.metric.BinaryAllocatorMetrics;
import org.apache.iotdb.commons.binaryallocator.utils.SizeClasses;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.utils.TestOnly;

import org.apache.tsfile.utils.PooledBinary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.ReferenceQueue;
import java.time.Duration;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public class BinaryAllocator {

  private static final Logger LOGGER = LoggerFactory.getLogger(BinaryAllocator.class);

  private final Arena[] heapArenas;
  private final AllocatorConfig allocatorConfig;

  private final ArenaStrategy arenaStrategy = new LeastUsedArenaStrategy();
  private final AtomicReference<BinaryAllocatorState> state =
      new AtomicReference<>(BinaryAllocatorState.UNINITIALIZED);

  private final BinaryAllocatorMetrics metrics;
  private Evictor sampleEvictor;
  private Releaser autoReleaser;
  private static final ThreadLocal<ThreadArenaRegistry> arenaRegistry =
      ThreadLocal.withInitial(ThreadArenaRegistry::new);

  private static final int WARNING_GC_TIME_PERCENTAGE = 20;
  private static final int HALF_GC_TIME_PERCENTAGE = 25;
  private static final int SHUTDOWN_GC_TIME_PERCENTAGE = 30;
  private static final int RESTART_GC_TIME_PERCENTAGE = 5;

  public final ReferenceQueue<PooledBinary> referenceQueue = new ReferenceQueue<>();

  // JDK 9+ Cleaner uses double-linked list and synchronized to manage references, which has worse
  // performance than lock-free hash set
  public final Set<PooledBinaryPhantomReference> phantomRefs =
      Collections.newSetFromMap(new ConcurrentHashMap<>());

  public BinaryAllocator(AllocatorConfig allocatorConfig) {
    this.allocatorConfig = allocatorConfig;

    heapArenas = new Arena[allocatorConfig.arenaNum];
    SizeClasses sizeClasses = new SizeClasses(allocatorConfig);

    for (int i = 0; i < heapArenas.length; i++) {
      Arena arena = new Arena(this, sizeClasses, i, allocatorConfig);
      heapArenas[i] = arena;
    }

    this.metrics = new BinaryAllocatorMetrics(this);

    if (allocatorConfig.enableBinaryAllocator) {
      start();
    } else {
      state.set(BinaryAllocatorState.CLOSE);
    }
  }

  public synchronized void start() {
    if (state.get() == BinaryAllocatorState.OPEN) {
      return;
    }

    state.set(BinaryAllocatorState.OPEN);
    MetricService.getInstance().addMetricSet(this.metrics);
    sampleEvictor =
        new SampleEvictor(
            ThreadName.BINARY_ALLOCATOR_SAMPLE_EVICTOR.getName(),
            allocatorConfig.durationShutdownTimeout,
            allocatorConfig.durationBetweenEvictorRuns);
    sampleEvictor.start();
    autoReleaser =
        new AutoReleaser(
            ThreadName.BINARY_ALLOCATOR_AUTO_RELEASER.getName(),
            allocatorConfig.durationShutdownTimeout);
    autoReleaser.start();
  }

  public synchronized void close(boolean forceClose) {
    if (forceClose) {
      state.set(BinaryAllocatorState.CLOSE);
      MetricService.getInstance().removeMetricSet(this.metrics);
    } else {
      state.set(BinaryAllocatorState.PENDING);
    }

    sampleEvictor.stop();
    autoReleaser.stop();
    for (Arena arena : heapArenas) {
      arena.close();
    }
  }

  public PooledBinary allocateBinary(int reqCapacity, boolean autoRelease) {
    if (reqCapacity < allocatorConfig.minAllocateSize
        || reqCapacity > allocatorConfig.maxAllocateSize
        || state.get() != BinaryAllocatorState.OPEN) {
      return new PooledBinary(new byte[reqCapacity]);
    }

    Arena arena = arenaStrategy.choose(heapArenas);

    return arena.allocate(reqCapacity, autoRelease);
  }

  public void deallocateBinary(PooledBinary binary) {
    if (binary != null
        && binary.getLength() >= allocatorConfig.minAllocateSize
        && binary.getLength() <= allocatorConfig.maxAllocateSize
        && state.get() == BinaryAllocatorState.OPEN) {
      int arenaIndex = binary.getArenaIndex();
      if (arenaIndex != -1) {
        Arena arena = heapArenas[arenaIndex];
        arena.deallocate(binary);
      }
    }
  }

  public long getTotalUsedMemory() {
    long totalUsedMemory = 0;
    for (Arena arena : heapArenas) {
      totalUsedMemory += arena.getTotalUsedMemory();
    }
    return totalUsedMemory;
  }

  public long getTotalActiveMemory() {
    long totalActiveMemory = 0;
    for (Arena arena : heapArenas) {
      totalActiveMemory += arena.getActiveMemory();
    }
    return totalActiveMemory;
  }

  @TestOnly
  public void resetArenaBinding() {
    arenaRegistry.get().unbindArena();
  }

  public BinaryAllocatorMetrics getMetrics() {
    return metrics;
  }

  private long evict(double ratio) {
    long evictedSize = 0;
    for (Arena arena : heapArenas) {
      evictedSize += arena.evict(ratio);
    }
    return evictedSize;
  }

  public static BinaryAllocator getInstance() {
    return BinaryAllocatorHolder.INSTANCE;
  }

  private static class BinaryAllocatorHolder {

    private static final BinaryAllocator INSTANCE =
        new BinaryAllocator(AllocatorConfig.DEFAULT_CONFIG);
  }

  private static class ThreadArenaRegistry {

    private Arena threadArenaBinding = null;

    public Arena getArena() {
      return threadArenaBinding;
    }

    public void bindArena(Arena arena) {
      threadArenaBinding = arena;
      arena.incRegisteredThread();
    }

    public void unbindArena() {
      Arena arena = threadArenaBinding;
      if (arena != null) {
        arena.decRegisteredThread();
        threadArenaBinding = null;
      }
    }

    @Override
    protected void finalize() {
      unbindArena();
    }
  }

  private static class LeastUsedArenaStrategy implements ArenaStrategy {

    @Override
    public Arena choose(Arena[] arenas) {
      Arena boundArena = arenaRegistry.get().getArena();
      if (boundArena != null) {
        return boundArena;
      }

      Arena minArena = arenas[0];

      for (int i = 1; i < arenas.length; i++) {
        Arena arena = arenas[i];
        if (arena.getNumRegisteredThread() < minArena.getNumRegisteredThread()) {
          minArena = arena;
        }
      }

      arenaRegistry.get().bindArena(minArena);
      return minArena;
    }
  }

  public void runGcEviction(long curGcTimePercent) {
    if (state.get() == BinaryAllocatorState.CLOSE) {
      return;
    }

    LOGGER.debug("Binary allocator running GC eviction");
    if (state.get() == BinaryAllocatorState.PENDING) {
      if (curGcTimePercent <= RESTART_GC_TIME_PERCENTAGE) {
        start();
      }
      return;
    }

    long evictedSize = 0;
    if (curGcTimePercent > SHUTDOWN_GC_TIME_PERCENTAGE) {
      LOGGER.info(
          "Binary allocator is shutting down because of high GC time percentage {}%.",
          curGcTimePercent);
      evictedSize = evict(1.0);
      close(false);
    } else if (curGcTimePercent > HALF_GC_TIME_PERCENTAGE) {
      evictedSize = evict(0.5);
    } else if (curGcTimePercent > WARNING_GC_TIME_PERCENTAGE) {
      evictedSize = evict(0.2);
    }
    metrics.updateGcEvictionCounter(evictedSize);
  }

  public class SampleEvictor extends Evictor {

    public SampleEvictor(
        String name, Duration evictorShutdownTimeoutDuration, Duration durationBetweenEvictorRuns) {
      super(name, evictorShutdownTimeoutDuration, durationBetweenEvictorRuns);
    }

    @Override
    public void run() {
      long evictedSize = 0;
      for (Arena arena : heapArenas) {
        evictedSize += arena.runSampleEviction();
      }
      metrics.updateSampleEvictionCounter(evictedSize);
    }
  }

  /** Process phantomly reachable objects and return their byte arrays to pool. */
  public class AutoReleaser extends Releaser {

    public AutoReleaser(String name, Duration shutdownTimeoutDuration) {
      super(name, shutdownTimeoutDuration);
    }

    @Override
    public void run() {
      PooledBinaryPhantomReference ref;
      try {
        while ((ref = (PooledBinaryPhantomReference) referenceQueue.remove()) != null) {
          phantomRefs.remove(ref);
          ref.slabRegion.deallocate(ref.byteArray);
        }
      } catch (InterruptedException e) {
        LOGGER.info("{} exits due to interruptedException.", name);
        Thread.currentThread().interrupt();
      }
    }
  }
}
