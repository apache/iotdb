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

import java.time.Duration;
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
  private static final ThreadLocal<ThreadArenaRegistry> arenaRegistry =
      ThreadLocal.withInitial(ThreadArenaRegistry::new);

  private static final int WARNING_GC_TIME_PERCENTAGE = 10;
  private static final int HALF_GC_TIME_PERCENTAGE = 20;
  private static final int SHUTDOWN_GC_TIME_PERCENTAGE = 30;
  private static final int RESTART_GC_TIME_PERCENTAGE = 5;

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
            allocatorConfig.durationEvictorShutdownTimeout);
    sampleEvictor.startEvictor(allocatorConfig.durationBetweenEvictorRuns);
  }

  public synchronized void close(boolean forceClose) {
    if (forceClose) {
      state.set(BinaryAllocatorState.CLOSE);
      MetricService.getInstance().removeMetricSet(this.metrics);
    } else {
      state.set(BinaryAllocatorState.PENDING);
    }

    sampleEvictor.stopEvictor();
    for (Arena arena : heapArenas) {
      arena.close();
    }
  }

  public PooledBinary allocateBinary(int reqCapacity) {
    if (reqCapacity < allocatorConfig.minAllocateSize
        || reqCapacity > allocatorConfig.maxAllocateSize
        || state.get() != BinaryAllocatorState.OPEN) {
      return new PooledBinary(new byte[reqCapacity]);
    }

    Arena arena = arenaStrategy.choose(heapArenas);

    return new PooledBinary(arena.allocate(reqCapacity), reqCapacity, arena.getArenaID());
  }

  public void deallocateBinary(PooledBinary binary) {
    if (binary != null
        && binary.getLength() >= allocatorConfig.minAllocateSize
        && binary.getLength() <= allocatorConfig.maxAllocateSize
        && state.get() == BinaryAllocatorState.OPEN) {
      int arenaIndex = binary.getArenaIndex();
      if (arenaIndex != -1) {
        Arena arena = heapArenas[arenaIndex];
        arena.deallocate(binary.getValues());
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

  private void evict(double ratio) {
    for (Arena arena : heapArenas) {
      arena.evict(ratio);
    }
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

    if (curGcTimePercent > SHUTDOWN_GC_TIME_PERCENTAGE) {
      LOGGER.info(
          "Binary allocator is shutting down because of high GC time percentage {}%.",
          curGcTimePercent);
      evict(1.0);
      close(false);
    } else if (curGcTimePercent > HALF_GC_TIME_PERCENTAGE) {
      evict(0.5);
    } else if (curGcTimePercent > WARNING_GC_TIME_PERCENTAGE) {
      evict(0.2);
    }
  }

  public class SampleEvictor extends Evictor {

    public SampleEvictor(String name, Duration evictorShutdownTimeoutDuration) {
      super(name, evictorShutdownTimeoutDuration);
    }

    @Override
    public void run() {
      for (Arena arena : heapArenas) {
        arena.runSampleEviction();
      }
    }
  }
}
