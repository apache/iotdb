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

package org.apache.iotdb.commons.utils.binaryallocator;

import org.apache.iotdb.commons.service.metric.JvmGcMonitorMetrics;
import org.apache.iotdb.commons.service.metric.MetricService;

import org.apache.tsfile.utils.PooledBinary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

public class BinaryAllocator {

  private static final Logger LOGGER = LoggerFactory.getLogger(BinaryAllocator.class);

  private final Arena[] heapArenas;
  private final AllocatorConfig allocatorConfig;

  public static final BinaryAllocator DEFAULT = new BinaryAllocator(AllocatorConfig.DEFAULT_CONFIG);
  private ArenaStrategy arenaStrategy = new LeastUsedArenaStrategy();
  private AtomicReference<BinaryAllocatorState> state =
      new AtomicReference<>(BinaryAllocatorState.UNINITIALIZED);

  private BinaryAllocatorMetrics metrics;
  private static ThreadLocal<ThreadArenaRegistry> arenaRegistry =
      ThreadLocal.withInitial(() -> new ThreadArenaRegistry());

  private Evictor evictor;
  private static final String GC_EVICTOR_NAME = "binary-allocator-gc-evictor";

  private static final int WARNING_GC_TIME_PERCENTAGE = 10;
  private static final int HALF_GC_TIME_PERCENTAGE = 20;
  private static final int SHUTDOWN_GC_TIME_PERCENTAGE = 30;
  private static final int RESTART_GC_TIME_PERCENTAGE = 20;

  public BinaryAllocator(AllocatorConfig allocatorConfig) {
    this.allocatorConfig = allocatorConfig;

    heapArenas = newArenaArray(allocatorConfig.arenaNum);
    SizeClasses sizeClasses = new SizeClasses(allocatorConfig);

    for (int i = 0; i < heapArenas.length; i++) {
      Arena arena = new Arena(this, sizeClasses, i, allocatorConfig);
      heapArenas[i] = arena;
    }

    this.metrics = new BinaryAllocatorMetrics(this);
    MetricService.getInstance().addMetricSet(this.metrics);

    if (allocatorConfig.enableBinaryAllocator) {
      state.set(BinaryAllocatorState.OPEN);
      evictor = new GCEvictor(GC_EVICTOR_NAME, allocatorConfig.durationEvictorShutdownTimeout);
      evictor.startEvictor(allocatorConfig.durationBetweenEvictorRuns);
    } else {
      state.set(BinaryAllocatorState.CLOSE);
      this.close(false);
    }
  }

  public PooledBinary allocateBinary(int reqCapacity) {
    if (reqCapacity < allocatorConfig.minAllocateSize
        || reqCapacity > allocatorConfig.maxAllocateSize) {
      return new PooledBinary(new byte[reqCapacity]);
    }

    Arena arena = arenaStrategy.choose(heapArenas);

    return new PooledBinary(arena.allocate(reqCapacity), reqCapacity, arena.getArenaID());
  }

  public void deallocateBinary(PooledBinary binary) {
    if (binary != null
        && binary.getLength() >= allocatorConfig.minAllocateSize
        && binary.getLength() <= allocatorConfig.maxAllocateSize) {
      int arenaIndex = binary.getArenaIndex();
      if (arenaIndex != -1) {
        Arena arena = heapArenas[arenaIndex];
        arena.deallocate(binary.getValues());
      }
    }
  }

  public void deallocateBatch(PooledBinary[] blobs) {
    for (PooledBinary blob : blobs) {
      deallocateBinary(blob);
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

  public void evict(double ratio) {
    for (Arena arena : heapArenas) {
      arena.evict(ratio);
    }
  }

  public boolean isOpen() {
    return state.get() == BinaryAllocatorState.OPEN;
  }

  public void close(boolean needReopen) {
    if (needReopen) {
      state.set(BinaryAllocatorState.TMP_CLOSE);
    } else {
      state.set(BinaryAllocatorState.CLOSE);
      if (evictor != null) {
        evictor.stopEvictor();
      }
    }
    for (Arena arena : heapArenas) {
      arena.close();
    }
  }

  private void restart() {
    state.set(BinaryAllocatorState.OPEN);
    for (Arena arena : heapArenas) {
      arena.restart();
    }
  }

  public BinaryAllocatorMetrics getMetrics() {
    return metrics;
  }

  @SuppressWarnings("unchecked")
  private static Arena[] newArenaArray(int size) {
    return new Arena[size];
  }

  private static class ThreadArenaRegistry {
    private Arena threadArenaBinding = null;

    public Arena getArena() {
      return threadArenaBinding;
    }

    public void bindArena(Arena arena) {
      threadArenaBinding = arena;
      arena.numRegisterThread.incrementAndGet();
    }

    public void unbindArena() {
      Arena arena = threadArenaBinding;
      if (arena != null) {
        arena.numRegisterThread.decrementAndGet();
      }
    }

    @Override
    protected void finalize() {
      unbindArena();
    }
  }

  private class LeastUsedArenaStrategy implements ArenaStrategy {
    @Override
    public Arena choose(Arena[] arenas) {
      Arena boundArena = arenaRegistry.get().getArena();
      if (boundArena != null) {
        return boundArena;
      }

      if (arenas == null || arenas.length == 0) {
        return null;
      }

      Arena minArena = arenas[0];

      for (int i = 1; i < arenas.length; i++) {
        Arena arena = arenas[i];
        if (arena.numRegisterThread.get() < minArena.numRegisterThread.get()) {
          minArena = arena;
        }
      }

      arenaRegistry.get().bindArena(minArena);
      return minArena;
    }
  }

  public class GCEvictor extends Evictor {
    public GCEvictor(String name, Duration evictorShutdownTimeoutDuration) {
      super(name, evictorShutdownTimeoutDuration);
    }

    @Override
    public void run() {
      LOGGER.debug("Binary allocator running evictor");
      long GcTimePercent = JvmGcMonitorMetrics.getInstance().getGcData().getGcTimePercentage();
      if (state.get() == BinaryAllocatorState.TMP_CLOSE) {
        if (GcTimePercent > RESTART_GC_TIME_PERCENTAGE) {
          restart();
        }
        return;
      }

      if (GcTimePercent > SHUTDOWN_GC_TIME_PERCENTAGE) {
        LOGGER.warn(
            "Binary allocator is shutting down because of high GC time percentage{}",
            GcTimePercent);
        for (Arena arena : heapArenas) {
          arena.evict(1.0);
        }
        close(true);
      } else if (GcTimePercent > HALF_GC_TIME_PERCENTAGE) {
        LOGGER.warn(
            "Binary allocator is half evicting because of high GC time percentage{}",
            GcTimePercent);
        for (Arena arena : heapArenas) {
          arena.evict(0.5);
        }
      } else if (GcTimePercent > WARNING_GC_TIME_PERCENTAGE) {
        LOGGER.warn(
            "Binary allocator is running evictor because of high GC time percentage{}",
            GcTimePercent);
        for (Arena arena : heapArenas) {
          arena.evict(0.2);
        }
      }
    }
  }
}
