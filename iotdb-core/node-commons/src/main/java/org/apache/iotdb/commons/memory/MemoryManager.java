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

package org.apache.iotdb.commons.memory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongUnaryOperator;

public class MemoryManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(MemoryManager.class);

  // TODO @spricoder: make it configurable
  /** Whether memory management is enabled */
  private static final boolean ENABLED = false;

  /** Max retry times for memory allocation */
  private static final int MEMORY_ALLOCATE_MAX_RETRIES = 3;

  /** Retry interval for memory allocation */
  private static final long MEMORY_ALLOCATE_RETRY_INTERVAL_IN_MS = 1000;

  /** Min memory size to allocate */
  private static final long MEMORY_ALLOCATE_MIN_SIZE_IN_BYTES = 32;

  /** Name of memory manager */
  private final String name;

  /** Total memory size in byte of memory manager */
  private long totalMemorySizeInBytes = 0L;

  /** Allocated memory size to allocate */
  private long allocatedMemorySizeInBytes = 0L;

  /** Parent memory manager, used to apply for memory */
  private final MemoryManager parentMemoryManager;

  /** Child memory manager, used to statistic memory */
  private final Map<String, MemoryManager> childrens = new ConcurrentHashMap<>();

  /** Allocated memory blocks of this memory manager */
  private final Set<MemoryBlock> allocatedMemoryBlocks = new HashSet<>();

  public MemoryManager(
      String name, MemoryManager parentMemoryManager, long totalMemorySizeInBytes) {
    this.name = name;
    this.parentMemoryManager = parentMemoryManager;
    this.parentMemoryManager.addChildMemoryManager(name, this);
    this.totalMemorySizeInBytes = totalMemorySizeInBytes;
  }

  /** Try to force allocate memory block with specified size in bytes. */
  private MemoryBlock forceAllocate(String name, long sizeInBytes, MemoryBlockType type) {
    if (!ENABLED) {
      return new MemoryBlock(name, this, sizeInBytes, type);
    }
    for (int i = 0; i < MEMORY_ALLOCATE_MAX_RETRIES; i++) {
      if (totalMemorySizeInBytes - allocatedMemorySizeInBytes >= sizeInBytes) {
        return registerMemoryBlock(name, sizeInBytes, type);
      }

      try {
        // TODO @spricoder: consider to find more memory in active way
        Thread.sleep(MEMORY_ALLOCATE_RETRY_INTERVAL_IN_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.warn("forceAllocate: interrupted while waiting for available memory", e);
      }
    }

    throw new MemoryException(
        String.format(
            "forceAllocate: failed to allocate memory after %d retries, "
                + "total memory size %d bytes, used memory size %d bytes, "
                + "requested memory size %d bytes",
            MEMORY_ALLOCATE_MAX_RETRIES,
            totalMemorySizeInBytes,
            allocatedMemorySizeInBytes,
            sizeInBytes));
  }

  /** Try to force allocate memory block with specified size in bytes when memory is sufficient. */
  public synchronized MemoryBlock forceAllocateIfSufficient(
      String name, long sizeInBytes, float usedThreshold) {
    if (usedThreshold < 0.0f || usedThreshold > 1.0f) {
      return null;
    }
    if (!ENABLED) {
      return new MemoryBlock(name, this, sizeInBytes);
    }
    if (totalMemorySizeInBytes - allocatedMemorySizeInBytes >= sizeInBytes
        && (float) allocatedMemorySizeInBytes / totalMemorySizeInBytes < usedThreshold) {
      return forceAllocate(name, sizeInBytes, MemoryBlockType.NONE);
    } else {
      // TODO @spricoder: consider to find more memory in active way
      LOGGER.debug(
          "forceAllocateIfSufficient: failed to allocate memory, "
              + "total memory size {} bytes, used memory size {} bytes, "
              + "requested memory size {} bytes, used threshold {}",
          totalMemorySizeInBytes,
          allocatedMemorySizeInBytes,
          sizeInBytes,
          usedThreshold);
    }

    return null;
  }

  /** Try to allocate memory block with customAllocateStrategy */
  public synchronized MemoryBlock tryAllocate(
      String name,
      long sizeInBytes,
      LongUnaryOperator customAllocateStrategy,
      MemoryBlockType type) {
    if (!ENABLED) {
      return new MemoryBlock(name, this, sizeInBytes);
    }

    if (totalMemorySizeInBytes - allocatedMemorySizeInBytes >= sizeInBytes) {
      return registerMemoryBlock(name, sizeInBytes, type);
    }

    long sizeToAllocateInBytes = sizeInBytes;
    while (sizeToAllocateInBytes > MEMORY_ALLOCATE_MIN_SIZE_IN_BYTES) {
      if (totalMemorySizeInBytes - allocatedMemorySizeInBytes >= sizeToAllocateInBytes) {
        LOGGER.info(
            "tryAllocate: allocated memory, "
                + "total memory size {} bytes, used memory size {} bytes, "
                + "original requested memory size {} bytes, "
                + "actual requested memory size {} bytes",
            totalMemorySizeInBytes,
            allocatedMemorySizeInBytes,
            sizeInBytes,
            sizeToAllocateInBytes);
        return registerMemoryBlock(name, sizeToAllocateInBytes, type);
      }

      sizeToAllocateInBytes =
          Math.max(
              customAllocateStrategy.applyAsLong(sizeToAllocateInBytes),
              MEMORY_ALLOCATE_MIN_SIZE_IN_BYTES);
    }

    // TODO @spricoder: consider to find more memory in active way
    LOGGER.warn(
        "tryAllocate: failed to allocate memory, "
            + "total memory size {} bytes, used memory size {} bytes, "
            + "requested memory size {} bytes",
        totalMemorySizeInBytes,
        allocatedMemorySizeInBytes,
        sizeInBytes);
    return registerMemoryBlock(name, 0, type);
  }

  /** Try to register memory block with specified size in bytes. */
  private MemoryBlock registerMemoryBlock(String name, long sizeInBytes, MemoryBlockType type) {
    allocatedMemorySizeInBytes += sizeInBytes;
    final MemoryBlock memoryBlock = new MemoryBlock(name, this, sizeInBytes, type);
    allocatedMemoryBlocks.add(memoryBlock);
    return memoryBlock;
  }

  /** Release memory block. */
  public synchronized void release(MemoryBlock block) {
    if (!ENABLED || block == null || block.isReleased()) {
      return;
    }

    block.markAsReleased();
    allocatedMemorySizeInBytes -= block.getMemoryUsageInBytes();
    allocatedMemoryBlocks.remove(block);

    this.notifyAll();
  }

  public synchronized void addChildMemoryManager(String name, MemoryManager childMemoryManager) {
    if (childMemoryManager != null) {
      childrens.put(name, childMemoryManager);
    }
  }

  public MemoryManager getMemoryManager(String... names) {
    return getMemoryManager(0, names);
  }

  public MemoryManager getMemoryManager(int index, String... names) {
    if (index >= names.length) return null;
    MemoryManager memoryManager = childrens.get(names[index]);
    if (memoryManager != null) {
      return getMemoryManager(index + 1, names);
    } else {
      return null;
    }
  }

  public long getFreeMemorySizeInBytes() {
    return totalMemorySizeInBytes - allocatedMemorySizeInBytes;
  }

  public long getUsedMemorySizeInBytes() {
    long memorySize =
        allocatedMemoryBlocks.stream().mapToLong(MemoryBlock::getMemoryUsageInBytes).sum();
    for (MemoryManager child : childrens.values()) {
      memorySize += child.getUsedMemorySizeInBytes();
    }
    return memorySize;
  }

  public long getAllocatedMemorySizeInBytes() {
    return allocatedMemorySizeInBytes;
  }

  public long getTotalMemorySizeInBytes() {
    return totalMemorySizeInBytes;
  }

  public static MemoryManager global() {
    return MemoryManagerHolder.GLOBAL;
  }

  private static class MemoryManagerHolder {

    private static final MemoryManager GLOBAL =
        new MemoryManager("GlobalMemoryManager", null, Runtime.getRuntime().totalMemory());

    private MemoryManagerHolder() {}
  }
}
