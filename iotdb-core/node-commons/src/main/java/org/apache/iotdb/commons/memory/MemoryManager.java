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

  /** The max retry times for memory allocation */
  private static final int MEMORY_ALLOCATE_MAX_RETRIES = 3;

  /** The retry interval for memory allocation */
  private static final long MEMORY_ALLOCATE_RETRY_INTERVAL_IN_MS = 1000;

  /** The min memory size to allocate */
  private static final long MEMORY_ALLOCATE_MIN_SIZE_IN_BYTES = 32;

  /** The name of memory manager */
  private final String name;

  /** Whether memory management is enabled */
  private final boolean enable;

  /** The total memory size in byte of memory manager */
  private long totalMemorySizeInBytes = 0L;

  /** The allocated memory size */
  private long allocatedMemorySizeInBytes = 0L;

  /** The parent memory manager */
  private final MemoryManager parentMemoryManager;

  /** The child memory manager */
  private final Map<String, MemoryManager> children = new ConcurrentHashMap<>();

  /** The allocated memory blocks of this memory manager */
  private final Set<MemoryBlock> allocatedMemoryBlocks = new HashSet<>();

  private MemoryManager(
      String name, MemoryManager parentMemoryManager, long totalMemorySizeInBytes) {
    this.name = name;
    this.parentMemoryManager = parentMemoryManager;
    this.totalMemorySizeInBytes = totalMemorySizeInBytes;
    this.enable = false;
  }

  private MemoryManager(
      String name, MemoryManager parentMemoryManager, long totalMemorySizeInBytes, boolean enable) {
    this.name = name;
    this.parentMemoryManager = parentMemoryManager;
    this.totalMemorySizeInBytes = totalMemorySizeInBytes;
    this.enable = enable;
  }

  // region memory block management

  /**
   * Try to force allocate memory block with specified size in bytes
   *
   * @param name the name of memory block
   * @param sizeInBytes the size in bytes of memory block try to allocate
   * @param type the type of memory block
   * @return the memory block if success, otherwise throw MemoryException
   */
  public MemoryBlock forceAllocate(String name, long sizeInBytes, MemoryBlockType type) {
    if (!enable) {
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

  /**
   * Try to force allocate memory block with total memory size in bytes
   *
   * @param name the name of memory block
   * @param memoryBlockType the type of memory block
   */
  public MemoryBlock forceAllocate(String name, MemoryBlockType memoryBlockType) {
    return forceAllocate(name, totalMemorySizeInBytes, memoryBlockType);
  }

  /**
   * Try to force allocate memory block with specified size in bytes when memory is sufficient.
   *
   * @param name the name of memory block
   * @param sizeInBytes the size in bytes of memory block try to allocate
   * @param usedThreshold the used threshold of allocatedMemorySizeInBytes / totalMemorySizeInBytes
   * @return the memory block if success, otherwise null
   */
  public synchronized MemoryBlock forceAllocateIfSufficient(
      String name, long sizeInBytes, float usedThreshold) {
    if (usedThreshold < 0.0f || usedThreshold > 1.0f) {
      return null;
    }
    if (!enable) {
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

  /**
   * Try to allocate memory block with customAllocateStrategy
   *
   * @param name the name of memory block
   * @param sizeInBytes the size in bytes of memory block try to allocate
   * @param customAllocateStrategy the custom allocate strategy when memory is insufficient
   * @param type the type of memory block
   * @return the memory block if success, otherwise null
   */
  public synchronized MemoryBlock tryAllocate(
      String name,
      long sizeInBytes,
      LongUnaryOperator customAllocateStrategy,
      MemoryBlockType type) {
    if (!enable) {
      return new MemoryBlock(name, this, sizeInBytes);
    }

    if (totalMemorySizeInBytes - allocatedMemorySizeInBytes >= sizeInBytes) {
      return registerMemoryBlock(name, sizeInBytes, type);
    }

    long sizeToAllocateInBytes = sizeInBytes;
    while (sizeToAllocateInBytes > MEMORY_ALLOCATE_MIN_SIZE_IN_BYTES) {
      if (totalMemorySizeInBytes - allocatedMemorySizeInBytes >= sizeToAllocateInBytes) {
        LOGGER.debug(
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

  /**
   * Try to register memory block with specified size in bytes
   *
   * @param name the name of memory block
   * @param sizeInBytes the size in bytes of memory block
   * @param type
   * @return the memory block
   */
  private MemoryBlock registerMemoryBlock(String name, long sizeInBytes, MemoryBlockType type) {
    allocatedMemorySizeInBytes += sizeInBytes;
    final MemoryBlock memoryBlock = new MemoryBlock(name, this, sizeInBytes, type);
    allocatedMemoryBlocks.add(memoryBlock);
    return memoryBlock;
  }

  /**
   * Release memory block and notify all waiting threads
   *
   * @param block the memory block to release
   */
  public synchronized void release(IMemoryBlock block) {
    if (!enable || block == null || block.isReleased()) {
      return;
    }
    releaseWithOutNotify(block);
    this.notifyAll();
  }

  /**
   * Release memory block without notify
   *
   * @param block the memory block to release
   */
  public synchronized void releaseWithOutNotify(IMemoryBlock block) {
    if (!enable || block == null || block.isReleased()) {
      return;
    }

    block.markAsReleased();
    allocatedMemorySizeInBytes -= block.getMemoryUsageInBytes();
    allocatedMemoryBlocks.remove(block);
  }

  // endregion

  // region memory manager management

  /**
   * Try to create a new memory manager with specified name and total memory size in bytes, then put
   * it into children map. NOTICE: if there are same name memory manager, it will return the
   * existing one.
   *
   * @param name the name of memory manager
   * @param totalMemorySizeInBytes the total memory size in bytes of memory manager
   * @param enable whether memory management is enabled
   * @return the memory manager
   */
  public MemoryManager gerOrCreateMemoryManager(
      String name, long totalMemorySizeInBytes, boolean enable) {
    MemoryManager result = new MemoryManager(name, this, totalMemorySizeInBytes, enable);
    return children.compute(
        name,
        (managerName, manager) -> {
          if (manager != null) {
            LOGGER.warn(
                "gerOrCreateMemoryManager failed: memory manager {} already exists, it's size is {}, enable is {}",
                managerName,
                manager.getTotalMemorySizeInBytes(),
                manager.isEnable());
            return manager;
          } else {
            return result;
          }
        });
  }

  /**
   * Try to create a new memory manager with specified name and total memory size in bytes, then put
   * it into children map. NOTICE: if there are same name memory manager, it will return the
   * existing one.
   *
   * @param name the name of memory manager
   * @param totalMemorySizeInBytes the total memory size in bytes of memory manager
   * @return the memory manager
   */
  public MemoryManager getOrCreateMemoryManager(String name, long totalMemorySizeInBytes) {
    return gerOrCreateMemoryManager(name, totalMemorySizeInBytes, false);
  }

  /**
   * Get the memory manager with specified names in levels
   *
   * @param names the names of memory manager in levels
   * @return the memory manager if find it, otherwise null
   */
  public MemoryManager getMemoryManager(String... names) {
    return getMemoryManager(0, names);
  }

  /**
   * Get the memory manager with specified names in levels
   *
   * @param index the index of names
   * @param names the names of memory manager in levels
   * @return the memory manager if find it, otherwise null
   */
  private MemoryManager getMemoryManager(int index, String... names) {
    if (index >= names.length) return null;
    MemoryManager memoryManager = children.get(names[index]);
    if (memoryManager != null) {
      return getMemoryManager(index + 1, names);
    } else {
      return null;
    }
  }

  /**
   * Remove the child memory manager with specified name
   *
   * @param memoryManagerName the name of memory manager
   */
  public void removeChildMemoryManager(String memoryManagerName) {
    children.remove(memoryManagerName);
  }

  /** Clear all memory blocks and child memory managers */
  public void clearAll() {
    for (MemoryManager child : children.values()) {
      child.clearAll();
    }
    children.clear();
    for (MemoryBlock block : allocatedMemoryBlocks) {
      releaseWithOutNotify(block);
    }
    allocatedMemoryBlocks.clear();
    parentMemoryManager.removeChildMemoryManager(name);
  }

  // endregion

  // region attribute related

  public String getName() {
    return name;
  }

  public boolean isEnable() {
    return enable;
  }

  /** Get total memory size in bytes of memory manager */
  public long getTotalMemorySizeInBytes() {
    return totalMemorySizeInBytes;
  }

  public void setTotalMemorySizeInBytes(long totalMemorySizeInBytes) {
    this.totalMemorySizeInBytes = totalMemorySizeInBytes;
  }

  /** Get available memory size in bytes of memory manager */
  public long getAvailableMemorySizeInBytes() {
    return totalMemorySizeInBytes - allocatedMemorySizeInBytes;
  }

  /** Get allocated memory size in bytes of memory manager */
  public long getAllocatedMemorySizeInBytes() {
    return allocatedMemorySizeInBytes;
  }

  /** Get actual used memory size in bytes of memory manager */
  public long getUsedMemorySizeInBytes() {
    long memorySize =
        allocatedMemoryBlocks.stream().mapToLong(MemoryBlock::getMemoryUsageInBytes).sum();
    for (MemoryManager child : children.values()) {
      memorySize += child.getUsedMemorySizeInBytes();
    }
    return memorySize;
  }

  // endregion

  // region global memory manager

  public static MemoryManager global() {
    return MemoryManagerHolder.GLOBAL;
  }

  private static class MemoryManagerHolder {

    private static final MemoryManager GLOBAL =
        new MemoryManager("GlobalMemoryManager", null, Runtime.getRuntime().totalMemory());

    private MemoryManagerHolder() {}
  }

  // endregion

  @Override
  public String toString() {
    return "MemoryManager{"
        + "name="
        + name
        + ", enable="
        + enable
        + ", totalMemorySizeInBytes="
        + totalMemorySizeInBytes
        + ", allocatedMemorySizeInBytes="
        + allocatedMemorySizeInBytes
        + '}';
  }
}
