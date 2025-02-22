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

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.utils.TestOnly;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongUnaryOperator;

public class MemoryManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(MemoryManager.class);
  private static final CommonConfig CONFIG = CommonDescriptor.getInstance().getConfig();
  private static final boolean ENABLE_MEMORY_TRANSFER = CONFIG.isEnableMemoryTransfer();
  private static final long MEMORY_CHECK_INTERVAL_IN_S = CONFIG.getMemoryCheckIntervalInS();

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

  /** The total allocate memory size in byte of memory manager */
  private final long allocateTotalMemorySizeInBytes;

  /** The total memory size in byte of memory manager */
  private long totalMemorySizeInBytes;

  /** The allocated memory size */
  private long allocatedMemorySizeInBytes = 0L;

  /** The parent memory manager */
  private final MemoryManager parentMemoryManager;

  /** The child memory manager */
  private final Map<String, MemoryManager> children = new ConcurrentHashMap<>();

  /** The allocated memory blocks of this memory manager */
  private final Map<String, IMemoryBlock> allocatedMemoryBlocks = new ConcurrentHashMap<>();

  @TestOnly
  public MemoryManager(long totalMemorySizeInBytes) {
    this.name = "Test";
    this.parentMemoryManager = null;
    this.allocateTotalMemorySizeInBytes = totalMemorySizeInBytes;
    this.totalMemorySizeInBytes = totalMemorySizeInBytes;
    this.enable = false;
  }

  private MemoryManager(
      String name, MemoryManager parentMemoryManager, long totalMemorySizeInBytes) {
    this.name = name;
    this.parentMemoryManager = parentMemoryManager;
    this.allocateTotalMemorySizeInBytes = totalMemorySizeInBytes;
    this.totalMemorySizeInBytes = totalMemorySizeInBytes;
    this.enable = false;
  }

  private MemoryManager(
      String name, MemoryManager parentMemoryManager, long totalMemorySizeInBytes, boolean enable) {
    this.name = name;
    this.parentMemoryManager = parentMemoryManager;
    this.allocateTotalMemorySizeInBytes = totalMemorySizeInBytes;
    this.totalMemorySizeInBytes = totalMemorySizeInBytes;
    this.enable = enable;
  }

  // region The Methods Of IMemoryBlock Management

  /**
   * Try to force allocate memory block with specified size in bytes
   *
   * @param name the name of memory block
   * @param sizeInBytes the size in bytes of memory block try to allocate
   * @param type the type of memory block
   * @return the memory block if success, otherwise throw MemoryException
   */
  public synchronized IMemoryBlock forceAllocate(
      String name, long sizeInBytes, MemoryBlockType type) {
    if (!enable) {
      return registerMemoryBlock(name, sizeInBytes, type);
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
  public synchronized IMemoryBlock forceAllocate(String name, MemoryBlockType memoryBlockType) {
    return forceAllocate(
        name, totalMemorySizeInBytes - allocatedMemorySizeInBytes, memoryBlockType);
  }

  /**
   * Try to force allocate memory block with specified size in bytes when memory is sufficient.
   *
   * @param name the name of memory block
   * @param sizeInBytes the size in bytes of memory block try to allocate
   * @param maxRatio the used threshold of allocatedMemorySizeInBytes / totalMemorySizeInBytes
   * @param memoryBlockType the type of memory block
   * @return the memory block if success, otherwise null
   */
  public synchronized IMemoryBlock forceAllocateIfSufficient(
      String name, long sizeInBytes, float maxRatio, MemoryBlockType memoryBlockType) {
    if (maxRatio < 0.0f || maxRatio > 1.0f) {
      return null;
    }
    if (!enable) {
      return registerMemoryBlock(name, sizeInBytes, memoryBlockType);
    }
    if (totalMemorySizeInBytes - allocatedMemorySizeInBytes >= sizeInBytes
        && (float) allocatedMemorySizeInBytes / totalMemorySizeInBytes < maxRatio) {
      return forceAllocate(name, sizeInBytes, memoryBlockType);
    } else {
      // TODO @spricoder: consider to find more memory in active way
      LOGGER.debug(
          "forceAllocateIfSufficient: failed to allocate memory, "
              + "total memory size {} bytes, used memory size {} bytes, "
              + "requested memory size {} bytes, used threshold {}",
          totalMemorySizeInBytes,
          allocatedMemorySizeInBytes,
          sizeInBytes,
          maxRatio);
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
  public synchronized IMemoryBlock tryAllocate(
      String name,
      long sizeInBytes,
      LongUnaryOperator customAllocateStrategy,
      MemoryBlockType type) {
    if (!enable) {
      return registerMemoryBlock(name, sizeInBytes, type);
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
   * @param name the name of memory block, UNIQUE
   * @param sizeInBytes the size in bytes of memory block
   * @param type the type of memory block
   * @return the memory block
   */
  private IMemoryBlock registerMemoryBlock(String name, long sizeInBytes, MemoryBlockType type) {
    if (sizeInBytes <= 0) {
      LOGGER.warn("forceAllocate {}: sizeInBytes should be positive", name);
    }
    return allocatedMemoryBlocks.compute(
        name,
        (blockName, block) -> {
          if (block != null) {
            LOGGER.warn("register memory block already exists: {}", block);
            return block;
          } else {
            allocatedMemorySizeInBytes += sizeInBytes;
            return new AtomicLongMemoryBlock(name, this, sizeInBytes, type);
          }
        });
  }

  /**
   * Release memory block and notify all waiting threads
   *
   * @param block the memory block to release
   */
  public synchronized void release(IMemoryBlock block) {
    if (block == null || block.isReleased()) {
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
    if (block == null || block.isReleased()) {
      return;
    }

    block.markAsReleased();
    allocatedMemorySizeInBytes -= block.getTotalMemorySizeInBytes();
    allocatedMemoryBlocks.remove(block.getName());
    try {
      block.close();
    } catch (Exception e) {
      LOGGER.error("releaseWithOutNotify: failed to close memory block", e);
    }
  }

  // endregion

  // region The Methods Of MemoryManager Management

  /**
   * Try to create a new memory manager with specified name and total memory size in bytes, then put
   * it into children map. NOTICE: if there are same name memory manager, it will return the
   * existing one.
   *
   * @param name the name of memory manager
   * @param sizeInBytes the total memory size in bytes of memory manager
   * @param enable whether memory management is enabled
   * @return the memory manager
   */
  public synchronized MemoryManager getOrCreateMemoryManager(
      String name, long sizeInBytes, boolean enable) {
    if (sizeInBytes <= 0) {
      LOGGER.warn("getOrCreateMemoryManager {}: sizeInBytes should be positive", name);
    }
    if (this.enable
        && sizeInBytes + this.allocatedMemorySizeInBytes > this.totalMemorySizeInBytes) {
      LOGGER.warn(
          "getOrCreateMemoryManager failed: total memory size {} bytes is less than allocated memory size {} bytes",
          sizeInBytes,
          allocatedMemorySizeInBytes);
      return null;
    }
    return children.compute(
        name,
        (managerName, manager) -> {
          if (manager != null) {
            LOGGER.warn(
                "getOrCreateMemoryManager failed: memory manager {} already exists, it's size is {}, enable is {}",
                managerName,
                manager.getTotalMemorySizeInBytes(),
                manager.isEnable());
            return manager;
          } else {
            allocatedMemorySizeInBytes += sizeInBytes;
            return new MemoryManager(name, this, sizeInBytes, enable);
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
  public synchronized MemoryManager getOrCreateMemoryManager(
      String name, long totalMemorySizeInBytes) {
    return getOrCreateMemoryManager(name, totalMemorySizeInBytes, false);
  }

  /**
   * Re-allocate memory according to ratio
   *
   * @param ratio the ratio of new total memory size to old total memory size
   */
  private void reAllocateMemoryAccordingToRatio(double ratio) {
    // first increase the total memory size of this memory manager
    this.totalMemorySizeInBytes *= ratio;
    // then re-allocate memory for all memory blocks
    for (IMemoryBlock block : allocatedMemoryBlocks.values()) {
      block.setTotalMemorySizeInBytes((long) (block.getTotalMemorySizeInBytes() * ratio));
    }
    // finally re-allocate memory for all child memory managers
    for (Map.Entry<String, MemoryManager> entry : children.entrySet()) {
      entry.getValue().reAllocateMemoryAccordingToRatio(ratio);
    }
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
    if (index >= names.length) return this;
    MemoryManager memoryManager = children.get(names[index]);
    if (memoryManager != null) {
      return memoryManager.getMemoryManager(index + 1, names);
    } else {
      return null;
    }
  }

  /**
   * Release the child memory manager with specified name
   *
   * @param memoryManagerName the name of memory manager
   */
  public void releaseChildMemoryManager(String memoryManagerName) {
    MemoryManager memoryManager = children.remove(memoryManagerName);
    if (memoryManager != null) {
      memoryManager.clearAll();
      allocatedMemorySizeInBytes -= memoryManager.getTotalMemorySizeInBytes();
    }
  }

  /** Clear all memory blocks and child memory managers */
  public synchronized void clearAll() {
    for (MemoryManager child : children.values()) {
      child.clearAll();
    }
    children.clear();
    for (IMemoryBlock block : allocatedMemoryBlocks.values()) {
      if (block == null || block.isReleased()) {
        continue;
      }

      block.markAsReleased();
      allocatedMemorySizeInBytes -= block.getTotalMemorySizeInBytes();

      try {
        block.close();
      } catch (Exception e) {
        LOGGER.error("releaseWithOutNotify: failed to close memory block", e);
      }
    }
    allocatedMemoryBlocks.clear();
    if (parentMemoryManager != null) {
      parentMemoryManager.releaseChildMemoryManager(name);
    }
  }

  // endregion

  // region The Methods of Attribute

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

  public void expandTotalMemorySizeInBytes(long totalMemorySizeInBytes) {
    this.totalMemorySizeInBytes += totalMemorySizeInBytes;
  }

  public void setTotalMemorySizeInBytesWithReload(long totalMemorySizeInBytes) {
    reAllocateMemoryAccordingToRatio((double) totalMemorySizeInBytes / this.totalMemorySizeInBytes);
  }

  /** Get available memory size in bytes of memory manager */
  public long getAvailableMemorySizeInBytes() {
    return totalMemorySizeInBytes - allocatedMemorySizeInBytes;
  }

  /** Get allocated memory size in bytes of memory manager */
  public long getAllocatedMemorySizeInBytes() {
    return allocatedMemorySizeInBytes;
  }

  /** Get used memory ratio */
  public double getUsedMemoryRatio() {
    return (double) getUsedMemorySizeInBytes() / totalMemorySizeInBytes;
  }

  /** Get actual used memory size in bytes of memory manager */
  public long getUsedMemorySizeInBytes() {
    long memorySize =
        allocatedMemoryBlocks.values().stream().mapToLong(IMemoryBlock::getUsedMemoryInBytes).sum();
    for (MemoryManager child : children.values()) {
      memorySize += child.getUsedMemorySizeInBytes();
    }
    return memorySize;
  }

  // endregion

  // region The Methods of GlobalMemoryManager

  public static MemoryManager global() {
    return MemoryManagerHolder.GLOBAL;
  }

  private static class MemoryManagerHolder {

    private static final MemoryManager GLOBAL =
        new MemoryManager("GlobalMemoryManager", null, Runtime.getRuntime().totalMemory());

    static {
      if (ENABLE_MEMORY_TRANSFER) {
        LOGGER.info(
            "Enable automatic memory transfer with an interval of {} s",
            MEMORY_CHECK_INTERVAL_IN_S);
        MemoryRuntimeAgent.getInstance()
            .registerPeriodicalJob(
                "GlobalMemoryManager#updateAllocate()",
                MemoryManagerHolder.GLOBAL::updateAllocate,
                MEMORY_CHECK_INTERVAL_IN_S);
      }
    }

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

  public void print() {
    print(0);
  }

  private void print(int index) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < index; i++) {
      sb.append("  ");
    }
    sb.append(this);
    LOGGER.info(sb.toString());
    for (IMemoryBlock block : allocatedMemoryBlocks.values()) {
      block.print(index + 2);
    }
    for (MemoryManager child : children.values()) {
      child.print(index + 1);
    }
  }

  /** Whether is able to shrink */
  public synchronized long shrink() {
    long shrinkSize =
        Math.min(
            getAvailableMemorySizeInBytes() / 10,
            totalMemorySizeInBytes - allocatedMemorySizeInBytes * 9 / 10);
    totalMemorySizeInBytes -= shrinkSize;
    return shrinkSize;
  }

  /** Whether is available to shrink */
  public boolean isAvailableToShrink() {
    return allocateTotalMemorySizeInBytes - totalMemorySizeInBytes
        < allocateTotalMemorySizeInBytes / 10;
  }

  public void updateAllocate() {
    if (children.isEmpty()) {
      double ratio = (double) totalMemorySizeInBytes / allocateTotalMemorySizeInBytes;
      for (IMemoryBlock memoryBlock : allocatedMemoryBlocks.values()) {
        memoryBlock.resizeByRatio(ratio);
      }
    } else {
      MemoryManager higherMemoryManager = null;
      MemoryManager lowerMemoryManager = null;
      // search the highest and lowest memory manager
      for (MemoryManager child : children.values()) {
        if (higherMemoryManager == null) {
          higherMemoryManager = child;
          lowerMemoryManager = child;
        } else {
          if (child.getUsedMemorySizeInBytes() > higherMemoryManager.getUsedMemorySizeInBytes()) {
            higherMemoryManager = child;
          }
          if (lowerMemoryManager.isAvailableToShrink()
              && child.getUsedMemorySizeInBytes() < lowerMemoryManager.getUsedMemorySizeInBytes()) {
            lowerMemoryManager = child;
          }
        }
      }
      if (higherMemoryManager != null && !higherMemoryManager.equals(lowerMemoryManager)) {
        // transfer
        long transferSize = higherMemoryManager.shrink();
        lowerMemoryManager.expandTotalMemorySizeInBytes(transferSize);
        LOGGER.info(
            "Transfer Memory Size {} from {} to {}",
            transferSize,
            lowerMemoryManager,
            higherMemoryManager);
      }
      for (MemoryManager memoryManager : children.values()) {
        memoryManager.updateAllocate();
      }
    }
  }
}
