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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.LongUnaryOperator;

public class MemoryManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(MemoryManager.class);

  private static final CommonConfig CONFIG = CommonDescriptor.getInstance().getConfig();

  /** The max retry times for memory allocation */
  private static final int MEMORY_ALLOCATE_MAX_RETRIES = 3;

  /** The retry interval for memory allocation */
  private static final long MEMORY_ALLOCATE_RETRY_INTERVAL_IN_MS = 1000;

  /** The min memory size to allocate */
  private static final long MEMORY_ALLOCATE_MIN_SIZE_IN_BYTES = 32;

  /** Whether to take all dynamic part into consideration when shrink */
  private boolean shrinkAll = CONFIG.isShrinkAll();

  /** The threshold of memory update */
  private double lowBound = CONFIG.getLowBound();

  private double stepRatio = CONFIG.getStepRatio();

  /** The name of memory manager */
  private final String name;

  /** Whether memory management is enabled */
  private final boolean enabled;

  /** The initial allocate memory size in byte of memory manager */
  private volatile long initialAllocatedMemorySizeInBytes;

  /** The total memory size in byte of memory manager */
  private volatile long totalMemorySizeInBytes;

  /** The allocated memory size */
  private volatile long allocatedMemorySizeInBytes = 0L;

  /** The parent memory manager */
  private final MemoryManager parentMemoryManager;

  /** The child memory manager */
  private final Map<String, MemoryManager> children = new ConcurrentHashMap<>();

  /** The allocated memory blocks of this memory manager */
  private final Map<String, IMemoryBlock> allocatedMemoryBlocks = new ConcurrentHashMap<>();

  protected final AtomicReference<BiConsumer<Long, Long>> updateCallback = new AtomicReference<>();

  @TestOnly
  public MemoryManager(long totalMemorySizeInBytes) {
    this.name = "Test";
    this.parentMemoryManager = null;
    this.initialAllocatedMemorySizeInBytes = totalMemorySizeInBytes;
    this.totalMemorySizeInBytes = totalMemorySizeInBytes;
    this.enabled = false;
  }

  MemoryManager(String name, MemoryManager parentMemoryManager, long totalMemorySizeInBytes) {
    this.name = name;
    this.parentMemoryManager = parentMemoryManager;
    this.initialAllocatedMemorySizeInBytes = totalMemorySizeInBytes;
    this.totalMemorySizeInBytes = totalMemorySizeInBytes;
    this.enabled = false;
  }

  private MemoryManager(
      String name,
      MemoryManager parentMemoryManager,
      long totalMemorySizeInBytes,
      boolean enabled) {
    this.name = name;
    this.parentMemoryManager = parentMemoryManager;
    this.initialAllocatedMemorySizeInBytes = totalMemorySizeInBytes;
    this.totalMemorySizeInBytes = totalMemorySizeInBytes;
    this.enabled = enabled;
  }

  // region The Methods Of IMemoryBlock Management

  /**
   * Allocate memory block with exact specified size in bytes
   *
   * @param name the name of memory block
   * @param sizeInBytes the size in bytes of memory block try to allocate
   * @param type the type of memory block
   * @return the allocated memory block
   * @throw MemoryException if fail to allocate after MEMORY_ALLOCATE_MAX_RETRIES retries
   */
  public synchronized IMemoryBlock exactAllocate(
      String name, long sizeInBytes, MemoryBlockType type) {
    if (!enabled) {
      return getOrRegisterMemoryBlock(name, sizeInBytes, type);
    }
    for (int i = 0; i < MEMORY_ALLOCATE_MAX_RETRIES; i++) {
      if (totalMemorySizeInBytes - allocatedMemorySizeInBytes >= sizeInBytes) {
        return getOrRegisterMemoryBlock(name, sizeInBytes, type);
      }

      try {
        this.wait(MEMORY_ALLOCATE_RETRY_INTERVAL_IN_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.warn("exactAllocate: interrupted while waiting for available memory", e);
      }
    }

    throw new MemoryException(
        String.format(
            "exactAllocate: failed to allocate memory after %d retries, "
                + "total memory size %d bytes, used memory size %d bytes, "
                + "requested memory size %d bytes",
            MEMORY_ALLOCATE_MAX_RETRIES,
            totalMemorySizeInBytes,
            allocatedMemorySizeInBytes,
            sizeInBytes));
  }

  /**
   * Try to allocate memory block with total memory size in bytes
   *
   * @param name the name of memory block
   * @param memoryBlockType the type of memory block
   */
  public synchronized IMemoryBlock exactAllocate(String name, MemoryBlockType memoryBlockType) {
    return exactAllocate(
        name, totalMemorySizeInBytes - allocatedMemorySizeInBytes, memoryBlockType);
  }

  /**
   * Try to allocate memory block with specified size in bytes when the proportion of used memory is
   * below the given maxRatio.
   *
   * @param name the name of memory block
   * @param sizeInBytes the size in bytes of memory block try to allocate
   * @param maxRatio the used threshold of allocatedMemorySizeInBytes / totalMemorySizeInBytes, must
   *     be within [0.0, 1.0
   * @param memoryBlockType the type of memory block
   * @return the memory block if success, otherwise null
   */
  public synchronized IMemoryBlock exactAllocateIfSufficient(
      String name, long sizeInBytes, float maxRatio, MemoryBlockType memoryBlockType) {
    if (maxRatio < 0.0f || maxRatio > 1.0f) {
      return null;
    }
    if (!enabled) {
      return getOrRegisterMemoryBlock(name, sizeInBytes, memoryBlockType);
    }
    if (totalMemorySizeInBytes - allocatedMemorySizeInBytes >= sizeInBytes
        && (float) allocatedMemorySizeInBytes / totalMemorySizeInBytes < maxRatio) {
      return exactAllocate(name, sizeInBytes, memoryBlockType);
    } else {
      // TODO @spricoder: consider to find more memory in active way
      LOGGER.debug(
          "exactAllocateIfSufficient: failed to allocate memory, "
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
    if (!enabled) {
      return getOrRegisterMemoryBlock(name, sizeInBytes, type);
    }

    if (totalMemorySizeInBytes - allocatedMemorySizeInBytes >= sizeInBytes) {
      return getOrRegisterMemoryBlock(name, sizeInBytes, type);
    }

    long sizeToAllocateInBytes = sizeInBytes;
    while (sizeToAllocateInBytes >= MEMORY_ALLOCATE_MIN_SIZE_IN_BYTES) {
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
        return getOrRegisterMemoryBlock(name, sizeToAllocateInBytes, type);
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
    return null;
  }

  /**
   * Try to register memory block with specified size in bytes
   *
   * @param name the name of memory block, UNIQUE
   * @param sizeInBytes the size in bytes of memory block
   * @param type the type of memory block
   * @return the memory block
   */
  private IMemoryBlock getOrRegisterMemoryBlock(
      String name, long sizeInBytes, MemoryBlockType type) {
    if (sizeInBytes < 0) {
      throw new MemoryException(
          String.format(
              "register memory block %s failed: sizeInBytes should be non-negative", name));
    }
    return allocatedMemoryBlocks.compute(
        name,
        (blockName, block) -> {
          if (block != null) {
            if (block.getTotalMemorySizeInBytes() != sizeInBytes) {
              LOGGER.warn(
                  "getOrRegisterMemoryBlock failed: memory block {} already exists, "
                      + "it's size is {}, requested size is {}",
                  blockName,
                  block.getTotalMemorySizeInBytes(),
                  sizeInBytes);
            }
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
      LOGGER.error("releaseWithOutNotify: failed to close memory block {}", block, e);
    }
  }

  // endregion

  // region The Methods Of MemoryManager Management

  /**
   * Try to create a new memory manager with specified name and total memory size in bytes, then put
   * it into children map. NOTICE: if there are same name memory manager, it will return the
   * existing one instead of creating a new one or update the existing one.
   *
   * @param name the name of memory manager
   * @param sizeInBytes the total memory size in bytes of memory manager
   * @param enabled whether memory management is enabled
   * @return the memory manager
   */
  public synchronized MemoryManager getOrCreateMemoryManager(
      String name, long sizeInBytes, boolean enabled) {
    return children.compute(
        name,
        (managerName, manager) -> {
          if (sizeInBytes < 0) {
            LOGGER.warn("getOrCreateMemoryManager {}: sizeInBytes should be positive", name);
            return null;
          }
          if (manager != null) {
            LOGGER.debug(
                "getMemoryManager: memory manager {} already exists, it's size is {}, enabled is {}",
                managerName,
                manager.getTotalMemorySizeInBytes(),
                manager.isEnable());
            return manager;
          } else {
            if (this.enabled
                && sizeInBytes + this.allocatedMemorySizeInBytes > this.totalMemorySizeInBytes) {
              LOGGER.warn(
                  "getOrCreateMemoryManager failed: total memory size {} bytes is less than allocated memory size {} bytes",
                  sizeInBytes,
                  allocatedMemorySizeInBytes);
              return null;
            }
            allocatedMemorySizeInBytes += sizeInBytes;
            return new MemoryManager(name, this, sizeInBytes, enabled);
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
    if (index >= names.length) {
      return this;
    }
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
    // release the child memory managers
    for (MemoryManager child : children.values()) {
      child.clearAll();
    }
    children.clear();
    // release the memory blocks
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
  }

  // endregion

  // region The Methods of Attribute

  public String getName() {
    return name;
  }

  public boolean isEnable() {
    return enabled;
  }

  /** Get total memory size in bytes of memory manager */
  public long getTotalMemorySizeInBytes() {
    return totalMemorySizeInBytes;
  }

  public long getInitialAllocatedMemorySizeInBytes() {
    return initialAllocatedMemorySizeInBytes;
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
        allocatedMemoryBlocks.values().stream().mapToLong(IMemoryBlock::getUsedMemoryInBytes).sum();
    for (MemoryManager child : children.values()) {
      memorySize += child.getUsedMemorySizeInBytes();
    }
    return memorySize;
  }

  @TestOnly
  public void setTotalMemorySizeInBytes(long totalMemorySizeInBytes) {
    this.totalMemorySizeInBytes = totalMemorySizeInBytes;
  }

  // endregion

  // region transfer memory

  /**
   * Re-allocate memory according to ratio
   *
   * @param ratio the ratio of new total memory size to old total memory size
   */
  public synchronized void resizeByRatio(double ratio) {
    // Update initial allocated memory size by ratio
    long beforeInitialAllocatedMemorySizeInBytes = this.initialAllocatedMemorySizeInBytes;
    this.initialAllocatedMemorySizeInBytes *= ratio;
    // Update total memory size by actual size
    long beforeTotalMemorySizeInBytes = this.totalMemorySizeInBytes;
    this.totalMemorySizeInBytes +=
        (this.initialAllocatedMemorySizeInBytes - beforeInitialAllocatedMemorySizeInBytes);
    // Get actual ratio of re-allocate memory size
    double actualRatio = (double) this.totalMemorySizeInBytes / beforeTotalMemorySizeInBytes;
    // Re-allocate memory for all memory blocks
    for (IMemoryBlock block : allocatedMemoryBlocks.values()) {
      this.allocatedMemorySizeInBytes += block.resizeByRatio(actualRatio);
    }
    // Re-allocate memory for all child memory managers
    for (Map.Entry<String, MemoryManager> entry : children.entrySet()) {
      entry.getValue().resizeByRatio(ratio);
    }
    if (updateCallback.get() != null) {
      try {
        updateCallback.get().accept(beforeTotalMemorySizeInBytes, totalMemorySizeInBytes);
      } catch (Exception e) {
        LOGGER.warn("Failed to execute the update callback.", e);
      }
    }
  }

  // endregion

  // region auto adapt memory

  long cachedStaticAllocatedMemorySizeInBytes = 0L;
  long cachedStaticUsedMemorySizeInBytes = 0L;
  long cachedUsedMemorySizeInBytes = 1L;

  void updateCache() {
    cachedStaticAllocatedMemorySizeInBytes = 0L;
    cachedStaticUsedMemorySizeInBytes = 0L;
    cachedUsedMemorySizeInBytes = 0L;
    for (IMemoryBlock block : allocatedMemoryBlocks.values()) {
      long usedMemoryInBytes = block.getUsedMemoryInBytes();
      if (block.getMemoryBlockType() == MemoryBlockType.STATIC) {
        cachedStaticAllocatedMemorySizeInBytes += block.getTotalMemorySizeInBytes();
        cachedStaticUsedMemorySizeInBytes += usedMemoryInBytes;
      }
      cachedUsedMemorySizeInBytes += usedMemoryInBytes;
    }
    for (MemoryManager memoryManager : children.values()) {
      memoryManager.updateCache();
      cachedStaticAllocatedMemorySizeInBytes +=
          memoryManager.cachedStaticAllocatedMemorySizeInBytes;
      cachedStaticUsedMemorySizeInBytes += memoryManager.cachedStaticUsedMemorySizeInBytes;
      cachedUsedMemorySizeInBytes += memoryManager.cachedUsedMemorySizeInBytes;
    }
  }

  /** Whether this memory manager is available to shrink */
  private synchronized boolean isAvailableToShrink() {
    if (!shrinkAll) {
      return totalMemorySizeInBytes >= initialAllocatedMemorySizeInBytes * lowBound
          && totalMemorySizeInBytes > allocatedMemorySizeInBytes;
    } else {
      return totalMemorySizeInBytes >= initialAllocatedMemorySizeInBytes * lowBound
          && (totalMemorySizeInBytes - cachedStaticAllocatedMemorySizeInBytes)
              > (cachedUsedMemorySizeInBytes - cachedStaticUsedMemorySizeInBytes);
    }
  }

  /** Try to shrink this memory manager */
  private synchronized long shrink() {
    if (!shrinkAll) {
      long shrinkSize =
          (long)
              Math.max(
                  0,
                  Math.min(
                      getAvailableMemorySizeInBytes() * stepRatio,
                      totalMemorySizeInBytes - initialAllocatedMemorySizeInBytes * lowBound));
      totalMemorySizeInBytes -= shrinkSize;
      return shrinkSize;
    } else {
      if (getAvailableMemorySizeInBytes() != 0) {
        // first we try to use the available memory
        long shrinkSize =
            (long)
                Math.max(
                    0,
                    Math.min(
                        getAvailableMemorySizeInBytes() * stepRatio,
                        totalMemorySizeInBytes - initialAllocatedMemorySizeInBytes * lowBound));
        totalMemorySizeInBytes -= shrinkSize;
        return shrinkSize;
      } else {
        double minRatio = 1.0;
        IMemoryBlock targetMemoryBlock = null;
        MemoryManager targetMemoryManager = null;
        // try to find min used ratio memory block
        if (isAvailableToShrink()) {
          for (IMemoryBlock memoryBlock : allocatedMemoryBlocks.values()) {
            if (memoryBlock.getMemoryBlockType() == MemoryBlockType.STATIC) {
              continue;
            }
            if (targetMemoryBlock == null) {
              targetMemoryBlock = memoryBlock;
              minRatio = memoryBlock.getScore();
            } else {
              double ratio = memoryBlock.getScore();
              if (ratio < minRatio) {
                targetMemoryBlock = memoryBlock;
                minRatio = ratio;
              }
            }
          }
        }
        // try to find min memory used ratio memory manager
        for (MemoryManager child : children.values()) {
          if (child.isAvailableToShrink()) {
            double ratio = child.getScore();
            if (ratio < minRatio) {
              targetMemoryManager = child;
              minRatio = ratio;
            }
          }
        }
        if (targetMemoryManager != null) {
          // if targetMemoryManager is not null, we shrink the targetMemoryManager
          return targetMemoryManager.shrink();
        } else if (targetMemoryBlock != null) {
          // if targetMemoryBlock is not null, we shrink the targetMemoryBlock
          long shrinkSize = -targetMemoryBlock.resizeByRatio(1 - (1 - lowBound) * stepRatio);
          long beforeTotalMemorySizeInBytes = totalMemorySizeInBytes;
          totalMemorySizeInBytes -= shrinkSize;
          allocatedMemorySizeInBytes -= shrinkSize;
          if (updateCallback.get() != null) {
            try {
              updateCallback.get().accept(beforeTotalMemorySizeInBytes, totalMemorySizeInBytes);
            } catch (Exception e) {
              LOGGER.warn("Failed to execute the update callback.", e);
            }
          }
          return shrinkSize;
        } else {
          return 0;
        }
      }
    }
  }

  /**
   * Whether this memory manager is available to expand. If there are one child memory manager or
   * memory block available to expand, return true.
   *
   * @return true if available to expand, otherwise false
   */
  private synchronized boolean isAvailableToExpand() {
    for (MemoryManager memoryManager : children.values()) {
      if (memoryManager.isAvailableToExpand()) {
        return true;
      }
    }
    for (IMemoryBlock memoryBlock : allocatedMemoryBlocks.values()) {
      if (memoryBlock.getMemoryBlockType() != MemoryBlockType.STATIC) {
        return true;
      }
    }
    return false;
  }

  private synchronized void expand(double ratio) {
    // Update total memory size by actual size
    long beforeTotalMemorySizeInBytes = this.totalMemorySizeInBytes;
    this.totalMemorySizeInBytes *= ratio;
    double actualRatio =
        (double) (this.totalMemorySizeInBytes - this.cachedStaticAllocatedMemorySizeInBytes)
            / (beforeTotalMemorySizeInBytes - cachedStaticAllocatedMemorySizeInBytes);
    // Re-allocate memory for dynamic memory blocks
    for (IMemoryBlock block : allocatedMemoryBlocks.values()) {
      if (block.getMemoryBlockType() == MemoryBlockType.STATIC) {
        continue;
      }
      long resizeSize = block.resizeByRatio(actualRatio);
      this.allocatedMemorySizeInBytes += resizeSize;
    }
    // Re-allocate memory for all child memory managers
    for (Map.Entry<String, MemoryManager> entry : children.entrySet()) {
      entry.getValue().expand(actualRatio);
    }
    if (updateCallback.get() != null) {
      try {
        updateCallback.get().accept(beforeTotalMemorySizeInBytes, totalMemorySizeInBytes);
      } catch (Exception e) {
        LOGGER.warn("Failed to execute the update callback.", e);
      }
    }
  }

  /** Set update callback */
  public MemoryManager setUpdateCallback(final BiConsumer<Long, Long> updateCallback) {
    this.updateCallback.set(updateCallback);
    return this;
  }

  public MemoryManager setShrinkAll(boolean shrink_all) {
    this.shrinkAll = shrink_all;
    return this;
  }

  public MemoryManager setLowBound(double lowBound) {
    this.lowBound = lowBound;
    return this;
  }

  public MemoryManager setStepRatio(double stepRatio) {
    this.stepRatio = stepRatio;
    return this;
  }

  /** Get the score of this memory manager */
  private synchronized double getScore() {
    return (double) (cachedUsedMemorySizeInBytes - cachedStaticUsedMemorySizeInBytes)
        / (totalMemorySizeInBytes - cachedStaticAllocatedMemorySizeInBytes);
  }

  /** Try to update allocation */
  public synchronized void transfer() {
    // Try to find memory manager with highest and lowest memory usage
    MemoryManager highestMemoryManager = null;
    MemoryManager lowestMemoryManager = null;
    for (MemoryManager child : children.values()) {
      if (highestMemoryManager == null) {
        highestMemoryManager = child;
        lowestMemoryManager = child;
      } else {
        if (highestMemoryManager.isAvailableToExpand()
            && child.getScore() > highestMemoryManager.getScore()) {
          highestMemoryManager = child;
        }
        if (lowestMemoryManager.isAvailableToShrink()
            && child.getScore() < lowestMemoryManager.getScore()) {
          lowestMemoryManager = child;
        }
      }
    }
    if (highestMemoryManager != null && !highestMemoryManager.equals(lowestMemoryManager)) {
      // transfer memory from the lowest memory manager to the highest memory manager
      long transferSize = lowestMemoryManager.shrink();
      if (transferSize != 0) {
        long beforeTotalMemorySizeInBytes = highestMemoryManager.getTotalMemorySizeInBytes();
        double ratio =
            (double) (transferSize + beforeTotalMemorySizeInBytes) / beforeTotalMemorySizeInBytes;
        // we need to update all memory to each part
        highestMemoryManager.expand(ratio);
        LOGGER.info(
            "Transfer Memory Size {} from {} to {}",
            transferSize,
            lowestMemoryManager,
            highestMemoryManager);
      }
    }
    for (MemoryManager memoryManager : children.values()) {
      memoryManager.transfer();
    }
  }

  // endregion

  @Override
  public String toString() {
    return "MemoryManager{"
        + "name="
        + name
        + ", enabled="
        + enabled
        + ", initialAllocatedMemorySizeInBytes="
        + initialAllocatedMemorySizeInBytes
        + ", totalMemorySizeInBytes="
        + totalMemorySizeInBytes
        + ", allocatedMemorySizeInBytes="
        + allocatedMemorySizeInBytes
        + '}';
  }

  public void print() {
    print(0);
  }

  private void print(int indent) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < indent; i++) {
      sb.append("  ");
    }
    sb.append(this);
    LOGGER.info(sb.toString());
    for (IMemoryBlock block : allocatedMemoryBlocks.values()) {
      block.print(indent + 2);
    }
    for (MemoryManager child : children.values()) {
      child.print(indent + 1);
    }
  }
}
