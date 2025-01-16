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
import java.util.Set;
import java.util.function.LongUnaryOperator;

public class IoTDBMemoryManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBMemoryManager.class);
  // TODO spricoder: make it configurable
  private static final boolean ENABLED = false;

  /** max retry times for memory allocation */
  private static final int MEMORY_ALLOCATE_MAX_RETRIES = 3;

  /** retry interval for memory allocation */
  private static final long MEMORY_ALLOCATE_RETRY_INTERVAL_IN_MS = 1000;

  /** min memory size to allocate */
  private static final long MEMORY_ALLOCATE_MIN_SIZE_IN_BYTES = 32;

  private long totalMemorySizeInBytes = 0L;
  private long usedMemorySizeInBytes = 0L;
  private IoTDBMemoryManager parentMemoryManager;
  private final Set<IoTDBMemoryManager> childrens = new HashSet<>();
  private final Set<IoTDBMemoryBlock> allocatedMemoryBlocks = new HashSet<>();

  public IoTDBMemoryManager(IoTDBMemoryManager parentMemoryManager, long totalMemorySizeInBytes) {
    this.parentMemoryManager = parentMemoryManager;
    this.parentMemoryManager.addChildMemoryManager(this);
    this.totalMemorySizeInBytes = totalMemorySizeInBytes;
  }

  private IoTDBMemoryBlock forceAllocate(long sizeInBytes, IoTDBMemoryBlockType type) {
    if (!ENABLED) {
      return new IoTDBMemoryBlock(this, sizeInBytes, type);
    }
    for (int i = 0; i < MEMORY_ALLOCATE_MAX_RETRIES; i++) {
      if (totalMemorySizeInBytes - usedMemorySizeInBytes >= sizeInBytes) {
        return registerMemoryBlock(sizeInBytes);
      }

      try {
        // TODO spricoder: try to find more memory
        Thread.sleep(MEMORY_ALLOCATE_RETRY_INTERVAL_IN_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.warn("forceAllocate: interrupted while waiting for available memory", e);
      }
    }

    throw new IoTDBMemoryException(
        String.format(
            "forceAllocate: failed to allocate memory after %d retries, "
                + "total memory size %d bytes, used memory size %d bytes, "
                + "requested memory size %d bytes",
            MEMORY_ALLOCATE_MAX_RETRIES,
            totalMemorySizeInBytes,
            usedMemorySizeInBytes,
            sizeInBytes));
  }

  public synchronized IoTDBMemoryBlock forceAllocateIfSufficient(
      long sizeInBytes, float usedThreshold) {
    if (usedThreshold < 0.0f || usedThreshold > 1.0f) {
      return null;
    }

    if (!ENABLED) {
      return new IoTDBMemoryBlock(this, sizeInBytes);
    }
    if (totalMemorySizeInBytes - usedMemorySizeInBytes >= sizeInBytes
        && (float) usedMemorySizeInBytes / totalMemorySizeInBytes < usedThreshold) {
      return forceAllocate(sizeInBytes, IoTDBMemoryBlockType.NONE);
    } else {
      // TODO spricoder: try to find more memory
      LOGGER.debug(
          "forceAllocateIfSufficient: failed to allocate memory, "
              + "total memory size {} bytes, used memory size {} bytes, "
              + "requested memory size {} bytes, used threshold {}",
          totalMemorySizeInBytes,
          usedMemorySizeInBytes,
          sizeInBytes,
          usedThreshold);
    }

    return null;
  }

  private IoTDBMemoryBlock registerMemoryBlock(long sizeInBytes) {
    return registerMemoryBlock(sizeInBytes, IoTDBMemoryBlockType.NONE);
  }

  private IoTDBMemoryBlock registerMemoryBlock(long sizeInBytes, IoTDBMemoryBlockType type) {
    usedMemorySizeInBytes += sizeInBytes;
    final IoTDBMemoryBlock memoryBlock = new IoTDBMemoryBlock(this, sizeInBytes, type);
    allocatedMemoryBlocks.add(memoryBlock);
    return memoryBlock;
  }

  public synchronized IoTDBMemoryBlock tryAllocate(
      long sizeInBytes, LongUnaryOperator customAllocateStrategy) {
    if (!ENABLED) {
      return new IoTDBMemoryBlock(this, sizeInBytes);
    }

    if (totalMemorySizeInBytes - usedMemorySizeInBytes >= sizeInBytes) {
      return registerMemoryBlock(sizeInBytes);
    }

    long sizeToAllocateInBytes = sizeInBytes;
    while (sizeToAllocateInBytes > MEMORY_ALLOCATE_MIN_SIZE_IN_BYTES) {
      if (totalMemorySizeInBytes - usedMemorySizeInBytes >= sizeToAllocateInBytes) {
        LOGGER.info(
            "tryAllocate: allocated memory, "
                + "total memory size {} bytes, used memory size {} bytes, "
                + "original requested memory size {} bytes, "
                + "actual requested memory size {} bytes",
            totalMemorySizeInBytes,
            usedMemorySizeInBytes,
            sizeInBytes,
            sizeToAllocateInBytes);
        return registerMemoryBlock(sizeToAllocateInBytes);
      }

      sizeToAllocateInBytes =
          Math.max(
              customAllocateStrategy.applyAsLong(sizeToAllocateInBytes),
              MEMORY_ALLOCATE_MIN_SIZE_IN_BYTES);
    }

    // TODO spricoder: try to shrink first
    LOGGER.warn(
        "tryAllocate: failed to allocate memory, "
            + "total memory size {} bytes, used memory size {} bytes, "
            + "requested memory size {} bytes",
        totalMemorySizeInBytes,
        usedMemorySizeInBytes,
        sizeInBytes);
    return registerMemoryBlock(0);
  }

  public synchronized void release(IoTDBMemoryBlock block) {
    if (!ENABLED || block == null || block.isReleased()) {
      return;
    }

    allocatedMemoryBlocks.remove(block);
    usedMemorySizeInBytes -= block.getMemoryUsageInBytes();
    block.markAsReleased();

    this.notifyAll();
  }

  public synchronized void addChildMemoryManager(IoTDBMemoryManager childMemoryManager) {
    if (childMemoryManager != null) {
      childrens.add(childMemoryManager);
    }
  }

  public long getUsedMemorySizeInBytes() {
    return usedMemorySizeInBytes;
  }

  public long getFreeMemorySizeInBytes() {
    return totalMemorySizeInBytes - usedMemorySizeInBytes;
  }

  public long getTotalMemorySizeInBytes() {
    return totalMemorySizeInBytes;
  }
}
