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

package org.apache.iotdb.db.queryengine.load.memory;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.load.exception.LoadRuntimeOutOfMemoryException;
import org.apache.iotdb.db.queryengine.load.memory.block.AbstractMemoryBlock;
import org.apache.iotdb.db.queryengine.load.memory.block.LoadMemoryBlock;
import org.apache.iotdb.db.queryengine.load.memory.block.QueryMemoryBlock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.function.LongUnaryOperator;

public class LoadTsFileMemoryManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadTsFileMemoryManager.class);
  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  // TODO: remove queryTotalMemorySizeInBytes after introducing queryEngine.allocate()
  private long queryTotalMemorySizeInBytes = CONFIG.getLoadMemoryTotalSizeFromQueryInBytes();
  private long totalMemorySizeInBytes = CONFIG.getInitLoadMemoryTotalSizeInBytes();
  private long usedMemorySizeInBytes;

  private static final int MEMORY_ALLOCATE_MAX_RETRIES = CONFIG.getLoadMemoryAllocateMaxRetries();
  private static final long MEMORY_ALLOCATE_RETRY_INTERVAL_IN_MS =
      CONFIG.getLoadMemoryAllocateRetryIntervalMs();
  private static final long MEMORY_ALLOCATE_MIN_SIZE_IN_BYTES =
      CONFIG.getLoadMemoryAllocateMinSizeInBytes();

  private final Set<AbstractMemoryBlock> allocatedBlocks = new HashSet<>();

  private synchronized QueryMemoryBlock allocatedFromQuery(long sizeInBytes) {
    // todo: queryEngine provides a method to allocate memory
    if (queryTotalMemorySizeInBytes >= totalMemorySizeInBytes) {
      totalMemorySizeInBytes += sizeInBytes;
      return new QueryMemoryBlock(sizeInBytes);
    } else {
      return new QueryMemoryBlock(0);
    }
  }

  public synchronized void releaseToQuery(QueryMemoryBlock block) {
    // todo: queryEngine provides a method to release memory
    if (block == null || block.isReleased()) {
      return;
    }

    totalMemorySizeInBytes -= block.getMemoryUsageInBytes();
  }

  public synchronized LoadMemoryBlock forceAllocate(long sizeInBytes) {
    for (int i = 0; i < MEMORY_ALLOCATE_MAX_RETRIES; i++) {
      // 1. check if there is enough memory in loadMemoryManager
      final long freeMemorySizeInBytes = totalMemorySizeInBytes - usedMemorySizeInBytes;
      if (freeMemorySizeInBytes >= sizeInBytes) {
        return registeredMemoryBlock(sizeInBytes);
      }

      // 2. allocate memory from queryEngine
      QueryMemoryBlock allocatedBlock = allocatedFromQuery(sizeInBytes - freeMemorySizeInBytes);
      if (totalMemorySizeInBytes - usedMemorySizeInBytes >= sizeInBytes) {
        return registeredMemoryBlock(sizeInBytes, allocatedBlock);
      }

      // 3. wait for available memory
      try {
        this.wait(MEMORY_ALLOCATE_RETRY_INTERVAL_IN_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.warn("forceAllocate: interrupted while waiting for available memory", e);
      }
    }

    throw new LoadRuntimeOutOfMemoryException(
        String.format(
            "forceAllocate: failed to allocate memory after %d retries, "
                + "total memory size %d bytes, used memory size %d bytes, "
                + "requested memory size %d bytes",
            MEMORY_ALLOCATE_MAX_RETRIES,
            totalMemorySizeInBytes,
            usedMemorySizeInBytes,
            sizeInBytes));
  }

  public synchronized LoadMemoryBlock tryAllocate(long sizeInBytes) {
    return tryAllocate(sizeInBytes, currentSize -> currentSize * 2 / 3);
  }

  public synchronized LoadMemoryBlock tryAllocate(
      long sizeInBytes, LongUnaryOperator customAllocateStrategy) {

    // 1. check if there is enough memory in loadMemoryManager
    if (totalMemorySizeInBytes - usedMemorySizeInBytes >= sizeInBytes) {
      return registeredMemoryBlock(sizeInBytes);
    }

    // 2. try to allocate memory
    long sizeToAllocateInBytes = sizeInBytes;
    while (sizeToAllocateInBytes > MEMORY_ALLOCATE_MIN_SIZE_IN_BYTES) {
      // 2.1 allocate memory from queryEngine
      QueryMemoryBlock queryMemoryBlock = allocatedFromQuery(sizeToAllocateInBytes);
      if (totalMemorySizeInBytes - usedMemorySizeInBytes >= sizeToAllocateInBytes) {
        LOGGER.info(
            "tryAllocate: allocated memory, "
                + "total memory size {} bytes, used memory size {} bytes, "
                + "original requested memory size {} bytes,"
                + "actual requested memory size {} bytes",
            totalMemorySizeInBytes,
            usedMemorySizeInBytes,
            sizeInBytes,
            sizeToAllocateInBytes);
        return registeredMemoryBlock(sizeToAllocateInBytes, queryMemoryBlock);
      } else {
        releaseToQuery(queryMemoryBlock);
      }

      sizeToAllocateInBytes =
          Math.max(
              customAllocateStrategy.applyAsLong(sizeToAllocateInBytes),
              MEMORY_ALLOCATE_MIN_SIZE_IN_BYTES);
    }

    LOGGER.warn(
        "tryAllocate: failed to allocate memory, "
            + "total memory size {} bytes, used memory size {} bytes, "
            + "requested memory size {} bytes",
        totalMemorySizeInBytes,
        usedMemorySizeInBytes,
        sizeInBytes);
    return registeredMemoryBlock(0);
  }

  private LoadMemoryBlock registeredMemoryBlock(long sizeInBytes) {
    return registeredMemoryBlock(sizeInBytes, null);
  }

  private LoadMemoryBlock registeredMemoryBlock(long sizeInBytes, QueryMemoryBlock queryBlock) {
    usedMemorySizeInBytes += sizeInBytes;

    final LoadMemoryBlock returnedMemoryBlock = new LoadMemoryBlock(sizeInBytes, queryBlock);
    allocatedBlocks.add(returnedMemoryBlock);
    return returnedMemoryBlock;
  }

  public synchronized void release(LoadMemoryBlock block) {
    if (block == null || block.isReleased()) {
      return;
    }

    // 1. release memory to queryEngine
    if (block.getQueryMemoryBlock() != null) {
      releaseToQuery(block.getQueryMemoryBlock());
    }

    // 2. release memory to loadMemoryManager
    allocatedBlocks.remove(block);
    usedMemorySizeInBytes -= block.getMemoryUsageInBytes();
    block.markAsReleased();

    this.notifyAll();
  }

  ///////////////////////////// SINGLETON /////////////////////////////
  private LoadTsFileMemoryManager() {}

  public static LoadTsFileMemoryManager getInstance() {
    return LoadTsFileMemoryManagerHolder.INSTANCE;
  }

  public static class LoadTsFileMemoryManagerHolder {
    private static final LoadTsFileMemoryManager INSTANCE = new LoadTsFileMemoryManager();
  }
}
