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

package org.apache.iotdb.db.storageengine.load.memory;

import org.apache.iotdb.db.exception.LoadRuntimeOutOfMemoryException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class LoadTsFileDataCacheMemoryBlock extends LoadTsFileAbstractMemoryBlock {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(LoadTsFileDataCacheMemoryBlock.class);
  private static final long MINIMUM_MEMORY_SIZE_IN_BYTES = 1024 * 1024L; // 1 MB
  private static final int MAX_ASK_FOR_MEMORY_COUNT = 256; // must be a power of 2
  private static final long EACH_ASK_MEMORY_SIZE_IN_BYTES =
      Math.max(
          MINIMUM_MEMORY_SIZE_IN_BYTES,
          LoadTsFileMemoryManager.MEMORY_TOTAL_SIZE_FROM_QUERY_IN_BYTES >> 4);

  private final AtomicLong limitedMemorySizeInBytes;
  private final AtomicLong memoryUsageInBytes;
  private final AtomicInteger askForMemoryCount;
  private final AtomicInteger referenceCount;

  LoadTsFileDataCacheMemoryBlock(long initialLimitedMemorySizeInBytes) {
    super();

    if (initialLimitedMemorySizeInBytes < MINIMUM_MEMORY_SIZE_IN_BYTES) {
      throw new LoadRuntimeOutOfMemoryException(
          String.format(
              "The initial limited memory size %d is less than the minimum memory size %d",
              initialLimitedMemorySizeInBytes, MINIMUM_MEMORY_SIZE_IN_BYTES));
    }

    this.limitedMemorySizeInBytes = new AtomicLong(initialLimitedMemorySizeInBytes);
    this.memoryUsageInBytes = new AtomicLong(0L);
    this.askForMemoryCount = new AtomicInteger(1);
    this.referenceCount = new AtomicInteger(0);
  }

  @Override
  public boolean hasEnoughMemory(long memoryTobeAddedInBytes) {
    return memoryUsageInBytes.get() + memoryTobeAddedInBytes <= limitedMemorySizeInBytes.get();
  }

  @Override
  public void addMemoryUsage(long memoryInBytes) {
    memoryUsageInBytes.addAndGet(memoryInBytes);

    askForMemoryCount.getAndUpdate(
        count -> {
          if ((count & (count - 1)) == 0) {
            // count is a power of 2
            long actuallyAllocateMemorySizeInBytes =
                MEMORY_MANAGER.tryAllocateFromQuery(EACH_ASK_MEMORY_SIZE_IN_BYTES);
            limitedMemorySizeInBytes.addAndGet(actuallyAllocateMemorySizeInBytes);
            if (actuallyAllocateMemorySizeInBytes < EACH_ASK_MEMORY_SIZE_IN_BYTES) {
              return (count & (MAX_ASK_FOR_MEMORY_COUNT - 1)) + 1;
            } else {
              return 1;
            }
          }
          return (count & (MAX_ASK_FOR_MEMORY_COUNT - 1)) + 1;
        });
  }

  @Override
  public void reduceMemoryUsage(long memoryInBytes) {
    memoryUsageInBytes.addAndGet(-memoryInBytes);
  }

  @Override
  public void forceResize(long newSizeInBytes) {
    throw new UnsupportedOperationException(
        "resize is not supported for LoadTsFileDataCacheMemoryBlock");
  }

  @Override
  protected void releaseAllMemory() {
    if (memoryUsageInBytes.get() != 0) {
      LOGGER.warn(
          "Try to release memory from a memory block {} which has not released all memory", this);
    }
    MEMORY_MANAGER.releaseToQuery(limitedMemorySizeInBytes.get());
  }

  public boolean doShrink(long shrinkMemoryInBytes) {
    if (shrinkMemoryInBytes < 0) {
      LOGGER.warn(
          "Try to shrink a negative memory size {} from memory block {}",
          shrinkMemoryInBytes,
          this);
      return false;
    } else if (shrinkMemoryInBytes == 0) {
      return true;
    }

    if (limitedMemorySizeInBytes.get() - shrinkMemoryInBytes <= MINIMUM_MEMORY_SIZE_IN_BYTES) {
      return false;
    }

    limitedMemorySizeInBytes.addAndGet(-shrinkMemoryInBytes);
    return true;
  }

  void updateReferenceCount(int delta) {
    referenceCount.addAndGet(delta);
  }

  int getReferenceCount() {
    return referenceCount.get();
  }

  @Override
  long getMemoryUsageInBytes() {
    return memoryUsageInBytes.get();
  }

  long getLimitedMemorySizeInBytes() {
    return limitedMemorySizeInBytes.get();
  }

  @Override
  public String toString() {
    return "LoadTsFileDataCacheMemoryBlock{"
        + "limitedMemorySizeInBytes="
        + limitedMemorySizeInBytes.get()
        + ", memoryUsageInBytes="
        + memoryUsageInBytes.get()
        + ", askForMemoryCount="
        + askForMemoryCount.get()
        + '}';
  }
}
