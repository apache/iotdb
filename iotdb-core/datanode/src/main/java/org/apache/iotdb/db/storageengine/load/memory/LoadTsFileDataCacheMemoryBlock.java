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

import org.apache.iotdb.db.exception.load.LoadRuntimeOutOfMemoryException;
import org.apache.iotdb.db.i18n.StorageEngineMessages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class LoadTsFileDataCacheMemoryBlock extends LoadTsFileAbstractMemoryBlock {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(LoadTsFileDataCacheMemoryBlock.class);
  private static final long MINIMUM_MEMORY_SIZE_IN_BYTES = 1024 * 1024L; // 1 MB

  private final AtomicLong limitedMemorySizeInBytes;
  private final AtomicLong memoryUsageInBytes;
  private final AtomicInteger referenceCount;

  LoadTsFileDataCacheMemoryBlock(long initialLimitedMemorySizeInBytes) {
    super();

    if (initialLimitedMemorySizeInBytes < MINIMUM_MEMORY_SIZE_IN_BYTES) {
      throw new LoadRuntimeOutOfMemoryException(
          String.format(
              StorageEngineMessages
                  .STORAGE_EXCEPTION_THE_INITIAL_LIMITED_MEMORY_SIZE_D_IS_LESS_THAN_THE_MINIMUM_FC044302,
              initialLimitedMemorySizeInBytes,
              MINIMUM_MEMORY_SIZE_IN_BYTES));
    }

    this.limitedMemorySizeInBytes = new AtomicLong(initialLimitedMemorySizeInBytes);
    this.memoryUsageInBytes = new AtomicLong(0L);
    this.referenceCount = new AtomicInteger(0);
  }

  @Override
  public boolean hasEnoughMemory(long memoryTobeAddedInBytes) {
    return memoryUsageInBytes.get() + memoryTobeAddedInBytes <= limitedMemorySizeInBytes.get();
  }

  @Override
  public synchronized void addMemoryUsage(long memoryInBytes) {
    // May temporarily exceed the max size
    if (memoryUsageInBytes.addAndGet(memoryInBytes) > limitedMemorySizeInBytes.get()) {
      LOGGER.debug(StorageEngineMessages.EXCEED_TOTAL_MEMORY_SIZE, this);
    }
  }

  @Override
  public synchronized void reduceMemoryUsage(long memoryInBytes) {
    memoryUsageInBytes.addAndGet(-memoryInBytes);
  }

  @Override
  public synchronized void forceResize(long newSizeInBytes) {
    throw new UnsupportedOperationException(
        StorageEngineMessages
            .STORAGE_EXCEPTION_SETTOTALMEMORYSIZEINBYTES_IS_NOT_SUPPORTED_FOR_LOADTSFILEDATACACHEMEMORYBLOCK_DFAB2A2A);
  }

  @Override
  protected void releaseAllMemory() {
    if (memoryUsageInBytes.get() != 0) {
      LOGGER.warn(
          StorageEngineMessages
              .STORAGE_LOG_TRY_TO_RELEASE_MEMORY_FROM_A_MEMORY_BLOCK_WHICH_HAS_NOT_874E7A08,
          this);
    }
    MEMORY_MANAGER.releaseToQuery(limitedMemorySizeInBytes.get());
  }

  public boolean doShrink(long shrinkMemoryInBytes) {
    if (shrinkMemoryInBytes < 0) {
      LOGGER.warn(
          StorageEngineMessages
              .STORAGE_LOG_TRY_TO_SHRINK_A_NEGATIVE_MEMORY_SIZE_FROM_MEMORY_BLOCK_60501B13,
          shrinkMemoryInBytes,
          this);
      return false;
    } else if (shrinkMemoryInBytes == 0) {
      return true;
    }

    if (limitedMemorySizeInBytes.get() - shrinkMemoryInBytes
        <= Math.max(MINIMUM_MEMORY_SIZE_IN_BYTES, memoryUsageInBytes.get())) {
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
        + ", usedMemoryInBytes="
        + memoryUsageInBytes.get()
        + '}';
  }
}
