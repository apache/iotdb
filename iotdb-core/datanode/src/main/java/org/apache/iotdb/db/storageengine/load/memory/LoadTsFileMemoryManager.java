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

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.load.LoadRuntimeOutOfMemoryException;
import org.apache.iotdb.db.i18n.StorageEngineMessages;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

public class LoadTsFileMemoryManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadTsFileMemoryManager.class);
  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private static final LocalExecutionPlanner QUERY_ENGINE_MEMORY_MANAGER =
      LocalExecutionPlanner.getInstance();
  public static final long MEMORY_TOTAL_SIZE_FROM_QUERY_IN_BYTES =
      QUERY_ENGINE_MEMORY_MANAGER.getAllocateMemoryForOperators();
  private static final int MEMORY_ALLOCATE_MAX_RETRIES = CONFIG.getLoadMemoryAllocateMaxRetries();
  private static final long MEMORY_ALLOCATE_RETRY_INTERVAL_IN_MS =
      CONFIG.getLoadMemoryAllocateRetryIntervalMs();

  private final AtomicLong usedMemorySizeInBytes = new AtomicLong(0);
  private LoadTsFileDataCacheMemoryBlock dataCacheMemoryBlock;

  private synchronized void forceAllocateFromQuery(long sizeInBytes)
      throws LoadRuntimeOutOfMemoryException {
    for (int i = 0; i < MEMORY_ALLOCATE_MAX_RETRIES; i++) {
      // allocate memory from queryEngine
      if (QUERY_ENGINE_MEMORY_MANAGER.forceAllocateFreeMemoryForOperators(sizeInBytes)) {
        usedMemorySizeInBytes.addAndGet(sizeInBytes);
        return;
      }

      // wait for available memory
      try {
        this.wait(MEMORY_ALLOCATE_RETRY_INTERVAL_IN_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.warn(StorageEngineMessages.FORCE_ALLOCATE_INTERRUPTED, e);
      }
    }

    throw new LoadRuntimeOutOfMemoryException(
        String.format(
            StorageEngineMessages
                .STORAGE_EXCEPTION_FORCEALLOCATE_FAILED_TO_ALLOCATE_MEMORY_FROM_QUERY_ENGINE_F91D5959,
            MEMORY_ALLOCATE_MAX_RETRIES,
            QUERY_ENGINE_MEMORY_MANAGER.getAllocateMemoryForOperators(),
            Math.max(0L, QUERY_ENGINE_MEMORY_MANAGER.getFreeMemoryForLoadTsFile()),
            usedMemorySizeInBytes.get(),
            sizeInBytes));
  }

  public synchronized long tryAllocateFromQuery(final long sizeInBytes) {
    final long actuallyAllocateMemoryInBytes =
        QUERY_ENGINE_MEMORY_MANAGER.tryAllocateFreeMemory4Load(sizeInBytes);
    usedMemorySizeInBytes.addAndGet(actuallyAllocateMemoryInBytes);
    return actuallyAllocateMemoryInBytes;
  }

  public synchronized void releaseToQuery(final long sizeInBytes) {
    if (sizeInBytes <= 0) {
      throw new IllegalArgumentException(
          String.format(
              StorageEngineMessages
                  .STORAGE_EXCEPTION_LOAD_INVALID_MEMORY_SIZE_D_BYTES_MUST_BE_POSITIVE_D6586ED3,
              sizeInBytes));
    }
    if (usedMemorySizeInBytes.get() < sizeInBytes) {
      LOGGER.error(
          StorageEngineMessages
              .STORAGE_LOG_LOAD_ATTEMPTING_TO_RELEASE_MORE_MEMORY_THAN_ALLOCATED_0E737996,
          sizeInBytes,
          usedMemorySizeInBytes.get());
    }
    final long sizeToRelease = Math.min(sizeInBytes, usedMemorySizeInBytes.get());
    usedMemorySizeInBytes.addAndGet(-sizeToRelease);
    QUERY_ENGINE_MEMORY_MANAGER.releaseToFreeMemoryForOperators(sizeToRelease);
    this.notifyAll();
  }

  public synchronized LoadTsFileMemoryBlock allocateMemoryBlock(long sizeInBytes)
      throws LoadRuntimeOutOfMemoryException {
    if (sizeInBytes <= 0) {
      throw new IllegalArgumentException(
          String.format(
              StorageEngineMessages
                  .STORAGE_EXCEPTION_LOAD_INVALID_MEMORY_SIZE_D_BYTES_MUST_BE_POSITIVE_D6586ED3,
              sizeInBytes));
    }
    try {
      forceAllocateFromQuery(sizeInBytes);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(StorageEngineMessages.LOAD_ALLOCATED_MEMORY_BLOCK, sizeInBytes);
      }
    } catch (LoadRuntimeOutOfMemoryException e) {
      if (dataCacheMemoryBlock != null && dataCacheMemoryBlock.doShrink(sizeInBytes)) {
        LOGGER.info(
            StorageEngineMessages
                .STORAGE_LOG_LOAD_QUERY_ENGINE_S_MEMORY_IS_NOT_SUFFICIENT_ALLOCATED_MEMORYBLOCK_44D5B5FB,
            sizeInBytes);
        return new LoadTsFileMemoryBlock(sizeInBytes);
      }
      throw e;
    }
    return new LoadTsFileMemoryBlock(sizeInBytes);
  }

  /**
   * Resize the memory block to the new size.
   *
   * @throws LoadRuntimeOutOfMemoryException if failed to allocate enough memory
   */
  synchronized void forceResize(LoadTsFileMemoryBlock memoryBlock, long newSizeInBytes)
      throws LoadRuntimeOutOfMemoryException {
    if (newSizeInBytes < 0) {
      throw new IllegalArgumentException(
          String.format(
              StorageEngineMessages
                  .STORAGE_EXCEPTION_LOAD_INVALID_MEMORY_SIZE_D_BYTES_MUST_BE_NON_NEGATIVE_A0146353,
              newSizeInBytes));
    }
    if (memoryBlock.getTotalMemorySizeInBytes() == newSizeInBytes) {
      return;
    }

    if (memoryBlock.getTotalMemorySizeInBytes() > newSizeInBytes) {

      if (memoryBlock.getMemoryUsageInBytes() > newSizeInBytes) {
        LOGGER.error(
            StorageEngineMessages
                .STORAGE_LOG_LOAD_FAILED_TO_SETTOTALMEMORYSIZEINBYTES_MEMORY_BLOCK_TO_DBE9BE56,
            memoryBlock,
            newSizeInBytes,
            memoryBlock.getMemoryUsageInBytes());
      }

      releaseToQuery(memoryBlock.getTotalMemorySizeInBytes() - newSizeInBytes);
      memoryBlock.setTotalMemorySizeInBytes(newSizeInBytes);
      return;
    }

    long bytesNeeded = newSizeInBytes - memoryBlock.getTotalMemorySizeInBytes();
    try {
      forceAllocateFromQuery(bytesNeeded);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            StorageEngineMessages
                .STORAGE_LOG_LOAD_FORCE_RESIZED_LOADTSFILEMEMORYBLOCK_WITH_MEMORY_FROM_33AC288A,
            bytesNeeded,
            newSizeInBytes);
      }
    } catch (LoadRuntimeOutOfMemoryException e) {
      if (dataCacheMemoryBlock != null && dataCacheMemoryBlock.doShrink(bytesNeeded)) {
        LOGGER.info(
            StorageEngineMessages
                .STORAGE_LOG_LOAD_QUERY_ENGINE_S_MEMORY_IS_NOT_SUFFICIENT_FORCE_RESIZED_9F85F4CA,
            bytesNeeded,
            newSizeInBytes);
      } else {
        throw e;
      }
    }
    memoryBlock.setTotalMemorySizeInBytes(newSizeInBytes);
  }

  public synchronized LoadTsFileDataCacheMemoryBlock allocateDataCacheMemoryBlock()
      throws LoadRuntimeOutOfMemoryException {
    if (dataCacheMemoryBlock == null) {
      final long actuallyAllocateMemoryInBytes =
          tryAllocateFromQuery(MEMORY_TOTAL_SIZE_FROM_QUERY_IN_BYTES >> 2);
      try {
        dataCacheMemoryBlock = new LoadTsFileDataCacheMemoryBlock(actuallyAllocateMemoryInBytes);
      } catch (RuntimeException e) {
        if (actuallyAllocateMemoryInBytes > 0) {
          try {
            releaseToQuery(actuallyAllocateMemoryInBytes);
          } catch (RuntimeException releaseException) {
            e.addSuppressed(releaseException);
          }
        }
        throw e;
      }
      LOGGER.info(
          StorageEngineMessages.STORAGE_LOG_CREATE_DATA_CACHE_MEMORY_BLOCK_ALLOCATE_MEMORY_5F3E041D,
          dataCacheMemoryBlock,
          actuallyAllocateMemoryInBytes);
    }
    dataCacheMemoryBlock.updateReferenceCount(1);
    return dataCacheMemoryBlock;
  }

  public synchronized void releaseDataCacheMemoryBlock() {
    dataCacheMemoryBlock.updateReferenceCount(-1);
    if (dataCacheMemoryBlock.getReferenceCount() == 0) {
      LOGGER.info(StorageEngineMessages.RELEASE_DATA_CACHE_MEMORY_BLOCK, dataCacheMemoryBlock);
      dataCacheMemoryBlock.close();
      dataCacheMemoryBlock = null;
    }
  }

  // used for Metrics
  public long getUsedMemorySizeInBytes() {
    return usedMemorySizeInBytes.get();
  }

  public long getDataCacheUsedMemorySizeInBytes() {
    return dataCacheMemoryBlock == null ? 0 : dataCacheMemoryBlock.getMemoryUsageInBytes();
  }

  public long getDataCacheLimitedMemorySizeInBytes() {
    return dataCacheMemoryBlock == null ? 0 : dataCacheMemoryBlock.getLimitedMemorySizeInBytes();
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
