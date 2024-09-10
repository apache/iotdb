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
import org.apache.iotdb.db.exception.LoadRuntimeOutOfMemoryException;
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
        LOGGER.warn("forceAllocate: interrupted while waiting for available memory", e);
      }
    }

    throw new LoadRuntimeOutOfMemoryException(
        String.format(
            "forceAllocate: failed to allocate memory from query engine after %s retries, "
                + "total query memory %s bytes, current available memory for load %s bytes, "
                + "current load used memory size %s bytes, load requested memory size %s bytes",
            MEMORY_ALLOCATE_MAX_RETRIES,
            QUERY_ENGINE_MEMORY_MANAGER.getAllocateMemoryForOperators(),
            QUERY_ENGINE_MEMORY_MANAGER.getFreeMemoryForLoadTsFile(),
            usedMemorySizeInBytes.get(),
            sizeInBytes));
  }

  public synchronized long tryAllocateFromQuery(long sizeInBytes) {
    long actuallyAllocateMemoryInBytes =
        Math.max(0L, QUERY_ENGINE_MEMORY_MANAGER.tryAllocateFreeMemoryForOperators(sizeInBytes));
    usedMemorySizeInBytes.addAndGet(actuallyAllocateMemoryInBytes);
    return actuallyAllocateMemoryInBytes;
  }

  public synchronized void releaseToQuery(long sizeInBytes) {
    usedMemorySizeInBytes.addAndGet(-sizeInBytes);
    QUERY_ENGINE_MEMORY_MANAGER.releaseToFreeMemoryForOperators(sizeInBytes);
    this.notifyAll();
  }

  public synchronized LoadTsFileAnalyzeSchemaMemoryBlock allocateAnalyzeSchemaMemoryBlock(
      long sizeInBytes) throws LoadRuntimeOutOfMemoryException {
    try {
      forceAllocateFromQuery(sizeInBytes);
    } catch (LoadRuntimeOutOfMemoryException e) {
      if (dataCacheMemoryBlock != null && dataCacheMemoryBlock.doShrink(sizeInBytes)) {
        return new LoadTsFileAnalyzeSchemaMemoryBlock(sizeInBytes);
      }
      throw e;
    }
    return new LoadTsFileAnalyzeSchemaMemoryBlock(sizeInBytes);
  }

  /**
   * Resize the memory block to the new size.
   *
   * @throws LoadRuntimeOutOfMemoryException if failed to allocate enough memory
   */
  synchronized void forceResize(LoadTsFileAnalyzeSchemaMemoryBlock memoryBlock, long newSizeInBytes)
      throws LoadRuntimeOutOfMemoryException {
    if (memoryBlock.getTotalMemorySizeInBytes() >= newSizeInBytes) {
      releaseToQuery(memoryBlock.getTotalMemorySizeInBytes() - newSizeInBytes);
      memoryBlock.setTotalMemorySizeInBytes(newSizeInBytes);
      return;
    }

    long bytesNeeded = newSizeInBytes - memoryBlock.getTotalMemorySizeInBytes();
    try {
      forceAllocateFromQuery(bytesNeeded);
    } catch (LoadRuntimeOutOfMemoryException e) {
      if (dataCacheMemoryBlock == null || !dataCacheMemoryBlock.doShrink(bytesNeeded)) {
        throw e;
      }
    }
    memoryBlock.setTotalMemorySizeInBytes(newSizeInBytes);
  }

  public synchronized LoadTsFileDataCacheMemoryBlock allocateDataCacheMemoryBlock()
      throws LoadRuntimeOutOfMemoryException {
    if (dataCacheMemoryBlock == null) {
      long actuallyAllocateMemoryInBytes =
          tryAllocateFromQuery(MEMORY_TOTAL_SIZE_FROM_QUERY_IN_BYTES >> 2);
      dataCacheMemoryBlock = new LoadTsFileDataCacheMemoryBlock(actuallyAllocateMemoryInBytes);
      LOGGER.info(
          "Create Data Cache Memory Block {}, allocate memory {}",
          dataCacheMemoryBlock,
          actuallyAllocateMemoryInBytes);
    }
    dataCacheMemoryBlock.updateReferenceCount(1);
    return dataCacheMemoryBlock;
  }

  public synchronized void releaseDataCacheMemoryBlock() {
    dataCacheMemoryBlock.updateReferenceCount(-1);
    if (dataCacheMemoryBlock.getReferenceCount() == 0) {
      LOGGER.info("Release Data Cache Memory Block {}", dataCacheMemoryBlock);
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
