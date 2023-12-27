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

package org.apache.iotdb.db.queryengine.load;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.LoadRuntimeOutOfMemoryException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

public class LoadTsFileMemoryManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadTsFileMemoryManager.class);
  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private final long QUERY_TOTAL_MEMORY_SIZE_IN_BYTES =
      CONFIG.getLoadMemoryTotalSizeFromQueryInBytes();
  private static final int MEMORY_ALLOCATE_MAX_RETRIES = CONFIG.getLoadMemoryAllocateMaxRetries();
  private static final long MEMORY_ALLOCATE_RETRY_INTERVAL_IN_MS =
      CONFIG.getLoadMemoryAllocateRetryIntervalMs();

  private final AtomicLong usedMemorySizeInBytes = new AtomicLong(0);
  private LoadTsFileDataCacheMemoryBlock dataCacheMemoryBlock;

  private synchronized void forceAllocatedFromQuery(long sizeInBytes)
      throws LoadRuntimeOutOfMemoryException {
    for (int i = 0; i < MEMORY_ALLOCATE_MAX_RETRIES; i++) {
      // allocate memory from queryEngine
      // TODO: queryEngine provides a method to allocate memory
      if (usedMemorySizeInBytes.get() + sizeInBytes <= QUERY_TOTAL_MEMORY_SIZE_IN_BYTES) {
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
            "forceAllocate: failed to allocate memory from query engine after %d retries, "
                + "total query memory %s, used memory size %d bytes, "
                + "requested memory size %d bytes",
            MEMORY_ALLOCATE_MAX_RETRIES,
            QUERY_TOTAL_MEMORY_SIZE_IN_BYTES,
            usedMemorySizeInBytes.get(),
            sizeInBytes));
  }

  public synchronized long tryAllocateFromQuery(long sizeInBytes) {
    // TODO: queryEngine provides a method to allocate memory
    long allocatedSizeInBytes =
        Math.min(sizeInBytes, QUERY_TOTAL_MEMORY_SIZE_IN_BYTES - usedMemorySizeInBytes.get());
    long result = Math.max(0L, allocatedSizeInBytes);
    usedMemorySizeInBytes.addAndGet(result);
    return result;
  }

  public synchronized void releaseToQuery(long sizeInBytes) {
    // todo: queryEngine provides a method to release memory
    usedMemorySizeInBytes.addAndGet(-sizeInBytes);
    this.notifyAll();
  }

  public synchronized LoadTsFileAnalyzeSchemaMemoryBlock allocateAnalyzeSchemaMemoryBlock(
      long sizeInBytes) throws LoadRuntimeOutOfMemoryException {
    try {
      forceAllocatedFromQuery(sizeInBytes);
    } catch (LoadRuntimeOutOfMemoryException e) {
      if (dataCacheMemoryBlock != null && dataCacheMemoryBlock.doShrink(sizeInBytes)) {
        return new LoadTsFileAnalyzeSchemaMemoryBlock(sizeInBytes);
      }
      throw e;
    }
    return new LoadTsFileAnalyzeSchemaMemoryBlock(sizeInBytes);
  }

  public synchronized LoadTsFileDataCacheMemoryBlock allocateDataCacheMemoryBlock()
      throws LoadRuntimeOutOfMemoryException {
    if (dataCacheMemoryBlock == null) {
      long actuallyAllocateMemoryInBytes =
          tryAllocateFromQuery(QUERY_TOTAL_MEMORY_SIZE_IN_BYTES >> 1);
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
  public long getUsedMemorySizeInMB() {
    return usedMemorySizeInBytes.get() / 1024 / 1024;
  }

  public long getDataCacheUsedMemorySizeInMB() {
    return dataCacheMemoryBlock == null
        ? 0
        : dataCacheMemoryBlock.getMemoryUsageInBytes() / 1024 / 1024;
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
