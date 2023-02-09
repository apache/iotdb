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
package org.apache.iotdb.db.metadata.mtree.store.disk.cache;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.db.metadata.mtree.store.CachedMTreeStore;
import org.apache.iotdb.db.metadata.mtree.store.disk.memcontrol.IMemManager;
import org.apache.iotdb.db.metadata.mtree.store.disk.memcontrol.MemManagerHolder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

/**
 * CacheMemoryManager is used to register the CachedMTreeStore and create the CacheManager.
 * CacheMemoryManager provides the {@link CacheMemoryManager#ensureMemoryStatus} interface, which
 * starts asynchronous threads to free and flush the disk when memory usage exceeds a threshold.
 */
public class CacheMemoryManager {

  private static final Logger logger = LoggerFactory.getLogger(CacheMemoryManager.class);

  private final List<CachedMTreeStore> storeList = new ArrayList<>();

  private final IMemManager memManager = MemManagerHolder.getMemManagerInstance();

  private static final int CONCURRENT_NUM = 10;

  private ExecutorService flushTaskExecutor;
  private ExecutorService releaseTaskExecutor;

  private volatile boolean hasFlushTask;
  private int flushCount = 0;

  private volatile boolean hasReleaseTask;
  private int releaseCount = 0;

  public ICacheManager createLRUCacheManager(CachedMTreeStore store) {
    synchronized (storeList) {
      ICacheManager cacheManager = new LRUCacheManager();
      storeList.add(store);
      return cacheManager;
    }
  }

  public void init() {
    flushTaskExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(
            CONCURRENT_NUM, ThreadName.SCHEMA_REGION_FLUSH_POOL.getName());
    releaseTaskExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(
            CONCURRENT_NUM, ThreadName.SCHEMA_REGION_RELEASE_POOL.getName());
  }

  public void ensureMemoryStatus() {
    if (memManager.isExceedReleaseThreshold() && !hasReleaseTask) {
      registerReleaseTask();
    }
  }

  private synchronized void registerReleaseTask() {
    if (hasReleaseTask) {
      return;
    }
    hasReleaseTask = true;
    releaseTaskExecutor.submit(
        () -> {
          try {
            tryExecuteMemoryRelease();
          } catch (Throwable throwable) {
            logger.error("Something wrong happened during MTree release.", throwable);
            throwable.printStackTrace();
            throw throwable;
          }
        });
  }

  /**
   * Execute cache eviction until the memory status is under safe mode or no node could be evicted.
   * If the memory status is still full, which means the nodes in memory are all volatile nodes, new
   * added or updated, fire flush task.
   */
  private void tryExecuteMemoryRelease() {
    synchronized (storeList) {
      CompletableFuture.allOf(
              storeList.stream()
                  .map(
                      store ->
                          CompletableFuture.runAsync(
                              () -> {
                                store.getLock().threadReadLock();
                                try {
                                  store.executeMemoryRelease();
                                } finally {
                                  store.getLock().threadReadUnlock();
                                }
                              },
                              releaseTaskExecutor))
                  .toArray(CompletableFuture[]::new))
          .join();
      releaseCount++;
      hasReleaseTask = false;
      if (memManager.isExceedFlushThreshold() && !hasFlushTask) {
        registerFlushTask();
      }
    }
  }

  private synchronized void registerFlushTask() {
    if (hasFlushTask) {
      return;
    }
    hasFlushTask = true;
    flushTaskExecutor.submit(
        () -> {
          try {
            tryFlushVolatileNodes();
          } catch (Throwable throwable) {
            logger.error("Something wrong happened during MTree flush.", throwable);
            throwable.printStackTrace();
            throw throwable;
          }
        });
  }

  /** Sync all volatile nodes to schemaFile and execute memory release after flush. */
  private void tryFlushVolatileNodes() {
    synchronized (storeList) {
      CompletableFuture.allOf(
              storeList.stream()
                  .map(
                      store ->
                          CompletableFuture.runAsync(
                              () -> {
                                store.getLock().writeLock();
                                try {
                                  store.flushVolatileNodes();
                                  store.executeMemoryRelease();
                                } finally {
                                  store.getLock().unlockWrite();
                                }
                              },
                              flushTaskExecutor))
                  .toArray(CompletableFuture[]::new))
          .join();
      hasFlushTask = false;
      flushCount++;
    }
  }

  public void clear() {
    if (releaseTaskExecutor != null) {
      while (true) {
        if (!hasReleaseTask) break;
      }
      releaseTaskExecutor.shutdown();
      while (true) {
        if (releaseTaskExecutor.isTerminated()) break;
      }
      releaseTaskExecutor = null;
    }
    // the release task may submit flush task, thus must be shut down and clear first
    if (flushTaskExecutor != null) {
      while (true) {
        if (!hasFlushTask) break;
      }
      flushTaskExecutor.shutdown();
      while (true) {
        if (flushTaskExecutor.isTerminated()) break;
      }
      flushTaskExecutor = null;
    }
  }

  private CacheMemoryManager() {}

  private static class GlobalCacheManagerHolder {
    private static final CacheMemoryManager INSTANCE = new CacheMemoryManager();

    private GlobalCacheManagerHolder() {}
  }

  public static CacheMemoryManager getInstance() {
    return CacheMemoryManager.GlobalCacheManagerHolder.INSTANCE;
  }
}
