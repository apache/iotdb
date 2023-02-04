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
import java.util.concurrent.Executors;

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

  private ExecutorService flushTaskExecutor =
      IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
          ThreadName.MTREE_FLUSH_THREAD_POOL.getName());
  private ExecutorService releaseTaskExecutor =
      IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
          ThreadName.MTREE_RELEASE_THREAD_POOL_NAME.getName());
  private ExecutorService flushTaskExecutor1 = Executors.newFixedThreadPool(CONCURRENT_NUM);
  //      IoTDBThreadPoolFactory.newFixedThreadPool(
  //          CONCURRENT_NUM, ThreadName.MTREE_FLUSH_THREAD_POOL.getName() + "1");
  private ExecutorService releaseTaskExecutor1 = Executors.newFixedThreadPool(CONCURRENT_NUM);
  //      IoTDBThreadPoolFactory.newFixedThreadPool(
  //          CONCURRENT_NUM, ThreadName.MTREE_RELEASE_THREAD_POOL_NAME.getName() + "1");

  private volatile boolean hasFlushTask;
  private int flushCount = 0;

  private volatile boolean hasReleaseTask;
  private int releaseCount = 0;

  public synchronized ICacheManager createLRUCacheManager(CachedMTreeStore store) {
    synchronized (storeList) {
      ICacheManager cacheManager = new LRUCacheManager();
      storeList.add(store);
      return cacheManager;
    }
  }

  public void init() {
    flushTaskExecutor =
        IoTDBThreadPoolFactory.newSingleThreadExecutor(
            ThreadName.MTREE_FLUSH_THREAD_POOL.getName());
    releaseTaskExecutor =
        IoTDBThreadPoolFactory.newSingleThreadExecutor(
            ThreadName.MTREE_RELEASE_THREAD_POOL_NAME.getName());
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
      for (int i = 0; i * CONCURRENT_NUM < storeList.size(); i++) {
        CompletableFuture<Void>[] completableFutures =
            new CompletableFuture[storeList.size() - i * CONCURRENT_NUM];
        for (int j = 0; j < completableFutures.length; j++) {
          CachedMTreeStore store = storeList.get(i * CONCURRENT_NUM + j);
          completableFutures[j] =
              CompletableFuture.runAsync(
                  () -> {
                    store.getLock().threadReadLock();
                    try {
                      store.executeMemoryRelease();
                    } finally {
                      store.getLock().threadReadUnlock();
                    }
                  },
                  releaseTaskExecutor1);
        }
        CompletableFuture.allOf(completableFutures).join();
        if (!memManager.isExceedReleaseThreshold() || memManager.isEmpty()) {
          break;
        }
      }
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
      for (int i = 0; i * CONCURRENT_NUM < storeList.size(); i++) {
        CompletableFuture<Void>[] completableFutures =
            new CompletableFuture[storeList.size() - i * CONCURRENT_NUM];
        for (int j = 0; j < completableFutures.length; j++) {
          CachedMTreeStore store = storeList.get(i * CONCURRENT_NUM + j);
          completableFutures[j] =
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
                  flushTaskExecutor1);
        }
        CompletableFuture.allOf(completableFutures).join();
      }
      hasFlushTask = false;
      flushCount++;
    }
  }

  public void clear() {
    if (releaseTaskExecutor != null) {
      releaseTaskExecutor.shutdown();
      while (!releaseTaskExecutor.isTerminated()) ;
      releaseTaskExecutor = null;
    }
    // the release task may submit flush task, thus must be shut down and clear first
    if (flushTaskExecutor != null) {
      flushTaskExecutor.shutdown();
      while (!flushTaskExecutor.isTerminated()) ;
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
