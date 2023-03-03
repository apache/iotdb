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
import org.apache.iotdb.commons.concurrent.threadpool.WrappedThreadPoolExecutor;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.metric.SchemaEngineCachedMetric;
import org.apache.iotdb.db.metadata.mtree.store.CachedMTreeStore;
import org.apache.iotdb.db.metadata.mtree.store.disk.memcontrol.IReleaseFlushStrategy;
import org.apache.iotdb.db.metadata.mtree.store.disk.memcontrol.MemManager;
import org.apache.iotdb.db.metadata.mtree.store.disk.memcontrol.ReleaseFlushStrategyNumBasedImpl;
import org.apache.iotdb.db.metadata.mtree.store.disk.memcontrol.ReleaseFlushStrategySizeBasedImpl;
import org.apache.iotdb.db.metadata.rescon.CachedSchemaEngineStatistics;
import org.apache.iotdb.db.metadata.rescon.ISchemaEngineStatistics;
import org.apache.iotdb.db.utils.concurrent.FiniteSemaphore;

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

  private CachedSchemaEngineStatistics engineStatistics;
  private SchemaEngineCachedMetric engineMetric;

  private static final int CONCURRENT_NUM = 10;

  private ExecutorService flushTaskProcessor;
  private ExecutorService flushTaskMonitor;
  private ExecutorService releaseTaskProcessor;
  private ExecutorService releaseTaskMonitor;

  private FiniteSemaphore flushSemaphore;
  private FiniteSemaphore releaseSemaphore;

  private volatile boolean hasFlushTask;

  private volatile boolean hasReleaseTask;

  private IReleaseFlushStrategy releaseFlushStrategy;

  private static final int MAX_WAITING_TIME_WHEN_RELEASING = 10_000;
  private final Object blockObject = new Object();

  /**
   * Create and allocate LRUCacheManager to the corresponding CachedMTreeStore.
   *
   * @param store CachedMTreeStore
   * @return LRUCacheManager
   */
  public ICacheManager createLRUCacheManager(CachedMTreeStore store, MemManager memManager) {
    synchronized (storeList) {
      ICacheManager cacheManager = new LRUCacheManager(memManager);
      storeList.add(store);
      return cacheManager;
    }
  }

  public void init(ISchemaEngineStatistics engineStatistics) {
    flushSemaphore = new FiniteSemaphore(2, 0);
    releaseSemaphore = new FiniteSemaphore(2, 0);
    this.engineStatistics = engineStatistics.getAsCachedSchemaEngineStatistics();
    if (IoTDBDescriptor.getInstance().getConfig().getCachedMNodeSizeInSchemaFileMode() >= 0) {
      releaseFlushStrategy = new ReleaseFlushStrategyNumBasedImpl(this.engineStatistics);
    } else {
      releaseFlushStrategy = new ReleaseFlushStrategySizeBasedImpl(this.engineStatistics);
    }
    flushTaskMonitor =
        IoTDBThreadPoolFactory.newSingleThreadExecutor(ThreadName.SCHEMA_FLUSH_MONITOR.getName());
    flushTaskProcessor =
        IoTDBThreadPoolFactory.newFixedThreadPool(
            CONCURRENT_NUM, ThreadName.SCHEMA_REGION_FLUSH_PROCESSOR.getName());
    releaseTaskMonitor =
        IoTDBThreadPoolFactory.newSingleThreadExecutor(ThreadName.SCHEMA_RELEASE_MONITOR.getName());
    releaseTaskProcessor =
        IoTDBThreadPoolFactory.newFixedThreadPool(
            CONCURRENT_NUM, ThreadName.SCHEMA_REGION_RELEASE_PROCESSOR.getName());
    releaseTaskMonitor.submit(
        () -> {
          try {
            while (!Thread.currentThread().isInterrupted()) {
              releaseSemaphore.acquire();
              try {
                if (isExceedReleaseThreshold()) {
                  hasReleaseTask = true;
                  tryExecuteMemoryRelease();
                }
              } catch (Throwable throwable) {
                hasReleaseTask = false;
                logger.error("Something wrong happened during MTree release.", throwable);
              }
            }
          } catch (InterruptedException e) {
            logger.info("ReleaseTaskMonitor thread is interrupted.");
            Thread.currentThread().interrupt();
          }
        });
    flushTaskMonitor.submit(
        () -> {
          try {
            while (!Thread.currentThread().isInterrupted()) {
              flushSemaphore.acquire();
              try {
                if (isExceedFlushThreshold()) {
                  hasFlushTask = true;
                  tryFlushVolatileNodes();
                }
              } catch (Throwable throwable) {
                hasFlushTask = false;
                logger.error("Something wrong happened during MTree flush.", throwable);
              }
            }
          } catch (InterruptedException e) {
            logger.info("FlushTaskMonitor thread is interrupted.");
            Thread.currentThread().interrupt();
          }
        });
  }

  public void setEngineMetric(SchemaEngineCachedMetric engineMetric) {
    this.engineMetric = engineMetric;
  }

  public boolean isExceedReleaseThreshold() {
    return releaseFlushStrategy.isExceedReleaseThreshold();
  }

  public boolean isExceedFlushThreshold() {
    return releaseFlushStrategy.isExceedFlushThreshold();
  }

  /**
   * Check the current memory usage. If the release threshold is exceeded, trigger the task to
   * perform an internal and external memory swap to release the memory.
   */
  public void ensureMemoryStatus() {
    if (isExceedReleaseThreshold()) {
      registerReleaseTask();
    }
  }

  /**
   * If there is a ReleaseTask or FlushTask, block the current thread to wait up to
   * MAX_WAITING_TIME_WHEN_RELEASING. The thread will be woken up if the ReleaseTask or FlushTask
   * ends or the wait time exceeds MAX_WAITING_TIME_WHEN_RELEASING.
   */
  public void waitIfReleasing() {
    synchronized (blockObject) {
      if (hasReleaseTask || hasFlushTask) {
        try {
          blockObject.wait(MAX_WAITING_TIME_WHEN_RELEASING);
        } catch (InterruptedException e) {
          logger.warn(
              "Interrupt because the release task and flush task did not finish within {} milliseconds.",
              MAX_WAITING_TIME_WHEN_RELEASING);
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  private void registerReleaseTask() {
    releaseSemaphore.release();
  }

  /**
   * Execute cache eviction until the memory status is under safe mode or no node could be evicted.
   * If the memory status is still full, which means the nodes in memory are all volatile nodes, new
   * added or updated, fire flush task.
   */
  private void tryExecuteMemoryRelease() {
    synchronized (storeList) {
      long startTime = System.currentTimeMillis();
      CompletableFuture.allOf(
              storeList.stream()
                  .map(
                      store ->
                          CompletableFuture.runAsync(
                              () -> {
                                store.getLock().threadReadLock();
                                try {
                                  executeMemoryRelease(store);
                                } finally {
                                  store.getLock().threadReadUnlock();
                                }
                              },
                              releaseTaskProcessor))
                  .toArray(CompletableFuture[]::new))
          .join();
      if (engineMetric != null) {
        engineMetric.recordRelease(System.currentTimeMillis() - startTime);
      }
      synchronized (blockObject) {
        hasReleaseTask = false;
        if (isExceedFlushThreshold()) {
          registerFlushTask();
        } else {
          blockObject.notifyAll();
        }
      }
    }
  }

  /**
   * Keep fetching evictable nodes from cacheManager until the memory status is under safe mode or
   * no node could be evicted. Update the memory status after evicting each node.
   */
  private void executeMemoryRelease(CachedMTreeStore store) {
    while (isExceedReleaseThreshold()) {
      // store try to release memory if not exceed release threshold
      if (store.executeMemoryRelease()) {
        // if store can not release memory, break
        break;
      }
    }
  }

  private void registerFlushTask() {
    flushSemaphore.release();
  }

  /** Sync all volatile nodes to schemaFile and execute memory release after flush. */
  private void tryFlushVolatileNodes() {
    synchronized (storeList) {
      long startTime = System.currentTimeMillis();
      CompletableFuture.allOf(
              storeList.stream()
                  .map(
                      store ->
                          CompletableFuture.runAsync(
                              () -> {
                                store.getLock().writeLock();
                                try {
                                  store.flushVolatileNodes();
                                  executeMemoryRelease(store);
                                } finally {
                                  store.getLock().unlockWrite();
                                }
                              },
                              flushTaskProcessor))
                  .toArray(CompletableFuture[]::new))
          .join();
      if (engineMetric != null) {
        engineMetric.recordFlush(System.currentTimeMillis() - startTime);
      }
      synchronized (blockObject) {
        hasFlushTask = false;
        blockObject.notifyAll();
      }
    }
  }

  public void clear() {
    if (releaseTaskMonitor != null) {
      releaseTaskMonitor.shutdownNow();
      while (true) {
        if (releaseTaskMonitor.isTerminated()) break;
      }
      releaseTaskMonitor = null;
    }
    if (flushTaskMonitor != null) {
      flushTaskMonitor.shutdownNow();
      while (true) {
        if (flushTaskMonitor.isTerminated()) break;
      }
      releaseTaskMonitor = null;
    }
    if (releaseTaskProcessor != null) {
      while (true) {
        if (!hasReleaseTask) break;
      }
      releaseTaskProcessor.shutdown();
      while (true) {
        if (releaseTaskProcessor.isTerminated()) break;
      }
      releaseTaskProcessor = null;
    }
    // the release task may submit flush task, thus must be shut down and clear first
    if (flushTaskProcessor != null) {
      while (true) {
        if (!hasFlushTask) break;
      }
      flushTaskProcessor.shutdown();
      while (true) {
        if (flushTaskProcessor.isTerminated()) break;
      }
      flushTaskProcessor = null;
    }
    storeList.clear();
    releaseFlushStrategy = null;
    engineStatistics = null;
    releaseSemaphore = null;
    flushSemaphore = null;
    engineMetric = null;
  }

  public int getReleaseThreadNum() {
    return ((WrappedThreadPoolExecutor) releaseTaskProcessor).getActiveCount();
  }

  public int getFlushThreadNum() {
    return ((WrappedThreadPoolExecutor) flushTaskProcessor).getActiveCount();
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
