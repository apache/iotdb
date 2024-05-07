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

package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.flush;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.WrappedThreadPoolExecutor;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.CachedMTreeStore;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.lock.LockManager;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.memcontrol.IReleaseFlushStrategy;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.memory.IMemoryManager;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.ISchemaFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class Scheduler {
  private static final Logger LOGGER = LoggerFactory.getLogger(Scheduler.class);

  /** configuration */
  private int BATCH_FLUSH_SUBTREE = 50;

  private int FLUSH_WORKER_NUM = 10;

  /** data structure */
  private final Map<Integer, CachedMTreeStore> regionToStore;

  // flushingRegionSet is used to avoid flush the same region concurrently, update will be
  // guaranteed by synchronized
  private final Set<Integer> flushingRegionSet;

  private final ExecutorService workerPool;
  private final IReleaseFlushStrategy releaseFlushStrategy;

  public Scheduler(
      Map<Integer, CachedMTreeStore> regionToStore,
      Set<Integer> flushingRegionSet,
      IReleaseFlushStrategy releaseFlushStrategy) {
    this.regionToStore = regionToStore;
    // When the thread pool is unable to handle a new task, it simply discards the task without
    // doing anything about it.
    this.workerPool =
        IoTDBThreadPoolFactory.newFixedThreadPool(
            FLUSH_WORKER_NUM,
            ThreadName.PBTREE_WORKER_POOL.getName(),
            new ThreadPoolExecutor.DiscardPolicy());
    this.flushingRegionSet = flushingRegionSet;
    this.releaseFlushStrategy = releaseFlushStrategy;
  }

  private void executeFlush(CachedMTreeStore store, int regionId, AtomicInteger remainToFlush) {
    IMemoryManager memoryManager = store.getMemoryManager();
    ISchemaFile file = store.getSchemaFile();
    LockManager lockManager = store.getLockManager();
    long startTime = System.currentTimeMillis();
    AtomicLong flushNodeNum = new AtomicLong(0);
    AtomicLong flushMemSize = new AtomicLong(0);
    try {
      PBTreeFlushExecutor flushExecutor;
      if (remainToFlush == null) {
        flushExecutor = new PBTreeFlushExecutor(memoryManager, file, lockManager);
      } else {
        flushExecutor = new PBTreeFlushExecutor(remainToFlush, memoryManager, file, lockManager);
      }
      flushExecutor.flushVolatileNodes(flushNodeNum, flushMemSize);
    } catch (MetadataException e) {
      LOGGER.warn(
          "Error occurred during MTree flush, current SchemaRegionId is {} because {}",
          regionId,
          e.getMessage(),
          e);
    } finally {
      long time = System.currentTimeMillis() - startTime;
      if (time > 10_000) {
        LOGGER.info("It takes {}ms to flush MTree in SchemaRegion {}", time, regionId);
      } else {
        LOGGER.debug("It takes {}ms to flush MTree in SchemaRegion {}", time, regionId);
      }
      store.recordFlushMetrics(time, flushNodeNum.get(), flushMemSize.get());
      flushingRegionSet.remove(regionId);
    }
  }

  private void executeRelease(CachedMTreeStore store, boolean force) {
    AtomicLong releaseNodeNum = new AtomicLong(0);
    AtomicLong releaseMemorySize = new AtomicLong(0);
    long startTime = System.currentTimeMillis();
    while (force || releaseFlushStrategy.isExceedReleaseThreshold()) {
      // store try to release memory if not exceed release threshold
      if (store.executeMemoryRelease(releaseNodeNum, releaseMemorySize)) {
        // if store can not release memory, break
        break;
      }
    }
    store.recordReleaseMetrics(
        System.currentTimeMillis() - startTime, releaseNodeNum.get(), releaseMemorySize.get());
  }

  /**
   * Force flush all volatile subtrees and updated database MNodes to disk. After flushing, the
   * MNodes will be placed into node cache. This method will return synchronously after all stores
   * are successfully flushed.
   */
  public synchronized CompletableFuture<Void> scheduleFlushAll() {
    List<Map.Entry<Integer, CachedMTreeStore>> flushEngineList = new ArrayList<>();
    for (Map.Entry<Integer, CachedMTreeStore> entry : regionToStore.entrySet()) {
      if (flushingRegionSet.contains(entry.getKey())) {
        continue;
      }
      flushingRegionSet.add(entry.getKey());
      flushEngineList.add(entry);
    }
    return CompletableFuture.allOf(
        flushEngineList.stream()
            .map(
                entry ->
                    CompletableFuture.runAsync(
                        () -> {
                          int regionId = entry.getKey();
                          CachedMTreeStore store = entry.getValue();
                          if (store == null) {
                            // store has been closed
                            return;
                          }
                          LockManager lockManager = store.getLockManager();
                          lockManager.globalReadLock();
                          if (!regionToStore.containsKey(regionId)) {
                            // double check store have not been closed
                            return;
                          }
                          try {
                            executeFlush(store, regionId, null);
                            executeRelease(store, false);
                          } finally {
                            lockManager.globalReadUnlock();
                          }
                        },
                        workerPool))
            .toArray(CompletableFuture[]::new));
  }

  /**
   * Keep fetching evictable nodes from memoryManager until the memory status is under safe mode or
   * no node could be evicted. Update the memory status after evicting each node.
   *
   * @param force true if force to evict all cache
   */
  public synchronized void scheduleRelease(boolean force) {
    CompletableFuture.allOf(
            regionToStore.entrySet().stream()
                .map(
                    entry ->
                        CompletableFuture.runAsync(
                            () -> {
                              int regionId = entry.getKey();
                              CachedMTreeStore store = entry.getValue();
                              if (store == null) {
                                // store has been closed
                                return;
                              }
                              LockManager lockManager = store.getLockManager();
                              lockManager.globalReadLock(true);
                              if (!regionToStore.containsKey(regionId)) {
                                // double check store have not been closed
                                return;
                              }
                              try {
                                executeRelease(store, force);
                              } finally {
                                lockManager.globalReadUnlock();
                              }
                            },
                            workerPool))
                .toArray(CompletableFuture[]::new))
        .join();
  }

  /**
   * Select some subtrees to flush. The subtrees are selected from the MTreeStore by the sequence of
   * the regionIds. The number of subtrees to flush is determined by parameter {@link
   * Scheduler#BATCH_FLUSH_SUBTREE}. It will return asynchronously. If worker pool is full, the task
   * will be discarded directly.
   *
   * @param regionIds determine the MTreeStore to select subtrees, the head of the list is the first
   *     MTreeStore to select subtrees
   */
  public synchronized void scheduleFlush(List<Integer> regionIds) {
    AtomicInteger remainToFlush = new AtomicInteger(BATCH_FLUSH_SUBTREE);
    for (int regionId : regionIds) {
      if (flushingRegionSet.contains(regionId)) {
        continue;
      }
      flushingRegionSet.add(regionId);
      workerPool.submit(
          () -> {
            CachedMTreeStore store = regionToStore.get(regionId);
            if (store == null) {
              // store has been closed
              return;
            }
            LockManager lockManager = store.getLockManager();
            lockManager.globalReadLock();
            if (!regionToStore.containsKey(regionId)) {
              // double check store have not been closed
              return;
            }
            try {

              executeFlush(store, regionId, remainToFlush);
            } finally {
              lockManager.globalReadUnlock();
            }
          });
      if (remainToFlush.get() <= 0) {
        break;
      }
    }
  }

  public int getActiveWorkerNum() {
    return ((WrappedThreadPoolExecutor) workerPool).getActiveCount();
  }

  public void clear() {
    workerPool.shutdown();
  }

  public boolean isTerminated() {
    return workerPool.isTerminated();
  }
}
