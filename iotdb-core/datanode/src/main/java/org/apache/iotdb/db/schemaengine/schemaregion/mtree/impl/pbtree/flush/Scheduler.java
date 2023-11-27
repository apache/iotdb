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
import org.apache.iotdb.commons.schema.node.role.IDatabaseMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.CachedMTreeStore;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.cache.ICacheManager;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.memcontrol.IReleaseFlushStrategy;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.ICachedMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.ISchemaFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

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
            ThreadName.PBTREE_FLUSH_PROCESSOR.getName(),
            new ThreadPoolExecutor.DiscardPolicy());
    this.flushingRegionSet = flushingRegionSet;
    this.releaseFlushStrategy = releaseFlushStrategy;
  }

  /**
   * Force flush all volatile subtrees and updated database MNodes to disk. After flushing, the
   * MNodes will be placed into node cache. This method will return synchronously after all stores
   * are successfully flushed.
   */
  public synchronized void forceFlushAll() {
    List<Map.Entry<Integer, CachedMTreeStore>> flushEngineList = new ArrayList<>();
    for (Map.Entry<Integer, CachedMTreeStore> entry : regionToStore.entrySet()) {
      if (flushingRegionSet.contains(entry.getKey())) {
        continue;
      }
      flushingRegionSet.add(entry.getKey());
      flushEngineList.add(entry);
    }
    CompletableFuture.allOf(
            flushEngineList.stream()
                .map(
                    entry ->
                        CompletableFuture.runAsync(
                            () -> {
                              CachedMTreeStore store = entry.getValue();
                              int regionId = entry.getKey();
                              ICacheManager cacheManager = store.getCacheManager();
                              ISchemaFile file = store.getSchemaFile();
                              long startTime = System.currentTimeMillis();
                              PBTreeFlushExecutor flushExecutor;
                              IDatabaseMNode<ICachedMNode> dbNode =
                                  cacheManager.collectUpdatedStorageGroupMNodes();
                              if (dbNode != null) {
                                flushExecutor =
                                    new PBTreeFlushExecutor(
                                        Collections.singletonList(dbNode.getAsMNode()),
                                        cacheManager,
                                        file);
                                try {
                                  flushExecutor.flushVolatileNodes();
                                } catch (MetadataException e) {
                                  LOGGER.warn(
                                      "Error occurred during MTree flush, current SchemaRegionId is {} because {}",
                                      regionId,
                                      e.getMessage(),
                                      e);
                                }
                              }
                              flushExecutor =
                                  new PBTreeFlushExecutor(
                                      cacheManager.collectVolatileSubtrees(), cacheManager, file);
                              try {
                                flushExecutor.flushVolatileNodes();
                              } catch (MetadataException e) {
                                LOGGER.warn(
                                    "Error occurred during MTree flush, current SchemaRegionId is {} because {}",
                                    regionId,
                                    e.getMessage(),
                                    e);
                              } finally {
                                long time = System.currentTimeMillis() - startTime;
                                if (time > 10_000) {
                                  LOGGER.info(
                                      "It takes {}ms to flush MTree in SchemaRegion {}",
                                      time,
                                      regionId);
                                } else {
                                  LOGGER.debug(
                                      "It takes {}ms to flush MTree in SchemaRegion {}",
                                      time,
                                      regionId);
                                }
                                flushingRegionSet.remove(regionId);
                              }
                            },
                            workerPool))
                .toArray(CompletableFuture[]::new))
        .join();
  }

  /**
   * Keep fetching evictable nodes from cacheManager until the memory status is under safe mode or
   * no node could be evicted. Update the memory status after evicting each node.
   *
   * @param force true if force to evict all cache
   */
  public synchronized void scheduleRelease(boolean force) {
    CompletableFuture.allOf(
            regionToStore.values().stream()
                .map(
                    store ->
                        CompletableFuture.runAsync(
                            () -> {
                              while (force || releaseFlushStrategy.isExceedReleaseThreshold()) {
                                // store try to release memory if not exceed release threshold
                                if (store.executeMemoryRelease()) {
                                  // if store can not release memory, break
                                  break;
                                }
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
            ICacheManager cacheManager = store.getCacheManager();
            ISchemaFile file = store.getSchemaFile();
            List<ICachedMNode> nodesToFlush = new ArrayList<>();
            long startTime = System.currentTimeMillis();
            IDatabaseMNode<ICachedMNode> dbNode = cacheManager.collectUpdatedStorageGroupMNodes();
            if (dbNode != null) {
              nodesToFlush.add(dbNode.getAsMNode());
            }
            Iterator<ICachedMNode> volatileSubtrees = cacheManager.collectVolatileSubtrees();
            while (volatileSubtrees.hasNext()) {
              nodesToFlush.add(volatileSubtrees.next());
              if (nodesToFlush.size() > remainToFlush.get()) {
                break;
              }
            }
            PBTreeFlushExecutor flushExecutor =
                new PBTreeFlushExecutor(nodesToFlush, cacheManager, file);
            try {
              flushExecutor.flushVolatileNodes();
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
              remainToFlush.addAndGet(-nodesToFlush.size());
              flushingRegionSet.remove(regionId);
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
