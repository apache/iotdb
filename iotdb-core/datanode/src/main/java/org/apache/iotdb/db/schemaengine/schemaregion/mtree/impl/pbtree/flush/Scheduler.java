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
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.cache.ICacheManager;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.ICachedMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.ISchemaFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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

  private final ExecutorService workerPool;

  public Scheduler(Map<Integer, CachedMTreeStore> regionToStore) {
    this.regionToStore = regionToStore;
    workerPool =
        IoTDBThreadPoolFactory.newFixedThreadPool(
            FLUSH_WORKER_NUM,
            ThreadName.PBTREE_FLUSH_PROCESSOR.getName(),
            new ThreadPoolExecutor.DiscardPolicy());
    // When the thread pool is unable to handle a new task, it simply discards the task without doing anything about it.
  }

  public synchronized void forceFlushAll() {
    // TODO
  }

  public synchronized void scheduleFlush(List<Integer> regionIds) {
    AtomicInteger remainToFlush = new AtomicInteger(BATCH_FLUSH_SUBTREE);
    for (int regionId : regionIds) {
      workerPool.submit(
          () -> {
            CachedMTreeStore store = regionToStore.get(regionId);
            ICacheManager cacheManager = store.getCacheManager();
            ISchemaFile file = store.getSchemaFile();
            List<ICachedMNode> nodesToFlush = new ArrayList<>();
            long startTime = System.currentTimeMillis();
            ICachedMNode node = cacheManager.collectUpdatedStorageGroupMNodes().getAsMNode();
            if (node != null) {
              nodesToFlush.add(node);
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
            }
          });
      if(remainToFlush.get()<=0){
        break;
      }
    }
  }

  public int getFlushThreadNum() {
    return ((WrappedThreadPoolExecutor) workerPool).getActiveCount();
  }
}
