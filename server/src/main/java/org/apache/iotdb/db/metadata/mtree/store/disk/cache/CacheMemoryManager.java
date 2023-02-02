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

import org.apache.iotdb.db.metadata.mtree.store.CachedMTreeStore;
import org.apache.iotdb.db.metadata.mtree.store.disk.MTreeFlushTaskManager;
import org.apache.iotdb.db.metadata.mtree.store.disk.MTreeReleaseTaskManager;
import org.apache.iotdb.db.metadata.mtree.store.disk.memcontrol.IMemManager;
import org.apache.iotdb.db.metadata.mtree.store.disk.memcontrol.MemManagerHolder;

import java.util.ArrayList;
import java.util.List;

public class CacheMemoryManager {

  private final List<CachedMTreeStore> storeList = new ArrayList<>();

  private final IMemManager memManager = MemManagerHolder.getMemManagerInstance();

  private final MTreeFlushTaskManager flushTaskManager = MTreeFlushTaskManager.getInstance();
  private int flushCount = 0;
  private volatile boolean hasFlushTask;

  private final MTreeReleaseTaskManager releaseTaskManager = MTreeReleaseTaskManager.getInstance();
  private volatile boolean hasReleaseTask;
  private int releaseCount = 0;

  public ICacheManager createLRUCacheManager(CachedMTreeStore store) {
    ICacheManager cacheManager = new LRUCacheManager();
    storeList.add(store);
    return cacheManager;
  }

  public void ensureMemoryStatus() {
    if (memManager.isExceedFlushThreshold() && !hasReleaseTask) {
      registerReleaseTask();
    }
  }

  private synchronized void registerReleaseTask() {
    if (hasReleaseTask) {
      return;
    }
    hasReleaseTask = true;
    releaseTaskManager.submit(this::tryExecuteMemoryRelease);
  }

  /**
   * Execute cache eviction until the memory status is under safe mode or no node could be evicted.
   * If the memory status is still full, which means the nodes in memory are all volatile nodes, new
   * added or updated, fire flush task.
   */
  private void tryExecuteMemoryRelease() {
    for (CachedMTreeStore store : storeList) {
      store.getLock().threadReadLock();
      try {
        store.executeMemoryRelease();
      } finally {
        store.getLock().threadReadUnlock();
      }
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

  private synchronized void registerFlushTask() {
    if (hasFlushTask) {
      return;
    }
    hasFlushTask = true;
    flushTaskManager.submit(this::tryFlushVolatileNodes);
  }

  /** Sync all volatile nodes to schemaFile and execute memory release after flush. */
  private void tryFlushVolatileNodes() {
    for (CachedMTreeStore store : storeList) {
      store.getLock().writeLock();
      try {
        store.flushVolatileNodes();
        store.executeMemoryRelease();
      } finally {
        store.getLock().unlockWrite();
      }
      if (!memManager.isExceedReleaseThreshold() || memManager.isEmpty()) {
        break;
      }
    }
    hasFlushTask = false;
    flushCount++;
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
