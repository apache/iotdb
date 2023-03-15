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

import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mtree.store.disk.memcontrol.MemManager;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PlainCacheManager extends CacheManager {

  // The nodes in nodeCache are all evictable if not pinned and may be selected to be evicted during
  // cache
  // eviction.
  private volatile Map<CacheEntry, IMNode> nodeCache = new ConcurrentHashMap<>();

  public PlainCacheManager(MemManager memManager) {
    super(memManager);
  }

  @Override
  protected void updateCacheStatusAfterAccess(CacheEntry cacheEntry) {}

  // MNode update operation like node replace may reset the mapping between cacheEntry and node,
  // thus it should be updated
  @Override
  protected void updateCacheStatusAfterUpdate(CacheEntry cacheEntry, IMNode node) {
    nodeCache.replace(cacheEntry, node);
  }

  @Override
  protected boolean isInNodeCache(CacheEntry cacheEntry) {
    return nodeCache.containsKey(cacheEntry);
  }

  @Override
  protected void addToNodeCache(CacheEntry cacheEntry, IMNode node) {
    nodeCache.put(cacheEntry, node);
  }

  @Override
  protected void removeFromNodeCache(CacheEntry cacheEntry) {
    nodeCache.remove(cacheEntry);
  }

  @Override
  protected IMNode getPotentialNodeTobeEvicted() {
    for (CacheEntry cacheEntry : nodeCache.keySet()) {
      if (!cacheEntry.isPinned()) {
        return nodeCache.get(cacheEntry);
      }
    }
    return null;
  }

  @Override
  protected void clearNodeCache() {
    nodeCache.clear();
  }

  @Override
  public long getCacheNodeNum() {
    return nodeCache.size();
  }
}
