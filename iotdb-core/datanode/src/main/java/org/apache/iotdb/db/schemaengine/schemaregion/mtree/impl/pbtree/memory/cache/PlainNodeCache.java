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
package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.memory.cache;

import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.ICachedMNode;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PlainNodeCache implements INodeCache {

  // The nodes in nodeCache are all evictable if not pinned and may be selected to be evicted during
  // cache
  // eviction.
  @SuppressWarnings("java:S3077")
  private volatile Map<CacheEntry, ICachedMNode> nodeCache = new ConcurrentHashMap<>();

  public PlainNodeCache() {}

  @Override
  public void updateCacheStatusAfterAccess(CacheEntry cacheEntry) {}

  @Override
  public void addToNodeCache(CacheEntry cacheEntry, ICachedMNode node) {
    nodeCache.put(cacheEntry, node);
  }

  @Override
  public void removeFromNodeCache(CacheEntry cacheEntry) {
    nodeCache.remove(cacheEntry);
  }

  @Override
  public ICachedMNode getPotentialNodeTobeEvicted() {
    for (CacheEntry cacheEntry : nodeCache.keySet()) {
      if (!cacheEntry.isPinned()) {
        return nodeCache.get(cacheEntry);
      }
    }
    return null;
  }

  @Override
  public void clear() {
    nodeCache.clear();
  }

  @Override
  public long getCacheNodeNum() {
    return nodeCache.size();
  }

  @Override
  public void initCacheEntryForNode(ICachedMNode node) {
    node.setCacheEntry(new CacheEntry());
  }
}
