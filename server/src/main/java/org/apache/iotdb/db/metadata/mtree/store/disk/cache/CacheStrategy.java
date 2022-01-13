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
import org.apache.iotdb.db.metadata.mtree.store.disk.CachedMNodeContainer;
import org.apache.iotdb.db.metadata.mtree.store.disk.ICachedMNodeContainer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CacheStrategy implements ICacheStrategy {

  Map<CacheEntry, IMNode> nodeCache = new ConcurrentHashMap<>();

  Map<CacheEntry, IMNode> nodeBuffer = new ConcurrentHashMap<>();

  @Override
  public void updateCacheStatusAfterRead(IMNode node) {
    if (node.getCacheEntry() != null) {
      return;
    }

    CacheEntry cacheEntry = new CacheEntry();
    node.setCacheEntry(new CacheEntry());
    nodeCache.put(cacheEntry, node);
  }

  @Override
  public void updateCacheStatusAfterAppend(IMNode node) {
    CacheEntry cacheEntry = new CacheEntry();
    node.setCacheEntry(new CacheEntry());
    nodeBuffer.put(cacheEntry, node);
  }

  @Override
  public void updateCacheStatusAfterUpdate(IMNode node) {
    CacheEntry cacheEntry = new CacheEntry();
    node.setCacheEntry(new CacheEntry());
    nodeBuffer.put(cacheEntry, node);
    getBelongedContainer(node).updateMNode(node.getName());
  }

  @Override
  public void updateCacheStatusAfterPersist(IMNode node) {
    ICachedMNodeContainer container = (ICachedMNodeContainer) node.getChildren();
    CacheEntry cacheEntry;
    for (IMNode child : container.values()) {
      cacheEntry = child.getCacheEntry();
      cacheEntry.setVolatile(false);
      nodeBuffer.remove(cacheEntry);
      nodeCache.put(cacheEntry, node);
    }
  }

  @Override
  public List<IMNode> collectVolatileMNodes(IMNode node) {
    List<IMNode> nodesToPersist = new ArrayList<>();
    collectVolatileNodes(node, nodesToPersist);
    return nodesToPersist;
  }

  private void collectVolatileNodes(IMNode node, List<IMNode> nodesToPersist) {
    CacheEntry cacheEntry;
    boolean needPersist = false;
    for (IMNode child : node.getChildren().values()) {
      cacheEntry = child.getCacheEntry();
      if (cacheEntry != null && cacheEntry.isVolatile()) {
        if (!needPersist) {
          nodesToPersist.add(child);
          needPersist = true;
        }
        collectVolatileNodes(child, nodesToPersist);
      }
    }
  }

  @Override
  public void remove(IMNode node) {
    CacheEntry cacheEntry = node.getCacheEntry();
    if (cacheEntry.isVolatile()) {
      nodeBuffer.remove(cacheEntry);
    } else {
      nodeCache.remove(cacheEntry);
    }
  }

  @Override
  public List<IMNode> evict() {
    IMNode node = null;
    List<IMNode> evictedMNodes = new ArrayList<>();
    if (!nodeCache.isEmpty()) {
      node = nodeCache.values().stream().findFirst().get();
    } else if (!nodeBuffer.isEmpty()) {
      node = nodeBuffer.values().stream().findFirst().get();
      evictedMNodes.add(node);
    }
    if (node != null) {
      remove(node);
      collectEvictedMNodes(node, evictedMNodes);
    }
    return evictedMNodes;
  }

  private void collectEvictedMNodes(IMNode node, List<IMNode> evictedMNodes) {
    for (IMNode child : node.getChildren().values()) {
      remove(child);
      evictedMNodes.add(node);
      collectVolatileNodes(child, evictedMNodes);
    }
  }

  @Override
  public void pinMNode(IMNode node) {}

  @Override
  public void unPinMNode(IMNode node) {}

  @Override
  public void clear() {
    nodeCache.clear();
    nodeBuffer.clear();
  }

  private CachedMNodeContainer getBelongedContainer(IMNode node) {
    return (CachedMNodeContainer) node.getParent().getChildren();
  }
}
