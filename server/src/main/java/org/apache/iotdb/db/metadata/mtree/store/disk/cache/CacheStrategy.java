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
import org.apache.iotdb.db.metadata.mtree.store.disk.ICachedMNodeContainer;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.iotdb.db.metadata.mtree.store.disk.ICachedMNodeContainer.getBelongedContainer;
import static org.apache.iotdb.db.metadata.mtree.store.disk.ICachedMNodeContainer.getCachedMNodeContainer;

public class CacheStrategy implements ICacheStrategy {

  Map<CacheEntry, IMNode> nodeCache = new ConcurrentHashMap<>();

  Map<CacheEntry, IMNode> nodeBuffer = new ConcurrentHashMap<>();

  @Override
  public boolean isCached(IMNode node) {
    return node.getCacheEntry() != null;
  }

  @Override
  public void cacheMNode(IMNode node) {
    node.setCacheEntry(new CacheEntry());
  }

  @Override
  public void updateCacheStatusAfterRead(IMNode node) {
    CacheEntry cacheEntry = node.getCacheEntry();
    getBelongedContainer(node).addChildToCache(node);
    if (cacheEntry.isPinned()) {
      return;
    }
    if (cacheEntry.isVolatile()) {
      nodeBuffer.putIfAbsent(cacheEntry, node);
    } else {
      nodeCache.putIfAbsent(cacheEntry, node);
    }
  }

  @Override
  public void updateCacheStatusAfterAppend(IMNode node) {
    CacheEntry cacheEntry = node.getCacheEntry();
    cacheEntry.setVolatile(true);
    getBelongedContainer(node).appendMNode(node);
  }

  @Override
  public void updateCacheStatusAfterUpdate(IMNode node) {
    CacheEntry cacheEntry = node.getCacheEntry();
    if (!cacheEntry.isVolatile()) {
      cacheEntry.setVolatile(true);
      getBelongedContainer(node).updateMNode(node.getName());
    }
  }

  @Override
  public void updateCacheStatusAfterPersist(IMNode node) {
    ICachedMNodeContainer container = getCachedMNodeContainer(node);
    Map<String, IMNode> persistedChildren = container.getNewChildBuffer();
    for (IMNode child : persistedChildren.values()) {
      updateCacheStatusAfterPersist(child, container);
    }

    persistedChildren = container.getUpdatedChildBuffer();
    for (IMNode child : persistedChildren.values()) {
      updateCacheStatusAfterPersist(child, container);
    }
  }

  private void updateCacheStatusAfterPersist(IMNode node, ICachedMNodeContainer container) {
    CacheEntry cacheEntry = node.getCacheEntry();
    cacheEntry.setVolatile(false);
    container.moveMNodeToCache(node.getName());
    if (!cacheEntry.isPinned()) {
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
      if (cacheEntry == null) {
        continue;
      }
      if (cacheEntry.isVolatile()) {
        if (!needPersist) {
          nodesToPersist.add(node);
          needPersist = true;
        }
      }
      collectVolatileNodes(child, nodesToPersist);
    }
  }

  @Override
  public List<IMNode> remove(IMNode node) {
    if (isPinned(node)) {
      unPinMNode(node.getParent());
    }
    List<IMNode> removedMNodes = new LinkedList<>();
    removeRecursively(node, removedMNodes);
    return removedMNodes;
  }

  private void removeOne(CacheEntry cacheEntry) {
    if (cacheEntry.isVolatile()) {
      nodeBuffer.remove(cacheEntry);
    } else {
      nodeCache.remove(cacheEntry);
    }
  }

  private void removeRecursively(IMNode node, List<IMNode> removedMNodes) {
    CacheEntry cacheEntry = node.getCacheEntry();
    if (cacheEntry == null) {
      return;
    }
    removeOne(cacheEntry);
    removedMNodes.add(node);
    for (IMNode child : node.getChildren().values()) {
      removeRecursively(child, removedMNodes);
    }
  }

  @Override
  public List<IMNode> evict() {
    IMNode node = null;
    List<IMNode> evictedMNodes = new ArrayList<>();
    if (!nodeCache.isEmpty()) {
      for (CacheEntry cacheEntry : nodeCache.keySet()) {
        if (!cacheEntry.isPinned()) {
          node = nodeCache.get(cacheEntry);
          break;
        }
      }
    } else if (!nodeBuffer.isEmpty()) {
      for (CacheEntry cacheEntry : nodeBuffer.keySet()) {
        if (!cacheEntry.isPinned()) {
          node = nodeBuffer.get(cacheEntry);
          break;
        }
      }
    }
    if (node != null) {
      getBelongedContainer(node).evictMNode(node.getName());
      node.getParent().deleteChild(node.getName());
      removeOne(node.getCacheEntry());
      evictedMNodes.add(node);
      collectEvictedMNodes(node, evictedMNodes);
    }
    return evictedMNodes;
  }

  private void collectEvictedMNodes(IMNode node, List<IMNode> evictedMNodes) {
    for (IMNode child : node.getChildren().values()) {
      removeOne(child.getCacheEntry());
      evictedMNodes.add(child);
      collectVolatileNodes(child, evictedMNodes);
    }
  }

  @Override
  public void pinMNode(IMNode node) {
    CacheEntry cacheEntry = node.getCacheEntry();
    if (!cacheEntry.isPinned()) {
      if (cacheEntry.isVolatile()) {
        nodeBuffer.remove(cacheEntry);
      } else {
        nodeCache.remove(cacheEntry);
      }

      IMNode parent = node.getParent();
      if (parent != null) {
        parent.getCacheEntry().pin();
      }
    }
    cacheEntry.pin();
  }

  @Override
  public List<IMNode> unPinMNode(IMNode node) {
    List<IMNode> releasedMNodes = new ArrayList<>();
    CacheEntry cacheEntry = node.getCacheEntry();
    cacheEntry.unPin();
    updateStatusAfterUnPinned(node);
    if (!cacheEntry.isPinned()) {
      releasedMNodes.add(node);
      IMNode parent = node.getParent();
      while (parent != null) {
        node = parent;
        parent = node.getParent();
        cacheEntry = node.getCacheEntry();
        cacheEntry.unPin();
        updateStatusAfterUnPinned(node);
        if (cacheEntry.isPinned()) {
          break;
        }
        releasedMNodes.add(node);
      }
    }
    return releasedMNodes;
  }

  private void updateStatusAfterUnPinned(IMNode node) {
    CacheEntry cacheEntry = node.getCacheEntry();
    if (cacheEntry.isPinned()) {
      return;
    }
    if (cacheEntry.isVolatile()) {
      nodeBuffer.put(cacheEntry, node);
    } else {
      nodeCache.put(cacheEntry, node);
    }
  }

  @Override
  public boolean isPinned(IMNode node) {
    CacheEntry cacheEntry = node.getCacheEntry();
    return cacheEntry != null && cacheEntry.isPinned();
  }

  @Override
  public void clear() {
    nodeCache.clear();
    nodeBuffer.clear();
  }
}
