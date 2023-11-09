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
package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.cache;

import org.apache.iotdb.commons.schema.node.role.IDatabaseMNode;
import org.apache.iotdb.db.exception.metadata.cache.MNodeNotCachedException;
import org.apache.iotdb.db.exception.metadata.cache.MNodeNotPinnedException;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.memcontrol.MemManager;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.ICachedMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.container.ICachedMNodeContainer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.container.ICachedMNodeContainer.getBelongedContainer;
import static org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.container.ICachedMNodeContainer.getCachedMNodeContainer;

/**
 * This class implemented the cache management, involving the cache status management on per MNode
 * and cache eviction. All the nodes in memory are still basically organized as a trie via the
 * container and parent reference in each node. Some extra data structure is used to help manage the
 * node cached in memory.
 *
 * <p>The cache eviction on node is actually evicting a subtree from the MTree.
 *
 * <p>All the cached nodes are divided into two parts by their cache status, evictable nodes and
 * none evictable nodes.
 *
 * <ol>
 *   <li>Evictable nodes are all placed in nodeCache, which prepares the nodes be selected by cache
 *       eviction.
 *   <li>None evictable nodes takes two parts:
 *       <ol>
 *         <li>The volatile nodes, new added or updated, which means the data has not been synced to
 *             disk.
 *         <li>The ancestors of the volatile nodes. If a node is not volatile but not in nodeCache,
 *             it means this node is an ancestor of some volatile node. Any volatile node is
 *             contained in the CachedContainer of its parent. The parent will be placed in
 *             nodeBuffer. If the parent is volatile as well, then the parent of the parent will be
 *             placed in nodeBuffer instead, which means the root node of a maximum volatile subtree
 *             will be placed in node buffer.
 *       </ol>
 * </ol>
 */
public abstract class CacheManager implements ICacheManager {

  private final MemManager memManager;

  // The nodeBuffer helps to quickly locate the volatile subtree
  private final NodeBuffer nodeBuffer = new NodeBuffer();

  protected CacheManager(MemManager memManager) {
    this.memManager = memManager;
  }

  public void initRootStatus(ICachedMNode root) {
    pinMNodeWithMemStatusUpdate(root);
  }

  /**
   * Some cache status of the given node may be updated based on concrete cache strategy after being
   * read in memory.
   *
   * @param node
   */
  @Override
  public void updateCacheStatusAfterMemoryRead(ICachedMNode node) throws MNodeNotCachedException {
    CacheEntry cacheEntry = getCacheEntry(node);
    if (cacheEntry == null) {
      throw new MNodeNotCachedException();
    }

    // the operation that changes the node's cache status should be synchronized
    synchronized (cacheEntry) {
      if (getCacheEntry(node) == null) {
        throw new MNodeNotCachedException();
      }
      pinMNodeWithMemStatusUpdate(node);
    }
    updateCacheStatusAfterAccess(cacheEntry);
  }

  /**
   * The node read from disk should be cached and added to nodeCache and the cache of its belonging
   * container.
   *
   * @param node
   */
  @Override
  public void updateCacheStatusAfterDiskRead(ICachedMNode node) {
    pinMNodeWithMemStatusUpdate(node);
    CacheEntry cacheEntry = getCacheEntry(node);
    getBelongedContainer(node).addChildToCache(node);
    addToNodeCache(cacheEntry, node);
  }

  /**
   * The new appended node should be cached and the volatile subtree it belonged should be added to
   * nodeBuffer.
   *
   * @param node
   */
  @Override
  public void updateCacheStatusAfterAppend(ICachedMNode node) {
    pinMNodeWithMemStatusUpdate(node);
    CacheEntry cacheEntry = getCacheEntry(node);
    cacheEntry.setVolatile(true);
    getBelongedContainer(node).appendMNode(node);
    addToBufferAfterAppend(node);
  }

  /**
   * look for the first none volatile ancestor of this node and add it to nodeBuffer. Through this
   * the volatile subtree the given node belong to will be record in nodeBuffer.
   *
   * @param node
   */
  private void addToBufferAfterAppend(ICachedMNode node) {
    removeAncestorsFromCache(node);
    ICachedMNode parent = node.getParent();
    CacheEntry cacheEntry = getCacheEntry(parent);
    if (!cacheEntry.isVolatile()) {
      // the cacheEntry may be set to volatile concurrently, the unVolatile node should not be added
      // to nodeBuffer, which prevent the duplicated collecting on subTree
      synchronized (cacheEntry) {
        if (!cacheEntry.isVolatile()) {
          nodeBuffer.put(cacheEntry, parent);
        }
      }
    }
  }

  /**
   * The ancestors of volatile node should not stay in nodeCache in which the node will be evicted.
   * When invoking this method, all the ancestors have been pinned.
   */
  private void removeAncestorsFromCache(ICachedMNode node) {
    ICachedMNode parent = node.getParent();
    ICachedMNode current = node;
    CacheEntry cacheEntry = getCacheEntry(parent);
    while (!current.isDatabase() && isInNodeCache(cacheEntry)) {
      removeFromNodeCache(cacheEntry);
      current = parent;
      parent = parent.getParent();
      cacheEntry = getCacheEntry(parent);
    }
  }

  /**
   * The updated node should be marked volatile and removed from nodeCache if necessary and the
   * volatile subtree it belonged should be added to nodeBuffer.
   *
   * @param node
   */
  @Override
  public void updateCacheStatusAfterUpdate(ICachedMNode node) {
    CacheEntry cacheEntry = getCacheEntry(node);
    if (!cacheEntry.isVolatile()) {
      if (!node.isDatabase()) {
        synchronized (cacheEntry) {
          // the status change affects the subTre collect in nodeBuffer
          cacheEntry.setVolatile(true);
        }
        // if node is StorageGroup, getBelongedContainer is null
        getBelongedContainer(node).updateMNode(node.getName());
        // MNode update operation like node replace may reset the mapping between cacheEntry and
        // node,
        // thus it should be updated
        updateCacheStatusAfterUpdate(cacheEntry, node);
        removeFromNodeCache(cacheEntry);
      }
      addToBufferAfterUpdate(node);
    }
  }

  /**
   * look for the first none volatile ancestor of this node and add it to nodeBuffer. Through this
   * the volatile subtree the given node belong to will be record in nodeBuffer.
   *
   * @param node
   */
  private void addToBufferAfterUpdate(ICachedMNode node) {
    if (node.isDatabase()) {
      nodeBuffer.setUpdatedStorageGroupMNode(node.getAsDatabaseMNode());
      return;
    }

    removeAncestorsFromCache(node);
    ICachedMNode parent = node.getParent();
    CacheEntry cacheEntry = getCacheEntry(parent);

    /*
     * The updated node may already exist in nodeBuffer, remove it and ensure
     * the volatile subtree it belongs to is in nodeBuffer
     */
    if (!cacheEntry.isVolatile()) {
      // make sure that the nodeBuffer contains all the root node of volatile subTree
      // give that root.sg.d.s, if sg and d have been persisted and s are volatile, then d
      // will be added to nodeBuffer
      synchronized (cacheEntry) {
        if (!cacheEntry.isVolatile()) {
          nodeBuffer.put(cacheEntry, parent);
        }
      }
    }

    nodeBuffer.remove(getCacheEntry(node));
  }

  private void addItselfAndAncestorToCache(ICachedMNode node) {
    // the volatile subtree of this node has been deleted, thus there's no need to flush it
    // add the node and its ancestors to cache
    // if there's flush failure, such node and ancestors will be removed from cache again by
    // #updateCacheStatusAfterFlushFailure
    ICachedMNode tmp = node;
    while (!tmp.isDatabase()
        && !isInNodeCache(getCacheEntry(tmp))
        && !getCachedMNodeContainer(tmp).hasChildrenInBuffer()
        && !getCacheEntry(tmp).isVolatile()) {
      nodeBuffer.remove(getCacheEntry(tmp));
      addToNodeCache(getCacheEntry(tmp), tmp);
      tmp = tmp.getParent();
    }
  }

  /**
   * Collect updated storage group node.
   *
   * @return null if not exist
   */
  @Override
  public IDatabaseMNode<ICachedMNode> collectUpdatedStorageGroupMNodes() {
    IDatabaseMNode<ICachedMNode> storageGroupMNode = nodeBuffer.getUpdatedStorageGroupMNode();
    nodeBuffer.setUpdatedStorageGroupMNode(null);
    return storageGroupMNode;
  }

  @Override
  public Iterator<ICachedMNode> collectVolatileSubtrees() {
    return new Iterator<ICachedMNode>() {

      private final Iterator<ICachedMNode> nodeBufferIterator = nodeBuffer.iterator();

      private ICachedMNode nextSubtree = null;

      @Override
      public boolean hasNext() {
        if (nextSubtree == null) {
          tryGetNext();
        }
        return nextSubtree != null;
      }

      @Override
      public ICachedMNode next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        ICachedMNode result = nextSubtree;
        nextSubtree = null;
        return result;
      }

      private void tryGetNext() {
        ICachedMNode node;
        while (nodeBufferIterator.hasNext()) {
          node = nodeBufferIterator.next();
          nodeBuffer.remove(getCacheEntry(node));
          if (getCachedMNodeContainer(node).hasChildrenInBuffer()) {
            nextSubtree = node;
            return;
          } else if (!node.isDatabase()) {
            addItselfAndAncestorToCache(node);
          }
        }
      }
    };
  }

  @Override
  public Iterator<ICachedMNode> updateCacheStatusAndRetrieveSubtreeAfterPersist(
      ICachedMNode subtreeRoot) {
    return new VolatileSubtreeIterator(getCachedMNodeContainer(subtreeRoot));
  }

  private class VolatileSubtreeIterator implements Iterator<ICachedMNode> {

    private final ICachedMNodeContainer container;
    private final Iterator<ICachedMNode> bufferedNodeIterator;

    private ICachedMNode nextSubtree = null;

    private VolatileSubtreeIterator(ICachedMNodeContainer container) {
      this.container = container;
      this.bufferedNodeIterator = container.getChildrenBufferIterator();
    }

    @Override
    public boolean hasNext() {
      if (nextSubtree == null) {
        tryGetNext();
      }
      return nextSubtree != null;
    }

    @Override
    public ICachedMNode next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      ICachedMNode result = nextSubtree;
      nextSubtree = null;
      return result;
    }

    private void tryGetNext() {
      ICachedMNode node;
      ICachedMNode tmp;
      CacheEntry cacheEntry;
      while (bufferedNodeIterator.hasNext()) {
        node = bufferedNodeIterator.next();

        // update cache status after persist
        // when process one node, all of its buffered child should be moved to cache
        // except those with volatile children
        cacheEntry = getCacheEntry(node);
        cacheEntry.setVolatile(false);
        container.moveMNodeToCache(node.getName());

        if (node.isMeasurement() || !getCachedMNodeContainer(node).hasChildrenInBuffer()) {
          // there's no volatile subtree under this node, thus there's no need to flush it
          // add the node and its ancestors to cache
          // if there's flush failure, such node and ancestors will be removed from cache again by
          // #updateCacheStatusAfterFlushFailure
          addToNodeCache(cacheEntry, node);
          tmp = node.getParent();
          while (!tmp.isDatabase()
              && !isInNodeCache(getCacheEntry(tmp))
              && !getCachedMNodeContainer(tmp).hasChildrenInBuffer()) {
            addToNodeCache(getCacheEntry(tmp), tmp);
            tmp = tmp.getParent();
          }
        } else {
          // nodes with volatile children should be treated as root of volatile subtree and return
          // for flush
          nextSubtree = node;
          return;
        }
      }
    }
  }

  @Override
  public void updateCacheStatusAfterFlushFailure(ICachedMNode subtreeRoot) {
    nodeBuffer.put(getCacheEntry(subtreeRoot), subtreeRoot);
    if (subtreeRoot.isDatabase()) {
      return;
    }
    removeAncestorsFromCache(subtreeRoot);
  }

  @Override
  public void remove(ICachedMNode node) {
    removeRecursively(node);
    addItselfAndAncestorToCache(node.getParent());
  }

  private void removeOne(CacheEntry cacheEntry, ICachedMNode node) {
    if (isInNodeCache(cacheEntry)) {
      removeFromNodeCache(cacheEntry);
    } else {
      nodeBuffer.remove(cacheEntry);
    }

    node.setCacheEntry(null);

    if (cacheEntry.isPinned()) {
      memManager.releasePinnedMemResource(node);
    }
    memManager.releaseMemResource(node);
  }

  private void removeRecursively(ICachedMNode node) {
    CacheEntry cacheEntry = getCacheEntry(node);
    if (cacheEntry == null) {
      return;
    }
    removeOne(cacheEntry, node);
    for (ICachedMNode child : node.getChildren().values()) {
      removeRecursively(child);
    }
  }

  /**
   * Choose an evictable node from nodeCache and evicted all the cached node in the subtree it
   * represented.
   *
   * @return whether evicted any MNode successfully
   */
  @Override
  public synchronized boolean evict() {
    ICachedMNode node = null;
    CacheEntry cacheEntry = null;
    List<ICachedMNode> evictedMNodes = new ArrayList<>();
    boolean isSuccess = false;
    while (!isSuccess) {
      node = getPotentialNodeTobeEvicted();
      if (node == null) {
        break;
      }
      cacheEntry = getCacheEntry(node);
      // the operation that may change the cache status of a node should be synchronized
      synchronized (cacheEntry) {
        if (!cacheEntry.isPinned() && isInNodeCache(cacheEntry)) {
          getBelongedContainer(node).evictMNode(node.getName());
          if (node.isMeasurement()) {
            String alias = node.getAsMeasurementMNode().getAlias();
            if (alias != null) {
              node.getParent().getAsDeviceMNode().deleteAliasChild(alias);
            }
          }
          removeFromNodeCache(getCacheEntry(node));
          node.setCacheEntry(null);
          evictedMNodes.add(node);
          isSuccess = true;
        }
      }
    }

    if (node != null) {
      collectEvictedMNodes(node, evictedMNodes);
    }

    memManager.releaseMemResource(evictedMNodes);
    return !evictedMNodes.isEmpty();
  }

  private void collectEvictedMNodes(ICachedMNode node, List<ICachedMNode> evictedMNodes) {
    for (ICachedMNode child : node.getChildren().values()) {
      removeFromNodeCache(getCacheEntry(child));
      child.setCacheEntry(null);
      evictedMNodes.add(child);
      collectEvictedMNodes(child, evictedMNodes);
    }
  }

  /**
   * Pin a node in memory, and it will not be evicted. The pin is implemented as a multi mayer lock.
   * When a thread/task pin the node, one mayer lock will be added to the node and its parent. This
   * help guarantees the cache assumption that if a node is cached/pinned, its parent should be
   * cached/pinned.
   *
   * @param node
   */
  @Override
  public void pinMNode(ICachedMNode node) throws MNodeNotPinnedException {
    CacheEntry cacheEntry = getCacheEntry(node);
    if (cacheEntry == null || !cacheEntry.isPinned()) {
      throw new MNodeNotPinnedException();
    }

    synchronized (cacheEntry) {
      cacheEntry = getCacheEntry(node);
      if (cacheEntry == null || !cacheEntry.isPinned()) {
        throw new MNodeNotPinnedException();
      }
      doPin(node);
    }
  }

  private void pinMNodeWithMemStatusUpdate(ICachedMNode node) {
    CacheEntry cacheEntry = getCacheEntry(node);
    // update memory status first
    if (cacheEntry == null) {
      memManager.requestPinnedMemResource(node);
      initCacheEntryForNode(node);
    } else if (!cacheEntry.isPinned()) {
      memManager.upgradeMemResource(node);
    }
    doPin(node);
  }

  private void doPin(ICachedMNode node) {
    CacheEntry cacheEntry = getCacheEntry(node);
    // do pin MNode in memory
    if (!cacheEntry.isPinned()) {
      ICachedMNode parent = node.getParent();
      if (!node.isDatabase()) {
        getCacheEntry(parent).pin();
      }
    }

    cacheEntry.pin();
  }

  /**
   * Unpin a node, and if the lock mayer on this node is zero, it will be evictable. Unpin a node
   * means decrease one mayer lock from the node. If the lock mayer reaches zero, the mayer lock of
   * its parent should decrease.
   *
   * @param node
   * @return
   */
  @Override
  public boolean unPinMNode(ICachedMNode node) {
    CacheEntry cacheEntry = getCacheEntry(node);
    if (cacheEntry == null) {
      return false;
    }

    return doUnPin(node);
  }

  private boolean doUnPin(ICachedMNode node) {
    CacheEntry cacheEntry = getCacheEntry(node);

    boolean isPinStatusChanged = false;
    synchronized (cacheEntry) {
      cacheEntry.unPin();
      if (!cacheEntry.isPinned()) {
        isPinStatusChanged = true;
        memManager.releasePinnedMemResource(node);
      }
    }

    if (isPinStatusChanged && !node.isDatabase()) {
      doUnPin(node.getParent());
    }

    return isPinStatusChanged;
  }

  @Override
  public void clear(ICachedMNode root) {
    clearMNodeInMemory(root);
    clearNodeCache();
    nodeBuffer.setUpdatedStorageGroupMNode(null);
    nodeBuffer.clear();
  }

  private void clearMNodeInMemory(ICachedMNode node) {
    CacheEntry cacheEntry = getCacheEntry(node);
    if (cacheEntry == null) {
      return;
    }

    if (cacheEntry.isPinned()) {
      memManager.releasePinnedMemResource(node);
    }
    memManager.releaseMemResource(node);

    Iterator<ICachedMNode> iterator = getCachedMNodeContainer(node).getChildrenIterator();
    while (iterator.hasNext()) {
      clearMNodeInMemory(iterator.next());
    }
  }

  protected CacheEntry getCacheEntry(ICachedMNode node) {
    return node.getCacheEntry();
  }

  @Override
  public long getBufferNodeNum() {
    return nodeBuffer.getBufferNodeNum();
  }

  protected void initCacheEntryForNode(ICachedMNode node) {
    node.setCacheEntry(new CacheEntry());
  }

  protected abstract void updateCacheStatusAfterAccess(CacheEntry cacheEntry);

  // MNode update operation like node replace may reset the mapping between cacheEntry and node,
  // thus it should be updated
  protected abstract void updateCacheStatusAfterUpdate(CacheEntry cacheEntry, ICachedMNode node);

  protected abstract boolean isInNodeCache(CacheEntry cacheEntry);

  protected abstract void addToNodeCache(CacheEntry cacheEntry, ICachedMNode node);

  protected abstract void removeFromNodeCache(CacheEntry cacheEntry);

  protected abstract ICachedMNode getPotentialNodeTobeEvicted();

  protected abstract void clearNodeCache();

  private static class NodeBuffer {

    private static final int MAP_NUM = 17;

    private IDatabaseMNode<ICachedMNode> updatedStorageGroupMNode;
    private final Map<CacheEntry, ICachedMNode>[] maps = new Map[MAP_NUM];

    private final Map<Integer, NodeBufferIterator> currentIteratorMap = new ConcurrentHashMap<>();

    NodeBuffer() {
      for (int i = 0; i < MAP_NUM; i++) {
        maps[i] = new ConcurrentHashMap<>();
      }
    }

    public IDatabaseMNode<ICachedMNode> getUpdatedStorageGroupMNode() {
      return updatedStorageGroupMNode;
    }

    public void setUpdatedStorageGroupMNode(IDatabaseMNode<ICachedMNode> updatedStorageGroupMNode) {
      this.updatedStorageGroupMNode = updatedStorageGroupMNode;
    }

    void put(CacheEntry cacheEntry, ICachedMNode node) {
      maps[getLoc(cacheEntry)].put(cacheEntry, node);
      if (!currentIteratorMap.isEmpty()) {
        for (NodeBufferIterator nodeBufferIterator : currentIteratorMap.values()) {
          nodeBufferIterator.checkHasNew(getLoc(cacheEntry));
        }
      }
    }

    void remove(CacheEntry cacheEntry) {
      maps[getLoc(cacheEntry)].remove(cacheEntry);
    }

    long getBufferNodeNum() {
      long res = updatedStorageGroupMNode == null ? 0 : 1;
      for (int i = 0; i < MAP_NUM; i++) {
        res += maps[i].size();
      }
      return res;
    }

    void clear() {
      for (Map<CacheEntry, ICachedMNode> map : maps) {
        map.clear();
      }
    }

    private int getLoc(CacheEntry cacheEntry) {
      int hash = cacheEntry.hashCode() % MAP_NUM;
      return hash < 0 ? hash + MAP_NUM : hash;
    }

    Iterator<ICachedMNode> iterator() {
      NodeBufferIterator iterator = new NodeBufferIterator();
      currentIteratorMap.put(iterator.hashCode, iterator);
      return iterator;
    }

    private class NodeBufferIterator implements Iterator<ICachedMNode> {
      volatile int mapIndex = 0;
      Iterator<ICachedMNode> currentIterator = maps[0].values().iterator();

      ICachedMNode nextNode = null;

      volatile boolean hasNew = false;

      private final int hashCode = super.hashCode();

      @Override
      public boolean hasNext() {
        if (nextNode == null) {
          tryGetNext();
          if (nextNode == null && hasNew) {
            synchronized (this) {
              hasNew = false;
              mapIndex = 0;
            }
            currentIterator = maps[0].values().iterator();
            tryGetNext();
          }
        }
        if (nextNode == null) {
          currentIteratorMap.remove(hashCode);
          return false;
        } else {
          return true;
        }
      }

      @Override
      public ICachedMNode next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        ICachedMNode result = nextNode;
        nextNode = null;
        return result;
      }

      private void tryGetNext() {
        if (mapIndex >= maps.length) {
          return;
        }

        while (!currentIterator.hasNext()) {
          currentIterator = null;

          synchronized (this) {
            mapIndex++;
          }

          if (mapIndex == maps.length) {
            return;
          }
          currentIterator = maps[mapIndex].values().iterator();
        }

        nextNode = currentIterator.next();
      }

      private void checkHasNew(int index) {
        if (mapIndex >= index) {
          synchronized (this) {
            if (mapIndex >= index) {
              hasNew = true;
            }
          }
        }
      }
    }
  }
}
