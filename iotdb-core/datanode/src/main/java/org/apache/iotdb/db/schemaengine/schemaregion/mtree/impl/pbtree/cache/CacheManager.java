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
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.lock.LockManager;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.memcontrol.MemManager;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.ICachedMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.container.ICachedMNodeContainer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

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

  private final LockManager lockManager;

  private final MemManager memManager;

  // The nodeBuffer helps to quickly locate the volatile subtree
  private final INodeBuffer nodeBuffer = new NodeBuffer();

  protected CacheManager(MemManager memManager, LockManager lockManager) {
    this.memManager = memManager;
    this.lockManager = lockManager;
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
    removeAncestorsFromCache(node);
    nodeBuffer.addNewNodeToBuffer(node);
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
    if (cacheEntry.isVolatile()) {
      return;
    }

    if (node.isDatabase()) {
      nodeBuffer.setUpdatedStorageGroupMNode(node.getAsDatabaseMNode());
      return;
    }

    getBelongedContainer(node).updateMNode(node.getName());
    // update operation like node replace may reset the mapping between cacheEntry and node,
    // thus it should be updated
    updateCacheStatusAfterUpdate(cacheEntry, node);
    synchronized (cacheEntry) {
      // the status change affects the subTre collect in nodeBuffer
      cacheEntry.setVolatile(true);
      if (!cacheEntry.hasVolatileDescendant()) {
        removeFromNodeCache(cacheEntry);
        removeAncestorsFromCache(node);
      }
    }

    nodeBuffer.addUpdatedNodeToBuffer(node);
  }

  /**
   * This method should be invoked after a node in memory be updated to volatile status, which could
   * be caused by addChild or updateNode in MTree. The ancestors of volatile node should not stay in
   * nodeCache in which the node will be evicted. When invoking this method, all the ancestors have
   * been pinned.
   */
  private void removeAncestorsFromCache(ICachedMNode node) {
    ICachedMNode current = node.getParent();
    CacheEntry cacheEntry = getCacheEntry(current);
    boolean isStatusChange;

    while (!current.isDatabase()) {
      isStatusChange = false;
      synchronized (cacheEntry) {
        if (!cacheEntry.hasVolatileDescendant()) {
          cacheEntry.incVolatileDescendant();
          isStatusChange = true;
          removeFromNodeCache(cacheEntry);
        } else {
          cacheEntry.incVolatileDescendant();
        }
      }

      if (!isStatusChange || cacheEntry.isVolatile()) {
        return;
      }

      current = current.getParent();
      cacheEntry = getCacheEntry(current);
    }
  }

  /**
   * This method should be invoked after a node in memory be updated to non-volatile status, which
   * could be caused by flushChildren or deleteChild in MTree.
   */
  private void addAncestorsToCache(ICachedMNode node) {
    ICachedMNode current = node.getParent();
    CacheEntry cacheEntry = getCacheEntry(current);

    while (!current.isDatabase()) {
      synchronized (cacheEntry) {
        cacheEntry.decVolatileDescendant();
        if (cacheEntry.hasVolatileDescendant() || cacheEntry.isVolatile()) {
          return;
        }

        addToNodeCache(getCacheEntry(current), current);
      }

      current = current.getParent();
      cacheEntry = getCacheEntry(current);
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
        if (nodeBufferIterator.hasNext()) {
          node = nodeBufferIterator.next();

          // if there's flush failure, such node and ancestors will be removed from cache again by
          // #updateCacheStatusAfterFlushFailure
          nodeBuffer.remove(getCacheEntry(node));

          nextSubtree = node;
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
      CacheEntry cacheEntry;
      while (bufferedNodeIterator.hasNext()) {
        node = bufferedNodeIterator.next();

        // update cache status after persist
        // when process one node, all of its buffered child should be moved to cache
        // except those with volatile children
        cacheEntry = getCacheEntry(node);

        synchronized (cacheEntry) {
          cacheEntry.setVolatile(false);
          container.moveMNodeToCache(node.getName());

          if (node.isMeasurement() || !getCachedMNodeContainer(node).hasChildrenInBuffer()) {
            // there's no direct volatile subtree under this node, thus there's no need to flush it
            // add the node and its ancestors to cache
            if (!cacheEntry.hasVolatileDescendant()) {
              addToNodeCache(cacheEntry, node);
              addAncestorsToCache(node);
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
  }

  @Override
  public void updateCacheStatusAfterFlushFailure(ICachedMNode subtreeRoot) {
    nodeBuffer.put(getCacheEntry(subtreeRoot), subtreeRoot);
  }

  /**
   * When this method is invoked, it should be guaranteed that the target node has no descendent.
   */
  @Override
  public void remove(ICachedMNode node) {
    CacheEntry cacheEntry = node.getCacheEntry();
    synchronized (cacheEntry) {
      if (cacheEntry.hasVolatileDescendant()) {
        throw new IllegalStateException(
            String.format(
                "There should not exist descendant under this node %s", node.getFullPath()));
      }
      if (cacheEntry.isVolatile()) {
        nodeBuffer.remove(getCacheEntry(node.getParent()));
        addAncestorsToCache(node);
      } else {
        removeFromNodeCache(cacheEntry);
      }

      node.setCacheEntry(null);
    }

    if (cacheEntry.isPinned()) {
      memManager.releasePinnedMemResource(node);
    }
    memManager.releaseMemResource(node);
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
      lockManager.threadReadLock(node.getParent(), true);
      try {
        cacheEntry = getCacheEntry(node);
        // the operation that may change the cache status of a node should be synchronized
        synchronized (cacheEntry) {
          if (cacheEntry.isPinned()
              || cacheEntry.isVolatile()
              || cacheEntry.hasVolatileDescendant()) {
            // since the node could be moved from cache to buffer after being taken from cache
            // this check here is necessary to ensure that the node could truly be evicted
            continue;
          }

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
      } finally {
        lockManager.threadReadUnlock(node.getParent());
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

  protected abstract void addToNodeCache(CacheEntry cacheEntry, ICachedMNode node);

  protected abstract void removeFromNodeCache(CacheEntry cacheEntry);

  protected abstract ICachedMNode getPotentialNodeTobeEvicted();

  protected abstract void clearNodeCache();
}
