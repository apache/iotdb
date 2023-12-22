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

package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.memory.buffer;

import org.apache.iotdb.commons.schema.node.role.IDatabaseMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.memory.cache.CacheEntry;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.ICachedMNode;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The ancestors of the volatile nodes. If a node is not volatile but not in nodeCache, it means
 * this node is an ancestor of some volatile node. Any volatile node is contained in the
 * CachedContainer of its parent. The parent will be placed in nodeBuffer. If the parent is volatile
 * as well, then the parent of the parent will be hold nodeBuffer instead, which means the root node
 * of a maximum volatile subtree will be placed in node buffer.
 */
public class NodeBuffer implements INodeBuffer {

  private static final int MAP_NUM = 17;

  private IDatabaseMNode<ICachedMNode> updatedDatabaseMNode;
  private final Map<CacheEntry, ICachedMNode>[] maps = new Map[MAP_NUM];

  public NodeBuffer() {
    for (int i = 0; i < MAP_NUM; i++) {
      maps[i] = new ConcurrentHashMap<>();
    }
  }

  @Override
  public IDatabaseMNode<ICachedMNode> getUpdatedDatabaseMNode() {
    return updatedDatabaseMNode;
  }

  @Override
  public void updateDatabaseNodeAfterStatusUpdate(
      IDatabaseMNode<ICachedMNode> updatedDatabaseMNode) {
    this.updatedDatabaseMNode = updatedDatabaseMNode;
  }

  @Override
  public void removeUpdatedDatabaseNode() {
    this.updatedDatabaseMNode = null;
  }

  @Override
  public void addNewNodeToBuffer(ICachedMNode node) {
    addNonVolatileAncestorToBuffer(node);
  }

  @Override
  public void addUpdatedNodeToBuffer(ICachedMNode node) {
    /*
     * The node may already exist in nodeBuffer before change it to volatile status, remove it and ensure
     * the volatile subtree it belongs to is in nodeBuffer
     */
    remove(node.getCacheEntry());
    addNonVolatileAncestorToBuffer(node);
  }

  @Override
  public void addBackToBufferAfterFlushFailure(ICachedMNode subTreeRoot) {
    put(subTreeRoot.getCacheEntry(), subTreeRoot);
  }

  /**
   * look for the first none volatile ancestor of this node and add it to nodeBuffer. Through this
   * the volatile subtree the given node belong to will be record in nodeBuffer.
   */
  private void addNonVolatileAncestorToBuffer(ICachedMNode node) {
    ICachedMNode parent = node.getParent();
    CacheEntry cacheEntry = parent.getCacheEntry();

    // make sure that the nodeBuffer contains all the root node of volatile subTree
    // give that root.sg.d.s, if sg and d have been persisted and s are volatile, then d
    // will be added to nodeBuffer
    synchronized (cacheEntry) {
      // the cacheEntry may be set to volatile concurrently, the unVolatile node should not be
      // added
      // to nodeBuffer, which prevent the duplicated collecting on subTree
      if (!cacheEntry.isVolatile()) {
        put(cacheEntry, parent);
      }
    }
  }

  private void put(CacheEntry cacheEntry, ICachedMNode node) {
    maps[getLoc(cacheEntry)].put(cacheEntry, node);
  }

  @Override
  public void remove(CacheEntry cacheEntry) {
    maps[getLoc(cacheEntry)].remove(cacheEntry);
  }

  @Override
  public long getBufferNodeNum() {
    long res = updatedDatabaseMNode == null ? 0 : 1;
    for (int i = 0; i < MAP_NUM; i++) {
      res += maps[i].size();
    }
    return res;
  }

  @Override
  public void clear() {
    for (Map<CacheEntry, ICachedMNode> map : maps) {
      map.clear();
    }
  }

  private int getLoc(CacheEntry cacheEntry) {
    int hash = cacheEntry.hashCode() % MAP_NUM;
    return hash < 0 ? hash + MAP_NUM : hash;
  }

  @Override
  public Iterator<ICachedMNode> iterator() {
    return new NodeBufferIterator();
  }

  private class NodeBufferIterator implements Iterator<ICachedMNode> {
    volatile int mapIndex = 0;
    Iterator<ICachedMNode> currentIterator = maps[0].values().iterator();

    ICachedMNode nextNode = null;

    @Override
    public boolean hasNext() {
      if (nextNode == null) {
        tryGetNext();
      }
      return nextNode != null;
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
  }
}
