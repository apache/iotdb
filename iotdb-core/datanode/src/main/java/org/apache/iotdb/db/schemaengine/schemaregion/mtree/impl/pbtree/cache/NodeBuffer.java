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
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.ICachedMNode;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

public class NodeBuffer implements INodeBuffer {

  private static final int MAP_NUM = 17;

  private IDatabaseMNode<ICachedMNode> updatedStorageGroupMNode;
  private final Map<CacheEntry, ICachedMNode>[] maps = new Map[MAP_NUM];

  private final Map<Integer, NodeBufferIterator> currentIteratorMap = new ConcurrentHashMap<>();

  NodeBuffer() {
    for (int i = 0; i < MAP_NUM; i++) {
      maps[i] = new ConcurrentHashMap<>();
    }
  }

  @Override
  public IDatabaseMNode<ICachedMNode> getUpdatedStorageGroupMNode() {
    return updatedStorageGroupMNode;
  }

  @Override
  public void setUpdatedStorageGroupMNode(IDatabaseMNode<ICachedMNode> updatedStorageGroupMNode) {
    this.updatedStorageGroupMNode = updatedStorageGroupMNode;
  }

  @Override
  public void addNewNodeToBuffer(ICachedMNode node) {
    addNodeToBuffer(node);
  }

  @Override
  public void addUpdatedNodeToBuffer(ICachedMNode node) {
    /*
     * The node may already exist in nodeBuffer before change it to volatile status, remove it and ensure
     * the volatile subtree it belongs to is in nodeBuffer
     */
    addNodeToBuffer(node);
    remove(node.getCacheEntry());
  }

  /**
   * look for the first none volatile ancestor of this node and add it to nodeBuffer. Through this
   * the volatile subtree the given node belong to will be record in nodeBuffer.
   */
  private void addNodeToBuffer(ICachedMNode node) {
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

  @Override
  public void put(CacheEntry cacheEntry, ICachedMNode node) {
    maps[getLoc(cacheEntry)].put(cacheEntry, node);
    if (!currentIteratorMap.isEmpty()) {
      for (NodeBufferIterator nodeBufferIterator : currentIteratorMap.values()) {
        nodeBufferIterator.checkHasNew(getLoc(cacheEntry));
      }
    }
  }

  @Override
  public void remove(CacheEntry cacheEntry) {
    maps[getLoc(cacheEntry)].remove(cacheEntry);
  }

  @Override
  public long getBufferNodeNum() {
    long res = updatedStorageGroupMNode == null ? 0 : 1;
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
