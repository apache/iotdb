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

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LRUCacheManager extends CacheManager {

  private static final int NUM_OF_LIST = 17;

  private final LRUCacheList[] lruCacheLists = new LRUCacheList[NUM_OF_LIST];

  public LRUCacheManager(MemManager memManager) {
    super(memManager);
    for (int i = 0; i < NUM_OF_LIST; i++) {
      lruCacheLists[i] = new LRUCacheList();
    }
  }

  @Override
  public void updateCacheStatusAfterAccess(CacheEntry cacheEntry) {
    LRUCacheEntry lruCacheEntry = getAsLRUCacheEntry(cacheEntry);
    getTargetCacheList(lruCacheEntry).updateCacheStatusAfterAccess(lruCacheEntry);
  }

  // MNode update operation like node replace may reset the mapping between cacheEntry and node,
  // thus it should be updated
  @Override
  protected void updateCacheStatusAfterUpdate(CacheEntry cacheEntry, IMNode node) {
    getAsLRUCacheEntry(cacheEntry).setNode(node);
  }

  @Override
  protected void initCacheEntryForNode(IMNode node) {
    LRUCacheEntry cacheEntry = new LRUCacheEntry(node);
    node.setCacheEntry(cacheEntry);
  }

  @Override
  protected boolean isInNodeCache(CacheEntry cacheEntry) {
    LRUCacheEntry lruCacheEntry = getAsLRUCacheEntry(cacheEntry);
    return getTargetCacheList(lruCacheEntry).isInCacheList(lruCacheEntry);
  }

  @Override
  protected void addToNodeCache(CacheEntry cacheEntry, IMNode node) {
    LRUCacheEntry lruCacheEntry = getAsLRUCacheEntry(cacheEntry);
    getTargetCacheList(lruCacheEntry).addToCacheList(lruCacheEntry, node);
  }

  @Override
  protected void removeFromNodeCache(CacheEntry cacheEntry) {
    LRUCacheEntry lruCacheEntry = getAsLRUCacheEntry(cacheEntry);
    getTargetCacheList(lruCacheEntry).removeFromCacheList(lruCacheEntry);
  }

  @Override
  protected IMNode getPotentialNodeTobeEvicted() {
    IMNode result = null;
    for (LRUCacheList cacheList : lruCacheLists) {
      result = cacheList.getPotentialNodeTobeEvicted();
      if (result != null) {
        break;
      }
    }
    return result;
  }

  @Override
  protected void clearNodeCache() {
    for (LRUCacheList lruCacheList : lruCacheLists) {
      lruCacheList.clear();
    }
  }

  private LRUCacheEntry getAsLRUCacheEntry(CacheEntry cacheEntry) {
    return (LRUCacheEntry) cacheEntry;
  }

  private LRUCacheList getTargetCacheList(LRUCacheEntry lruCacheEntry) {
    return lruCacheLists[getCacheListLoc(lruCacheEntry)];
  }

  private int getCacheListLoc(LRUCacheEntry lruCacheEntry) {
    int hash = lruCacheEntry.hashCode() % NUM_OF_LIST;
    return hash < 0 ? hash + NUM_OF_LIST : hash;
  }

  @Override
  public long getCacheNodeNum() {
    long res = 0;
    for (LRUCacheList cacheList : lruCacheLists) {
      res += cacheList.size.get();
    }
    return res;
  }

  private static class LRUCacheEntry extends CacheEntry {

    // although the node instance may be replaced, the name and full path of the node won't be
    // changed, which means the cacheEntry always map to only one logic node
    protected volatile IMNode node;

    private volatile LRUCacheEntry pre = null;

    private volatile LRUCacheEntry next = null;

    public LRUCacheEntry(IMNode node) {
      this.node = node;
    }

    public IMNode getNode() {
      return node;
    }

    public void setNode(IMNode node) {
      this.node = node;
    }

    LRUCacheEntry getPre() {
      return pre;
    }

    void setPre(LRUCacheEntry pre) {
      this.pre = pre;
    }

    LRUCacheEntry getNext() {
      return next;
    }

    void setNext(LRUCacheEntry next) {
      this.next = next;
    }

    @Override
    public int hashCode() {
      return node.getName().hashCode();
    }
  }

  private static class LRUCacheList {

    private volatile LRUCacheEntry first;

    private volatile LRUCacheEntry last;

    private final AtomicLong size = new AtomicLong(0);

    private final Lock lock = new ReentrantLock();

    private void updateCacheStatusAfterAccess(LRUCacheEntry lruCacheEntry) {
      lock.lock();
      try {
        if (isInCacheList(lruCacheEntry)) {
          moveToFirst(lruCacheEntry);
        }
      } finally {
        lock.unlock();
      }
    }

    private void addToCacheList(LRUCacheEntry lruCacheEntry, IMNode node) {
      lock.lock();
      try {
        lruCacheEntry.setNode(node);
        moveToFirst(lruCacheEntry);
        size.getAndIncrement();
      } finally {
        lock.unlock();
      }
    }

    private void removeFromCacheList(LRUCacheEntry lruCacheEntry) {
      lock.lock();
      try {
        removeOne(lruCacheEntry);
        size.getAndDecrement();
      } finally {
        lock.unlock();
      }
    }

    private IMNode getPotentialNodeTobeEvicted() {
      lock.lock();
      try {
        LRUCacheEntry target = last;
        while (target != null && target.isPinned()) {
          target = target.getPre();
        }

        return target == null ? null : target.getNode();
      } finally {
        lock.unlock();
      }
    }

    private void clear() {
      first = null;
      last = null;
      size.getAndSet(0);
    }

    private void moveToFirst(LRUCacheEntry entry) {
      if (first == null || last == null) { // empty linked list
        first = last = entry;
        return;
      }

      if (first == entry) {
        return;
      }
      if (entry.getPre() != null) {
        entry.getPre().setNext(entry.getNext());
      }
      if (entry.getNext() != null) {
        entry.getNext().setPre(entry.getPre());
      }

      if (entry == last) {
        last = last.getPre();
      }

      entry.setNext(first);
      first.setPre(entry);
      first = entry;
      first.setPre(null);
    }

    private void removeOne(LRUCacheEntry entry) {
      if (entry.getPre() != null) {
        entry.getPre().setNext(entry.getNext());
      }
      if (entry.getNext() != null) {
        entry.getNext().setPre(entry.getPre());
      }
      if (entry == first) {
        first = entry.getNext();
      }
      if (entry == last) {
        last = entry.getPre();
      }

      entry.setPre(null);
      entry.setNext(null);
    }

    private boolean isInCacheList(LRUCacheEntry entry) {
      return entry.getPre() != null || entry.getNext() != null || first == entry || last == entry;
    }
  }
}
