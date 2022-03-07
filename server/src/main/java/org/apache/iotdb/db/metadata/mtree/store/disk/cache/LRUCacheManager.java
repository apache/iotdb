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

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LRUCacheManager extends CacheManager {

  private volatile LRUCacheEntry first;

  private volatile LRUCacheEntry last;

  private final Lock lock = new ReentrantLock();

  @Override
  public void updateCacheStatusAfterAccess(CacheEntry cacheEntry) {
    lock.lock();
    try {
      LRUCacheEntry lruCacheEntry = getAsLRUCacheEntry(cacheEntry);
      if (isInCacheList(lruCacheEntry)) {
        moveToFirst(lruCacheEntry);
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  protected CacheEntry initCacheEntryForNode(IMNode node) {
    LRUCacheEntry cacheEntry = new LRUCacheEntry();
    node.setCacheEntry(cacheEntry);
    cacheEntry.setNode(node);
    return cacheEntry;
  }

  @Override
  protected boolean isInNodeCache(CacheEntry cacheEntry) {
    return isInCacheList(getAsLRUCacheEntry(cacheEntry));
  }

  @Override
  protected void addToNodeCache(CacheEntry cacheEntry, IMNode node) {
    lock.lock();
    try {
      LRUCacheEntry lruCacheEntry = getAsLRUCacheEntry(cacheEntry);
      lruCacheEntry.setNode(node);
      moveToFirst(lruCacheEntry);
    } finally {
      lock.unlock();
    }
  }

  @Override
  protected void removeFromNodeCache(CacheEntry cacheEntry) {
    lock.lock();
    try {
      removeOne(getAsLRUCacheEntry(cacheEntry));
    } finally {
      lock.unlock();
    }
  }

  @Override
  protected IMNode getPotentialNodeTobeEvicted() {
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

  @Override
  protected void clearNodeCache() {
    first = null;
    last = null;
  }

  private LRUCacheEntry getAsLRUCacheEntry(CacheEntry cacheEntry) {
    return (LRUCacheEntry) cacheEntry;
  }

  private boolean isInCacheList(LRUCacheEntry entry) {
    return entry.getPre() != null || entry.getNext() != null || first == entry || last == entry;
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
}
