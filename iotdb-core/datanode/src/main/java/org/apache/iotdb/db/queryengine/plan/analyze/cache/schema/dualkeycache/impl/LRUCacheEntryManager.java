/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.dualkeycache.impl;

import java.util.Random;

/**
 * This class implements the cache entry manager with LRU policy.
 *
 * @param <SK> The second key of cache value.
 * @param <V> The cache value.
 */
class LRUCacheEntryManager<SK, V> implements ICacheEntryManager<SK, V> {

  private static final int SLOT_NUM = 128;

  private final LRULinkedList[] lruLinkedLists = new LRULinkedList[SLOT_NUM];

  private final Random idxGenerator = new Random();

  @Override
  public void access(final CacheEntry<SK, V> cacheEntry) {
    getBelongedList(cacheEntry).moveToHead(cacheEntry);
  }

  @Override
  public void put(final CacheEntry<SK, V> cacheEntry) {
    getBelongedList(cacheEntry).add(cacheEntry);
  }

  @Override
  public CacheEntry<SK, V> evict() {
    int startIndex = idxGenerator.nextInt(SLOT_NUM);
    LRULinkedList<SK, V> lruLinkedList;
    CacheEntry<SK, V> cacheEntry;
    for (int i = 0; i < SLOT_NUM; i++) {
      if (startIndex == SLOT_NUM) {
        startIndex = 0;
      }
      lruLinkedList = lruLinkedLists[startIndex];
      if (lruLinkedList != null) {
        cacheEntry = lruLinkedList.evict();
        if (cacheEntry != null) {
          return cacheEntry;
        }
      }
      startIndex++;
    }
    return null;
  }

  @Override
  public void cleanUp() {
    synchronized (lruLinkedLists) {
      for (int i = 0; i < SLOT_NUM; i++) {
        lruLinkedLists[i] = null;
      }
    }
  }

  private LRULinkedList getBelongedList(CacheEntry<SK, V> cacheEntry) {
    int slotIndex = cacheEntry.hashCode() % SLOT_NUM;
    slotIndex = slotIndex < 0 ? slotIndex + SLOT_NUM : slotIndex;
    LRULinkedList lruLinkedList = lruLinkedLists[slotIndex];
    if (lruLinkedList == null) {
      synchronized (lruLinkedLists) {
        lruLinkedList = lruLinkedLists[slotIndex];
        if (lruLinkedList == null) {
          lruLinkedList = new LRULinkedList();
          lruLinkedLists[slotIndex] = lruLinkedList;
        }
      }
    }
    return lruLinkedList;
  }

  private static class LRULinkedList<SK, V> extends CacheLinkedList<SK, V> {
    synchronized void moveToHead(final CacheEntry<SK, V> cacheEntry) {
      if (cacheEntry.isInvalidated.get()) {
        // this cache entry has been evicted
        return;
      }
      // remove cache entry from the list
      cacheEntry.pre.next = cacheEntry.next;
      cacheEntry.next.pre = cacheEntry.pre;
      // add cache entry to the head
      cacheEntry.next = head.next;
      cacheEntry.pre = head;
      head.next.pre = cacheEntry;
      head.next = cacheEntry;
    }
  }
}
