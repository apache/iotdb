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

package org.apache.iotdb.db.metadata.cache.dualkeycache.impl;

import java.util.Objects;
import java.util.Random;

/**
 * This class implements the cache entry manager with LRU policy.
 *
 * @param <FK> The first key of cache value.
 * @param <SK> The second key of cache value.
 * @param <V> The cache value.
 */
class LRUCacheEntryManager<FK, SK, V>
    implements ICacheEntryManager<FK, SK, V, LRUCacheEntryManager.LRUCacheEntry<SK, V>> {

  private static final int SLOT_NUM = 128;

  private final LRULinkedList[] lruLinkedLists = new LRULinkedList[SLOT_NUM];

  private final Random idxGenerator = new Random();

  @Override
  public LRUCacheEntry<SK, V> createCacheEntry(
      SK secondKey, V value, ICacheEntryGroup<FK, SK, V, LRUCacheEntry<SK, V>> cacheEntryGroup) {
    return new LRUCacheEntry<>(secondKey, value, cacheEntryGroup);
  }

  @Override
  public void access(LRUCacheEntry<SK, V> cacheEntry) {
    getBelongedList(cacheEntry).moveToHead(cacheEntry);
  }

  @Override
  public void put(LRUCacheEntry<SK, V> cacheEntry) {
    getBelongedList(cacheEntry).add(cacheEntry);
  }

  @Override
  public LRUCacheEntry<SK, V> evict() {
    int startIndex = idxGenerator.nextInt(SLOT_NUM);
    LRULinkedList lruLinkedList;
    LRUCacheEntry<SK, V> cacheEntry;
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

  private LRULinkedList getBelongedList(LRUCacheEntry<SK, V> cacheEntry) {
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

  static class LRUCacheEntry<SK, V> implements ICacheEntry<SK, V> {

    private final SK secondKey;
    private final ICacheEntryGroup cacheEntryGroup;

    private V value;

    private LRUCacheEntry<SK, V> pre;
    private LRUCacheEntry<SK, V> next;

    private LRUCacheEntry(SK secondKey, V value, ICacheEntryGroup cacheEntryGroup) {
      this.secondKey = secondKey;
      this.value = value;
      this.cacheEntryGroup = cacheEntryGroup;
    }

    @Override
    public SK getSecondKey() {
      return secondKey;
    }

    @Override
    public V getValue() {
      return value;
    }

    @Override
    public ICacheEntryGroup getBelongedGroup() {
      return cacheEntryGroup;
    }

    @Override
    public void replaceValue(V newValue) {
      this.value = newValue;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      LRUCacheEntry<?, ?> that = (LRUCacheEntry<?, ?>) o;
      return Objects.equals(secondKey, that.secondKey)
          && Objects.equals(cacheEntryGroup, that.cacheEntryGroup);
    }

    @Override
    public int hashCode() {
      return cacheEntryGroup.hashCode() * 31 + secondKey.hashCode();
    }
  }

  private static class LRULinkedList {

    private LRUCacheEntry head;
    private LRUCacheEntry tail;

    synchronized void add(LRUCacheEntry cacheEntry) {
      if (head == null) {
        head = cacheEntry;
        tail = cacheEntry;
        return;
      }

      head.pre = cacheEntry;
      cacheEntry.next = head;

      head = cacheEntry;
    }

    synchronized LRUCacheEntry evict() {
      if (tail == null) {
        return null;
      }

      LRUCacheEntry cacheEntry = tail;
      tail = tail.pre;

      if (tail == null) {
        head = null;
      } else {
        tail.next = null;
      }

      cacheEntry.pre = null;

      return cacheEntry;
    }

    synchronized void moveToHead(LRUCacheEntry cacheEntry) {
      if (head == null) {
        // this cache entry has been evicted and the cache list is empty
        return;
      }

      if (cacheEntry.pre == null) {
        // this entry is head
        return;
      }

      cacheEntry.pre.next = cacheEntry.next;

      if (cacheEntry.next != null) {
        cacheEntry.next.pre = cacheEntry.pre;
      }

      cacheEntry.pre = null;

      head.pre = cacheEntry;
      cacheEntry.next = head;

      head = cacheEntry;
    }
  }
}
