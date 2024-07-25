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
  public void invalid(LRUCacheEntry<SK, V> cacheEntry) {
    cacheEntry.next.pre = cacheEntry.pre;
    cacheEntry.pre.next = cacheEntry.next;
    cacheEntry.next = null;
    cacheEntry.pre = null;
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

    @SuppressWarnings("java:S3077")
    private volatile ICacheEntryGroup cacheEntryGroup;

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
    public void setBelongedGroup(ICacheEntryGroup belongedGroup) {
      this.cacheEntryGroup = belongedGroup;
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

    // head.next is the most recently used entry
    private final LRUCacheEntry head;
    private final LRUCacheEntry tail;

    public LRULinkedList() {
      head = new LRUCacheEntry(null, null, null);
      tail = new LRUCacheEntry(null, null, null);
      head.next = tail;
      tail.pre = head;
    }

    synchronized void add(LRUCacheEntry cacheEntry) {
      cacheEntry.next = head.next;
      cacheEntry.pre = head;
      head.next.pre = cacheEntry;
      head.next = cacheEntry;
    }

    synchronized LRUCacheEntry evict() {
      if (tail.pre == head) {
        return null;
      }
      LRUCacheEntry cacheEntry = tail.pre;
      cacheEntry.pre.next = cacheEntry.next;
      cacheEntry.next.pre = cacheEntry.pre;
      cacheEntry.next = null;
      cacheEntry.pre = null;
      return cacheEntry;
    }

    synchronized void moveToHead(LRUCacheEntry cacheEntry) {
      if (cacheEntry.next == null || cacheEntry.pre == null) {
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
