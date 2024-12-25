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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class FIFOCacheEntryManager<FK, SK, V>
    implements ICacheEntryManager<FK, SK, V, FIFOCacheEntryManager.FIFOCacheEntry<SK, V>> {

  private static final int SLOT_NUM = 128;

  private final FIFOLinkedList[] fifoLinkedLists = new FIFOLinkedList[SLOT_NUM];

  private final AtomicInteger cachePutRoundRobinIndex = new AtomicInteger(0);

  private final AtomicInteger cacheEvictRoundRobinIndex = new AtomicInteger(0);

  @Override
  public FIFOCacheEntry<SK, V> createCacheEntry(
      final SK secondKey,
      final V value,
      final ICacheEntryGroup<FK, SK, V, FIFOCacheEntry<SK, V>> cacheEntryGroup) {
    return new FIFOCacheEntry<>(secondKey, value, cacheEntryGroup);
  }

  @Override
  public void access(final FIFOCacheEntry<SK, V> cacheEntry) {
    // do nothing
  }

  @Override
  public void put(final FIFOCacheEntry<SK, V> cacheEntry) {
    getNextList(cachePutRoundRobinIndex).add(cacheEntry);
  }

  @Override
  public boolean invalidate(final FIFOCacheEntry<SK, V> cacheEntry) {
    if (cacheEntry.isInvalidated.getAndSet(true)) {
      return false;
    }

    cacheEntry.next.pre = cacheEntry.pre;
    cacheEntry.pre.next = cacheEntry.next;
    cacheEntry.next = null;
    cacheEntry.pre = null;
    return true;
  }

  @Override
  public FIFOCacheEntry<SK, V> evict() {
    int startIndex = getNextIndex(cacheEvictRoundRobinIndex);
    FIFOLinkedList fifoLinkedList;
    FIFOCacheEntry<SK, V> cacheEntry;
    for (int i = 0; i < SLOT_NUM; i++) {
      if (startIndex == SLOT_NUM) {
        startIndex = 0;
      }
      fifoLinkedList = fifoLinkedLists[startIndex];
      if (fifoLinkedList != null) {
        cacheEntry = fifoLinkedList.evict();
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
    synchronized (fifoLinkedLists) {
      for (int i = 0; i < SLOT_NUM; i++) {
        fifoLinkedLists[i] = null;
      }
    }
  }

  private FIFOLinkedList getNextList(final AtomicInteger roundRobinIndex) {
    int listIndex = getNextIndex(roundRobinIndex);
    FIFOLinkedList fifoLinkedList = fifoLinkedLists[listIndex];
    if (fifoLinkedList == null) {
      synchronized (fifoLinkedLists) {
        fifoLinkedList = fifoLinkedLists[listIndex];
        if (fifoLinkedList == null) {
          fifoLinkedList = new FIFOLinkedList();
          fifoLinkedLists[listIndex] = fifoLinkedList;
        }
      }
    }
    return fifoLinkedList;
  }

  private int getNextIndex(final AtomicInteger roundRobinIndex) {
    return roundRobinIndex.getAndUpdate(
        currentValue -> {
          currentValue = currentValue + 1;
          return currentValue >= SLOT_NUM ? 0 : currentValue;
        });
  }

  static class FIFOCacheEntry<SK, V> implements ICacheEntry<SK, V> {

    private final SK secondKey;

    @SuppressWarnings("java:S3077")
    private volatile ICacheEntryGroup cacheEntryGroup;

    private V value;

    private FIFOCacheEntry<SK, V> pre = null;
    private FIFOCacheEntry<SK, V> next = null;

    private final AtomicBoolean isInvalidated = new AtomicBoolean(false);

    private FIFOCacheEntry(
        final SK secondKey, final V value, final ICacheEntryGroup cacheEntryGroup) {
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
    public void setBelongedGroup(final ICacheEntryGroup belongedGroup) {
      this.cacheEntryGroup = belongedGroup;
    }

    @Override
    public void replaceValue(final V newValue) {
      this.value = newValue;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final FIFOCacheEntry<?, ?> that = (FIFOCacheEntry<?, ?>) o;
      return Objects.equals(secondKey, that.secondKey)
          && Objects.equals(cacheEntryGroup, that.cacheEntryGroup);
    }

    @Override
    public int hashCode() {
      return cacheEntryGroup.hashCode() * 31 + secondKey.hashCode();
    }
  }

  private static class FIFOLinkedList<SK, V> {

    // head.next is the newest
    private final FIFOCacheEntry<SK, V> head;
    private final FIFOCacheEntry<SK, V> tail;

    public FIFOLinkedList() {
      head = new FIFOCacheEntry<>(null, null, null);
      tail = new FIFOCacheEntry<>(null, null, null);
      head.next = tail;
      tail.pre = head;
    }

    synchronized void add(final FIFOCacheEntry<SK, V> cacheEntry) {
      FIFOCacheEntry<SK, V> nextEntry;

      do {
        nextEntry = head.next;
      } while (nextEntry.isInvalidated.get());

      cacheEntry.next = nextEntry;
      cacheEntry.pre = head;
      nextEntry.pre = cacheEntry;
      head.next = cacheEntry;
    }

    synchronized FIFOCacheEntry<SK, V> evict() {
      FIFOCacheEntry<SK, V> cacheEntry;

      do {
        cacheEntry = tail.pre;
        if (cacheEntry == head) {
          return null;
        }

      } while (cacheEntry.isInvalidated.compareAndSet(false, true));

      cacheEntry.pre.next = cacheEntry.next;
      cacheEntry.next.pre = cacheEntry.pre;
      cacheEntry.next = null;
      cacheEntry.pre = null;
      return cacheEntry;
    }
  }
}
