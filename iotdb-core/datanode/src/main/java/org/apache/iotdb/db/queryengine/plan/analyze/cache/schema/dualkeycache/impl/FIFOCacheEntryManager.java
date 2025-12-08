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

import java.util.concurrent.atomic.AtomicInteger;

public class FIFOCacheEntryManager<SK, V> implements ICacheEntryManager<SK, V> {

  private static final int SLOT_NUM = 128;

  private final FIFOLinkedList[] fifoLinkedLists = new FIFOLinkedList[SLOT_NUM];

  private final AtomicInteger cachePutRoundRobinIndex = new AtomicInteger(0);

  private final AtomicInteger cacheEvictRoundRobinIndex = new AtomicInteger(0);

  @Override
  public void access(final CacheEntry<SK, V> cacheEntry) {
    // do nothing
  }

  @Override
  public void put(final CacheEntry<SK, V> cacheEntry) {
    getNextList(cachePutRoundRobinIndex).add(cacheEntry);
  }

  @Override
  public boolean invalidate(final CacheEntry<SK, V> cacheEntry) {
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
  public CacheEntry<SK, V> evict() {
    int startIndex = getNextIndex(cacheEvictRoundRobinIndex);
    FIFOLinkedList fifoLinkedList;
    CacheEntry<SK, V> cacheEntry;
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

  private static class FIFOLinkedList<SK, V> {

    // head.next is the newest
    private final CacheEntry<SK, V> head;
    private final CacheEntry<SK, V> tail;

    public FIFOLinkedList() {
      head = new CacheEntry<>(null, null, null);
      tail = new CacheEntry<>(null, null, null);
      head.next = tail;
      tail.pre = head;
    }

    synchronized void add(final CacheEntry<SK, V> cacheEntry) {
      CacheEntry<SK, V> nextEntry;

      do {
        nextEntry = head.next;
      } while (nextEntry.isInvalidated.get());

      cacheEntry.next = nextEntry;
      cacheEntry.pre = head;
      nextEntry.pre = cacheEntry;
      head.next = cacheEntry;
    }

    synchronized CacheEntry<SK, V> evict() {
      CacheEntry<SK, V> cacheEntry;

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
