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

  private final CacheLinkedList[] CacheLinkedLists = new CacheLinkedList[SLOT_NUM];

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
  public CacheEntry<SK, V> evict() {
    int startIndex = getNextIndex(cacheEvictRoundRobinIndex);
    CacheLinkedList CacheLinkedList;
    CacheEntry<SK, V> cacheEntry;
    for (int i = 0; i < SLOT_NUM; i++) {
      if (startIndex == SLOT_NUM) {
        startIndex = 0;
      }
      CacheLinkedList = CacheLinkedLists[startIndex];
      if (CacheLinkedList != null) {
        cacheEntry = CacheLinkedList.evict();
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
    synchronized (CacheLinkedLists) {
      for (int i = 0; i < SLOT_NUM; i++) {
        CacheLinkedLists[i] = null;
      }
    }
  }

  private CacheLinkedList getNextList(final AtomicInteger roundRobinIndex) {
    int listIndex = getNextIndex(roundRobinIndex);
    CacheLinkedList CacheLinkedList = CacheLinkedLists[listIndex];
    if (CacheLinkedList == null) {
      synchronized (CacheLinkedLists) {
        CacheLinkedList = CacheLinkedLists[listIndex];
        if (CacheLinkedList == null) {
          CacheLinkedList = new CacheLinkedList();
          CacheLinkedLists[listIndex] = CacheLinkedList;
        }
      }
    }
    return CacheLinkedList;
  }

  private int getNextIndex(final AtomicInteger roundRobinIndex) {
    return roundRobinIndex.getAndUpdate(
        currentValue -> {
          currentValue = currentValue + 1;
          return currentValue >= SLOT_NUM ? 0 : currentValue;
        });
  }
}
