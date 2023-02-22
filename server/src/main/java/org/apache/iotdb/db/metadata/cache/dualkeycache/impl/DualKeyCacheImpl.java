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

import org.apache.iotdb.db.metadata.cache.dualkeycache.IDualKeyCache;
import org.apache.iotdb.db.metadata.cache.dualkeycache.IDualKeyCacheComputation;
import org.apache.iotdb.db.metadata.cache.dualkeycache.IDualKeyCacheStats;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;

class DualKeyCacheImpl<FK, SK, V, T extends ICacheEntry<SK, V>>
    implements IDualKeyCache<FK, SK, V> {

  private final SegmentedConcurrentHashMap<FK, ICacheEntryGroup<FK, SK, V, T>> firstKeyMap =
      new SegmentedConcurrentHashMap<>();

  private final ICacheEntryManager<FK, SK, V, T> cacheEntryManager;

  private final ICacheSizeComputer<FK, SK, V> sizeComputer;

  private final CacheStats cacheStats;

  private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock(true);

  DualKeyCacheImpl(
      ICacheEntryManager<FK, SK, V, T> cacheEntryManager,
      ICacheSizeComputer<FK, SK, V> sizeComputer,
      long memoryCapacity) {
    this.cacheEntryManager = cacheEntryManager;
    this.sizeComputer = sizeComputer;
    this.cacheStats = new CacheStats(memoryCapacity);
  }

  @Override
  public V get(FK firstKey, SK secondKey) {
    readWriteLock.readLock().lock();
    try {
      ICacheEntryGroup<FK, SK, V, T> cacheEntryGroup = firstKeyMap.get(firstKey);
      if (cacheEntryGroup == null) {
        cacheStats.recordMiss(1);
        return null;
      } else {
        T cacheEntry = cacheEntryGroup.getCacheEntry(secondKey);
        if (cacheEntry == null) {
          cacheStats.recordMiss(1);
          return null;
        } else {
          cacheEntryManager.access(cacheEntry);
          cacheStats.recordHit(1);
          return cacheEntry.getValue();
        }
      }
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  @Override
  public void compute(IDualKeyCacheComputation<FK, SK, V> computation) {
    readWriteLock.readLock().lock();
    try {
      FK firstKey = computation.getFirstKey();
      ICacheEntryGroup<FK, SK, V, T> cacheEntryGroup = firstKeyMap.get(firstKey);
      SK[] secondKeyList = computation.getSecondKeyList();
      if (cacheEntryGroup == null) {
        for (int i = 0; i < secondKeyList.length; i++) {
          computation.computeValue(i, null);
        }
        cacheStats.recordMiss(secondKeyList.length);
      } else {
        T cacheEntry;
        int hitCount = 0;
        for (int i = 0; i < secondKeyList.length; i++) {
          cacheEntry = cacheEntryGroup.getCacheEntry(secondKeyList[i]);
          if (cacheEntry == null) {
            computation.computeValue(i, null);
          } else {
            computation.computeValue(i, cacheEntry.getValue());
            cacheEntryManager.access(cacheEntry);
            hitCount++;
          }
        }
        cacheStats.recordHit(hitCount);
        cacheStats.recordMiss(secondKeyList.length - hitCount);
      }
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  @Override
  public void put(FK firstKey, SK secondKey, V value) {
    readWriteLock.readLock().lock();
    try {
      int usedMemorySize = putToCache(firstKey, secondKey, value);
      cacheStats.increaseMemoryUsage(usedMemorySize);
      if (cacheStats.isExceedMemoryCapacity()) {
        executeCacheEviction(usedMemorySize);
      }
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  private int putToCache(FK firstKey, SK secondKey, V value) {
    AtomicInteger usedMemorySize = new AtomicInteger(0);
    firstKeyMap.compute(
        firstKey,
        (k, cacheEntryGroup) -> {
          if (cacheEntryGroup == null) {
            cacheEntryGroup = new CacheEntryGroupImpl<>(firstKey);
            usedMemorySize.getAndAdd(sizeComputer.computeFirstKeySize(firstKey));
          }
          ICacheEntryGroup<FK, SK, V, T> finalCacheEntryGroup = cacheEntryGroup;
          cacheEntryGroup.computeCacheEntry(
              secondKey,
              (sk, cacheEntry) -> {
                if (cacheEntry == null) {
                  cacheEntry =
                      cacheEntryManager.createCacheEntry(secondKey, value, finalCacheEntryGroup);
                  cacheEntryManager.put(cacheEntry);
                  usedMemorySize.getAndAdd(sizeComputer.computeSecondKeySize(sk));
                } else {
                  V existingValue = cacheEntry.getValue();
                  if (existingValue != value && !existingValue.equals(value)) {
                    cacheEntry.replaceValue(value);
                    usedMemorySize.getAndAdd(-sizeComputer.computeValueSize(existingValue));
                  }
                  // update the cache status
                  cacheEntryManager.access(cacheEntry);
                }
                usedMemorySize.getAndAdd(sizeComputer.computeValueSize(value));
                return cacheEntry;
              });
          return cacheEntryGroup;
        });
    return usedMemorySize.get();
  }

  /**
   * Each thread putting new cache value only needs to evict cache values, total memory equals that
   * the new cache value occupied.
   */
  private void executeCacheEviction(int targetSize) {
    int evictedSize;
    while (targetSize > 0 && cacheStats.memoryUsage() > 0) {
      evictedSize = evictOneCacheEntry();
      cacheStats.decreaseMemoryUsage(evictedSize);
      targetSize -= evictedSize;
    }
  }

  private int evictOneCacheEntry() {

    ICacheEntry<SK, V> evictCacheEntry = cacheEntryManager.evict();
    if (evictCacheEntry == null) {
      return 0;
    }
    AtomicInteger evictedSize = new AtomicInteger(0);
    evictedSize.getAndAdd(sizeComputer.computeValueSize(evictCacheEntry.getValue()));

    ICacheEntryGroup<FK, SK, V, T> belongedGroup = evictCacheEntry.getBelongedGroup();
    belongedGroup.removeCacheEntry(evictCacheEntry.getSecondKey());
    evictedSize.getAndAdd(sizeComputer.computeSecondKeySize(evictCacheEntry.getSecondKey()));

    if (belongedGroup.isEmpty()) {
      firstKeyMap.compute(
          belongedGroup.getFirstKey(),
          (firstKey, cacheEntryGroup) -> {
            if (cacheEntryGroup == null) {
              // has been removed by other threads
              return null;
            }
            if (cacheEntryGroup.isEmpty()) {
              evictedSize.getAndAdd(sizeComputer.computeFirstKeySize(firstKey));
              return null;
            }

            // some other thread has put value to it
            return cacheEntryGroup;
          });
    }
    return evictedSize.get();
  }

  @Override
  public void invalidateAll() {
    readWriteLock.writeLock().lock();
    try {
      executeInvalidateAll();
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  private void executeInvalidateAll() {
    firstKeyMap.clear();
    cacheEntryManager.cleanUp();
    cacheStats.resetMemoryUsage();
  }

  @Override
  public void cleanUp() {
    readWriteLock.writeLock().lock();
    try {
      executeInvalidateAll();
      cacheStats.reset();
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  @Override
  public IDualKeyCacheStats stats() {
    return cacheStats;
  }

  /**
   * Since the capacity of one instance of ConcurrentHashMap is about 4 million, a number of
   * instances are united for more capacity.
   */
  private static class SegmentedConcurrentHashMap<K, V> {

    private static final int SLOT_NUM = 31;

    private final Map<K, V>[] maps = new ConcurrentHashMap[SLOT_NUM];

    V get(K key) {
      return getBelongedMap(key).get(key);
    }

    V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      return getBelongedMap(key).compute(key, remappingFunction);
    }

    void clear() {
      synchronized (maps) {
        for (int i = 0; i < SLOT_NUM; i++) {
          maps[i] = null;
        }
      }
    }

    Map<K, V> getBelongedMap(K key) {
      int slotIndex = key.hashCode() % SLOT_NUM;
      slotIndex = slotIndex < 0 ? slotIndex + SLOT_NUM : slotIndex;
      Map<K, V> map = maps[slotIndex];
      if (map == null) {
        synchronized (maps) {
          map = maps[slotIndex];
          if (map == null) {
            map = new ConcurrentHashMap<>();
            maps[slotIndex] = map;
          }
        }
      }
      return map;
    }
  }
}
