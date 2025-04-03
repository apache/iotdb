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

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.dualkeycache.IDualKeyCache;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.dualkeycache.IDualKeyCacheStats;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;

class DualKeyCacheImpl<FK, SK, V, T extends ICacheEntry<SK, V>>
    implements IDualKeyCache<FK, SK, V> {

  private final SegmentedConcurrentHashMap<FK, ICacheEntryGroup<FK, SK, V, T>> firstKeyMap =
      new SegmentedConcurrentHashMap<>();

  private final ICacheEntryManager<FK, SK, V, T> cacheEntryManager;

  private final ICacheSizeComputer<FK, SK, V> sizeComputer;

  private final CacheStats cacheStats;

  DualKeyCacheImpl(
      final ICacheEntryManager<FK, SK, V, T> cacheEntryManager,
      final ICacheSizeComputer<FK, SK, V> sizeComputer,
      final long memoryCapacity) {
    this.cacheEntryManager = cacheEntryManager;
    this.sizeComputer = sizeComputer;
    this.cacheStats = new CacheStats(memoryCapacity);
  }

  @Override
  public V get(final FK firstKey, final SK secondKey) {
    final ICacheEntryGroup<FK, SK, V, T> cacheEntryGroup = firstKeyMap.get(firstKey);
    if (cacheEntryGroup == null) {
      cacheStats.recordMiss(1);
      return null;
    } else {
      final T cacheEntry = cacheEntryGroup.getCacheEntry(secondKey);
      if (cacheEntry == null) {
        cacheStats.recordMiss(1);
        return null;
      } else {
        cacheEntryManager.access(cacheEntry);
        cacheStats.recordHit(1);
        return cacheEntry.getValue();
      }
    }
  }

  @Override
  public void update(
      final FK firstKey,
      final @Nonnull SK secondKey,
      final V value,
      final ToIntFunction<V> updater,
      final boolean createIfNotExists) {
    final AtomicInteger usedMemorySize = new AtomicInteger(0);

    final ICacheEntryGroup<FK, SK, V, T> cacheEntryGroup =
        createIfNotExists
            ? firstKeyMap.compute(
                firstKey,
                (k, entryGroup) -> {
                  if (entryGroup == null) {
                    entryGroup = new CacheEntryGroupImpl<>(firstKey);
                    usedMemorySize.getAndAdd(sizeComputer.computeFirstKeySize(firstKey));
                  }
                  return entryGroup;
                })
            : firstKeyMap.get(firstKey);

    if (Objects.isNull(cacheEntryGroup)) {
      return;
    }

    cacheEntryGroup.computeCacheEntry(
        secondKey,
        (sk, cacheEntry) -> {
          if (Objects.isNull(cacheEntry)) {
            if (!createIfNotExists) {
              return null;
            }
            cacheEntry = cacheEntryManager.createCacheEntry(secondKey, value, cacheEntryGroup);
            cacheEntryManager.put(cacheEntry);
            cacheStats.increaseEntryCount();
            usedMemorySize.getAndAdd(
                sizeComputer.computeSecondKeySize(sk)
                    + sizeComputer.computeValueSize(cacheEntry.getValue()));
          }
          usedMemorySize.getAndAdd(updater.applyAsInt(cacheEntry.getValue()));
          return cacheEntry;
        });

    increaseMemoryUsageAndMayEvict(usedMemorySize.get());
  }

  @Override
  public void update(
      final FK firstKey, final Predicate<SK> secondKeyChecker, final ToIntFunction<V> updater) {
    final AtomicInteger usedMemorySize = new AtomicInteger(0);

    final ICacheEntryGroup<FK, SK, V, T> entryGroup = firstKeyMap.get(firstKey);
    if (Objects.nonNull(entryGroup)) {
      entryGroup
          .getAllCacheEntries()
          .forEachRemaining(
              entry -> {
                if (!secondKeyChecker.test(entry.getKey())) {
                  return;
                }
                entryGroup.computeCacheEntry(
                    entry.getKey(),
                    (secondKey, cacheEntry) -> {
                      if (Objects.nonNull(cacheEntry)) {
                        usedMemorySize.getAndAdd(updater.applyAsInt(cacheEntry.getValue()));
                      }
                      return cacheEntry;
                    });
              });
    }
    increaseMemoryUsageAndMayEvict(usedMemorySize.get());
  }

  @Override
  public void update(
      final Predicate<FK> firstKeyChecker,
      final Predicate<SK> secondKeyChecker,
      final ToIntFunction<V> updater) {
    final AtomicInteger usedMemorySize = new AtomicInteger(0);
    for (final FK firstKey : firstKeyMap.getAllKeys()) {
      if (!firstKeyChecker.test(firstKey)) {
        continue;
      }
      final ICacheEntryGroup<FK, SK, V, T> entryGroup = firstKeyMap.get(firstKey);
      if (Objects.nonNull(entryGroup)) {
        entryGroup
            .getAllCacheEntries()
            .forEachRemaining(
                entry -> {
                  if (!secondKeyChecker.test(entry.getKey())) {
                    return;
                  }
                  entryGroup.computeCacheEntry(
                      entry.getKey(),
                      (secondKey, cacheEntry) -> {
                        if (Objects.nonNull(cacheEntry)) {
                          usedMemorySize.getAndAdd(updater.applyAsInt(cacheEntry.getValue()));
                        }
                        return cacheEntry;
                      });
                });
      }
    }
    increaseMemoryUsageAndMayEvict(usedMemorySize.get());
  }

  private void increaseMemoryUsageAndMayEvict(final int memorySize) {
    cacheStats.increaseMemoryUsage(memorySize);
    while (cacheStats.isExceedMemoryCapacity()) {
      cacheStats.decreaseMemoryUsage(evictOneCacheEntry());
    }
  }

  private int evictOneCacheEntry() {
    final ICacheEntry<SK, V> evictCacheEntry = cacheEntryManager.evict();
    if (evictCacheEntry == null) {
      return 0;
    }

    final AtomicInteger evictedSize = new AtomicInteger(0);

    final ICacheEntryGroup<FK, SK, V, T> belongedGroup = evictCacheEntry.getBelongedGroup();
    evictCacheEntry.setBelongedGroup(null);

    belongedGroup.computeCacheEntry(
        evictCacheEntry.getSecondKey(),
        (secondKey, cacheEntry) -> {
          cacheStats.decreaseEntryCount();
          evictedSize.getAndAdd(
              sizeComputer.computeValueSize(cacheEntry.getValue())
                  + sizeComputer.computeSecondKeySize(cacheEntry.getSecondKey()));
          return null;
        });

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

    return evictedSize.get();
  }

  @Override
  public void invalidateAll() {
    executeInvalidateAll();
  }

  private void executeInvalidateAll() {
    firstKeyMap.clear();
    cacheEntryManager.cleanUp();
    cacheStats.resetMemoryUsageAndEntriesCount();
  }

  @Override
  public void cleanUp() {
    executeInvalidateAll();
    cacheStats.reset();
  }

  @Override
  public IDualKeyCacheStats stats() {
    return cacheStats;
  }

  @Override
  @TestOnly
  public void evictOneEntry() {
    cacheStats.decreaseMemoryUsage(evictOneCacheEntry());
  }

  @Override
  public void invalidate(final FK firstKey) {
    int estimateSize = 0;
    final ICacheEntryGroup<FK, SK, V, T> cacheEntryGroup = firstKeyMap.remove(firstKey);
    if (cacheEntryGroup != null) {
      estimateSize += sizeComputer.computeFirstKeySize(firstKey);
      for (final Iterator<Map.Entry<SK, T>> it = cacheEntryGroup.getAllCacheEntries();
          it.hasNext(); ) {
        final Map.Entry<SK, T> entry = it.next();
        if (cacheEntryManager.invalidate(entry.getValue())) {
          cacheStats.decreaseEntryCount();
          estimateSize +=
              sizeComputer.computeSecondKeySize(entry.getKey())
                  + sizeComputer.computeValueSize(entry.getValue().getValue());
        }
      }
      cacheStats.decreaseMemoryUsage(estimateSize);
    }
  }

  @Override
  public void invalidate(final FK firstKey, final SK secondKey) {
    final AtomicInteger usedMemorySize = new AtomicInteger(0);

    firstKeyMap.compute(
        firstKey,
        (key, cacheEntryGroup) -> {
          if (cacheEntryGroup == null) {
            // has been removed by other threads
            return null;
          }

          final T entry = cacheEntryGroup.getCacheEntry(secondKey);
          if (Objects.nonNull(entry) && cacheEntryManager.invalidate(entry)) {
            cacheStats.decreaseEntryCount();
            usedMemorySize.getAndAdd(
                sizeComputer.computeSecondKeySize(entry.getSecondKey())
                    + sizeComputer.computeValueSize(entry.getValue()));
            cacheEntryGroup.removeCacheEntry(entry.getSecondKey());
          }

          if (cacheEntryGroup.isEmpty()) {
            usedMemorySize.getAndAdd(sizeComputer.computeFirstKeySize(firstKey));
            return null;
          }

          return cacheEntryGroup;
        });
    cacheStats.decreaseMemoryUsage(usedMemorySize.get());
  }

  @Override
  public void invalidate(final FK firstKey, final Predicate<SK> secondKeyChecker) {
    final AtomicInteger estimateSize = new AtomicInteger(0);
    firstKeyMap.compute(
        firstKey,
        (key, cacheEntryGroup) -> {
          if (cacheEntryGroup == null) {
            // has been removed by other threads
            return null;
          }

          for (final Iterator<Map.Entry<SK, T>> it = cacheEntryGroup.getAllCacheEntries();
              it.hasNext(); ) {
            final Map.Entry<SK, T> entry = it.next();
            if (secondKeyChecker.test(entry.getKey())
                && cacheEntryManager.invalidate(entry.getValue())) {
              cacheStats.decreaseEntryCount();
              cacheEntryGroup.removeCacheEntry(entry.getKey());
              estimateSize.addAndGet(
                  sizeComputer.computeSecondKeySize(entry.getKey())
                      + sizeComputer.computeValueSize(entry.getValue().getValue()));
            }
          }

          if (cacheEntryGroup.isEmpty()) {
            estimateSize.getAndAdd(sizeComputer.computeFirstKeySize(firstKey));
            return null;
          }

          return cacheEntryGroup;
        });

    cacheStats.decreaseMemoryUsage(estimateSize.get());
  }

  @Override
  public void invalidate(
      final Predicate<FK> firstKeyChecker, final Predicate<SK> secondKeyChecker) {
    final AtomicInteger estimateSize = new AtomicInteger(0);
    for (final FK firstKey : firstKeyMap.getAllKeys()) {
      if (!firstKeyChecker.test(firstKey)) {
        continue;
      }

      firstKeyMap.compute(
          firstKey,
          (fk, cacheEntryGroup) -> {
            if (cacheEntryGroup == null) {
              // has been removed by other threads
              return null;
            }

            for (final Iterator<Map.Entry<SK, T>> it = cacheEntryGroup.getAllCacheEntries();
                it.hasNext(); ) {
              final Map.Entry<SK, T> entry = it.next();

              if (secondKeyChecker.test(entry.getKey())
                  && cacheEntryManager.invalidate(entry.getValue())) {
                cacheStats.decreaseEntryCount();
                cacheEntryGroup.removeCacheEntry(entry.getKey());
                estimateSize.addAndGet(
                    sizeComputer.computeSecondKeySize(entry.getKey())
                        + sizeComputer.computeValueSize(entry.getValue().getValue()));
              }
            }

            if (cacheEntryGroup.isEmpty()) {
              estimateSize.getAndAdd(sizeComputer.computeFirstKeySize(firstKey));
              return null;
            }
            return cacheEntryGroup;
          });
    }
    cacheStats.decreaseMemoryUsage(estimateSize.get());
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

    V remove(K key) {
      return getBelongedMap(key).remove(key);
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

    // Copied list, deletion-safe
    List<K> getAllKeys() {
      List<K> res = new ArrayList<>();
      Arrays.stream(maps)
          .iterator()
          .forEachRemaining(
              map -> {
                if (map != null) {
                  res.addAll(map.keySet());
                }
              });
      return res;
    }
  }
}
