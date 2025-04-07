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
import java.util.function.Predicate;
import java.util.function.ToIntFunction;

class DualKeyCacheImpl<FK, SK, V, T extends ICacheEntry<SK, V>>
    implements IDualKeyCache<FK, SK, V> {

  private final SegmentedConcurrentHashMap<FK, ICacheEntryGroup<FK, SK, V, T>> firstKeyMap =
      new SegmentedConcurrentHashMap<>();

  private final ICacheEntryManager<FK, SK, V, T> cacheEntryManager;

  private final ICacheSizeComputer<FK, SK, V> sizeComputer;

  private final CacheStats<FK> cacheStats;

  DualKeyCacheImpl(
      final ICacheEntryManager<FK, SK, V, T> cacheEntryManager,
      final ICacheSizeComputer<FK, SK, V> sizeComputer,
      final long memoryCapacity) {
    this.cacheEntryManager = cacheEntryManager;
    this.sizeComputer = sizeComputer;
    this.cacheStats = new CacheStats<>(memoryCapacity, this::getMemory);
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

    final ICacheEntryGroup<FK, SK, V, T> cacheEntryGroup = firstKeyMap.get(firstKey);
    if (Objects.isNull(cacheEntryGroup) && createIfNotExists) {
      firstKeyMap.put(firstKey, new CacheEntryGroupImpl<>(firstKey, sizeComputer));
    }

    if (Objects.isNull(cacheEntryGroup)) {
      return;
    }

    cacheEntryGroup.computeCacheEntry(
        secondKey,
        memory ->
            (sk, cacheEntry) -> {
              if (Objects.isNull(cacheEntry)) {
                if (!createIfNotExists) {
                  return null;
                }
                cacheEntry = cacheEntryManager.createCacheEntry(secondKey, value, cacheEntryGroup);
                cacheEntryManager.put(cacheEntry);
                cacheStats.increaseEntryCount();
                memory.getAndAdd(
                    sizeComputer.computeSecondKeySize(sk)
                        + sizeComputer.computeValueSize(cacheEntry.getValue()));
              }
              memory.getAndAdd(updater.applyAsInt(cacheEntry.getValue()));
              return cacheEntry;
            });

    mayEvict();
  }

  @Override
  public void update(
      final FK firstKey, final Predicate<SK> secondKeyChecker, final ToIntFunction<V> updater) {
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
                    memory ->
                        (secondKey, cacheEntry) -> {
                          if (Objects.nonNull(cacheEntry)) {
                            memory.getAndAdd(updater.applyAsInt(cacheEntry.getValue()));
                          }
                          return cacheEntry;
                        });
              });
    }
    mayEvict();
  }

  @Override
  public void update(
      final Predicate<FK> firstKeyChecker,
      final Predicate<SK> secondKeyChecker,
      final ToIntFunction<V> updater) {
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
                      memory ->
                          (secondKey, cacheEntry) -> {
                            memory.getAndAdd(updater.applyAsInt(cacheEntry.getValue()));
                            return cacheEntry;
                          });
                });
      }
      mayEvict();
    }
  }

  private void mayEvict() {
    long exceedMemory;
    while ((exceedMemory = cacheStats.getExceedNum()) > 0) {
      // Not compute each time to save time when FK is too many
      // The hard-coded size is 100
      do  {
        exceedMemory -= evictOneCacheEntry();
      } while (exceedMemory > 0 && firstKeyMap.size() > 100);
    }
  }

  private long evictOneCacheEntry() {
    final ICacheEntry<SK, V> evictCacheEntry = cacheEntryManager.evict();
    if (evictCacheEntry == null) {
      return 0;
    }

    final ICacheEntryGroup<FK, SK, V, T> belongedGroup = evictCacheEntry.getBelongedGroup();
    evictCacheEntry.setBelongedGroup(null);

    long memory = belongedGroup.removeCacheEntry(evictCacheEntry.getSecondKey());

    final ICacheEntryGroup<FK, SK, V, T> cacheEntryGroup =
        firstKeyMap.get(belongedGroup.getFirstKey());
    if (Objects.nonNull(cacheEntryGroup) && cacheEntryGroup.isEmpty()) {
      firstKeyMap.remove(belongedGroup.getFirstKey());
      memory += sizeComputer.computeFirstKeySize(belongedGroup.getFirstKey());
    }
    return memory;
  }

  @Override
  public void invalidateAll() {
    executeInvalidateAll();
  }

  private void executeInvalidateAll() {
    firstKeyMap.clear();
    cacheEntryManager.cleanUp();
    cacheStats.resetEntriesCount();
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
    evictOneCacheEntry();
  }

  @Override
  public void invalidate(final FK firstKey) {
    final ICacheEntryGroup<FK, SK, V, T> cacheEntryGroup = firstKeyMap.remove(firstKey);
    if (cacheEntryGroup != null) {
      for (final Iterator<Map.Entry<SK, T>> it = cacheEntryGroup.getAllCacheEntries();
          it.hasNext(); ) {
        final Map.Entry<SK, T> entry = it.next();
        if (cacheEntryManager.invalidate(entry.getValue())) {
          cacheStats.decreaseEntryCount();
        }
      }
    }
  }

  @Override
  public void invalidate(final FK firstKey, final SK secondKey) {
    final ICacheEntryGroup<FK, SK, V, T> cacheEntryGroup = firstKeyMap.get(firstKey);
    if (Objects.isNull(cacheEntryGroup)) {
      return;
    }

    final T entry = cacheEntryGroup.getCacheEntry(secondKey);
    if (Objects.nonNull(entry) && cacheEntryManager.invalidate(entry)) {
      cacheStats.decreaseEntryCount();
      cacheEntryGroup.removeCacheEntry(entry.getSecondKey());
    }

    if (cacheEntryGroup.isEmpty()) {
      firstKeyMap.remove(firstKey);
    }
  }

  @Override
  public void invalidate(final FK firstKey, final Predicate<SK> secondKeyChecker) {
    final ICacheEntryGroup<FK, SK, V, T> cacheEntryGroup = firstKeyMap.get(firstKey);
    if (Objects.isNull(cacheEntryGroup)) {
      return;
    }
    for (final Iterator<Map.Entry<SK, T>> it = cacheEntryGroup.getAllCacheEntries();
        it.hasNext(); ) {
      final Map.Entry<SK, T> entry = it.next();
      if (secondKeyChecker.test(entry.getKey()) && cacheEntryManager.invalidate(entry.getValue())) {
        cacheStats.decreaseEntryCount();
        cacheEntryGroup.removeCacheEntry(entry.getKey());
      }
    }

    if (cacheEntryGroup.isEmpty()) {
      firstKeyMap.remove(firstKey);
    }
  }

  @Override
  public void invalidate(
      final Predicate<FK> firstKeyChecker, final Predicate<SK> secondKeyChecker) {
    for (final FK firstKey : firstKeyMap.getAllKeys()) {
      if (!firstKeyChecker.test(firstKey)) {
        continue;
      }

      final ICacheEntryGroup<FK, SK, V, T> cacheEntryGroup = firstKeyMap.get(firstKey);
      if (Objects.isNull(cacheEntryGroup)) {
        return;
      }

      for (final Iterator<Map.Entry<SK, T>> it = cacheEntryGroup.getAllCacheEntries();
          it.hasNext(); ) {
        final Map.Entry<SK, T> entry = it.next();

        if (secondKeyChecker.test(entry.getKey())
            && cacheEntryManager.invalidate(entry.getValue())) {
          cacheStats.decreaseEntryCount();
          cacheEntryGroup.removeCacheEntry(entry.getKey());
        }
      }

      if (cacheEntryGroup.isEmpty()) {
        firstKeyMap.remove(firstKey);
      }
    }
  }

  private long getMemory() {
    return Arrays.stream(firstKeyMap.maps)
        .flatMap(map -> map.values().stream())
        .map(ICacheEntryGroup::getMemory)
        .reduce(0L, Long::sum);
  }

  /**
   * Since the capacity of one instance of ConcurrentHashMap is about 4 million, a number of
   * instances are united for more capacity.
   */
  private static class SegmentedConcurrentHashMap<K, V> {

    private static final int SLOT_NUM = 31;

    private final Map<K, V>[] maps = new ConcurrentHashMap[SLOT_NUM];

    V get(final K key) {
      return getBelongedMap(key).get(key);
    }

    V remove(final K key) {
      return getBelongedMap(key).remove(key);
    }

    V put(final K key, final V value) {
      return getBelongedMap(key).put(key, value);
    }

    void clear() {
      synchronized (maps) {
        Arrays.fill(maps, null);
      }
    }

    Map<K, V> getBelongedMap(final K key) {
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
      final List<K> res = new ArrayList<>();
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

    int size() {
      return Arrays.stream(maps).map(Map::size).reduce(0, Integer::sum);
    }
  }
}
