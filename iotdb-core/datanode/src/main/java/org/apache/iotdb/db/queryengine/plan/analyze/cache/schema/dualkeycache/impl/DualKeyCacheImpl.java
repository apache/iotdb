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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.dualkeycache.IDualKeyCache;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.dualkeycache.IDualKeyCacheStats;

import org.apache.tsfile.utils.RamUsageEstimator;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
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
    this.cacheStats = new CacheStats(memoryCapacity, this::getMemory, this::getEntriesCount);
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
  public <R> boolean batchApply(
      final Map<FK, Map<SK, R>> inputMap, final BiFunction<V, R, Boolean> mappingFunction) {
    for (final Map.Entry<FK, Map<SK, R>> fkMapEntry : inputMap.entrySet()) {
      final ICacheEntryGroup<FK, SK, V, T> cacheEntryGroup = firstKeyMap.get(fkMapEntry.getKey());
      if (cacheEntryGroup == null) {
        return false;
      }
      for (final Map.Entry<SK, R> skrEntry : fkMapEntry.getValue().entrySet()) {
        final T cacheEntry = cacheEntryGroup.getCacheEntry(skrEntry.getKey());
        if (cacheEntry == null) {
          return false;
        }
        if (!mappingFunction.apply(cacheEntry.getValue(), skrEntry.getValue())) {
          return false;
        }
      }
    }
    return true;
  }

  @Override
  public void update(
      final FK firstKey,
      final @Nonnull SK secondKey,
      final V value,
      final ToIntFunction<V> updater,
      final boolean createIfNotExists) {

    ICacheEntryGroup<FK, SK, V, T> cacheEntryGroup = firstKeyMap.get(firstKey);
    if (Objects.isNull(cacheEntryGroup)) {
      if (createIfNotExists) {
        cacheEntryGroup = new CacheEntryGroupImpl<>(firstKey, sizeComputer);
        firstKeyMap.put(firstKey, cacheEntryGroup);
      } else {
        return;
      }
    }

    final ICacheEntryGroup<FK, SK, V, T> finalCacheEntryGroup = cacheEntryGroup;
    cacheEntryGroup.computeCacheEntry(
        secondKey,
        memory ->
            (sk, cacheEntry) -> {
              if (Objects.isNull(cacheEntry)) {
                if (!createIfNotExists) {
                  return null;
                }
                cacheEntry =
                    cacheEntryManager.createCacheEntry(secondKey, value, finalCacheEntryGroup);
                cacheEntryManager.put(cacheEntry);
                memory.getAndAdd(
                    sizeComputer.computeSecondKeySize(sk)
                        + sizeComputer.computeValueSize(cacheEntry.getValue())
                        + RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY);
              }
              memory.getAndAdd(updater.applyAsInt(cacheEntry.getValue()));
              return cacheEntry;
            });

    mayEvict();
  }

  @Override
  public void update(
      final FK firstKey, final Predicate<SK> secondKeyChecker, final ToIntFunction<V> updater) {
    clearSecondEntry(firstKeyMap.get(firstKey), secondKeyChecker, updater);
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
      clearSecondEntry(firstKeyMap.get(firstKey), secondKeyChecker, updater);
    }
    mayEvict();
  }

  public void clearSecondEntry(
      final ICacheEntryGroup<FK, SK, V, T> entryGroup,
      final Predicate<SK> secondKeyChecker,
      final ToIntFunction<V> updater) {
    if (Objects.nonNull(entryGroup)) {
      entryGroup
          .getAllCacheEntries()
          .forEachRemaining(
              entry -> {
                if (!secondKeyChecker.test(entry.getKey())) {
                  return;
                }
                entryGroup.computeCacheEntryIfPresent(
                    entry.getKey(),
                    memory ->
                        (secondKey, cacheEntry) -> {
                          memory.getAndAdd(updater.applyAsInt(cacheEntry.getValue()));
                          return cacheEntry;
                        });
              });
    }
  }

  private void mayEvict() {
    long exceedMemory;
    final int threshold =
        IoTDBDescriptor.getInstance().getConfig().getCacheEvictionMemoryComputationThreshold();
    while ((exceedMemory = cacheStats.getExceedMemory()) > 0) {
      // Not compute each time to save time when FK is too many
      do {
        exceedMemory -= evictOneCacheEntry();
      } while (exceedMemory > 0 && firstKeyMap.size() > threshold);
    }
  }

  // The returned delta may have some error, but it's OK
  // Because the delta is only for loop round estimation
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
      // The removal is non-atomic, but it's ok because it's just a cache and does not affect the
      // consistency if you evicts some entries being added
      if (Objects.nonNull(firstKeyMap.remove(belongedGroup.getFirstKey()))) {
        memory +=
            sizeComputer.computeFirstKeySize(belongedGroup.getFirstKey())
                + RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY;
      }
    }
    return memory;
  }

  @Override
  public void invalidateAll() {
    firstKeyMap.clear();
    cacheEntryManager.cleanUp();
  }

  @Override
  public IDualKeyCacheStats stats() {
    return cacheStats;
  }

  @Override
  public void invalidate(final FK firstKey) {
    final ICacheEntryGroup<FK, SK, V, T> cacheEntryGroup = firstKeyMap.remove(firstKey);
    if (cacheEntryGroup != null) {
      for (final Iterator<Map.Entry<SK, T>> it = cacheEntryGroup.getAllCacheEntries();
          it.hasNext(); ) {
        cacheEntryManager.invalidate(it.next().getValue());
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
          cacheEntryGroup.removeCacheEntry(entry.getKey());
        }
      }

      if (cacheEntryGroup.isEmpty()) {
        firstKeyMap.remove(firstKey);
      }
    }
  }

  private long getMemory() {
    long memory = 0;
    for (final Map<FK, ICacheEntryGroup<FK, SK, V, T>> map : firstKeyMap.maps) {
      if (Objects.nonNull(map)) {
        for (final ICacheEntryGroup<FK, SK, V, T> group : map.values()) {
          memory += group.getMemory();
        }
      }
    }
    return memory;
  }

  private long getEntriesCount() {
    long count = 0;
    for (final Map<FK, ICacheEntryGroup<FK, SK, V, T>> map : firstKeyMap.maps) {
      if (Objects.nonNull(map)) {
        for (final ICacheEntryGroup<FK, SK, V, T> group : map.values()) {
          count += group.getEntriesCount();
        }
      }
    }
    return count;
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
      int size = 0;
      for (final Map<K, V> map : maps) {
        if (Objects.nonNull(map)) {
          size += map.size();
        }
      }
      return size;
    }
  }
}
