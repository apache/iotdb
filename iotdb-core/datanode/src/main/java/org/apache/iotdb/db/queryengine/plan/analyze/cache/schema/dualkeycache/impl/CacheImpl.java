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

import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.dualkeycache.ICache;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.dualkeycache.ICacheStats;

import org.apache.tsfile.utils.RamUsageEstimator;

import javax.annotation.Nonnull;

import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;

class CacheImpl<SK, V, T extends ICacheEntry<SK, V>> implements ICache<SK, V> {
  private final ICacheEntryGroup<SK, V, T> cacheEntryGroup;

  private final ICacheEntryManager<SK, V, T> cacheEntryManager;

  private final ICacheSizeComputer<SK, V> sizeComputer;

  private final CacheStats cacheStats;

  CacheImpl(
      final ICacheEntryManager<SK, V, T> cacheEntryManager,
      final ICacheSizeComputer<SK, V> sizeComputer,
      final long memoryCapacity) {
    this.cacheEntryManager = cacheEntryManager;
    this.sizeComputer = sizeComputer;
    this.cacheStats = new CacheStats(memoryCapacity, this::getMemory, this::getEntriesCount);
    this.cacheEntryGroup = new CacheEntryGroupImpl<>(sizeComputer);
  }

  @Override
  public V get(final SK secondKey) {
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

  @Override
  public <R> boolean batchApply(
      final Map<SK, R> inputMap, final BiFunction<V, R, Boolean> mappingFunction) {
    for (final Map.Entry<SK, R> skrEntry : inputMap.entrySet()) {
      final T cacheEntry = cacheEntryGroup.getCacheEntry(skrEntry.getKey());
      if (cacheEntry == null) {
        return false;
      }
      if (!mappingFunction.apply(cacheEntry.getValue(), skrEntry.getValue())) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void update(
      final @Nonnull SK secondKey,
      final V value,
      final ToIntFunction<V> updater,
      final boolean createIfNotExists) {

    final ICacheEntryGroup<SK, V, T> finalCacheEntryGroup = cacheEntryGroup;
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
  public void update(final Predicate<SK> secondKeyChecker, final ToIntFunction<V> updater) {
    clearSecondEntry(cacheEntryGroup, secondKeyChecker, updater);
    mayEvict();
  }

  public void clearSecondEntry(
      final ICacheEntryGroup<SK, V, T> entryGroup,
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
    while ((exceedMemory = cacheStats.getExceedMemory()) > 0) {
      do {
        exceedMemory -= evictOneCacheEntry();
      } while (exceedMemory > 0);
    }
  }

  // The returned delta may have some error, but it's OK
  // Because the delta is only for loop round estimation
  private long evictOneCacheEntry() {
    final ICacheEntry<SK, V> evictCacheEntry = cacheEntryManager.evict();
    if (evictCacheEntry == null) {
      return 0;
    }

    final ICacheEntryGroup<SK, V, T> belongedGroup = evictCacheEntry.getBelongedGroup();
    evictCacheEntry.setBelongedGroup(null);

    return belongedGroup.removeCacheEntry(evictCacheEntry.getSecondKey());
  }

  @Override
  public void invalidateAll() {
    cacheEntryManager.cleanUp();
  }

  @Override
  public ICacheStats stats() {
    return cacheStats;
  }

  @Override
  public void invalidate(final SK secondKey) {

    final T entry = cacheEntryGroup.getCacheEntry(secondKey);
    if (Objects.nonNull(entry) && cacheEntryManager.invalidate(entry)) {
      cacheEntryGroup.removeCacheEntry(entry.getSecondKey());
    }
  }

  @Override
  public void invalidate(final Predicate<SK> secondKeyChecker) {
    for (final Iterator<Map.Entry<SK, T>> it = cacheEntryGroup.getAllCacheEntries();
        it.hasNext(); ) {
      final Map.Entry<SK, T> entry = it.next();
      if (secondKeyChecker.test(entry.getKey()) && cacheEntryManager.invalidate(entry.getValue())) {
        cacheEntryGroup.removeCacheEntry(entry.getKey());
      }
    }
  }

  private long getMemory() {
    return cacheEntryGroup.getMemory();
  }

  private long getEntriesCount() {
    return cacheEntryGroup.getEntriesCount();
  }
}
