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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternUtil;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.SchemaCacheEntry;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.dualkeycache.IDualKeyCache;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.dualkeycache.IDualKeyCacheComputation;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.dualkeycache.IDualKeyCacheStats;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.dualkeycache.IDualKeyCacheUpdating;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.lastcache.DataNodeLastCacheManager;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TableId;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;

class DualKeyCacheImpl<FK, SK, V, T extends ICacheEntry<SK, V>>
    implements IDualKeyCache<FK, SK, V> {

  private final SegmentedConcurrentHashMap<FK, ICacheEntryGroup<FK, SK, V, T>> firstKeyMap =
      new SegmentedConcurrentHashMap<>();

  private final ICacheEntryManager<FK, SK, V, T> cacheEntryManager;

  private final ICacheSizeComputer<FK, SK, V> sizeComputer;

  private final CacheStats cacheStats;

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
  }

  @Override
  public void compute(IDualKeyCacheComputation<FK, SK, V> computation) {
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
  }

  @Override
  public void update(IDualKeyCacheUpdating<FK, SK, V> updating) {
    FK firstKey = updating.getFirstKey();
    ICacheEntryGroup<FK, SK, V, T> cacheEntryGroup = firstKeyMap.get(firstKey);
    SK[] secondKeyList = updating.getSecondKeyList();
    if (cacheEntryGroup == null) {
      for (int i = 0; i < secondKeyList.length; i++) {
        updating.updateValue(i, null);
      }
      cacheStats.recordMiss(secondKeyList.length);
    } else {
      T cacheEntry;
      int hitCount = 0;
      for (int i = 0; i < secondKeyList.length; i++) {
        cacheEntry = cacheEntryGroup.getCacheEntry(secondKeyList[i]);
        if (cacheEntry == null) {
          updating.updateValue(i, null);
        } else {
          int changeSize = 0;
          synchronized (cacheEntry) {
            if (cacheEntry.getBelongedGroup() != null) {
              // Only update the value when the cache entry is not evicted.
              // If the cache entry is evicted, getBelongedGroup is null.
              // Synchronized is to guarantee the cache entry is not evicted during the update.
              changeSize = updating.updateValue(i, cacheEntry.getValue());
              cacheEntryManager.access(cacheEntry);
              if (changeSize != 0) {
                cacheStats.increaseMemoryUsage(changeSize);
              }
            }
          }
          if (changeSize != 0 && cacheStats.isExceedMemoryCapacity()) {
            executeCacheEviction(changeSize);
          }
          hitCount++;
        }
      }
      cacheStats.recordHit(hitCount);
      cacheStats.recordMiss(secondKeyList.length - hitCount);
    }
  }

  @Override
  public void put(FK firstKey, SK secondKey, V value) {
    int usedMemorySize = putToCache(firstKey, secondKey, value);
    cacheStats.increaseMemoryUsage(usedMemorySize);
    if (cacheStats.isExceedMemoryCapacity()) {
      executeCacheEviction(usedMemorySize);
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
    synchronized (evictCacheEntry) {
      AtomicInteger evictedSize = new AtomicInteger(0);
      evictedSize.getAndAdd(sizeComputer.computeValueSize(evictCacheEntry.getValue()));

      ICacheEntryGroup<FK, SK, V, T> belongedGroup = evictCacheEntry.getBelongedGroup();
      evictCacheEntry.setBelongedGroup(null);
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
  }

  @Override
  public void invalidateLastCache(PartialPath path) {
    String measurement = path.getMeasurement();
    PartialPath devicePath = path.getDevicePath();
    Function<FK, Boolean> deviceFilter = null;
    Function<SK, Boolean> measurementFilter = null;

    if (PathPatternUtil.hasWildcard(devicePath.getFullPath())) {
      deviceFilter = d -> devicePath.matchFullPath((PartialPath) d);
    }
    if (PathPatternUtil.isMultiLevelMatchWildcard(measurement)) {
      measurementFilter = m -> true;
    }
    if (deviceFilter == null) {
      deviceFilter = d -> d.equals(devicePath);
    }

    if (measurementFilter == null) {
      measurementFilter = m -> PathPatternUtil.isNodeMatch(measurement, m.toString());
    }

    for (FK device : firstKeyMap.getAllKeys()) {
      if (Boolean.TRUE.equals(deviceFilter.apply(device))) {
        ICacheEntryGroup<FK, SK, V, T> entryGroup = firstKeyMap.get(device);
        for (Iterator<Map.Entry<SK, T>> it = entryGroup.getAllCacheEntries(); it.hasNext(); ) {
          Map.Entry<SK, T> entry = it.next();
          if (Boolean.TRUE.equals(measurementFilter.apply(entry.getKey()))) {
            T cacheEntry = entry.getValue();
            synchronized (cacheEntry) {
              SchemaCacheEntry schemaCacheEntry = (SchemaCacheEntry) cacheEntry.getValue();
              cacheStats.decreaseMemoryUsage(
                  DataNodeLastCacheManager.invalidateLastCache(schemaCacheEntry));
            }
          }
        }
      }
    }
  }

  @Override
  public void invalidateDataRegionLastCache(String database) {
    for (FK device : firstKeyMap.getAllKeys()) {
      if (device.toString().startsWith(database)) {
        ICacheEntryGroup<FK, SK, V, T> entryGroup = firstKeyMap.get(device);
        for (Iterator<Map.Entry<SK, T>> it = entryGroup.getAllCacheEntries(); it.hasNext(); ) {
          Map.Entry<SK, T> entry = it.next();
          T cacheEntry = entry.getValue();
          synchronized (cacheEntry) {
            SchemaCacheEntry schemaCacheEntry = (SchemaCacheEntry) cacheEntry.getValue();
            cacheStats.decreaseMemoryUsage(
                DataNodeLastCacheManager.invalidateLastCache(schemaCacheEntry));
          }
        }
      }
    }
  }

  @Override
  public void invalidateAll() {
    executeInvalidateAll();
  }

  @Override
  public void invalidate(String database) {
    int estimateSize = 0;
    for (FK device : firstKeyMap.getAllKeys()) {
      if (device.toString().startsWith(database)) {
        estimateSize += sizeComputer.computeFirstKeySize(device);
        ICacheEntryGroup<FK, SK, V, T> entryGroup = firstKeyMap.get(device);
        for (Iterator<Map.Entry<SK, T>> it = entryGroup.getAllCacheEntries(); it.hasNext(); ) {
          Map.Entry<SK, T> entry = it.next();
          estimateSize += sizeComputer.computeSecondKeySize(entry.getKey());
          estimateSize += sizeComputer.computeValueSize(entry.getValue().getValue());
          cacheEntryManager.invalid(entry.getValue());
        }
        firstKeyMap.remove(device);
      }
    }
    cacheStats.decreaseMemoryUsage(estimateSize);
  }

  @Override
  public void invalidate(List<? extends PartialPath> partialPathList) {
    int estimateSize = 0;
    for (PartialPath path : partialPathList) {
      String measurement = path.getMeasurement();
      Function<FK, Boolean> deviceFilter;
      Function<SK, Boolean> measurementFilter;
      if (PathPatternUtil.isMultiLevelMatchWildcard(measurement)) {
        deviceFilter = d -> d.toString().startsWith(path.getDevicePath().getFullPath());
        measurementFilter = m -> true;
      } else {
        deviceFilter = d -> d.toString().equals(path.getDevicePath().getFullPath());
        measurementFilter = m -> PathPatternUtil.isNodeMatch(measurement, m.toString());
      }
      for (FK device : firstKeyMap.getAllKeys()) {
        if (Boolean.TRUE.equals(deviceFilter.apply(device))) {
          boolean allSKInvalid = true;
          ICacheEntryGroup<FK, SK, V, T> entryGroup = firstKeyMap.get(device);
          for (Iterator<Map.Entry<SK, T>> it = entryGroup.getAllCacheEntries(); it.hasNext(); ) {
            Map.Entry<SK, T> entry = it.next();
            if (Boolean.TRUE.equals(measurementFilter.apply(entry.getKey()))) {
              estimateSize += sizeComputer.computeSecondKeySize(entry.getKey());
              estimateSize += sizeComputer.computeValueSize(entry.getValue().getValue());
              cacheEntryManager.invalid(entry.getValue());
              it.remove();
            } else {
              allSKInvalid = false;
            }
          }
          if (allSKInvalid) {
            firstKeyMap.remove(device);
            estimateSize += sizeComputer.computeFirstKeySize(device);
          }
        }
      }
    }

    cacheStats.decreaseMemoryUsage(estimateSize);
  }

  private void executeInvalidateAll() {
    firstKeyMap.clear();
    cacheEntryManager.cleanUp();
    cacheStats.resetMemoryUsage();
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
  public void invalidate(FK firstKey) {
    int estimateSize = 0;
    ICacheEntryGroup<FK, SK, V, T> cacheEntryGroup = firstKeyMap.remove(firstKey);
    if (cacheEntryGroup != null) {
      estimateSize += sizeComputer.computeFirstKeySize(firstKey);
      for (Iterator<Map.Entry<SK, T>> it = cacheEntryGroup.getAllCacheEntries(); it.hasNext(); ) {
        Map.Entry<SK, T> entry = it.next();
        estimateSize += sizeComputer.computeSecondKeySize(entry.getKey());
        estimateSize += sizeComputer.computeValueSize(entry.getValue().getValue());
        cacheEntryManager.invalid(entry.getValue());
      }
      cacheStats.decreaseMemoryUsage(estimateSize);
    }
  }

  @Override
  public void invalidateForTable(String database) {
    int estimateSize = 0;
    for (FK firstKey : firstKeyMap.getAllKeys()) {
      TableId tableId = (TableId) firstKey;
      if (tableId.belongTo(database)) {
        estimateSize += sizeComputer.computeFirstKeySize(firstKey);
        ICacheEntryGroup<FK, SK, V, T> entryGroup = firstKeyMap.get(firstKey);
        for (Iterator<Map.Entry<SK, T>> it = entryGroup.getAllCacheEntries(); it.hasNext(); ) {
          Map.Entry<SK, T> entry = it.next();
          estimateSize += sizeComputer.computeSecondKeySize(entry.getKey());
          estimateSize += sizeComputer.computeValueSize(entry.getValue().getValue());
          cacheEntryManager.invalid(entry.getValue());
        }
        firstKeyMap.remove(firstKey);
      }
    }
    cacheStats.decreaseMemoryUsage(estimateSize);
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
