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

import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;

public class CacheEntryGroupImpl<SK, V> implements ICacheEntryGroup<SK, V> {

  private final Map<SK, CacheEntry<SK, V>> cacheEntryMap = new ConcurrentHashMap<>();
  private final ICacheSizeComputer<SK, V> sizeComputer;
  private final AtomicLong memory;

  CacheEntryGroupImpl(final ICacheSizeComputer<SK, V> sizeComputer) {
    this.sizeComputer = sizeComputer;
    this.memory = new AtomicLong(0L);
  }

  @Override
  public CacheEntry<SK, V> getCacheEntry(final SK secondKey) {
    return secondKey == null ? null : cacheEntryMap.get(secondKey);
  }

  @Override
  public Iterator<Map.Entry<SK, CacheEntry<SK, V>>> getAllCacheEntries() {
    return cacheEntryMap.entrySet().iterator();
  }

  @Override
  public CacheEntry<SK, V> computeCacheEntry(
      final SK secondKey,
      final Function<AtomicLong, BiFunction<SK, CacheEntry<SK, V>, CacheEntry<SK, V>>>
          computation) {
    return cacheEntryMap.compute(secondKey, computation.apply(memory));
  }

  @Override
  public CacheEntry<SK, V> computeCacheEntryIfPresent(
      final SK secondKey,
      final Function<AtomicLong, BiFunction<SK, CacheEntry<SK, V>, CacheEntry<SK, V>>>
          computation) {
    return cacheEntryMap.computeIfPresent(secondKey, computation.apply(memory));
  }

  @Override
  public long removeCacheEntry(final SK secondKey) {
    final CacheEntry<SK, V> result = cacheEntryMap.remove(secondKey);
    if (Objects.nonNull(result)) {
      final long delta =
          sizeComputer.computeSecondKeySize(result.getSecondKey())
              + sizeComputer.computeValueSize(result.getValue())
              + RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY;
      memory.addAndGet(-delta);
      return delta;
    }
    return 0;
  }

  @Override
  public boolean isEmpty() {
    return cacheEntryMap.isEmpty();
  }

  @Override
  public long getMemory() {
    return memory.get();
  }

  @Override
  public int getEntriesCount() {
    return cacheEntryMap.size();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final CacheEntryGroupImpl<?, ?> that = (CacheEntryGroupImpl<?, ?>) o;
    return Objects.equals(cacheEntryMap, that.cacheEntryMap);
  }

  @Override
  public int hashCode() {
    return cacheEntryMap.hashCode();
  }
}
