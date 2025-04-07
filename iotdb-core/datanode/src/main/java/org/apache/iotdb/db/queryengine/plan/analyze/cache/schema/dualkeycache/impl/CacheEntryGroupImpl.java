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

import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;

public class CacheEntryGroupImpl<FK, SK, V, T extends ICacheEntry<SK, V>>
    implements ICacheEntryGroup<FK, SK, V, T> {

  private final FK firstKey;

  private final Map<SK, T> cacheEntryMap = new ConcurrentHashMap<>();
  private final ICacheSizeComputer<FK, SK, V> sizeComputer;
  private AtomicLong memory;

  CacheEntryGroupImpl(final FK firstKey, final ICacheSizeComputer<FK, SK, V> sizeComputer) {
    this.firstKey = firstKey;
    this.sizeComputer = sizeComputer;
    this.memory = new AtomicLong(sizeComputer.computeFirstKeySize(firstKey));
  }

  @Override
  public FK getFirstKey() {
    return firstKey;
  }

  @Override
  public T getCacheEntry(final SK secondKey) {
    return secondKey == null ? null : cacheEntryMap.get(secondKey);
  }

  @Override
  public Iterator<Map.Entry<SK, T>> getAllCacheEntries() {
    return cacheEntryMap.entrySet().iterator();
  }

  @Override
  public T computeCacheEntry(final SK secondKey, final BiFunction<SK, T, T> computation) {
    return cacheEntryMap.compute(secondKey, computation);
  }

  @Override
  public T removeCacheEntry(final SK secondKey) {
    final T result = cacheEntryMap.remove(secondKey);
    memory.addAndGet(
        -sizeComputer.computeSecondKeySize(result.getSecondKey())
            - sizeComputer.computeValueSize(result.getValue()));
    return result;
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
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final CacheEntryGroupImpl<?, ?, ?, ?> that = (CacheEntryGroupImpl<?, ?, ?, ?>) o;
    return Objects.equals(firstKey, that.firstKey);
  }

  @Override
  public int hashCode() {
    return firstKey.hashCode();
  }
}
