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

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

public class CacheEntryGroupImpl<FK, SK, V, T extends ICacheEntry<SK, V>>
    implements ICacheEntryGroup<FK, SK, V, T> {

  private final FK firstKey;

  private final Map<SK, T> cacheEntryMap = new ConcurrentHashMap<>();

  CacheEntryGroupImpl(FK firstKey) {
    this.firstKey = firstKey;
  }

  @Override
  public FK getFirstKey() {
    return firstKey;
  }

  @Override
  public T getCacheEntry(SK secondKey) {
    return cacheEntryMap.get(secondKey);
  }

  @Override
  public T computeCacheEntry(SK secondKey, BiFunction<SK, T, T> computation) {
    return cacheEntryMap.compute(secondKey, computation);
  }

  @Override
  public T removeCacheEntry(SK secondKey) {
    return cacheEntryMap.remove(secondKey);
  }

  @Override
  public boolean isEmpty() {
    return cacheEntryMap.isEmpty();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CacheEntryGroupImpl<?, ?, ?, ?> that = (CacheEntryGroupImpl<?, ?, ?, ?>) o;
    return Objects.equals(firstKey, that.firstKey);
  }

  @Override
  public int hashCode() {
    return firstKey.hashCode();
  }
}
