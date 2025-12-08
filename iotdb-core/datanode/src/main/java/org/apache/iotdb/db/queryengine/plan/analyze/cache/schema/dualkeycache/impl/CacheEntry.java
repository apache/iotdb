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

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This interface defines the behaviour of a cache entry holding the cache value. The cache entry is
 * mainly accessed via second key from cache entry group and managed by cache entry manager for
 * cache eviction.
 *
 * @param <SK> The second key of cache value.
 * @param <V> The cache value.
 */
public class CacheEntry<SK, V> {

  protected final SK secondKey;

  @SuppressWarnings("java:S3077")
  protected volatile ICacheEntryGroup cacheEntryGroup;

  protected V value;

  CacheEntry<SK, V> pre = null;
  CacheEntry<SK, V> next = null;

  protected final AtomicBoolean isInvalidated = new AtomicBoolean(false);

  protected CacheEntry(SK secondKey, V value, ICacheEntryGroup cacheEntryGroup) {
    this.secondKey = secondKey;
    this.value = value;
    this.cacheEntryGroup = cacheEntryGroup;
  }

  public SK getSecondKey() {
    return secondKey;
  }

  public V getValue() {
    return value;
  }

  public ICacheEntryGroup getBelongedGroup() {
    return cacheEntryGroup;
  }

  public void setBelongedGroup(ICacheEntryGroup belongedGroup) {
    this.cacheEntryGroup = belongedGroup;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final CacheEntry<?, ?> that = (CacheEntry<?, ?>) o;
    return Objects.equals(secondKey, that.secondKey)
        && Objects.equals(cacheEntryGroup, that.cacheEntryGroup);
  }

  @Override
  public int hashCode() {
    return cacheEntryGroup.hashCode() * 31 + secondKey.hashCode();
  }
}
