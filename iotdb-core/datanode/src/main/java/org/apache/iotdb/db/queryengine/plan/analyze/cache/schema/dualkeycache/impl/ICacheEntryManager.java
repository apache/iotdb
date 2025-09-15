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

/**
 * This interface defines the behaviour of a cache entry manager, which takes the responsibility of
 * cache value status management and cache eviction.
 *
 * @param <FK> The first key of cache value.
 * @param <SK> The second key of cache value.
 * @param <V> The cache value.
 * @param <T> The cache entry holding cache value.
 */
interface ICacheEntryManager<FK, SK, V, T extends ICacheEntry<SK, V>> {

  T createCacheEntry(
      final SK secondKey, final V value, final ICacheEntryGroup<FK, SK, V, T> cacheEntryGroup);

  void access(final T cacheEntry);

  void put(final T cacheEntry);

  // A cacheEntry is removed iff the caller has called "invalid" and it returns "true"
  // Shall never remove a cacheEntry directly or when the "invalid" returns false
  boolean invalidate(final T cacheEntry);

  // The "evict" is allowed to be synchronized called
  // Inner implementation guarantees that an entry won't be concurrently evicted
  T evict();

  void cleanUp();
}
