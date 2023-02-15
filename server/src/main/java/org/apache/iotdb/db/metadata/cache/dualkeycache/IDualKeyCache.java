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

package org.apache.iotdb.db.metadata.cache.dualkeycache;

/**
 * This interfaces defines the behaviour of a dual key cache. A dual key cache supports manage cache
 * values via two keys, first key and second key. Simply, the structure is like fk -> sk-> value.
 *
 * @param <FK> The first key of cache value
 * @param <SK> The second key of cache value
 * @param <V> The cache value
 */
public interface IDualKeyCache<FK, SK, V> {

  /** Get the cache value with given first key and second key. */
  V get(FK firstKey, SK secondKey);

  /**
   * Traverse target cache values via given first key and second keys provided in computation and
   * execute the defined computation logic.
   */
  void compute(IDualKeyCacheComputation<FK, SK, V> computation);

  /** put the cache value into cache */
  void put(FK firstKey, SK secondKey, V value);

  /**
   * Invalidate all cache values in the cache and clear related cache keys. The cache status and
   * statistics won't be clear and they can still be accessed via cache.stats().
   */
  void invalidateAll();

  /**
   * Clean up all data and info of this cache, including cache keys, cache values and cache stats.
   */
  void cleanUp();

  /** Return all the current cache status and statistics. */
  IDualKeyCacheStats stats();
}
