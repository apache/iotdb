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

import java.util.function.BiFunction;

/**
 * This interface defines the behaviour of a cache entry group, which is mainly accessed via first
 * key from dual key cache and holds all the second keys and cache values indexed by the first key.
 *
 * @param <FK> The first key of cache value.
 * @param <SK> The second key of cache value.
 * @param <V> The cache value.
 * @param <T> The cache entry holding cache value.
 */
interface ICacheEntryGroup<FK, SK, V, T extends ICacheEntry<SK, V>> {

  FK getFirstKey();

  T getCacheEntry(SK secondKey);

  T computeCacheEntry(SK secondKey, BiFunction<SK, T, T> computation);

  T removeCacheEntry(SK secondKey);

  boolean isEmpty();
}
