/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.commons.external.caffeine.cache;

import com.google.errorprone.annotations.Var;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.commons.external.caffeine.cache.Caffeine.calculateHashMapCapacity;
import static org.apache.iotdb.commons.external.caffeine.cache.Caffeine.hasMethodOverride;
import static org.apache.iotdb.commons.external.caffeine.cache.Caffeine.toUnchecked;

/**
 * This class provides a skeletal implementation of the {@link LoadingCache} interface to minimize
 * the effort required to implement a {@link LocalCache}.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
@SuppressWarnings("all")
interface LocalLoadingCache<K, V> extends LocalManualCache<K, V>, LoadingCache<K, V> {
  /** Returns the {@link AsyncCacheLoader} used by this cache. */
  CacheLoader<? super K, V> cacheLoader();

  /** Returns the {@link CacheLoader#load} as a mapping function. */
  Function<K, V> mappingFunction();

  /** Returns the {@link CacheLoader#loadAll} as a mapping function, if implemented. */
  Function<Set<? extends K>, Map<K, V>> bulkMappingFunction();

  @Override
  default V get(K key) {
    return cache().computeIfAbsent(key, mappingFunction());
  }

  @Override
  default Map<K, V> getAll(Iterable<? extends K> keys) {
    Function<Set<? extends K>, Map<K, V>> mappingFunction = bulkMappingFunction();
    return (mappingFunction == null) ? loadSequentially(keys) : getAll(keys, mappingFunction);
  }

  /** Sequentially loads each missing entry. */
  default Map<K, V> loadSequentially(Iterable<? extends K> keys) {
    var result = new LinkedHashMap<K, V>(calculateHashMapCapacity(keys));
    for (K key : keys) {
      result.put(requireNonNull(key), null);
    }

    @Var int count = 0;
    int size = result.size();
    try {
      for (var iter = result.entrySet().iterator(); iter.hasNext(); ) {
        Map.Entry<K, V> entry = iter.next();
        count++;

        V value = get(entry.getKey());
        if (value == null) {
          iter.remove();
        } else {
          entry.setValue(value);
        }
      }
    } catch (Throwable t) {
      cache().statsCounter().recordMisses(size - count);
      throw t;
    }
    @SuppressWarnings({"NullableProblems", "NullAway"})
    Map<K, V> unmodifiable = Collections.unmodifiableMap(result);
    return unmodifiable;
  }

  /** Returns a mapping function that adapts to {@link CacheLoader#load}. */
  static <K, V> Function<K, V> newMappingFunction(CacheLoader<? super K, V> cacheLoader) {
    return key -> {
      try {
        return cacheLoader.load(key);
      } catch (Exception e) {
        throw toUnchecked(e);
      }
    };
  }

  /** Returns a mapping function that adapts to {@link CacheLoader#loadAll}, if implemented. */
  static <K, V> Function<Set<? extends K>, Map<K, V>> newBulkMappingFunction(
      CacheLoader<? super K, V> cacheLoader) {
    if (!hasLoadAll(cacheLoader)) {
      return null;
    }
    return keysToLoad -> {
      try {
        @SuppressWarnings("unchecked")
        var loaded = (Map<K, V>) cacheLoader.loadAll(keysToLoad);
        return loaded;
      } catch (Exception e) {
        throw toUnchecked(e);
      }
    };
  }

  /** Returns whether the supplied cache loader has bulk load functionality. */
  static boolean hasLoadAll(CacheLoader<?, ?> cacheLoader) {
    return hasMethodOverride(CacheLoader.class, cacheLoader, "loadAll", Set.class);
  }
}
