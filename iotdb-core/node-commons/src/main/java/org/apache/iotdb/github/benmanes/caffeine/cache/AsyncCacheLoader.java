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

package org.apache.iotdb.github.benmanes.caffeine.cache;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Computes or retrieves values asynchronously, based on a key, for use in populating a {@link
 * AsyncLoadingCache}.
 *
 * <p>Most implementations will only need to implement {@link #asyncLoad}. Other methods may be
 * overridden as desired.
 *
 * <p>Usage example:
 *
 * <pre>{@code
 * AsyncCacheLoader<Key, Graph> loader = (key, executor) ->
 *     createExpensiveGraphAsync(key, executor);
 * AsyncLoadingCache<Key, Graph> cache = Caffeine.newBuilder().buildAsync(loader);
 * }</pre>
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
@FunctionalInterface
public interface AsyncCacheLoader<K, V> {

  /**
   * Asynchronously computes or retrieves the value corresponding to {@code key}.
   *
   * @param key the non-null key whose value should be loaded
   * @param executor the executor with which the entry is asynchronously loaded
   * @return the future value associated with {@code key}
   */
  CompletableFuture<V> asyncLoad(K key, Executor executor);

  /**
   * Asynchronously computes or retrieves the values corresponding to {@code keys}. This method is
   * called by {@link AsyncLoadingCache#getAll}.
   *
   * <p>If the returned map doesn't contain all requested {@code keys} then the entries it does
   * contain will be cached and {@code getAll} will return the partial results. If the returned map
   * contains extra keys not present in {@code keys} then all returned entries will be cached, but
   * only the entries for {@code keys} will be returned from {@code getAll}.
   *
   * <p>This method should be overridden when bulk retrieval is significantly more efficient than
   * many individual lookups. Note that {@link AsyncLoadingCache#getAll} will defer to individual
   * calls to {@link AsyncLoadingCache#get} if this method is not overridden.
   *
   * @param keys the unique, non-null keys whose values should be loaded
   * @param executor the executor with which the entries are asynchronously loaded
   * @return a future containing the map from each key in {@code keys} to the value associated with
   *     that key; <b>may not contain null values</b>
   */
  default CompletableFuture<Map<K, V>> asyncLoadAll(Iterable<? extends K> keys, Executor executor) {
    throw new UnsupportedOperationException();
  }

  /**
   * Asynchronously computes or retrieves a replacement value corresponding to an already-cached
   * {@code key}. If the replacement value is not found then the mapping will be removed if {@code
   * null} is computed. This method is called when an existing cache entry is refreshed by {@link
   * Caffeine#refreshAfterWrite}, or through a call to {@link LoadingCache#refresh}.
   *
   * <p><b>Note:</b> <i>all exceptions thrown by this method will be logged and then swallowed</i>.
   *
   * @param key the non-null key whose value should be loaded
   * @param oldValue the non-null old value corresponding to {@code key}
   * @param executor the executor with which the entry is asynchronously loaded
   * @return a future containing the new value associated with {@code key}, or containing {@code
   *     null} if the mapping is to be removed
   */
  default CompletableFuture<V> asyncReload(K key, V oldValue, Executor executor) {
    return asyncLoad(key, executor);
  }
}
