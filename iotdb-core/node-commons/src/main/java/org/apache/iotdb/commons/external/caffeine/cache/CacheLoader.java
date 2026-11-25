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

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * Computes or retrieves values, based on a key, for use in populating a {@link LoadingCache} or
 * {@link AsyncLoadingCache}.
 *
 * <p>Most implementations will only need to implement {@link #load}. Other methods may be
 * overridden as desired.
 *
 * <p>Usage example:
 *
 * {@snippet
 *  class=org.apache.iotdb.commons.external.caffeine.cache.Snippets region=loader_basic lang=java}
 *
 * @param <K> the type of keys
 * @param <V> the type of values. A loader may return null values if and only if it declares a
 *     nullable value type. (Null values are still never <i>stored</i> in the cache.)
 * @author ben.manes@gmail.com (Ben Manes)
 */
@FunctionalInterface
@SuppressWarnings({
  "all",
  "FunctionalInterfaceMethodChanged",
  "JavadocDeclaration",
  "JavadocReference",
  "PMD.SignatureDeclareThrowsException"
})
public interface CacheLoader<K, V> {

  /**
   * Computes or retrieves the value corresponding to {@code key}.
   *
   * <p><b>Warning:</b> loading <b>must not</b> attempt to update any mappings of this cache
   * directly.
   *
   * @param key the non-null key whose value should be loaded
   * @return the value associated with {@code key} or {@code null} if not found
   * @throws Exception or Error, in which case the mapping is unchanged
   * @throws InterruptedException if this method is interrupted. {@code InterruptedException} is
   *     treated like any other {@code Exception} in all respects except that, when it is caught,
   *     the thread's interrupt status is set
   */
  V load(K key) throws Exception;

  /**
   * Computes or retrieves the values corresponding to {@code keys}. This method is called by {@link
   * LoadingCache#getAll}.
   *
   * <p>If the returned map doesn't contain all requested {@code keys}, then the entries it does
   * contain will be cached, and {@code getAll} will return the partial results. If the returned map
   * contains extra keys not present in {@code keys} then all returned entries will be cached, but
   * only the entries for {@code keys} will be returned from {@code getAll}.
   *
   * <p>This method should be overridden when bulk retrieval is significantly more efficient than
   * many individual lookups. Note that {@link LoadingCache#getAll} will defer to individual calls
   * to {@link LoadingCache#get} if this method is not overridden.
   *
   * <p><b>Warning:</b> loading <b>must not</b> attempt to update any mappings of this cache
   * directly.
   *
   * @param keys the unique, non-null keys whose values should be loaded
   * @return a map from each key in {@code keys} to the value associated with that key; <b>may not
   *     contain null values</b>
   * @throws Exception or Error, in which case the mappings are unchanged
   * @throws InterruptedException if this method is interrupted. {@code InterruptedException} is
   *     treated like any other {@code Exception} in all respects except that, when it is caught,
   *     the thread's interrupt status is set
   */
  default Map<? extends K, ? extends V> loadAll(Set<? extends K> keys) throws Exception {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns a cache loader that delegates to the supplied mapping function for retrieving the
   * values. Note that {@link #load} will silently discard any additional mappings loaded when
   * retrieving the {@code key} prior to returning the value to the cache.
   *
   * <p>Usage example:
   *
   * {@snippet
   *  class=org.apache.iotdb.commons.external.caffeine.cache.Snippets region=loader_bulk lang=java}
   *
   * @param <K> the key type
   * @param <V> the value type
   * @param mappingFunction the function to compute the values
   * @return a cache loader that delegates to the supplied {@code mappingFunction}
   * @throws NullPointerException if the mappingFunction is null
   */
  @SuppressWarnings("FunctionalInterfaceClash")
  static <K, V> CacheLoader<K, V> bulk(
      Function<? super Set<? extends K>, ? extends Map<? extends K, ? extends V>> mappingFunction) {
    return new BulkCacheLoader<>(mappingFunction);
  }
}

/** A cache loader that delegates to a function that loads the values in bulk. */
@SuppressWarnings("all")
final class BulkCacheLoader<K, V> implements CacheLoader<K, V>, Serializable {
  private static final long serialVersionUID = 1L;

  @SuppressWarnings("serial")
  final Function<? super Set<? extends K>, ? extends Map<? extends K, ? extends V>> mappingFunction;

  BulkCacheLoader(
      Function<? super Set<? extends K>, ? extends Map<? extends K, ? extends V>> mappingFunction) {
    this.mappingFunction = requireNonNull(mappingFunction);
  }

  /*
   * If the caller passes a mapping function that may ever return partial results, then calls to
   * load() may return null. In that case, the caller should type the return value of bulk(...)
   * as a CacheLoader<Foo, Bar>, rather than a CacheLoader<Foo, Bar>.
   */
  @SuppressWarnings("NullAway")
  @Override
  public V load(K key) {
    return loadAll(Set.of(key)).get(key);
  }

  @Override
  public Map<? extends K, ? extends V> loadAll(Set<? extends K> keys) {
    return mappingFunction.apply(keys);
  }
}
