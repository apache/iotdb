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

import org.apache.iotdb.commons.i18n.CommonMessages;

import java.io.Serializable;

import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.commons.external.caffeine.cache.Caffeine.requireArgument;

/**
 * Calculates the weights of cache entries. The total weight threshold is used to determine when an
 * eviction is required.
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 * @author ben.manes@gmail.com (Ben Manes)
 */
@FunctionalInterface
@SuppressWarnings("all")
public interface Weigher<K, V> {

  /**
   * Returns the weight of a cache entry. There is no unit for entry weights; rather they are simply
   * relative to each other.
   *
   * @param key the key to weigh
   * @param value the value to weigh
   * @return the weight of the entry; must be non-negative
   */
  int weigh(K key, V value);

  /**
   * Returns a weigher where an entry has a weight of {@code 1}.
   *
   * @param <K> the type of keys
   * @param <V> the type of values
   * @return a weigher where an entry has a weight of {@code 1}
   */
  static <K, V> Weigher<K, V> singletonWeigher() {
    @SuppressWarnings("unchecked")
    var instance = (Weigher<K, V>) SingletonWeigher.INSTANCE;
    return instance;
  }

  /**
   * Returns a weigher that enforces that the weight is non-negative.
   *
   * @param delegate the weigher to weighs the entry
   * @param <K> the type of keys
   * @param <V> the type of values
   * @return a weigher that enforces that the weight is non-negative
   */
  static <K, V> Weigher<K, V> boundedWeigher(Weigher<K, V> delegate) {
    return new BoundedWeigher<>(delegate);
  }
}

@SuppressWarnings("all")
enum SingletonWeigher implements Weigher<Object, Object> {
  INSTANCE;

  @Override
  public int weigh(Object key, Object value) {
    return 1;
  }
}

@SuppressWarnings("all")
final class BoundedWeigher<K, V> implements Weigher<K, V>, Serializable {
  private static final long serialVersionUID = 1;

  @SuppressWarnings("serial")
  final Weigher<? super K, ? super V> delegate;

  BoundedWeigher(Weigher<? super K, ? super V> delegate) {
    this.delegate = requireNonNull(delegate);
  }

  @Override
  public int weigh(K key, V value) {
    int weight = delegate.weigh(key, value);
    requireArgument(
        weight >= 0, CommonMessages.EXCEPTION_WEIGHT_CANNOT_BE_NEGATIVE_ARG_224339E2, weight);
    return weight;
  }

  Object writeReplace() {
    return delegate;
  }
}
