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

import org.apache.iotdb.github.benmanes.caffeine.cache.References.WeakKeyReference;

import java.lang.ref.ReferenceQueue;

/**
 * <em>WARNING: GENERATED CODE</em>
 *
 * <p>A factory for cache nodes optimized for a particular configuration.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
interface NodeFactory<K, V> {
  Object RETIRED_STRONG_KEY = new Object();

  Object DEAD_STRONG_KEY = new Object();

  WeakKeyReference<Object> RETIRED_WEAK_KEY = new WeakKeyReference<Object>(null, null);

  WeakKeyReference<Object> DEAD_WEAK_KEY = new WeakKeyReference<Object>(null, null);

  /** Returns a node optimized for the specified features. */
  Node<K, V> newNode(
      K key,
      ReferenceQueue<K> keyReferenceQueue,
      V value,
      ReferenceQueue<V> valueReferenceQueue,
      int weight,
      long now);

  /** Returns a node optimized for the specified features. */
  Node<K, V> newNode(
      Object keyReference, V value, ReferenceQueue<V> valueReferenceQueue, int weight, long now);

  /**
   * Returns a key suitable for inserting into the cache. If the cache holds keys strongly then the
   * key is returned. If the cache holds keys weakly then a {@link WeakKeyReference<K>} holding the
   * key argument is returned.
   */
  default Object newReferenceKey(K key, ReferenceQueue<K> referenceQueue) {
    return key;
  }

  /**
   * Returns a key suitable for looking up an entry in the cache. If the cache holds keys strongly
   * then the key is returned. If the cache holds keys weakly then a {@link
   * org.apache.iotdb.github.benmanes.caffeine.cache.References.LookupKeyReference} holding the key
   * argument is returned.
   */
  default Object newLookupKey(Object key) {
    return key;
  }

  /** Returns a factory optimized for the specified features. */
  static <K, V> NodeFactory<K, V> newFactory(Caffeine<K, V> builder, boolean isAsync) {
    StringBuilder sb = new StringBuilder("org.apache.iotdb.github.benmanes.caffeine.cache.");
    if (builder.isStrongKeys()) {
      sb.append('P');
    } else {
      sb.append('F');
    }
    if (builder.isStrongValues()) {
      sb.append('S');
    } else if (builder.isWeakValues()) {
      sb.append('W');
    } else {
      sb.append('D');
    }
    if (builder.expiresVariable()) {
      if (builder.refreshAfterWrite()) {
        sb.append('A');
        if (builder.evicts()) {
          sb.append('W');
        }
      } else {
        sb.append('W');
      }
    } else {
      if (builder.expiresAfterAccess()) {
        sb.append('A');
      }
      if (builder.expiresAfterWrite()) {
        sb.append('W');
      }
    }
    if (builder.refreshAfterWrite()) {
      sb.append('R');
    }
    if (builder.evicts()) {
      sb.append('M');
      if (isAsync || (builder.isWeighted() && (builder.weigher != Weigher.singletonWeigher()))) {
        sb.append('W');
      } else {
        sb.append('S');
      }
    }
    try {
      Class<?> clazz = Class.forName(sb.toString());
      @SuppressWarnings("unchecked")
      NodeFactory<K, V> factory = (NodeFactory<K, V>) clazz.getDeclaredConstructor().newInstance();
      return factory;
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(sb.toString(), e);
    }
  }

  /** Returns whether this factory supports weak values. */
  default boolean weakValues() {
    return false;
  }

  /** Returns whether this factory supports soft values. */
  default boolean softValues() {
    return false;
  }
}
