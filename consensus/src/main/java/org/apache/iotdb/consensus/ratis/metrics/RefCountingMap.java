/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.consensus.ratis.metrics;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * A map of K to V, but does ref counting for added and removed values. The values are not added
 * directly, but instead requested from the given Supplier if ref count == 0. Each put() call will
 * increment the ref count, and each remove() will decrement it. The values are removed from the map
 * iff ref count == 0.
 */
class RefCountingMap<K, V> {
  private static class Payload<V> {
    private final V value;
    private final AtomicInteger refCount = new AtomicInteger();

    Payload(V v) {
      this.value = v;
    }

    V get() {
      return value;
    }

    V increment() {
      return refCount.incrementAndGet() > 0 ? value : null;
    }

    RefCountingMap.Payload<V> decrement() {
      return refCount.decrementAndGet() > 0 ? this : null;
    }
  }

  private final ConcurrentMap<K, RefCountingMap.Payload<V>> map = new ConcurrentHashMap<>();

  V put(K k, Supplier<V> supplier) {
    return map.compute(
            k, (k1, old) -> old != null ? old : new RefCountingMap.Payload<>(supplier.get()))
        .increment();
  }

  static <V> V get(RefCountingMap.Payload<V> p) {
    return p == null ? null : p.get();
  }

  V get(K k) {
    return get(map.get(k));
  }

  /**
   * Decrements the ref count of k, and removes from map if ref count == 0.
   *
   * @param k the key to remove
   * @return the value associated with the specified key or null if key is removed from map.
   */
  V remove(K k) {
    return get(map.computeIfPresent(k, (k1, v) -> v.decrement()));
  }

  void clear() {
    map.clear();
  }

  Set<K> keySet() {
    return map.keySet();
  }

  Collection<V> values() {
    return map.values().stream().map(RefCountingMap.Payload::get).collect(Collectors.toList());
  }

  int size() {
    return map.size();
  }
}
