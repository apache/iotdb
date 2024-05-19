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

package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.container;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

// The value in this map shall not be null.
// Therefore, when using compute method, use v==null to judge if there's existing value.
public class KeyNullableConcurrentHashMap<K, V> implements Map<K, V> {

  private final Map<Optional<K>, V> map = new ConcurrentHashMap<>();

  @Override
  public int size() {
    return map.size();
  }

  @Override
  public boolean isEmpty() {
    return map.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    return map.containsKey(Optional.ofNullable(key));
  }

  @Override
  public boolean containsValue(Object value) {
    return map.containsValue(value);
  }

  @Override
  public V get(Object key) {
    return map.get(Optional.ofNullable(key));
  }

  @Override
  public V put(K key, V value) {
    return map.put(Optional.ofNullable(key), value);
  }

  @Override
  public V remove(Object key) {
    return map.remove(Optional.ofNullable(key));
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> m) {
    m.forEach((k, v) -> map.put(Optional.ofNullable(k), v));
  }

  @Override
  public void clear() {
    map.clear();
  }

  @Override
  public Set<K> keySet() {
    return map.keySet().stream().map(k -> k.orElse(null)).collect(Collectors.toSet());
  }

  @Override
  public Collection<V> values() {
    return map.values();
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    return map.entrySet().stream()
        .map(
            o ->
                new Entry<K, V>() {
                  @Override
                  public K getKey() {
                    return o.getKey().orElse(null);
                  }

                  @Override
                  public V getValue() {
                    return o.getValue();
                  }

                  @Override
                  public V setValue(V value) {
                    return o.setValue(value);
                  }
                })
        .collect(Collectors.toSet());
  }

  @Override
  public V getOrDefault(Object key, V defaultValue) {
    return map.getOrDefault(Optional.ofNullable(key), defaultValue);
  }

  @Override
  public void forEach(BiConsumer<? super K, ? super V> action) {
    map.forEach((k, v) -> action.accept(k.orElse(null), v));
  }

  @Override
  public void replaceAll(BiFunction<? super K, ? super V, ? extends V> function) {
    map.replaceAll((k, v) -> function.apply(k.orElse(null), v));
  }

  @Override
  public V putIfAbsent(K key, V value) {
    return map.putIfAbsent(Optional.ofNullable(key), value);
  }

  @Override
  public boolean remove(Object key, Object value) {
    return map.remove(Optional.ofNullable(key), value);
  }

  @Override
  public boolean replace(K key, V oldValue, V newValue) {
    return map.replace(Optional.ofNullable(key), oldValue, newValue);
  }

  @Override
  public V replace(K key, V value) {
    return map.replace(Optional.ofNullable(key), value);
  }

  @Override
  public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
    return map.computeIfAbsent(
        Optional.ofNullable(key), k -> mappingFunction.apply(k.orElse(null)));
  }

  @Override
  public V computeIfPresent(
      K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
    return map.computeIfPresent(
        Optional.ofNullable(key), (k, v) -> remappingFunction.apply(k.orElse(null), v));
  }

  @Override
  public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
    return map.compute(
        Optional.ofNullable(key), (k, v) -> remappingFunction.apply(k.orElse(null), v));
  }

  @Override
  public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
    return map.merge(Optional.ofNullable(key), value, remappingFunction);
  }
}
