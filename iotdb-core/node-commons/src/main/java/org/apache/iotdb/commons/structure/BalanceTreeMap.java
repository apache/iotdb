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

package org.apache.iotdb.commons.structure;

import org.apache.iotdb.commons.utils.TestOnly;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeMap;

/**
 * This class is used to store key-value pairs. It supports the following operations: 1. Put a
 * key-value pair. 2. Get key with minimum value.
 *
 * @param <K> The type of Key
 * @param <V> The type of Value, should be Comparable
 */
public class BalanceTreeMap<K, V extends Comparable<V>> {

  private final HashMap<K, V> keyValueMap;
  private final TreeMap<V, Set<K>> valueKeysMap;

  public BalanceTreeMap() {
    this.keyValueMap = new HashMap<>();
    this.valueKeysMap = new TreeMap<>();
  }

  /**
   * Put or modify a key-value pair.
   *
   * @param key Key
   * @param value Value
   */
  public void put(K key, V value) {
    // Update keyValueMap
    V oldValue = keyValueMap.put(key, value);

    // Update valueKeyMap
    if (oldValue != null) {
      Set<K> keysSet = valueKeysMap.get(oldValue);
      keysSet.remove(key);
      if (keysSet.isEmpty()) {
        valueKeysMap.remove(oldValue);
      }
    }
    valueKeysMap.computeIfAbsent(value, empty -> new HashSet<>()).add(key);
  }

  /**
   * Get key with minimum value.
   *
   * @return Key with minimum value
   */
  public K getKeyWithMinValue() {
    return valueKeysMap.firstEntry().getValue().iterator().next();
  }

  public V get(K key) {
    return keyValueMap.getOrDefault(key, null);
  }

  public boolean containsKey(K key) {
    return keyValueMap.containsKey(key);
  }

  public int size() {
    return keyValueMap.size();
  }

  @TestOnly
  public void remove(K key) {
    V value = keyValueMap.remove(key);
    if (value != null) {
      Set<K> keysSet = valueKeysMap.get(value);
      keysSet.remove(key);
      if (keysSet.isEmpty()) {
        valueKeysMap.remove(value);
      }
    }
  }

  @TestOnly
  public boolean isEmpty() {
    return keyValueMap.isEmpty() && valueKeysMap.isEmpty();
  }
}
