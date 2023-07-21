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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeMap;

public class BalanceTreeMap<K, V extends Comparable<V>> {

  private final HashMap<K, V> keyValueMap;
  private final TreeMap<V, Set<K>> valueKeyMap;

  public BalanceTreeMap() {
    this.keyValueMap = new HashMap<>();
    this.valueKeyMap = new TreeMap<>();
  }

  /**
   * Put or modify a key-value pair.
   *
   * @param key Key
   * @param value Value
   */
  public void put(K key, V value) {
    V oldValue = keyValueMap.getOrDefault(key, null);

    // Update keyValueMap
    keyValueMap.put(key, value);

    // Update valueKeyMap
    if (oldValue != null) {
      valueKeyMap.get(oldValue).remove(key);
      if (valueKeyMap.get(oldValue).isEmpty()) {
        valueKeyMap.remove(oldValue);
      }
    }
    valueKeyMap.computeIfAbsent(value, empty -> new HashSet<>()).add(key);
  }

  public V get(K key) {
    return keyValueMap.getOrDefault(key, null);
  }

  public boolean containsKey(K key) {
    return keyValueMap.containsKey(key);
  }

  /**
   * Get key with minimum value.
   *
   * @return Key with minimum value
   */
  public K getKeyWithMinValue() {
    return valueKeyMap.firstEntry().getValue().iterator().next();
  }
}
