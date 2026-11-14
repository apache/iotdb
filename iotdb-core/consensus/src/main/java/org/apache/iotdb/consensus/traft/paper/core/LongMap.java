/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.consensus.traft.paper.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.LongPredicate;

/**
 * An open-addressing map from long keys to values: lookups by a log index box no Long, and an entry
 * costs two array slots instead of a node and a boxed key. Long.MIN_VALUE cannot be a key.
 */
final class LongMap<V> {
  private static final long FREE = Long.MIN_VALUE;
  private long[] keys;
  private Object[] values;
  private int size;

  LongMap() {
    allocate(16);
  }

  private void allocate(int capacity) {
    keys = new long[capacity];
    Arrays.fill(keys, FREE);
    values = new Object[capacity];
  }

  private int slot(long key) {
    long h = key * 0x9E3779B97F4A7C15L;
    return (int) (h ^ (h >>> 32)) & (keys.length - 1);
  }

  private int find(long key) {
    for (int i = slot(key); ; i = (i + 1) & (keys.length - 1)) {
      if (keys[i] == key) {
        return i;
      }
      if (keys[i] == FREE) {
        return -1;
      }
    }
  }

  @SuppressWarnings("unchecked")
  V get(long key) {
    int i = find(key);
    return i < 0 ? null : (V) values[i];
  }

  boolean containsKey(long key) {
    return find(key) >= 0;
  }

  @SuppressWarnings("unchecked")
  V put(long key, V value) {
    if (key == FREE) {
      throw new IllegalArgumentException("reserved key");
    }
    Objects.requireNonNull(value);
    if (2 * (size + 1) > keys.length) {
      resize(2 * keys.length);
    }
    int i = slot(key);
    for (; keys[i] != FREE; i = (i + 1) & (keys.length - 1)) {
      if (keys[i] == key) {
        V old = (V) values[i];
        values[i] = value;
        return old;
      }
    }
    keys[i] = key;
    values[i] = value;
    size++;
    return null;
  }

  @SuppressWarnings("unchecked")
  V remove(long key) {
    int i = find(key);
    if (i < 0) {
      return null;
    }
    V old = (V) values[i];
    // Backward-shift deletion keeps every probe chain unbroken without tombstones.
    int mask = keys.length - 1;
    for (int j = (i + 1) & mask; keys[j] != FREE; j = (j + 1) & mask) {
      int home = slot(keys[j]);
      if (((j - home) & mask) >= ((j - i) & mask)) {
        keys[i] = keys[j];
        values[i] = values[j];
        i = j;
      }
    }
    keys[i] = FREE;
    values[i] = null;
    size--;
    return old;
  }

  int size() {
    return size;
  }

  boolean isEmpty() {
    return size == 0;
  }

  void clear() {
    if (keys.length > 16) {
      allocate(16);
    } else {
      Arrays.fill(keys, FREE);
      Arrays.fill(values, null);
    }
    size = 0;
  }

  void putAll(Map<Long, ? extends V> map) {
    for (Map.Entry<Long, ? extends V> e : map.entrySet()) {
      put(e.getKey(), e.getValue());
    }
  }

  /** The keys, in no particular order. */
  long[] keys() {
    long[] result = new long[size];
    int n = 0;
    for (long key : keys) {
      if (key != FREE) {
        result[n++] = key;
      }
    }
    return result;
  }

  /** The values, in no particular order. */
  @SuppressWarnings("unchecked")
  List<V> values() {
    List<V> result = new ArrayList<>(size);
    for (int i = 0; i < keys.length; i++) {
      if (keys[i] != FREE) {
        result.add((V) values[i]);
      }
    }
    return result;
  }

  void removeKeysIf(LongPredicate predicate) {
    for (long key : keys()) {
      if (predicate.test(key)) {
        remove(key);
      }
    }
  }

  /** Whether this map holds exactly the mappings of {@code map}. */
  boolean sameAs(Map<Long, ?> map) {
    if (map.size() != size) {
      return false;
    }
    for (Map.Entry<Long, ?> e : map.entrySet()) {
      if (!Objects.equals(get(e.getKey()), e.getValue())) {
        return false;
      }
    }
    return true;
  }

  private void resize(int capacity) {
    long[] oldKeys = keys;
    Object[] oldValues = values;
    allocate(capacity);
    size = 0;
    for (int i = 0; i < oldKeys.length; i++) {
      if (oldKeys[i] != FREE) {
        int j = slot(oldKeys[i]);
        while (keys[j] != FREE) {
          j = (j + 1) & (keys.length - 1);
        }
        keys[j] = oldKeys[i];
        values[j] = oldValues[i];
        size++;
      }
    }
  }
}
