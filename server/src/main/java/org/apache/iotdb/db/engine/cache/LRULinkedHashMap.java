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

package org.apache.iotdb.db.engine.cache;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.iotdb.tsfile.common.cache.Accountable;

/**
 * This class is an LRU cache. <b>Note: It's not thread safe.</b>
 */
public abstract class LRULinkedHashMap<K extends Accountable, V> {

  private static final float LOAD_FACTOR_MAP = 0.75f;
  private static final int INITIAL_CAPACITY = 128;
  private static final float RETAIN_PERCENT = 0.9f;
  private static final int MAP_ENTRY_SIZE = 40;

  private final LinkedHashMap<K, V> linkedHashMap;

  /**
   * maximum memory threshold.
   */
  private final long maxMemory;
  /**
   * current used memory.
   */
  private long usedMemory;

  /**
   * memory size we need to retain while the cache is full
   */
  private final long retainMemory;

  protected int count = 0;
  protected long averageSize = 0;

  public LRULinkedHashMap(long maxMemory) {
    this.linkedHashMap = new LinkedHashMap<>(INITIAL_CAPACITY, LOAD_FACTOR_MAP);
    this.maxMemory = maxMemory;
    this.retainMemory = (long) (maxMemory * RETAIN_PERCENT);
  }

  public V put(K key, V value) {
    long size = calEntrySize(key, value) + MAP_ENTRY_SIZE;
    key.setRamSize(size);
    usedMemory += size;
    V v = linkedHashMap.put(key, value);
    if (usedMemory > maxMemory) {
      Iterator<Entry<K, V>> iterator = linkedHashMap.entrySet().iterator();
      while (usedMemory > retainMemory && iterator.hasNext()) {
        Entry<K, V> entry = iterator.next();
        usedMemory -= entry.getKey().getRamSize();
        iterator.remove();
      }
    }
    return v;
  }

  public V get(K key) {
    return linkedHashMap.get(key);
  }

  public boolean containsKey(K key) {
    return linkedHashMap.containsKey(key);
  }

  public void clear() {
    linkedHashMap.clear();
    usedMemory = 0;
    count = 0;
    averageSize = 0;
  }

  public V remove(K key) {
    V v = linkedHashMap.remove(key);
    if (v != null && key != null) {
      usedMemory -= key.getRamSize();
    }
    return v;
  }

  /**
   * approximately estimate the additional size of key and value.
   */
  protected abstract long calEntrySize(K key, V value);

  /**
   * calculate the proportion of used memory.
   */
  public double getUsedMemoryProportion() {
    return usedMemory * 1.0 / maxMemory;
  }

  public long getUsedMemory() {
    return usedMemory;
  }

  public long getMaxMemory() {
    return maxMemory;
  }

  public long getAverageSize() {
    return averageSize;
  }

  public Set<Entry<K, V>> entrySet() {
    return linkedHashMap.entrySet();
  }

  public boolean isEmpty() {
    return linkedHashMap.isEmpty();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    return super.equals(o);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }
}
