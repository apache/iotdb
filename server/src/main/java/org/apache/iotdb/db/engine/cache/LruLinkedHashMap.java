/**
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

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * This class is an LRU cache. <b>Note: It's not thread safe.</b>
 */
public abstract class LRULinkedHashMap<K, V> extends LinkedHashMap<K, V> {

  private static final long serialVersionUID = 1290160928914532649L;
  private static final float LOAD_FACTOR_MAP = 0.75f;
  private static final int INITIAL_CAPACITY = 128;
  /**
   * maximum memory threshold.
   */
  private long maxMemInB;
  /**
   * current used memory.
   */
  private long usedMemInB;

  public LRULinkedHashMap(long maxMemInB, boolean isLru) {
    super(INITIAL_CAPACITY, LOAD_FACTOR_MAP, isLru);
    this.maxMemInB = maxMemInB;
  }

  @Override
  protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
    if (usedMemInB > maxMemInB) {
      usedMemInB -= calEntrySize(eldest.getKey(), eldest.getValue());
      return true;
    } else {
      return false;
    }
  }

  @Override
  public V put(K key, V value) {
    usedMemInB += calEntrySize(key, value);
    return super.put(key, value);
  }

  /**
   * approximately estimate the additional size of key and value.
   */
  protected abstract long calEntrySize(K key, V value);

  /**
   * calculate the proportion of used memory.
   */
  public double getUsedMemoryProportion() {
    return usedMemInB * 1.0 / maxMemInB;
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
