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
package org.apache.iotdb.tsfile.common.cache;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * This class is not thread safe.
 */
public abstract class LRUCache<K, T> implements Cache<K, T> {

  private int cacheSize;
  private Map<K, T> cache;

  public LRUCache(int cacheSize) {
    this.cacheSize = cacheSize;
    this.cache = new LinkedHashMap<>();
  }

  @Override
  public T get(K key) throws IOException {
    if (cache.containsKey(key)) {
      moveObjectToTail(key);
    } else {
      removeFirstObjectIfCacheIsFull();
      cache.put(key, loadObjectByKey(key));
    }
    return cache.get(key);
  }

  @Override
  public void clear() {
    this.cache.clear();
  }

  private void moveObjectToTail(K key) {
    T value = cache.get(key);
    cache.remove(key);
    cache.put(key, value);
  }

  private void removeFirstObjectIfCacheIsFull() {
    if (cache.size() == this.cacheSize) {
      removeFirstObject();
    }
  }

  private void removeFirstObject() {
    if (cache.size() == 0) {
      return;
    }
    K key = cache.keySet().iterator().next();
    cache.remove(key);
  }

  /**
   * function for putting a key-value pair.
   */
  public void put(K key, T value) {
    cache.remove(key);
    removeFirstObjectIfCacheIsFull();
    cache.put(key, value);
  }

  public abstract T loadObjectByKey(K key) throws IOException;

}
