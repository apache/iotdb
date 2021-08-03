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
package org.apache.iotdb.db.utils;

import org.apache.iotdb.tsfile.common.cache.Cache;
import org.apache.iotdb.tsfile.exception.cache.CacheException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class RandomDeleteCache<K, V> implements Cache<K, V> {

  private int cacheSize;
  private Map<K, V> cache;

  public RandomDeleteCache(int cacheSize) {
    this.cacheSize = cacheSize;
    this.cache = new ConcurrentHashMap<>();
  }

  @Override
  public V get(K key) throws CacheException {
    V v = cache.get(key);
    if (v == null) {
      randomRemoveObjectIfCacheIsFull();
      cache.put(key, loadObjectByKey(key));
      v = cache.get(key);
    }
    return v;
  }

  private void randomRemoveObjectIfCacheIsFull() {
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

  public abstract V loadObjectByKey(K key) throws CacheException;

  public void removeObject(K key) {
    cache.remove(key);
  }

  @Override
  public void clear() {
    cache.clear();
  }

  public int size() {
    return cache.size();
  }
}
