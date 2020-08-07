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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iotdb.tsfile.exception.cache.CacheException;

public abstract class RandomDeleteCache<K, V>  {

  private int cacheSize;
  private Map<K, V> cache;

  protected RandomDeleteCache(int cacheSize) {
    this.cacheSize = cacheSize;
    this.cache = new ConcurrentHashMap<>();
  }


  public V get(K key, List<K> keyNodes) throws CacheException {
    V v = cache.get(key);
    if (v == null) {
      randomRemoveObjectIfCacheIsFull();
      cache.put(key, loadObjectByKey(key, keyNodes));
      v = cache.get(key);
    }
    return v;
  }

  public abstract V loadObjectByKey(K key, List<K> keyNodes) throws CacheException;


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

  public void removeObject(K key) {
    cache.remove(key);
  }

  public void clear() {
    cache.clear();
  }

  public int size() {
    return cache.size();
  }
}
