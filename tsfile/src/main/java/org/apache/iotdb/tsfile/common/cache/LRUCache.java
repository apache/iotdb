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
package org.apache.iotdb.tsfile.common.cache;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/** This class is not thread safe. */
public abstract class LRUCache<K, T> implements Cache<K, T> {

  protected Map<K, T> cache;

  public LRUCache(int cacheSize) {
    this.cache =
        new LinkedHashMap<K, T>(cacheSize, 0.75f, true) {
          @Override
          protected boolean removeEldestEntry(Map.Entry eldest) {
            return size() > cacheSize;
          }
        };
  }

  @Override
  public synchronized T get(K key) throws IOException {
    if (cache.containsKey(key)) {
      return cache.get(key);
    } else {
      T value = loadObjectByKey(key);
      if (value != null) {
        cache.put(key, value);
      }
      return value;
    }
  }

  @Override
  public synchronized void clear() {
    cache.clear();
  }

  public synchronized void put(K key, T value) {
    cache.put(key, value);
  }

  protected abstract T loadObjectByKey(K key) throws IOException;

  public synchronized void removeItem(K key) {
    cache.remove(key);
  }
}
