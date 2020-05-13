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

import java.util.HashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LastCacheManager {
  private static final Logger logger = LoggerFactory.getLogger(LastCacheManager.class);

  private HashMap<String, TimeValuePair> lastCache;
  private final ReadWriteLock lock = new ReentrantReadWriteLock();

  private LastCacheManager() {
    lastCache = new HashMap<>();
  }

  public static LastCacheManager getInstance() {
    return SingletonClassInstance.instance;
  }

  public TimeValuePair get(String key) {
    try {
      lock.readLock().lock();
      if (lastCache.containsKey(key)) {
        return lastCache.get(key);
      }
      return new TimeValuePair(0, null);
    } finally {
      lock.readLock().unlock();
    }
  }

  public void put(String key, TimeValuePair timeValuePair, boolean highPriorityUpdate, Long latestFlushedTime) {
    if (timeValuePair == null || timeValuePair.getValue() == null) return;

    try {
      lock.writeLock().lock();
      if (lastCache.containsKey(key)) {
        TimeValuePair cachedPair = lastCache.get(key);
        if (timeValuePair.getTimestamp() > cachedPair.getTimestamp()
            || (timeValuePair.getTimestamp() == cachedPair.getTimestamp()
            && highPriorityUpdate)) {
          cachedPair.setTimestamp(timeValuePair.getTimestamp());
          cachedPair.setValue(timeValuePair.getValue());
        }
      } else {
        if (!highPriorityUpdate || latestFlushedTime <= timeValuePair.getTimestamp()) {
          TimeValuePair cachedPair =
              new TimeValuePair(timeValuePair.getTimestamp(), timeValuePair.getValue());
          lastCache.put(key, cachedPair);
        }
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  private static class SingletonClassInstance{
    private static final LastCacheManager instance = new LastCacheManager();
  }
}
