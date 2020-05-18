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

import java.sql.Time;
import java.util.HashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LastCacheManager {

  private HashMap<String, StorageGroupLastCache> storageGroupMap;

  private LastCacheManager() {
    storageGroupMap = new HashMap<>();
  }

  public static LastCacheManager getInstance() {
    return SingletonClassInstance.instance;
  }

  public TimeValuePair get(String timeseriesPath) throws MetadataException {
    String storageGroupName = MManager.getInstance().getStorageGroupName(timeseriesPath);
    if (storageGroupMap.containsKey(storageGroupName)) {
      return storageGroupMap.get(storageGroupName).get(timeseriesPath);
    } else {
      StorageGroupLastCache newEntry = new StorageGroupLastCache(storageGroupName);
      storageGroupMap.put(storageGroupName, newEntry);
      return newEntry.get(timeseriesPath);
    }
  }

  public void put(String timeseriesPath, TimeValuePair timeValuePair, boolean highPriorityUpdate)
      throws MetadataException {
    String storageGroupName = MManager.getInstance().getStorageGroupName(timeseriesPath);
    if (storageGroupMap.containsKey(storageGroupName)) {
      storageGroupMap.get(storageGroupName).put(timeseriesPath, timeValuePair, highPriorityUpdate);
    } else {
      StorageGroupLastCache newEntry = new StorageGroupLastCache(storageGroupName);
      newEntry.put(timeseriesPath, timeValuePair, highPriorityUpdate);
      storageGroupMap.put(storageGroupName, newEntry);
    }
  }

  public void clearCache(String timeseriesPath) throws MetadataException {
    String storageGroupName = MManager.getInstance().getStorageGroupName(timeseriesPath);
    if (storageGroupMap.get(storageGroupName) != null) {
      storageGroupMap.get(storageGroupName).clear();
    }
  }

  private static class SingletonClassInstance{
    private static final LastCacheManager instance = new LastCacheManager();
  }

  private static class StorageGroupLastCache {
    private String storageGroupName;
    private HashMap<String, TimeValuePair> lastCache;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public StorageGroupLastCache(String name) {
      this.storageGroupName = name;
      this.lastCache = new HashMap<>();
    }

    TimeValuePair get(String key) {
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

    void put(String key, TimeValuePair timeValuePair, boolean highPriorityUpdate) {
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
          TimeValuePair cachedPair =
              new TimeValuePair(timeValuePair.getTimestamp(), timeValuePair.getValue());
          lastCache.put(key, cachedPair);
        }
      } finally {
        lock.writeLock().unlock();
      }
    }

    void clear() {
      lock.writeLock().lock();
      if (lastCache != null) {
        lastCache.clear();
      }
      lock.writeLock().unlock();
    }

    String getName() {
      return storageGroupName;
    }

    void setName(String name) {
      this.storageGroupName = name;
    }
  }
}
