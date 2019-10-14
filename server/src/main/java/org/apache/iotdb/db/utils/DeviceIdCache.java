package org.apache.iotdb.db.utils;

import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.StorageGroupException;
import org.apache.iotdb.tsfile.exception.cache.CacheException;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public abstract class DeviceIdCache {

  private int cacheSize;
  private List<String> cache;

  public DeviceIdCache(int cacheSize) {
    this.cacheSize = cacheSize;
    this.cache= new CopyOnWriteArrayList<>();
  }

  public void get(String deviceId) throws CacheException, StorageGroupException, PathErrorException {
    if (!cache.contains(deviceId)) {
      randomRemoveObjectIfCacheIsFull();
      loadObject(deviceId);
      cache.add(deviceId);
    }
  }

  private void randomRemoveObjectIfCacheIsFull() throws CacheException {
    if (cache.size() == this.cacheSize) {
      removeFirstObject();
    }
  }

  private void removeFirstObject() throws CacheException {
    if (cache.size() == 0) {
      return;
    }
    String deviceId = cache.iterator().next();
    cache.remove(deviceId);
  }

  public abstract void beforeRemove(String deviceId) throws CacheException;

  public abstract void loadObject(String deviceId) throws StorageGroupException, PathErrorException;

  public void clear() {
    cache.size();
  }

  public int size() {
    return cache.size();
  }
}
