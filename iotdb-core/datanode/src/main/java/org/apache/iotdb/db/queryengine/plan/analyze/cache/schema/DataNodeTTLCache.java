package org.apache.iotdb.db.queryengine.plan.analyze.cache.schema;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.schema.ttl.TTLCache;

import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DataNodeTTLCache {
  private final TTLCache ttlCache;

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  private DataNodeTTLCache() {
    ttlCache = new TTLCache();
  }

  public static DataNodeTTLCache getInstance() {
    return DataNodeTTLCacheHolder.INSTANCE;
  }

  private static class DataNodeTTLCacheHolder {
    private static final DataNodeTTLCache INSTANCE = new DataNodeTTLCache();
  }

  public void setTTL(String path, long ttl) {
    lock.writeLock().lock();
    try {
      ttlCache.setTTL(path.split(String.valueOf(IoTDBConstant.PATH_SEPARATOR)), ttl);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void setTTL(Map<String, Long> pathTTLs) {
    lock.writeLock().lock();
    try {
      pathTTLs.forEach(
          (k, v) -> ttlCache.setTTL(k.split(String.valueOf(IoTDBConstant.PATH_SEPARATOR)), v));
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void unsetTTL(String path) {
    lock.writeLock().lock();
    try {
      ttlCache.unsetTTL(path.split(String.valueOf(IoTDBConstant.PATH_SEPARATOR)));
    } finally {
      lock.writeLock().unlock();
    }
  }
}
