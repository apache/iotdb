package org.apache.iotdb.db.queryengine.plan.analyze.cache.schema;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.schema.ttl.TTLManager;

import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DataNodeTTLCache {
  private final TTLManager ttlManager;

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  private DataNodeTTLCache() {
    ttlManager = new TTLManager();
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
      ttlManager.setTTL(path.split(String.valueOf(IoTDBConstant.PATH_SEPARATOR)), ttl);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void setTTL(Map<String, Long> pathTTLs) {
    lock.writeLock().lock();
    try {
      pathTTLs.forEach(
          (k, v) -> ttlManager.setTTL(k.split(String.valueOf(IoTDBConstant.PATH_SEPARATOR)), v));
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void unsetTTL(String path) {
    lock.writeLock().lock();
    try {
      ttlManager.unsetTTL(path.split(String.valueOf(IoTDBConstant.PATH_SEPARATOR)));
    } finally {
      lock.writeLock().unlock();
    }
  }
}
