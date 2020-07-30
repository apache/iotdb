package org.apache.iotdb.db.rescon;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CachedStringPool {

  private Map<String, String> cachedStringPool = new ConcurrentHashMap<>();

  public Map<String, String> getCachedStringPool() {
    return cachedStringPool;
  }

  public static CachedStringPool getInstance() {
    return CachedStringPool.InstanceHolder.INSTANCE;
  }

  private static class InstanceHolder {

    private static final CachedStringPool INSTANCE = new CachedStringPool();

    private InstanceHolder() {
    }
  }
}
