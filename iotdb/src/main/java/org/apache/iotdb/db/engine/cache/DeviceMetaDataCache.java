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
package org.apache.iotdb.db.engine.cache;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to cache <code>DeviceMetaDataCache</code> of tsfile in IoTDB.
 */
public class DeviceMetaDataCache {

  private static final Logger logger = LoggerFactory.getLogger(DeviceMetaDataCache.class);

  private static final int CACHE_SIZE = 100;
  /**
   * key: the file path + deviceId.
   */
  private LinkedHashMap<String, TsDeviceMetadata> lruCache;

  private AtomicLong cacheHintNum = new AtomicLong();
  private AtomicLong cacheRequestNum = new AtomicLong();

  private DeviceMetaDataCache(int cacheSize) {
    lruCache = new LruLinkedHashMap(cacheSize, true);
  }

  public static DeviceMetaDataCache getInstance() {
    return RowGroupBlockMetaDataCacheSingleton.INSTANCE;
  }

  /**
   * get {@link TsDeviceMetadata}. THREAD SAFE.
   */
  public TsDeviceMetadata get(String filePath, String deviceId, TsFileMetaData fileMetaData)
      throws IOException {
    // The key(the tsfile path and deviceId) for the lruCache

    String jointPath = filePath + deviceId;
    Object jointPathObject = jointPath.intern();
    synchronized (lruCache) {
      cacheRequestNum.incrementAndGet();
      if (lruCache.containsKey(jointPath)) {
        cacheHintNum.incrementAndGet();
        if (logger.isDebugEnabled()) {
          logger.debug(
              "Cache hint: the number of requests for cache is {}, "
                  + "the number of hints for cache is {}",
              cacheRequestNum.get(), cacheHintNum.get());
        }
        return lruCache.get(jointPath);
      }
    }
    synchronized (jointPathObject) {
      synchronized (lruCache) {
        if (lruCache.containsKey(jointPath)) {
          return lruCache.get(jointPath);
        }
      }
      if (logger.isDebugEnabled()) {
        logger.debug("Cache didn't hint: the number of requests for cache is {}",
            cacheRequestNum.get());
      }
      TsDeviceMetadata blockMetaData = TsFileMetadataUtils
          .getTsRowGroupBlockMetaData(filePath, deviceId,
              fileMetaData);
      synchronized (lruCache) {
        lruCache.put(jointPath, blockMetaData);
        return lruCache.get(jointPath);
      }
    }
  }

  /**
   * clear LRUCache.
   */
  public void clear() {
    synchronized (lruCache) {
      lruCache.clear();
    }
  }

  /**
   * the default LRU cache size is 100. The singleton pattern.
   */
  private static class RowGroupBlockMetaDataCacheSingleton {

    private static final DeviceMetaDataCache INSTANCE = new
        DeviceMetaDataCache(CACHE_SIZE);
  }

  /**
   * This class is a map used to cache the <code>RowGroupBlockMetaData</code>. The caching strategy
   * is LRU.
   *
   */
  private class LruLinkedHashMap extends LinkedHashMap<String, TsDeviceMetadata> {

    private static final long serialVersionUID = 1290160928914532649L;
    private static final float LOAD_FACTOR_MAP = 0.75f;
    private int maxCapacity;

    public LruLinkedHashMap(int maxCapacity, boolean isLru) {
      super(maxCapacity, LOAD_FACTOR_MAP, isLru);
      this.maxCapacity = maxCapacity;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<String, TsDeviceMetadata> eldest) {
      return size() > maxCapacity;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      return super.equals(o);
    }

    @Override
    public int hashCode() {
      return super.hashCode();
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DeviceMetaDataCache that = (DeviceMetaDataCache) o;
    return Objects.equals(lruCache, that.lruCache) &&
            Objects.equals(cacheHintNum, that.cacheHintNum) &&
            Objects.equals(cacheRequestNum, that.cacheRequestNum);
  }

  @Override
  public int hashCode() {
    return Objects.hash(lruCache, cacheHintNum, cacheRequestNum);
  }
}
