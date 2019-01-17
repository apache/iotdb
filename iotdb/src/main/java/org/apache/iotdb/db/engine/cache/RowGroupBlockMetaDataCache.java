/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.engine.cache;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to cache <code>RowGroupBlockMetaDataCache</code> of tsfile in IoTDB.
 *
 * @author liukun
 */
public class RowGroupBlockMetaDataCache {

  private static final int cacheSize = 100;
  private static Logger LOGGER = LoggerFactory.getLogger(RowGroupBlockMetaDataCache.class);
  /**
   * key: the file path + deviceId.
   */
  private LinkedHashMap<String, TsDeviceMetadata> lruCache;

  private AtomicLong cacheHintNum = new AtomicLong();
  private AtomicLong cacheRequestNum = new AtomicLong();

  private RowGroupBlockMetaDataCache(int cacheSize) {
    lruCache = new LruLinkedHashMap(cacheSize, true);
  }

  public static RowGroupBlockMetaDataCache getInstance() {
    return RowGroupBlockMetaDataCacheSingleton.INSTANCE;
  }

  /**
   * get {@link TsDeviceMetadata}. THREAD SAFE.
   */
  public TsDeviceMetadata get(String filePath, String deviceId, TsFileMetaData fileMetaData)
      throws IOException {
    // The key(the tsfile path and deviceId) for the lruCache

    String jointPath = filePath + deviceId;
    jointPath = jointPath.intern();
    synchronized (lruCache) {
      cacheRequestNum.incrementAndGet();
      if (lruCache.containsKey(jointPath)) {
        cacheHintNum.incrementAndGet();
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(
              "Cache hint: the number of requests for cache is {}, "
                  + "the number of hints for cache is {}",
              cacheRequestNum.get(), cacheHintNum.get());
        }
        return lruCache.get(jointPath);
      }
    }
    synchronized (jointPath) {
      synchronized (lruCache) {
        if (lruCache.containsKey(jointPath)) {
          return lruCache.get(jointPath);
        }
      }
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Cache didn't hint: the number of requests for cache is {}",
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

    private static final RowGroupBlockMetaDataCache INSTANCE = new RowGroupBlockMetaDataCache(
        cacheSize);
  }

  /**
   * This class is a map used to cache the <code>RowGroupBlockMetaData</code>. The caching strategy
   * is LRU.
   *
   * @author liukun
   */
  private class LruLinkedHashMap extends LinkedHashMap<String, TsDeviceMetadata> {

    private static final long serialVersionUID = 1290160928914532649L;
    private static final float loadFactor = 0.75f;
    private int maxCapacity;

    public LruLinkedHashMap(int maxCapacity, boolean isLru) {
      super(maxCapacity, loadFactor, isLru);
      this.maxCapacity = maxCapacity;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<String, TsDeviceMetadata> eldest) {
      return size() > maxCapacity;
    }
  }
}
