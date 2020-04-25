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

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.utils.FileLoaderUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class is used to cache <code>TsFileMetaData</code> of tsfile in IoTDB.
 */
public class TsFileMetaDataCache {

  private static final Logger logger = LoggerFactory.getLogger(TsFileMetaDataCache.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private static boolean cacheEnable = config.isMetaDataCacheEnable();
  private static final long MEMORY_THRESHOLD_IN_B = config.getAllocateMemoryForFileMetaDataCache();

  /**
   * TsFile path -> TsFileMetaData
   */
  private final LRULinkedHashMap<String, TsFileMetadata> cache;
  private final ReadWriteLock lock = new ReentrantReadWriteLock();

  private AtomicLong cacheHitNum = new AtomicLong();
  private AtomicLong cacheRequestNum = new AtomicLong();

  /**
   * estimated size of a deviceMetaDataMap entry in TsFileMetaData.
   */
  private long deviceIndexMapEntrySize = 0;

  private TsFileMetaDataCache() {
    logger.info("TsFileMetaDataCache size = " + MEMORY_THRESHOLD_IN_B);
    cache = new LRULinkedHashMap<String, TsFileMetadata>(MEMORY_THRESHOLD_IN_B, true) {
      @Override
      protected long calEntrySize(String key, TsFileMetadata value) {
        if (deviceIndexMapEntrySize == 0 && value.getDeviceMetadataIndex() != null
            && value.getDeviceMetadataIndex().size() > 0) {
          deviceIndexMapEntrySize = RamUsageEstimator
              .sizeOf(value.getDeviceMetadataIndex().iterator().next());
        }
        // totalChunkNum, invalidChunkNum
        long valueSize = 4 + 4L;

        // deviceMetadataIndex
        if (value.getDeviceMetadataIndex() != null) {
          valueSize += value.getDeviceMetadataIndex().size() * deviceIndexMapEntrySize;
        }

        // versionInfo
        if (value.getVersionInfo() != null) {
          valueSize += value.getVersionInfo().size() * 16;
        }
        return key.getBytes(TSFileConfig.STRING_CHARSET).length + valueSize;
      }
    };
  }

  public static TsFileMetaDataCache getInstance() {
    return TsFileMetaDataCacheHolder.INSTANCE;
  }

  /**
   * get the TsFileMetaData for given TsFile.
   *
   * @param filePath -given TsFile
   */
  public TsFileMetadata get(String filePath) throws IOException {
    if (!cacheEnable) {
      return FileLoaderUtils.getTsFileMetadata(filePath);
    }

    cacheRequestNum.incrementAndGet();

    lock.readLock().lock();
    try {
      if (cache.containsKey(filePath)) {
        cacheHitNum.incrementAndGet();
        printCacheLog(true);
        return cache.get(filePath);
      }
    } finally {
      lock.readLock().unlock();
    }

    lock.writeLock().lock();
    try {
      if (cache.containsKey(filePath)) {
        cacheHitNum.incrementAndGet();
        printCacheLog(true);
        return cache.get(filePath);
      }
      printCacheLog(false);
      TsFileMetadata fileMetaData = FileLoaderUtils.getTsFileMetadata(filePath);
      cache.put(filePath, fileMetaData);
      return fileMetaData;
    } finally {
      lock.writeLock().unlock();
    }
  }

  private void printCacheLog(boolean isHit) {
    if (!logger.isDebugEnabled()) {
      return;
    }
    if (isHit) {
      logger.debug(
          "[TsFileMetaData cache hit] The number of requests for cache is {}, hit rate is {}.",
          cacheRequestNum.get(), cacheHitNum.get() * 1.0 / cacheRequestNum.get());
    } else {
      logger.debug(
          "[TsFileMetaData cache didn't hit] The number of requests for cache is {}, hit rate is {}.",
          cacheRequestNum.get(), cacheHitNum.get() * 1.0 / cacheRequestNum.get());
    }
  }

  double calculateTsFileMetaDataHitRatio() {
    if (cacheRequestNum.get() != 0) {
      return cacheHitNum.get() * 1.0 / cacheRequestNum.get();
    } else {
      return 0;
    }
  }

  public void remove(TsFileResource resource) {
    synchronized (cache) {
      cache.remove(resource.getPath());
    }
  }

  public void clear() {
    synchronized (cache) {
      cache.clear();
    }
  }

  /**
   * Singleton pattern
   */
  private static class TsFileMetaDataCacheHolder {

    private TsFileMetaDataCacheHolder() {
    }

    private static final TsFileMetaDataCache INSTANCE = new TsFileMetaDataCache();
  }
}
