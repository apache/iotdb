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

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.BloomFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class is used to cache <code>List<ChunkMetaData></code> of tsfile in IoTDB. The caching
 * strategy is LRU.
 */
public class DeviceMetaDataCache {

  private static final Logger logger = LoggerFactory.getLogger(DeviceMetaDataCache.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final long MEMORY_THRESHOLD_IN_B = config.getAllocateMemoryForChunkMetaDataCache();
  private static boolean cacheEnable = config.isMetaDataCacheEnable();
  /**
   * key: file path dot deviceId dot sensorId.
   * <p>
   * value: chunkMetaData list of one timeseries in the file.
   */
  private final LRULinkedHashMap<String, List<ChunkMetadata>> lruCache;

  private AtomicLong cacheHitNum = new AtomicLong();
  private AtomicLong cacheRequestNum = new AtomicLong();

  /**
   * approximate estimation of chunkMetaData size
   */
  private long chunkMetaDataSize = 0;

  private DeviceMetaDataCache(long memoryThreshold) {
    lruCache = new LRULinkedHashMap<String, List<ChunkMetadata>>(memoryThreshold, true) {
      @Override
      protected long calEntrySize(String key, List<ChunkMetadata> value) {
        if (chunkMetaDataSize == 0 && !value.isEmpty()) {
          chunkMetaDataSize = RamUsageEstimator.sizeOf(value.get(0));
        }
        return value.size() * chunkMetaDataSize + key.length() * 2;
      }
    };
  }

  public static DeviceMetaDataCache getInstance() {
    return RowGroupBlockMetaDataCacheSingleton.INSTANCE;
  }

  /**
   * get {@link ChunkMetadata}. THREAD SAFE.
   */
  public List<ChunkMetadata> get(TsFileResource resource, Path seriesPath)
      throws IOException {
    if (!cacheEnable) {
      TsFileMetadata fileMetaData = TsFileMetaDataCache.getInstance().get(resource);
      // bloom filter part
      BloomFilter bloomFilter = fileMetaData.getBloomFilter();
      if (bloomFilter != null && !bloomFilter.contains(seriesPath.getFullPath())) {
        if (logger.isDebugEnabled()) {
          logger.debug("path not found by bloom filter, file is: " + resource.getFile() + " path is: " + seriesPath);
        }
        return new ArrayList<>();
      }
      // If timeseries isn't included in the tsfile, empty list is returned.
      TsFileSequenceReader tsFileReader = FileReaderManager.getInstance().get(resource, true);
      return tsFileReader.getChunkMetadataList(seriesPath);
    }

    String key = (resource.getFile().getPath() + IoTDBConstant.PATH_SEPARATOR
        + seriesPath.getDevice() + seriesPath.getMeasurement()).intern();

    synchronized (lruCache) {
      cacheRequestNum.incrementAndGet();
      if (lruCache.containsKey(key)) {
        cacheHitNum.incrementAndGet();
        printCacheLog(true);
        return new ArrayList<>(lruCache.get(key));
      }
    }
    synchronized (key) {
      synchronized (lruCache) {
        if (lruCache.containsKey(key)) {
          printCacheLog(true);
          cacheHitNum.incrementAndGet();
          return new ArrayList<>(lruCache.get(key));
        }
      }
      printCacheLog(false);
      TsFileMetadata fileMetaData = TsFileMetaDataCache.getInstance().get(resource);
      // bloom filter part
      BloomFilter bloomFilter = fileMetaData.getBloomFilter();
      if (bloomFilter != null && !bloomFilter.contains(seriesPath.getFullPath())) {
        if (logger.isDebugEnabled()) {
          logger.debug("path not found by bloom filter, file is: " + resource.getFile() + " path is: " + seriesPath);
        }
        return new ArrayList<>();
      }
      List<ChunkMetadata> chunkMetaDataList = TsFileMetadataUtils.getChunkMetaDataList(seriesPath, resource);
      synchronized (lruCache) {
        if (!lruCache.containsKey(key)) {
          lruCache.put(key, chunkMetaDataList);
        }
        return chunkMetaDataList;
      }
    }
  }

  private void printCacheLog(boolean isHit) {
    if (!logger.isDebugEnabled()) {
      return;
    }
    logger.debug(
        "[ChunkMetaData cache {}hit] The number of requests for cache is {}, hit rate is {}.",
        isHit ? "" : "didn't ", cacheRequestNum.get(),
        cacheHitNum.get() * 1.0 / cacheRequestNum.get());
  }

  double calculateChunkMetaDataHitRatio() {
    if (cacheRequestNum.get() != 0) {
      return cacheHitNum.get() * 1.0 / cacheRequestNum.get();
    } else {
      return 0;
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

  public void remove(TsFileResource resource) {
    synchronized (lruCache) {
      lruCache.entrySet().removeIf(e -> e.getKey().startsWith(resource.getFile().getPath()));
    }
  }

  /**
   * singleton pattern.
   */
  private static class RowGroupBlockMetaDataCacheSingleton {

    private static final DeviceMetaDataCache INSTANCE = new
        DeviceMetaDataCache(MEMORY_THRESHOLD_IN_B);
  }
}