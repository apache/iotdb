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
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
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
  private static StorageEngine storageEngine = StorageEngine.getInstance();
  private static boolean cacheEnable = config.isMetaDataCacheEnable();
  /**
   * key: file path dot deviceId dot sensorId.
   * <p>
   * value: chunkMetaData list of one timeseries in the file.
   */
  private LRULinkedHashMap<String, List<ChunkMetaData>> lruCache;

  private AtomicLong cacheHitNum = new AtomicLong();
  private AtomicLong cacheRequestNum = new AtomicLong();

  /**
   * approximate estimation of chunkMetaData size
   */
  private long chunkMetaDataSize = 0;

  private DeviceMetaDataCache(long memoryThreshold) {
    lruCache = new LRULinkedHashMap<String, List<ChunkMetaData>>(memoryThreshold, true) {
      @Override
      protected long calEntrySize(String key, List<ChunkMetaData> value) {
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
   * get {@link ChunkMetaData}. THREAD SAFE.
   */
  public List<ChunkMetaData> get(TsFileResource resource, Path seriesPath)
      throws IOException {
    if (!cacheEnable) {
      TsFileMetaData fileMetaData = TsFileMetaDataCache.getInstance().get(resource);
      // bloom filter part
      BloomFilter bloomFilter = fileMetaData.getBloomFilter();
      if (bloomFilter != null && !bloomFilter.contains(seriesPath.getFullPath())) {
        if (logger.isDebugEnabled()) {
          logger.debug("path not found by bloom filter, file is: " + resource.getFile() + " path is: " + seriesPath);
        }
        return new ArrayList<>();
      }
      //
      TsDeviceMetadata deviceMetaData = TsFileMetadataUtils
          .getTsDeviceMetaData(resource, seriesPath, fileMetaData);
      // If measurement isn't included in the tsfile, empty list is returned.
      if (deviceMetaData == null) {
        return new ArrayList<>();
      }
      return TsFileMetadataUtils.getChunkMetaDataList(seriesPath.getMeasurement(), deviceMetaData);
    }

    StringBuilder builder = new StringBuilder(resource.getFile().getPath()).append(".")
        .append(seriesPath
            .getDevice());
    String pathDeviceStr = builder.toString();
    String key = builder.append(".").append(seriesPath.getMeasurement()).toString();
    Object devicePathObject = pathDeviceStr.intern();

    synchronized (lruCache) {
      cacheRequestNum.incrementAndGet();
      if (lruCache.containsKey(key)) {
        cacheHitNum.incrementAndGet();
        printCacheLog(true);
        return new ArrayList<>(lruCache.get(key));
      }
    }
    synchronized (devicePathObject) {
      synchronized (lruCache) {
        if (lruCache.containsKey(key)) {
          printCacheLog(true);
          cacheHitNum.incrementAndGet();
          return new ArrayList<>(lruCache.get(key));
        }
      }
      printCacheLog(false);
      TsFileMetaData fileMetaData = TsFileMetaDataCache.getInstance().get(resource);
      // bloom filter part
      BloomFilter bloomFilter = fileMetaData.getBloomFilter();
      if (bloomFilter != null && !bloomFilter.contains(seriesPath.getFullPath())) {
        if (logger.isDebugEnabled()) {
          logger.debug("path not found by bloom filter, file is: " + resource.getFile() + " path is: " + seriesPath);
        }
        return new ArrayList<>();
      }
      //
      TsDeviceMetadata deviceMetaData = TsFileMetadataUtils
          .getTsDeviceMetaData(resource, seriesPath, fileMetaData);
      // If measurement isn't included in the tsfile, empty list is returned.
      if (deviceMetaData == null) {
        return new ArrayList<>();
      }
      Map<Path, List<ChunkMetaData>> chunkMetaData = TsFileMetadataUtils
          .getChunkMetaDataList(calHotSensorSet(seriesPath), deviceMetaData);
      synchronized (lruCache) {
        chunkMetaData.forEach((path, chunkMetaDataList) -> {
          String k = pathDeviceStr + "." + path.getMeasurement();
          if (!lruCache.containsKey(k)) {
            lruCache.put(k, chunkMetaDataList);
          }
        });
        if (chunkMetaData.containsKey(seriesPath)) {
          return new ArrayList<>(chunkMetaData.get(seriesPath));
        }
        return new ArrayList<>();
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

  public double calculateChunkMetaDataHitRatio() {
    if (cacheRequestNum.get() != 0) {
      return cacheHitNum.get() * 1.0 / cacheRequestNum.get();
    } else {
      return 0;
    }
  }

  /**
   * calculate the most frequently query measurements set.
   *
   * @param seriesPath the series to be queried in a query statements.
   */
  private Set<String> calHotSensorSet(Path seriesPath) throws IOException {
    double usedMemProportion = lruCache.getUsedMemoryProportion();

    if (usedMemProportion < 0.6) {
      return new HashSet<>();
    } else {
      double hotSensorProportion;
      if (usedMemProportion < 0.8) {
        hotSensorProportion = 0.1;
      } else {
        hotSensorProportion = 0.05;
      }
      try {
        return storageEngine
            .calTopKMeasurement(seriesPath.getDevice(), seriesPath.getMeasurement(),
                hotSensorProportion);
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }

  /**
   * clear LRUCache.
   */
  public void clear() {
    synchronized (lruCache) {
      if (lruCache != null) {
        lruCache.clear();
      }
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