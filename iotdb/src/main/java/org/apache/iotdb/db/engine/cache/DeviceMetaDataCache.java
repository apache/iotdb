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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to cache <code>List<ChunkMetaData></code> of tsfile in IoTDB. The caching
 * strategy is LRU.
 */
public class DeviceMetaDataCache {

  private static final Logger logger = LoggerFactory.getLogger(DeviceMetaDataCache.class);

  private static StorageEngine storageEngine = StorageEngine.getInstance();

  private static final int MEMORY_THRESHOLD_IN_B = (int) (50*0.3*1024*1024*1024);
  /**
   * key: the file path. value: key: series path, value: list of chunkMetaData.
   */
  private LruLinkedHashMap<String, ConcurrentHashMap<Path, List<ChunkMetaData>>> lruCache;

  private AtomicLong cacheHintNum = new AtomicLong();
  private AtomicLong cacheRequestNum = new AtomicLong();

  private DeviceMetaDataCache(int cacheSize) {
    lruCache = new LruLinkedHashMap(cacheSize, true);
  }

  public static DeviceMetaDataCache getInstance() {
    return RowGroupBlockMetaDataCacheSingleton.INSTANCE;
  }

  /**
   * get {@link ChunkMetaData}. THREAD SAFE.
   */
  public List<ChunkMetaData> get(String filePath, Path seriesPath)
      throws IOException {
    String jointPath = filePath + "." + seriesPath.getDevice();
    Object jointPathObject = jointPath.intern();
    synchronized (lruCache) {
      cacheRequestNum.incrementAndGet();
      if (lruCache.containsKey(filePath) && lruCache.get(filePath).containsKey(seriesPath)) {
        cacheHintNum.incrementAndGet();
        if (logger.isDebugEnabled()) {
          logger.debug(
              "Cache hint: the number of requests for cache is {}, "
                  + "the number of hints for cache is {}",
              cacheRequestNum.get(), cacheHintNum.get());
        }
        return new ArrayList<>(lruCache.get(filePath).get(seriesPath));
      }
    }
    synchronized (jointPathObject) {
      synchronized (lruCache) {
        if (lruCache.containsKey(filePath) && lruCache.get(filePath).containsKey(seriesPath)) {
          return new ArrayList<>(lruCache.get(filePath).get(seriesPath));
        }
      }
      if (logger.isDebugEnabled()) {
        logger.debug("Cache didn't hint: the number of requests for cache is {}",
            cacheRequestNum.get());
      }
      TsFileMetaData fileMetaData = TsFileMetaDataCache.getInstance().get(filePath);
      TsDeviceMetadata blockMetaData = TsFileMetadataUtils
          .getTsRowGroupBlockMetaData(filePath, seriesPath.getDevice(),
              fileMetaData);
      ConcurrentHashMap<Path, List<ChunkMetaData>> chunkMetaData = TsFileMetadataUtils
          .getChunkMetaDataList(calHotSensorSet(seriesPath), blockMetaData);
      synchronized (lruCache) {
        lruCache.putIfAbsent(filePath, new ConcurrentHashMap<>());
        lruCache.get(filePath).putAll(chunkMetaData);
        if (chunkMetaData.containsKey(seriesPath)) {
          return chunkMetaData.get(seriesPath);
        }
        return new ArrayList<>();
      }
    }
  }

  private Set<String> calHotSensorSet(Path seriesPath) throws IOException {
    double porportion = lruCache.getUsedMemoryProportion();
    if (porportion < 0.6) {
      return null;
    } else {
      try {
        return storageEngine.calTopKSensor(seriesPath.getDevice(), seriesPath.getMeasurement(), 0.1);
      }
      catch (Exception e){
        throw new IOException(e);
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
        DeviceMetaDataCache(MEMORY_THRESHOLD_IN_B);
  }
}