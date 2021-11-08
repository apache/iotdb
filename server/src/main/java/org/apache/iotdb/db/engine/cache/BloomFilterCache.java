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
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.utils.BloomFilter;
import org.apache.iotdb.tsfile.utils.RamUsageEstimator;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Weigher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/** This class is used to cache <code>BloomFilter</code> in IoTDB. The caching strategy is LRU. */
public class BloomFilterCache {

  private static final Logger logger = LoggerFactory.getLogger(BloomFilterCache.class);
  private static final Logger DEBUG_LOGGER = LoggerFactory.getLogger("QUERY_DEBUG");
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final long MEMORY_THRESHOLD_IN_BLOOM_FILTER_CACHE =
      config.getAllocateMemoryForBloomFilterCache();
  private static final boolean CACHE_ENABLE = config.isMetaDataCacheEnable();

  private final LoadingCache<String, BloomFilter> lruCache;

  private BloomFilterCache() {
    if (CACHE_ENABLE) {
      logger.info("BloomFilterCache size = " + MEMORY_THRESHOLD_IN_BLOOM_FILTER_CACHE);
    }
    lruCache =
        Caffeine.newBuilder()
            .maximumWeight(MEMORY_THRESHOLD_IN_BLOOM_FILTER_CACHE)
            .weigher(
                (Weigher<String, BloomFilter>)
                    (filePath, bloomFilter) ->
                        (int)
                            (RamUsageEstimator.NUM_BYTES_OBJECT_REF
                                + RamUsageEstimator.sizeOf(bloomFilter))) // TODO: how to calculate?
            .recordStats()
            .build(
                filePath -> {
                  try {
                    TsFileSequenceReader reader =
                        FileReaderManager.getInstance().get(filePath, true);
                    return reader.readBloomFilter();
                  } catch (IOException e) {
                    logger.error(
                        "Something wrong happened in reading bloom filter in tsfile {}",
                        filePath,
                        e);
                    throw e;
                  }
                });
  }

  public static BloomFilterCache getInstance() {
    return BloomFilterCacheHolder.INSTANCE;
  }

  public BloomFilter get(String filePath) throws IOException {
    return get(filePath, false);
  }

  public BloomFilter get(String filePath, boolean debug) throws IOException {
    if (!CACHE_ENABLE) {
      TsFileSequenceReader reader = FileReaderManager.getInstance().get(filePath, true);
      return reader.readBloomFilter();
    }

    BloomFilter bloomFilter = lruCache.get(filePath);

    if (debug) {
      DEBUG_LOGGER.info("get bloomFilter from cache where filePath is: " + filePath);
    }

    return bloomFilter;
  }

  public double calculateChunkHitRatio() {
    return lruCache.stats().hitRate();
  }

  public long getEvictionCount() {
    return lruCache.stats().evictionCount();
  }

  public long getMaxMemory() {
    return MEMORY_THRESHOLD_IN_BLOOM_FILTER_CACHE;
  }

  public double getAverageLoadPenalty() {
    return lruCache.stats().averageLoadPenalty();
  }

  /** clear LRUCache. */
  public void clear() {
    lruCache.invalidateAll();
    lruCache.cleanUp();
  }

  public void remove(String filePath) {
    lruCache.invalidate(filePath);
  }

  /** singleton pattern. */
  private static class BloomFilterCacheHolder {
    private static final BloomFilterCache INSTANCE = new BloomFilterCache();
  }
}
