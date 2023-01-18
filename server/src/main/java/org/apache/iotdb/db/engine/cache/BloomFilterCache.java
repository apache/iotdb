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

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.utils.BloomFilter;
import org.apache.iotdb.tsfile.utils.FilePathUtils;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.RamUsageEstimator;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Weigher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/** This class is used to cache <code>BloomFilter</code> in IoTDB. The caching strategy is LRU. */
public class BloomFilterCache {

  private static final Logger logger = LoggerFactory.getLogger(BloomFilterCache.class);
  private static final Logger DEBUG_LOGGER = LoggerFactory.getLogger("QUERY_DEBUG");
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final long MEMORY_THRESHOLD_IN_BLOOM_FILTER_CACHE =
      config.getAllocateMemoryForBloomFilterCache();
  private static final boolean CACHE_ENABLE = config.isMetaDataCacheEnable();
  private final AtomicLong entryAverageSize = new AtomicLong(0);

  private final LoadingCache<BloomFilterCacheKey, BloomFilter> lruCache;

  private BloomFilterCache() {
    if (CACHE_ENABLE) {
      logger.info("BloomFilterCache size = {}", MEMORY_THRESHOLD_IN_BLOOM_FILTER_CACHE);
    }
    lruCache =
        Caffeine.newBuilder()
            .maximumWeight(MEMORY_THRESHOLD_IN_BLOOM_FILTER_CACHE)
            .weigher(
                (Weigher<BloomFilterCacheKey, BloomFilter>)
                    (key, bloomFilter) ->
                        (int)
                            (RamUsageEstimator.shallowSizeOf(key)
                                + RamUsageEstimator.sizeOf(key.tsFilePrefixPath)
                                + RamUsageEstimator.sizeOf(bloomFilter)))
            .recordStats()
            .build(
                key -> {
                  try {
                    TsFileSequenceReader reader =
                        FileReaderManager.getInstance().get(key.filePath, true);
                    return reader.readBloomFilter();
                  } catch (IOException e) {
                    logger.error(
                        "Something wrong happened in reading bloom filter in tsfile {}",
                        key.filePath,
                        e);
                    throw e;
                  }
                });
  }

  public static BloomFilterCache getInstance() {
    return BloomFilterCacheHolder.INSTANCE;
  }

  public BloomFilter get(BloomFilterCacheKey key) throws IOException {
    return get(key, false);
  }

  public BloomFilter get(BloomFilterCacheKey key, boolean debug) throws IOException {
    if (!CACHE_ENABLE) {
      TsFileSequenceReader reader = FileReaderManager.getInstance().get(key.filePath, true);
      return reader.readBloomFilter();
    }

    BloomFilter bloomFilter = lruCache.get(key);

    if (debug) {
      DEBUG_LOGGER.info("get bloomFilter from cache where filePath is: {}", key.filePath);
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

  public long getAverageSize() {
    return entryAverageSize.get();
  }

  /** clear LRUCache. */
  public void clear() {
    lruCache.invalidateAll();
    lruCache.cleanUp();
  }

  @TestOnly
  public void remove(BloomFilterCacheKey key) {
    lruCache.invalidate(key);
  }

  @TestOnly
  public BloomFilter getIfPresent(BloomFilterCacheKey key) {
    return lruCache.getIfPresent(key);
  }

  public static class BloomFilterCacheKey {

    // There is no need to add this field size while calculating the size of BloomFilterCacheKey,
    // because filePath is get from TsFileResource, different BloomFilterCacheKey of the same file
    // share this String.
    private final String filePath;
    private final String tsFilePrefixPath;
    private final long tsFileVersion;
    // high 32 bit is compaction level, low 32 bit is merge count
    private final long compactionVersion;

    public BloomFilterCacheKey(String filePath) {
      this.filePath = filePath;
      Pair<String, long[]> tsFilePrefixPathAndTsFileVersionPair =
          FilePathUtils.getTsFilePrefixPathAndTsFileVersionPair(filePath);
      this.tsFilePrefixPath = tsFilePrefixPathAndTsFileVersionPair.left;
      this.tsFileVersion = tsFilePrefixPathAndTsFileVersionPair.right[0];
      this.compactionVersion = tsFilePrefixPathAndTsFileVersionPair.right[1];
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      BloomFilterCache.BloomFilterCacheKey that = (BloomFilterCache.BloomFilterCacheKey) o;
      return tsFileVersion == that.tsFileVersion
          && compactionVersion == that.compactionVersion
          && tsFilePrefixPath.equals(that.tsFilePrefixPath);
    }

    @Override
    public int hashCode() {
      return Objects.hash(tsFilePrefixPath, tsFileVersion, compactionVersion);
    }
  }

  /** singleton pattern. */
  private static class BloomFilterCacheHolder {
    private static final BloomFilterCache INSTANCE = new BloomFilterCache();
  }
}
