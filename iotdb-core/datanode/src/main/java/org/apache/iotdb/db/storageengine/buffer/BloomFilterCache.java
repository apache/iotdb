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

package org.apache.iotdb.db.storageengine.buffer;

import org.apache.iotdb.commons.exception.IoTDBIORuntimeException;
import org.apache.iotdb.commons.memory.IMemoryBlock;
import org.apache.iotdb.commons.memory.MemoryBlockType;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.DataNodeMemoryConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.dataregion.read.control.FileReaderManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileID;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Weigher;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.utils.BloomFilter;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.LongConsumer;

/** This class is used to cache <code>BloomFilter</code> in IoTDB. The caching strategy is LRU. */
@SuppressWarnings("squid:S6548")
public class BloomFilterCache {

  private static final Logger LOGGER = LoggerFactory.getLogger(BloomFilterCache.class);
  private static final Logger DEBUG_LOGGER = LoggerFactory.getLogger("QUERY_DEBUG");
  private static final DataNodeMemoryConfig MEMORY_CONFIG =
      IoTDBDescriptor.getInstance().getMemoryConfig();
  private static final IMemoryBlock CACHE_MEMORY_BLOCK;
  private static final boolean CACHE_ENABLE = MEMORY_CONFIG.isMetaDataCacheEnable();
  private final AtomicLong entryAverageSize = new AtomicLong(0);

  private final Cache<BloomFilterCacheKey, BloomFilter> lruCache;

  static {
    CACHE_MEMORY_BLOCK =
        MEMORY_CONFIG
            .getBloomFilterCacheMemoryManager()
            .exactAllocate("BloomFilterCache", MemoryBlockType.STATIC);
    // TODO @spricoder: find a way to get the size of the BloomFilterCache
    CACHE_MEMORY_BLOCK.allocate(CACHE_MEMORY_BLOCK.getTotalMemorySizeInBytes());
  }

  private BloomFilterCache() {
    if (CACHE_ENABLE) {
      LOGGER.info("BloomFilterCache size = {}", CACHE_MEMORY_BLOCK.getTotalMemorySizeInBytes());
    }
    lruCache =
        Caffeine.newBuilder()
            .maximumWeight(CACHE_MEMORY_BLOCK.getTotalMemorySizeInBytes())
            .weigher(
                (Weigher<BloomFilterCacheKey, BloomFilter>)
                    (key, bloomFilter) ->
                        (int) (key.getRetainedSizeInBytes() + bloomFilter.getRetainedSizeInBytes()))
            .recordStats()
            .build();
  }

  public static BloomFilterCache getInstance() {
    return BloomFilterCacheHolder.INSTANCE;
  }

  @TestOnly
  public BloomFilter get(BloomFilterCacheKey key) throws IOException {
    LongConsumer emptyConsumer = l -> {};
    return get(key, false, emptyConsumer, emptyConsumer, emptyConsumer);
  }

  public BloomFilter get(
      BloomFilterCacheKey key,
      boolean debug,
      LongConsumer ioSizeRecorder,
      LongConsumer cacheHitAdder,
      LongConsumer cacheMissAdder)
      throws IOException {
    BloomFilterLoader loader = new BloomFilterLoader(ioSizeRecorder);
    try {
      if (!CACHE_ENABLE) {
        return loader.apply(key);
      }

      BloomFilter bloomFilter = lruCache.get(key, loader);

      if (debug) {
        DEBUG_LOGGER.info("get bloomFilter from cache where filePath is: {}", key.filePath);
      }

      return bloomFilter;
    } catch (IoTDBIORuntimeException e) {
      throw e.getCause();
    } finally {
      if (loader.isCacheMiss()) {
        cacheMissAdder.accept(1);
      } else {
        cacheHitAdder.accept(1);
      }
    }
  }

  public double calculateBloomFilterHitRatio() {
    return lruCache.stats().hitRate();
  }

  public long getEvictionCount() {
    return lruCache.stats().evictionCount();
  }

  public long getMaxMemory() {
    return CACHE_MEMORY_BLOCK.getTotalMemorySizeInBytes();
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

    private static final long INSTANCE_SIZE =
        RamUsageEstimator.shallowSizeOfInstance(BloomFilterCacheKey.class);

    // There is no need to add this field size while calculating the size of BloomFilterCacheKey,
    // because filePath is get from TsFileResource, different BloomFilterCacheKey of the same file
    // share this String.
    private final String filePath;
    private final TsFileID tsFileID;

    public BloomFilterCacheKey(String filePath, TsFileID tsFileID) {
      this.filePath = filePath;
      this.tsFileID = tsFileID;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      BloomFilterCacheKey that = (BloomFilterCacheKey) o;
      return Objects.equals(tsFileID, that.tsFileID);
    }

    @Override
    public int hashCode() {
      return Objects.hash(tsFileID);
    }

    public long getRetainedSizeInBytes() {
      return INSTANCE_SIZE;
    }
  }

  private static class BloomFilterLoader implements Function<BloomFilterCacheKey, BloomFilter> {

    private boolean cacheMiss = false;
    private final LongConsumer ioSizeRecorder;

    private BloomFilterLoader(LongConsumer ioSizeRecorder) {
      this.ioSizeRecorder = ioSizeRecorder;
    }

    @Override
    public BloomFilter apply(BloomFilterCacheKey bloomFilterCacheKey) {
      try {
        cacheMiss = true;
        TsFileSequenceReader reader =
            FileReaderManager.getInstance()
                .get(
                    bloomFilterCacheKey.filePath,
                    bloomFilterCacheKey.tsFileID,
                    true,
                    ioSizeRecorder);
        return reader.readBloomFilter(ioSizeRecorder);
      } catch (IOException e) {
        throw new IoTDBIORuntimeException(e);
      }
    }

    public boolean isCacheMiss() {
      return cacheMiss;
    }
  }

  /** singleton pattern. */
  private static class BloomFilterCacheHolder {
    private static final BloomFilterCache INSTANCE = new BloomFilterCache();
  }
}
