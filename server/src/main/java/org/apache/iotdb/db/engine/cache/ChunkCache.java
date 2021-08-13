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
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.utils.RamUsageEstimator;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Weigher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class is used to cache <code>Chunk</code> of <code>ChunkMetaData</code> in IoTDB. The
 * caching strategy is LRU.
 */
public class ChunkCache {

  private static final Logger logger = LoggerFactory.getLogger(ChunkCache.class);
  private static final Logger DEBUG_LOGGER = LoggerFactory.getLogger("QUERY_DEBUG");
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final long MEMORY_THRESHOLD_IN_CHUNK_CACHE =
      config.getAllocateMemoryForChunkCache();
  private static final boolean CACHE_ENABLE = config.isMetaDataCacheEnable();

  private final LoadingCache<ChunkMetadata, Chunk> lruCache;

  private final AtomicLong entryAverageSize = new AtomicLong(0);

  private ChunkCache() {
    if (CACHE_ENABLE) {
      logger.info("ChunkCache size = " + MEMORY_THRESHOLD_IN_CHUNK_CACHE);
    }
    lruCache =
        Caffeine.newBuilder()
            .maximumWeight(MEMORY_THRESHOLD_IN_CHUNK_CACHE)
            .weigher(
                new Weigher<ChunkMetadata, Chunk>() {

                  int count = 0;
                  int averageSize = 0;

                  @Override
                  public int weigh(ChunkMetadata chunkMetadata, Chunk chunk) {
                    int currentSize;
                    if (count < 10) {
                      currentSize =
                          (int)
                              (RamUsageEstimator.NUM_BYTES_OBJECT_REF
                                  + RamUsageEstimator.sizeOf(chunk));
                      averageSize = ((averageSize * count) + currentSize) / (++count);
                      entryAverageSize.set(averageSize);
                    } else if (count < 100000) {
                      count++;
                      currentSize = averageSize;
                    } else {
                      averageSize =
                          (int)
                              (RamUsageEstimator.NUM_BYTES_OBJECT_REF
                                  + RamUsageEstimator.sizeOf(chunk));
                      count = 1;
                      currentSize = averageSize;
                      entryAverageSize.set(averageSize);
                    }
                    return currentSize;
                  }
                })
            .recordStats()
            .build(
                new CacheLoader<ChunkMetadata, Chunk>() {
                  @Override
                  public Chunk load(ChunkMetadata chunkMetadata) throws Exception {
                    try {
                      TsFileSequenceReader reader =
                          FileReaderManager.getInstance()
                              .get(chunkMetadata.getFilePath(), chunkMetadata.isClosed());
                      return reader.readMemChunk(chunkMetadata);
                    } catch (IOException e) {
                      logger.error("Something wrong happened in reading {}", chunkMetadata, e);
                      throw e;
                    }
                  }
                });
  }

  public static ChunkCache getInstance() {
    return ChunkCacheHolder.INSTANCE;
  }

  public Chunk get(ChunkMetadata chunkMetaData) throws IOException {
    return get(chunkMetaData, false);
  }

  public Chunk get(ChunkMetadata chunkMetaData, boolean debug) throws IOException {
    if (!CACHE_ENABLE) {
      TsFileSequenceReader reader =
          FileReaderManager.getInstance()
              .get(chunkMetaData.getFilePath(), chunkMetaData.isClosed());
      Chunk chunk = reader.readMemChunk(chunkMetaData);
      return new Chunk(
          chunk.getHeader(),
          chunk.getData().duplicate(),
          chunk.getDeleteIntervalList(),
          chunkMetaData.getStatistics());
    }

    Chunk chunk = lruCache.get(chunkMetaData);

    if (debug) {
      DEBUG_LOGGER.info("get chunk from cache whose meta data is: " + chunkMetaData);
    }

    return new Chunk(
        chunk.getHeader(),
        chunk.getData().duplicate(),
        chunk.getDeleteIntervalList(),
        chunkMetaData.getStatistics());
  }

  public double calculateChunkHitRatio() {
    return lruCache.stats().hitRate();
  }

  public long getEvictionCount() {
    return lruCache.stats().evictionCount();
  }

  public long getMaxMemory() {
    return MEMORY_THRESHOLD_IN_CHUNK_CACHE;
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

  public void remove(ChunkMetadata chunkMetaData) {
    lruCache.invalidate(chunkMetaData);
  }

  @TestOnly
  public boolean isEmpty() {
    return lruCache.asMap().isEmpty();
  }

  /** singleton pattern. */
  private static class ChunkCacheHolder {

    private static final ChunkCache INSTANCE = new ChunkCache();
  }
}
