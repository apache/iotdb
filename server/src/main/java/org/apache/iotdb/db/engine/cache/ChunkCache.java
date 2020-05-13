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
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This class is used to cache <code>Chunk</code> of <code>ChunkMetaData</code> in IoTDB. The caching
 * strategy is LRU.
 */
public class ChunkCache {

  private static final Logger logger = LoggerFactory.getLogger(ChunkCache.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final long MEMORY_THRESHOLD_IN_CHUNK_CACHE = config.getAllocateMemoryForChunkCache();
  private static boolean cacheEnable = config.isMetaDataCacheEnable();

  private final LRULinkedHashMap<ChunkMetadata, Chunk> lruCache;

  private AtomicLong cacheHitNum = new AtomicLong();
  private AtomicLong cacheRequestNum = new AtomicLong();

  private final ReadWriteLock lock = new ReentrantReadWriteLock();


  private ChunkCache() {
    if (cacheEnable) {
      logger.info("ChunkCache size = " + MEMORY_THRESHOLD_IN_CHUNK_CACHE);
    }
    lruCache = new LRULinkedHashMap<ChunkMetadata, Chunk>(MEMORY_THRESHOLD_IN_CHUNK_CACHE) {

      @Override
      protected long calEntrySize(ChunkMetadata key, Chunk value) {
        long currentSize;
        if (count < 10) {
          currentSize = RamUsageEstimator.shallowSizeOf(key) + RamUsageEstimator.sizeOf(value);
          averageSize = ((averageSize * count) + currentSize) / (++count);
        } else if (count < 100000) {
          count++;
          currentSize = averageSize;
        } else {
          averageSize = RamUsageEstimator.shallowSizeOf(key) + RamUsageEstimator.sizeOf(value);
          count = 1;
          currentSize = averageSize;
        }
        key.setRAMSize(currentSize);
        return currentSize;
      }
    };
  }

  public static ChunkCache getInstance() {
    return ChunkCacheHolder.INSTANCE;
  }

  public Chunk get(ChunkMetadata chunkMetaData, TsFileSequenceReader reader) throws IOException {
    if (!cacheEnable) {
      Chunk chunk = reader.readMemChunk(chunkMetaData);
      return new Chunk(chunk.getHeader(), chunk.getData().duplicate(), chunk.getDeletedAt(), reader.getEndianType());
    }

    cacheRequestNum.incrementAndGet();

    try {
      lock.readLock().lock();
      if (lruCache.containsKey(chunkMetaData)) {
        cacheHitNum.incrementAndGet();
        printCacheLog(true);
        Chunk chunk = lruCache.get(chunkMetaData);
        return new Chunk(chunk.getHeader(), chunk.getData().duplicate(), chunk.getDeletedAt(), reader.getEndianType());
      }
    } finally {
      lock.readLock().unlock();
    }

    Lock cacheLock = lock.writeLock();
    try {
      cacheLock.lock();
      if (lruCache.containsKey(chunkMetaData)) {
        try {
          cacheLock = lock.readLock();
          cacheLock.lock();
        } finally {
          lock.writeLock().unlock();
        }
        cacheHitNum.incrementAndGet();
        printCacheLog(true);
        Chunk chunk = lruCache.get(chunkMetaData);
        return new Chunk(chunk.getHeader(), chunk.getData().duplicate(), chunk.getDeletedAt(), reader.getEndianType());
      }
      printCacheLog(false);
      Chunk chunk = reader.readMemChunk(chunkMetaData);
      lruCache.put(chunkMetaData, chunk);
      return new Chunk(chunk.getHeader(), chunk.getData().duplicate(), chunk.getDeletedAt(), reader.getEndianType());
    } catch (IOException e) {
      logger.error("something wrong happened while reading {}", reader.getFileName());
      throw e;
    } finally {
      cacheLock.unlock();
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

  public double calculateChunkHitRatio() {
    if (cacheRequestNum.get() != 0) {
      return cacheHitNum.get() * 1.0 / cacheRequestNum.get();
    } else {
      return 0;
    }
  }

  public long getUsedMemory() {
    return lruCache.getUsedMemory();
  }

  public long getMaxMemory() {
    return lruCache.getMaxMemory();
  }

  public double getUsedMemoryProportion() {
    return lruCache.getUsedMemoryProportion();
  }

  public long getAverageSize() {
    return lruCache.getAverageSize();
  }


  /**
   * clear LRUCache.
   */
  public void clear() {
    lock.writeLock().lock();
    if (lruCache != null) {
      lruCache.clear();
    }
    lock.writeLock().unlock();
  }

  public void remove(ChunkMetadata chunkMetaData) {
    lock.writeLock().lock();
    if (chunkMetaData != null) {
      lruCache.remove(chunkMetaData);
    }
    lock.writeLock().unlock();
  }

  /**
   * singleton pattern.
   */
  private static class ChunkCacheHolder {

    private static final ChunkCache INSTANCE = new ChunkCache();
  }
}
