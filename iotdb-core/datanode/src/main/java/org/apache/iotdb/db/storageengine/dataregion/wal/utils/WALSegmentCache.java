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

package org.apache.iotdb.db.storageengine.dataregion.wal.utils;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Weigher;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Set;

public class WALSegmentCache implements WALCache {

  private static final Logger LOGGER = LoggerFactory.getLogger(WALSegmentCache.class);

  private final LoadingCache<WALEntrySegmentPosition, ByteBuffer> bufferCache;
  private final int WALEntrySegmentPositionSize = 10 * 8 * 8;

  public WALSegmentCache(long maxSize, Set<Long> memTablesNeedSearch) {
    this.bufferCache =
        Caffeine.newBuilder()
            .maximumWeight(maxSize)
            .weigher(
                (Weigher<WALEntrySegmentPosition, ByteBuffer>)
                    (position, buffer) -> {
                      return buffer.capacity() + WALEntrySegmentPositionSize;
                    })
            .recordStats()
            .build(new WALEntryCacheLoader());
  }

  @Override
  public ByteBuffer load(WALEntrySegmentPosition key) {
    ByteBuffer buffer = null;
    synchronized (key.getWalSegmentMeta()) {
      buffer = bufferCache.get(key);
    }

    if (buffer == null) {
      LOGGER.warn("WALSegmentCache load failed, key: {}", key);
      return null;
    }

    return WALCache.getEntryBySegment(key, buffer);
  }

  @Override
  public void invalidateAll() {
    bufferCache.invalidateAll();
  }

  @Override
  public CacheStats stats() {
    return bufferCache.stats();
  }

  private static class WALEntryCacheLoader
      implements CacheLoader<WALEntrySegmentPosition, ByteBuffer> {

    @Override
    public @Nullable ByteBuffer load(@NonNull final WALEntrySegmentPosition key) throws Exception {
      return key.getSegmentBuffer();
    }
  }
}
