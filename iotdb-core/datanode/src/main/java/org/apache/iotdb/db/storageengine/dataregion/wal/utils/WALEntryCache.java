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

import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntryType;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Weigher;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class WALEntryCache implements WALCache {

  private static final Logger LOGGER = LoggerFactory.getLogger(WALEntryCache.class);

  private final LoadingCache<WALEntrySegmentPosition, ByteBuffer> bufferCache;
  private final Set<Long> memTablesNeedSearch;

  public WALEntryCache(long maxSize, Set<Long> memTablesNeedSearch) {
    this.bufferCache =
        Caffeine.newBuilder()
            .maximumWeight(maxSize)
            .weigher(
                (Weigher<WALEntrySegmentPosition, ByteBuffer>)
                    (position, buffer) -> {
                      return position.getSize();
                    })
            .recordStats()
            .build(new WALEntryCacheLoader());
    this.memTablesNeedSearch = memTablesNeedSearch;
  }

  @Override
  public ByteBuffer load(WALEntrySegmentPosition key) {
    ByteBuffer buffer = null;
    if (PipeConfig.getInstance().getWALCacheBatchLoadEnabled()) {
      synchronized (key.getWalSegmentMeta()) {
        buffer = bufferCache.getAll(Collections.singleton(key)).get(key);
      }
    } else {
      buffer = bufferCache.get(key);
    }

    if (buffer == null) {
      LOGGER.warn("WALEntryCache load failed, key: {}", key);
      return null;
    }

    return ByteBuffer.wrap(buffer.array());
  }

  @Override
  public void invalidateAll() {
    bufferCache.invalidateAll();
  }

  public CacheStats stats() {
    return bufferCache.stats();
  }

  private class WALEntryCacheLoader implements CacheLoader<WALEntrySegmentPosition, ByteBuffer> {

    @Override
    public @Nullable ByteBuffer load(@NonNull final WALEntrySegmentPosition key) throws Exception {
      ByteBuffer buffer = key.getSegmentBuffer();
      return WALCache.getEntryBySegment(key, buffer);
    }

    /** Batch load all wal entries in the file when any one key is absent. */
    @Override
    public @NonNull Map<@NonNull WALEntrySegmentPosition, @NonNull ByteBuffer> loadAll(
        @NonNull final Iterable<? extends @NonNull WALEntrySegmentPosition> walEntryPositions) {
      final Map<WALEntrySegmentPosition, ByteBuffer> loadedEntries = new HashMap<>();

      for (final WALEntrySegmentPosition walEntrySegmentPosition : walEntryPositions) {
        if (loadedEntries.containsKey(walEntrySegmentPosition)
            || !walEntrySegmentPosition.canRead()) {
          continue;
        }

        final long walFileVersionId = walEntrySegmentPosition.getWalFileVersionId();

        long maxCacheSize = PipeConfig.getInstance().getPipeWALCacheEntryPageSize();
        try {
          byte[] segment = walEntrySegmentPosition.getSegmentBuffer().array();
          List<Integer> list = walEntrySegmentPosition.getWalSegmentMetaBuffersSize();
          int pos = 0;
          for (int size : list) {
            if (walEntrySegmentPosition.getPosition() < pos) {
              pos += size;
              continue;
            }

            final byte[] data = new byte[size];
            System.arraycopy(segment, pos, data, 0, size);
            ByteBuffer buffer = ByteBuffer.wrap(data);

            final WALEntryType type = WALEntryType.valueOf(buffer.get());
            final long memTableId = buffer.getLong();
            if ((memTablesNeedSearch.contains(memTableId)
                    || walEntrySegmentPosition.getPosition() == pos)
                && type.needSearch()) {
              maxCacheSize -= size;
              buffer.clear();
              loadedEntries.put(
                  new WALEntrySegmentPosition(
                      walEntrySegmentPosition.getIdentifier(),
                      walFileVersionId,
                      pos,
                      size,
                      walEntrySegmentPosition.getWalSegmentMeta()),
                  buffer);
            }
            pos += size;
            if (maxCacheSize <= 0) {
              break;
            }
          }
        } catch (final IOException e) {
          LOGGER.info(
              "Fail to cache wal entries from the wal file with version id {}",
              walFileVersionId,
              e);
        }
      }

      return loadedEntries;
    }
  }
}
