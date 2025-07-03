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

  private final LoadingCache<WALEntryPosition, ByteBuffer> bufferCache;
  private final Set<Long> memTablesNeedSearch;

  public WALEntryCache(long maxSize, Set<Long> memTablesNeedSearch) {
    this.bufferCache =
        Caffeine.newBuilder()
            .maximumWeight(maxSize / 2)
            .weigher(
                (Weigher<WALEntryPosition, ByteBuffer>)
                    (position, buffer) -> {
                      return position.getSize();
                    })
            .recordStats()
            .build(new WALEntryCacheLoader());
    this.memTablesNeedSearch = memTablesNeedSearch;
  }

  @Override
  public ByteBuffer load(WALEntryPosition key) {
    if (PipeConfig.getInstance().getWALCacheBatchLoadEnabled()) {
      synchronized (key.getWalSegmentMeta()) {
        return bufferCache.getAll(Collections.singleton(key)).get(key);
      }
    }

    return bufferCache.get(key);
  }

  @Override
  public void invalidateAll() {
    bufferCache.invalidateAll();
  }

  public CacheStats stats() {
    return bufferCache.stats();
  }

  private class WALEntryCacheLoader implements CacheLoader<WALEntryPosition, ByteBuffer> {

    @Override
    public @Nullable ByteBuffer load(@NonNull final WALEntryPosition key) throws Exception {
      ByteBuffer buffer = key.getSegmentBuffer();
      return WALCache.getEntryBySegment(key, buffer);
    }

    /** Batch load all wal entries in the file when any one key is absent. */
    @Override
    public @NonNull Map<@NonNull WALEntryPosition, @NonNull ByteBuffer> loadAll(
        @NonNull final Iterable<? extends @NonNull WALEntryPosition> walEntryPositions) {
      final Map<WALEntryPosition, ByteBuffer> loadedEntries = new HashMap<>();

      for (final WALEntryPosition walEntryPosition : walEntryPositions) {
        if (loadedEntries.containsKey(walEntryPosition) || !walEntryPosition.canRead()) {
          continue;
        }

        final long walFileVersionId = walEntryPosition.getWalFileVersionId();

        long maxCacheSize = PipeConfig.getInstance().getPipeWALCacheEntryPageSize();
        try {
          ByteBuffer segment = walEntryPosition.getSegmentBuffer();
          List<Integer> list = walEntryPosition.getWalSegmentMeta().getBuffersSize();
          int pos = 0;
          for (int size : list) {
            if (walEntryPosition.getPosition() < pos) {
              pos += size;
              continue;
            }
            segment.position(pos);
            segment.limit(pos + size);
            ByteBuffer buffer = segment.slice();

            final WALEntryType type = WALEntryType.valueOf(buffer.get());
            final long memTableId = buffer.getLong();
            if ((memTablesNeedSearch.contains(memTableId) || walEntryPosition.getPosition() == pos)
                && type.needSearch()) {
              buffer.clear();
              loadedEntries.put(
                  new WALEntryPosition(
                      walEntryPosition.getIdentifier(),
                      walFileVersionId,
                      pos,
                      size,
                      walEntryPosition.getWalSegmentMeta()),
                  buffer);
            }
            pos += size;
            maxCacheSize -= size;
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
