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

  private final LoadingCache<WALEntryPosition, ByteBuffer> bufferCache;

  public WALSegmentCache(long maxSize, Set<Long> memTablesNeedSearch) {
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
  }

  @Override
  public ByteBuffer load(WALEntryPosition key) {
    ByteBuffer buffer = null;
    synchronized (key.getWalSegmentMeta()) {
      buffer = bufferCache.get(key);
    }

    if (buffer == null) {
      LOGGER.warn("WALSegmentCache load failed, key: {}", key);
    }

    return WALCache.getEntryBySegment(key, buffer);
  }

  @Override
  public void invalidateAll() {
    bufferCache.invalidateAll();
  }

  public CacheStats stats() {
    return bufferCache.stats();
  }

  private static class WALEntryCacheLoader implements CacheLoader<WALEntryPosition, ByteBuffer> {

    @Override
    public @Nullable ByteBuffer load(@NonNull final WALEntryPosition key) throws Exception {
      return key.getSegmentBuffer();
    }
  }
}
