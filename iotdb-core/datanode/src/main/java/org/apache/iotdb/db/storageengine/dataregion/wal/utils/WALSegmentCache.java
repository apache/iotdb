package org.apache.iotdb.db.storageengine.dataregion.wal.utils;

import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntryType;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALByteBufReader;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Weigher;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class WALSegmentCache implements WALCache {

  private Logger LOGGER = LoggerFactory.getLogger(WALSegmentCache.class);

  private final Cache<WALEntryPosition, ByteBuffer> bufferCache;
  private final Set<Long> memTablesNeedSearch;

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
    this.memTablesNeedSearch = memTablesNeedSearch;
  }

  @Override
  public ByteBuffer load(WALEntryPosition key) throws Exception {
    return null;
  }

  @Override
  public ByteBuffer loadAll(WALEntryPosition walEntryPositions) {
    return null;
  }

  class WALEntryCacheLoader implements CacheLoader<WALEntryPosition, ByteBuffer> {

    @Override
    public @Nullable ByteBuffer load(@NonNull final WALEntryPosition key) throws Exception {
      return key.read();
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

        // load one when wal file is not sealed
        if (!walEntryPosition.isInSealedFile()) {
          try {
            loadedEntries.put(walEntryPosition, load(walEntryPosition));
          } catch (final Exception e) {
            LOGGER.info(
                "Fail to cache wal entries from the wal file with version id {}",
                walFileVersionId,
                e);
          }
          continue;
        }

        // batch load when wal file is sealed
        long position = 0;
        try (final WALByteBufReader walByteBufReader = new WALByteBufReader(walEntryPosition)) {
          while (walByteBufReader.hasNext()) {
            // see WALInfoEntry#serialize, entry type + memtable id + plan node type
            final ByteBuffer buffer = walByteBufReader.next();

            final int size = buffer.capacity();
            final WALEntryType type = WALEntryType.valueOf(buffer.get());
            final long memTableId = buffer.getLong();

            if ((memTablesNeedSearch.contains(memTableId)
                    || walEntryPosition.getPosition() == position)
                && type.needSearch()) {
              buffer.clear();
              loadedEntries.put(
                  new WALEntryPosition(
                      walEntryPosition.getIdentifier(), walFileVersionId, position, size),
                  buffer);
            }

            position += size;
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
