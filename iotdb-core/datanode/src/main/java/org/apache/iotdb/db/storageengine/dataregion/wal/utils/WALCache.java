package org.apache.iotdb.db.storageengine.dataregion.wal.utils;

import com.github.benmanes.caffeine.cache.stats.CacheStats;

import java.nio.ByteBuffer;
import java.util.List;

public interface WALCache {

  ByteBuffer load(final WALEntryPosition key);

  void invalidateAll();

  CacheStats stats();

  public static ByteBuffer getEntryBySegment(WALEntryPosition key, ByteBuffer segment) {
    List<Integer> list = key.getWalSegmentMeta().getBuffersSize();
    int pos = 0;
    for (int size : list) {
      if (key.getPosition() == pos) {
        segment.position(pos);
        segment.limit(pos + size);
        return segment.slice();
      }
      pos += size;
    }
    return null;
  }
}
