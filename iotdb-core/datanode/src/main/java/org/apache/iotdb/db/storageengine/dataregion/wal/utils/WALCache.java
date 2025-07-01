package org.apache.iotdb.db.storageengine.dataregion.wal.utils;

import java.nio.ByteBuffer;

public interface WALCache {

  ByteBuffer load(final WALEntryPosition key) throws Exception;

  ByteBuffer loadAll(final WALEntryPosition walEntryPositions);
}
