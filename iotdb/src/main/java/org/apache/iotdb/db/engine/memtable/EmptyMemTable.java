package org.apache.iotdb.db.engine.memtable;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

/**
 * Only used in sync flush and async close,
 * This memtable is not managed by MemTablePool and does not store any data.
 */
public class EmptyMemTable extends AbstractMemTable {

  @Override
  protected IWritableMemChunk genMemSeries(TSDataType dataType, String path) {
    return null;
  }

  @Override
  public IMemTable copy() {
    return null;
  }

  @Override
  public boolean isManagedByMemPool() {
    return false;
  }
}
