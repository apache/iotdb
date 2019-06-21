package org.apache.iotdb.db.engine.memtable;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class EmptyMemTable extends AbstractMemTable {

  @Override
  protected IWritableMemChunk genMemSeries(TSDataType dataType) {
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
