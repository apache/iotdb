package org.apache.iotdb.db.engine.memtable;

import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

@FunctionalInterface
public interface MemTableFlushCallBack {

  void afterFlush(IMemTable memTable, TsFileIOWriter tsFileIOWriter);
}
