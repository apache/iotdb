package org.apache.iotdb.db.utils.datastructure;

import org.apache.iotdb.db.engine.memtable.WritableMemChunk;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.junit.Test;

public class LongTVListTest {

  @Test
  public void testLongTVListInsertTime() {

    long start = System.currentTimeMillis();
    LongTVList tvList = new LongTVList();
    for (long i = 0; i < 1000000; i ++) {
      tvList.putLong(i, i);
    }
    start = System.currentTimeMillis() - start;
    System.out.println("tvList insert time: " + start);

    long time = System.currentTimeMillis();
    WritableMemChunk writableMemChunk = new WritableMemChunk(TSDataType.INT64);
    for (long i = 0; i < 1000000; i ++) {
      writableMemChunk.putLong(i, i);
    }

    time = System.currentTimeMillis() - time;
    System.out.println("writable memchunk insert time: " + time);
  }

}