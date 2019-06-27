package org.apache.iotdb.db.utils.datastructure;

import java.sql.Time;
import java.util.List;
import org.apache.iotdb.db.engine.memtable.DeduplicatedSortedData;
import org.apache.iotdb.db.engine.memtable.WritableMemChunk;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.junit.Test;

public class LongTVListTest {

  @Test
  public void compareLongTVListInsertTime() {

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

  @Test
  public void compareLongTVListSortTime() {

    LongTVList tvList = new LongTVList();
    for (long i = 0; i < 1000000; i ++) {
      tvList.putLong(i, i);
    }

    WritableMemChunk writableMemChunk = new WritableMemChunk(TSDataType.INT64);
    for (long i = 0; i < 1000000; i ++) {
      writableMemChunk.putLong(i, i);
    }

    long start = System.currentTimeMillis();
    tvList.sort();
    for (int i = 0; i < tvList.size; i ++) {
      tvList.getLong(i);
      tvList.getTime(i);
    }
    start = System.currentTimeMillis() - start;
    System.out.println("tvList sort time: " + start);


    long time1 = System.currentTimeMillis();
    List<TimeValuePair> timeValuePairs = writableMemChunk.getSortedTimeValuePairList();
    for (int i = 0; i < timeValuePairs.size(); i++) {
      timeValuePairs.get(i);
    }
    time1 = System.currentTimeMillis() - time1;
    System.out.println("writable memchunk getSortedTimeValuePairList time: " + time1);


    long time2 = System.currentTimeMillis();
    DeduplicatedSortedData deduplicatedSortedData = writableMemChunk.getDeduplicatedSortedData();
    while(deduplicatedSortedData.hasNext()) {
      deduplicatedSortedData.next();
    }
    time2 = System.currentTimeMillis() - time2;
    System.out.println("writable memchunk getDeduplicatedSortedData time: " + time2);
  }


}