package org.apache.iotdb.db.utils.datastructure;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.iotdb.db.engine.memtable.DeduplicatedSortedData;
import org.apache.iotdb.db.engine.memtable.WritableMemChunk;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.db.utils.TsPrimitiveType.TsLong;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.junit.Assert;
import org.junit.Test;

public class LongTVListTest {


  @Test
  public void testLongTVList1() {
    LongTVList tvList = new LongTVList();
    for (long i = 0; i < 1000; i++) {
      tvList.putLong(i, i);
    }
    tvList.sort();
    for (long i = 0; i < tvList.size; i++) {
      Assert.assertEquals(i, tvList.getLong((int)i));
      Assert.assertEquals(i, tvList.getTime((int)i));
    }
  }

  @Test
  public void testLongTVList2() {
    LongTVList tvList = new LongTVList();
    for (long i = 1000; i >= 0; i--) {
      tvList.putLong(i, i);
    }
    tvList.sort();
    for (long i = 0; i < tvList.size; i++) {
      Assert.assertEquals(i, tvList.getLong((int)i));
      Assert.assertEquals(i, tvList.getTime((int)i));
    }
  }

  @Test
  public void testLongTVList3() {
    Random random = new Random();
    LongTVList tvList = new LongTVList();
    List<TimeValuePair> inputs = new ArrayList<>();
    for (long i = 0; i < 10000; i++) {
      long time = random.nextInt(10000);
      long value = random.nextInt(10000);
      tvList.putLong(time, value);
      inputs.add(new TimeValuePair(time, new TsLong(value)));
    }
    tvList.sort();
    inputs.sort(TimeValuePair::compareTo);
    for (long i = 0; i < tvList.size; i++) {
      Assert.assertEquals(inputs.get((int)i).getTimestamp(), tvList.getTime((int)i));
      Assert.assertEquals(inputs.get((int)i).getValue().getLong(), tvList.getLong((int)i));
    }
  }


  @Test
  public void compareLongTVListSortTime() {

    long start = System.currentTimeMillis();

    for (int j = 0; j < 100; j++) {
      LongTVList tvList = new LongTVList();
      for (long i = 0; i < 1000; i++) {
        tvList.putLong(i, i);
      }
      tvList.sort();
      for (int i = 0; i < tvList.size; i++) {
        tvList.getLong(i);
        tvList.getTime(i);
      }
    }
    start = System.currentTimeMillis() - start;
    System.out.println("tvList sort time: " + start);

  }


  @Test
  public void compareGetSortedTimeValuePairTime() {
    long time1 = System.currentTimeMillis();
    for (int j = 0; j < 100; j++) {
      WritableMemChunk writableMemChunk = new WritableMemChunk(TSDataType.INT64);
      for (long i = 0; i < 1000; i++) {
        writableMemChunk.putLong(i, i);
      }
      List<TimeValuePair> timeValuePairs = writableMemChunk.getSortedTimeValuePairList();
      for (int i = 0; i < timeValuePairs.size(); i++) {
        timeValuePairs.get(i);
      }
    }
    time1 = System.currentTimeMillis() - time1;
    System.out.println("writable memchunk getSortedTimeValuePairList time: " + time1);

  }

  @Test
  public void compareGetDeduplicatedDataTime() {
    long time2 = System.currentTimeMillis();
    for (int j = 0; j < 100; j++) {
      WritableMemChunk writableMemChunk = new WritableMemChunk(TSDataType.INT64);
      for (long i = 0; i < 1000; i++) {
        writableMemChunk.putLong(i, i);
      }
      DeduplicatedSortedData deduplicatedSortedData = writableMemChunk.getDeduplicatedSortedData();
      while (deduplicatedSortedData.hasNext()) {
        deduplicatedSortedData.next();
      }
    }
    time2 = System.currentTimeMillis() - time2;
    System.out.println("writable memchunk getDeduplicatedSortedData time: " + time2);
  }

}