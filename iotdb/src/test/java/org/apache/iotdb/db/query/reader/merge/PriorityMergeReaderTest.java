package org.apache.iotdb.db.query.reader.merge;

import org.apache.iotdb.db.query.reader.IReader;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.db.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 * Test {@code PriorityMergeReader}
 */
public class PriorityMergeReaderTest {

  @Test
  public void test() throws IOException {
    FakedPrioritySeriesReader reader1 = new FakedPrioritySeriesReader(100, 80, 5, 11);
    FakedPrioritySeriesReader reader2 = new FakedPrioritySeriesReader(150, 60, 6, 19);
    FakedPrioritySeriesReader reader3 = new FakedPrioritySeriesReader(180, 50, 7, 31);

    PriorityMergeReader priorityMergeReader = new PriorityMergeReader();
    priorityMergeReader.addReaderWithPriority(reader1, 3);
    priorityMergeReader.addReaderWithPriority(reader2, 2);
    priorityMergeReader.addReaderWithPriority(reader3, 1);

    int cnt = 0;
    while (priorityMergeReader.hasNext()) {
      TimeValuePair timeValuePair = priorityMergeReader.next();
      long time = timeValuePair.getTimestamp();
      long value = timeValuePair.getValue().getLong();

      //System.out.println(time + "," + value);
      if (time <= 500 && (time - 100) % 5 == 0) {
        Assert.assertEquals(time % 11, value);
      } else if (time <= 510 && (time - 150) % 6 == 0) {
        Assert.assertEquals(time % 19, value);
      } else {
        Assert.assertEquals(time % 31, value);
      }
      cnt++;
    }
    Assert.assertEquals(162, cnt);
  }


  public static class FakedPrioritySeriesReader implements IReader {
    private Iterator<TimeValuePair> iterator;

    FakedPrioritySeriesReader(long startTime, int size, int interval, int modValue) {
      long time = startTime;
      List<TimeValuePair> list = new ArrayList<>();
      for (int i = 0; i < size; i++) {
        list.add(new TimeValuePair(time, TsPrimitiveType.getByType(TSDataType.INT64, time % modValue)));
        //System.out.println(time + "," + time % modValue);
        time += interval;
      }
      iterator = list.iterator();
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public TimeValuePair next() {
      return iterator.next();
    }

    @Override
    public void skipCurrentTimeValuePair() {
      iterator.next();
    }

    @Override
    public void close() {
    }

    @Override
    public boolean hasNextBatch() {
      return false;
    }

    @Override
    public BatchData nextBatch() {
      return null;
    }

    @Override
    public BatchData currentBatch() {
      return null;
    }
  }
}
