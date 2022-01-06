package org.apache.iotdb.tsfile.read.query.iterator;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class TimeSeriesQueryDataSetTest {

  static class TestTimeSeries implements TimeSeries {

    private final List<Object[]> values;
    private int index;

    public TestTimeSeries() {
      values = Arrays.asList(
          new Object[]{1L, 1, (float)1.5},
          new Object[]{2L, 2, (float)3.0}
      );
      index = 0;
    }

    @Override
    public TSDataType[] getSpecification() {
      return new TSDataType[]{TSDataType.INT32, TSDataType.FLOAT};
    }

    @Override
    public boolean hasNext() {
      return index < values.size();
    }

    @Override
    public Object[] next() {
      index++;
      return (Object[]) this.values.get(index - 1);
    }
  }

  @Test
  public void testDataset() throws IOException {
    TimeSeriesQueryDataSet dataSet = new TimeSeriesQueryDataSet(new TestTimeSeries());

    while (dataSet.hasNext()) {
      RowRecord next = dataSet.next();
      System.out.println("Record: " + next);
    }
  }
}