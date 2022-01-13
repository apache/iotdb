package org.apache.iotdb.tsfile.read.query.iterator;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.read.query.iterator.TimeSeries;

import java.io.IOException;
import java.util.List;

public class TimeSeriesQueryDataSet extends QueryDataSet {

  private final TimeSeries timeSeries;

  public TimeSeriesQueryDataSet(TimeSeries timeSeries) {
    this.timeSeries = timeSeries;
  }

  @Override
  public boolean hasNextWithoutConstraint() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public RowRecord nextWithoutConstraint() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean hasNext() throws IOException {
    return this.timeSeries.hasNext();
  }

  @Override
  public RowRecord next() throws IOException {
    Object[] next = this.timeSeries.next();
    if (next == null) {
      return null;
    }
    RowRecord record = new RowRecord((Long) next[0]);
    for (int i = 0; i < this.timeSeries.getSpecification().length; i++) {
      TSDataType dataType = this.timeSeries.getSpecification()[i];
      record.addField(next[i + 1], dataType);
    }
    return record;
  }
}
