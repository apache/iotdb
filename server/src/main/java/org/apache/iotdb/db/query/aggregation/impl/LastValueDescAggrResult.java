package org.apache.iotdb.db.query.aggregation.impl;

import java.io.IOException;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;

public class LastValueDescAggrResult extends LastValueAggrResult {

  public LastValueDescAggrResult(TSDataType dataType) {
    super(dataType);
  }


  @Override
  public void updateResultFromStatistics(Statistics statistics) {
    if (hasResult()) {
      return;
    }
    Object lastVal = statistics.getLastValue();
    setValue(lastVal);
    timestamp = statistics.getEndTime();
  }

  @Override
  public void updateResultFromPageData(BatchData dataInThisPage, long minBound, long maxBound)
      throws IOException {
    if (hasResult()) {
      return;
    }
    long time = Long.MIN_VALUE;
    Object lastVal = null;
    if (dataInThisPage.hasCurrent()
        && dataInThisPage.currentTime() < maxBound
        && dataInThisPage.currentTime() >= minBound) {
      time = dataInThisPage.currentTime();
      lastVal = dataInThisPage.currentValue();
      dataInThisPage.next();
    }

    if (time != Long.MIN_VALUE) {
      setValue(lastVal);
      timestamp = time;
    }
  }

  @Override
  public void updateResultUsingTimestamps(long[] timestamps, int length,
      IReaderByTimestamp dataReader) throws IOException {
    if (hasResult()) {
      return;
    }
    long time = Long.MIN_VALUE;
    Object lastVal = null;
    for (int i = 0; i < length; i++) {
      Object value = dataReader.getValueInTimestamp(timestamps[i]);
      if (value != null) {
        time = timestamps[i];
        lastVal = value;
        break;
      }
    }
    if (time != Long.MIN_VALUE) {
      setValue(lastVal);
      timestamp = time;
    }
  }

  @Override
  public boolean isCalculatedAggregationResult() {
    return hasResult();
  }


  @Override
  public boolean needAscReader() {
    return false;
  }
}
