package org.apache.iotdb.db.query.aggregation.impl;

import java.io.IOException;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;

public class MinTimeDescAggrResult extends MinTimeAggrResult {

  @Override
  public void updateResultFromStatistics(Statistics statistics) {
    long time = statistics.getStartTime();
    setValue(time);
  }

  @Override
  public void updateResultFromPageData(BatchData dataInThisPage, long minBound, long maxBound)
      throws IOException {
    while (dataInThisPage.hasCurrent() && dataInThisPage.currentTime() >= minBound) {
      setValue(dataInThisPage.currentTime());
      dataInThisPage.next();
    }
  }


  @Override
  public void updateResultUsingTimestamps(long[] timestamps, int length,
      IReaderByTimestamp dataReader) throws IOException {
    for (int i = 0; i < length; i++) {
      Object value = dataReader.getValueInTimestamp(timestamps[i]);
      if (value != null) {
        setLongValue(timestamps[i]);
      }
    }
  }

  @Override
  public boolean isCalculatedAggregationResult() {
    return false;
  }

  @Override
  public boolean needAscReader() {
    return false;
  }
}
