package org.apache.iotdb.db.query.aggregation.impl;

import java.io.IOException;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;

public class MaxTimeDescAggrResult extends MaxTimeAggrResult {


  @Override
  public void updateResultFromStatistics(Statistics statistics) {
    if (hasResult()) {
      return;
    }
    super.updateResultFromStatistics(statistics);
  }

  @Override
  public void updateResultFromPageData(BatchData dataInThisPage, long minBound, long maxBound)
      throws IOException {
    if (hasResult()) {
      return;
    }
    if (dataInThisPage.hasCurrent()
        && dataInThisPage.currentTime() < maxBound
        && dataInThisPage.currentTime() >= minBound) {
      updateMaxTimeResult(dataInThisPage.currentTime());
    }
  }

  @Override
  public void updateResultUsingTimestamps(long[] timestamps, int length,
      IReaderByTimestamp dataReader) throws IOException {
    if (hasResult()) {
      return;
    }
    long time = -1;
    for (int i = 0; i < length; i++) {
      Object value = dataReader.getValueInTimestamp(timestamps[i]);
      if (value != null) {
        time = timestamps[i];
        break;
      }
    }

    if (time == -1) {
      return;
    }
    updateMaxTimeResult(time);
  }

  @Override
  public boolean isCalculatedAggregationResult() {
    return hasResult;
  }

  @Override
  public boolean needAscReader() {
    return false;
  }
}
