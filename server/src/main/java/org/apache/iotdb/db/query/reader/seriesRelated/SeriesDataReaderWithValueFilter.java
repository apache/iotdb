package org.apache.iotdb.db.query.reader.seriesRelated;

import java.io.IOException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

/**
 * @Author: LiuDaWei
 * @Create: 2020年01月05日
 */
public class SeriesDataReaderWithValueFilter extends SeriesDataReaderWithoutValueFilter {

  private final Filter valueFilter;
  private boolean hasCachedTimeValuePair;
  private BatchData batchData;
  private TimeValuePair timeValuePair;

  public SeriesDataReaderWithValueFilter(Path seriesPath, TSDataType dataType, Filter valueFilter,
      QueryContext context) throws StorageEngineException, IOException {
    super(seriesPath, dataType, null, context);
    this.valueFilter = valueFilter;
  }

  @Override
  protected boolean canUseStatistics(Statistics statistics) {
    return false;
  }

  public boolean hasNext() throws IOException {
    if (hasCachedTimeValuePair) {
      return true;
    }

    if (hasNextSatisfiedInCurrentBatch()) {
      return true;
    }

    // has not cached timeValuePair
    while (hasNextBatch()) {
      batchData = super.nextBatch();
      if (hasNextSatisfiedInCurrentBatch()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean hasNextBatch() throws IOException {
    while (hasNextChunk()) {
      while (hasNextPage()) {
        if (super.hasNextBatch()) {
          return true;
        }
      }
    }
    return false;
  }

  private boolean hasNextSatisfiedInCurrentBatch() {
    while (batchData != null && batchData.hasCurrent()) {
      if (valueFilter.satisfy(batchData.currentTime(), batchData.currentValue())) {
        timeValuePair = new TimeValuePair(batchData.currentTime(),
            batchData.currentTsPrimitiveType());
        hasCachedTimeValuePair = true;
        batchData.next();
        return true;
      }
      batchData.next();
    }
    return false;
  }

  public TimeValuePair next() throws IOException {
    if (hasCachedTimeValuePair || hasNext()) {
      hasCachedTimeValuePair = false;
      return timeValuePair;
    } else {
      throw new IOException("no next data");
    }
  }

  public TimeValuePair current() {
    return timeValuePair;
  }
}
