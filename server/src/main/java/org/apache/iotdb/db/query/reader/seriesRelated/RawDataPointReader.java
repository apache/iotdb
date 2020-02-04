package org.apache.iotdb.db.query.reader.seriesRelated;

import java.io.IOException;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.IPointReader;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;


public class RawDataPointReader implements IPointReader {

  private final SeriesReader seriesReader;

  private boolean hasCachedTimeValuePair;
  private BatchData batchData;
  private TimeValuePair timeValuePair;


  public RawDataPointReader(SeriesReader seriesReader) {
    this.seriesReader = seriesReader;
  }

  public RawDataPointReader(Path seriesPath, TSDataType dataType, QueryContext context,
      QueryDataSource dataSource, Filter timeFilter, Filter valueFilter) {
    this.seriesReader = new SeriesReader(seriesPath, dataType, context, dataSource, timeFilter,
        valueFilter);
  }

  private boolean hasNext() throws IOException {
    while (seriesReader.hasNextChunk()) {
      while (seriesReader.hasNextPage()) {
        if (seriesReader.hasNextOverlappedPage()) {
          return true;
        }
      }
    }
    return false;
  }

  private boolean hasNextSatisfiedInCurrentBatch() {
    while (batchData != null && batchData.hasCurrent()) {
      timeValuePair = new TimeValuePair(batchData.currentTime(),
          batchData.currentTsPrimitiveType());
      hasCachedTimeValuePair = true;
      batchData.next();
      return true;
    }
    return false;
  }

  @Override
  public boolean hasNextTimeValuePair() throws IOException {
    if (hasCachedTimeValuePair) {
      return true;
    }

    if (hasNextSatisfiedInCurrentBatch()) {
      return true;
    }

    // has not cached timeValuePair
    while (hasNext()) {
      batchData = seriesReader.nextOverlappedPage();
      if (hasNextSatisfiedInCurrentBatch()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public TimeValuePair nextTimeValuePair() throws IOException {
    if (hasCachedTimeValuePair || hasNextTimeValuePair()) {
      hasCachedTimeValuePair = false;
      return timeValuePair;
    } else {
      throw new IOException("no next data");
    }
  }

  @Override
  public TimeValuePair currentTimeValuePair() throws IOException {
    return timeValuePair;
  }

  @Override
  public void close() throws IOException {
  }
}
