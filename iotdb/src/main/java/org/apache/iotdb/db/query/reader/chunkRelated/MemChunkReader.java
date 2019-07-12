package org.apache.iotdb.db.query.reader.chunkRelated;

import java.util.Iterator;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.query.reader.IAggregateReader;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

public class MemChunkReader implements IPointReader, IAggregateReader {

  private Iterator<TimeValuePair> timeValuePairIterator;
  private Filter filter;
  private boolean hasCachedTimeValuePair;
  private TimeValuePair cachedTimeValuePair;

  private TSDataType dataType;

  /**
   * memory data reader.
   */
  public MemChunkReader(ReadOnlyMemChunk readableChunk, Filter filter) {
    timeValuePairIterator = readableChunk.getIterator();
    this.filter = filter;
    this.dataType = readableChunk.getDataType();
  }

  @Override
  public boolean hasNext() {
    if (hasCachedTimeValuePair) {
      return true;
    }
    while (timeValuePairIterator.hasNext()) {
      TimeValuePair timeValuePair = timeValuePairIterator.next();
      if (filter == null || filter
          .satisfy(timeValuePair.getTimestamp(), timeValuePair.getValue().getValue())) {
        hasCachedTimeValuePair = true;
        cachedTimeValuePair = timeValuePair;
        break;
      }
    }
    return hasCachedTimeValuePair;
  }

  @Override
  public TimeValuePair next() {
    if (hasCachedTimeValuePair) {
      hasCachedTimeValuePair = false;
      return cachedTimeValuePair;
    } else {
      return timeValuePairIterator.next();
    }
  }

  @Override
  public TimeValuePair current() {
    if (!hasCachedTimeValuePair) {
      cachedTimeValuePair = timeValuePairIterator.next();
      hasCachedTimeValuePair = true;
    }
    return cachedTimeValuePair;
  }

  @Override
  public BatchData nextBatch() {
    BatchData batchData = new BatchData(dataType, true);
    if (hasCachedTimeValuePair) {
      hasCachedTimeValuePair = false;
      batchData.putTime(cachedTimeValuePair.getTimestamp());
      batchData.putAnObject(cachedTimeValuePair.getValue().getValue());
    }
    while (timeValuePairIterator.hasNext()) {
      TimeValuePair timeValuePair = timeValuePairIterator.next();
      if (filter == null || filter
          .satisfy(timeValuePair.getTimestamp(), timeValuePair.getValue().getValue())) {
        batchData.putTime(timeValuePair.getTimestamp());
        batchData.putAnObject(timeValuePair.getValue().getValue());
      }
    }
    return batchData;
  }

  @Override
  public void close() {
    // Do nothing because mem chunk reader will not open files
  }

  @Override
  public PageHeader nextPageHeader() {
    return null;
  }

  @Override
  public void skipPageData() {
    nextBatch();
  }
}