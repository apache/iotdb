package org.apache.iotdb.db.query.reader.seriesRelated;

import java.io.IOException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

public class SeriesReaderWithValueFilter extends SeriesReaderWithoutValueFilter {

  private Filter filter;
  private boolean hasCachedValue;
  private TimeValuePair timeValuePair;

  public SeriesReaderWithValueFilter(Path path, Filter filter, QueryContext context)
      throws StorageEngineException, IOException {
    super(path, filter, context, false);
    this.filter = filter;
  }

  @Override
  public boolean hasNext() throws IOException {
    if (hasCachedValue) {
      return true;
    }
    while (super.hasNext()) {
      timeValuePair = super.next();
      if (filter.satisfy(timeValuePair.getTimestamp(), timeValuePair.getValue().getValue())) {
        hasCachedValue = true;
        return true;
      }
    }
    return false;
  }

  @Override
  public TimeValuePair next() throws IOException {
    if (hasCachedValue || hasNext()) {
      hasCachedValue = false;
      return timeValuePair;
    } else {
      throw new IOException("data reader is out of bound.");
    }
  }


  @Override
  public TimeValuePair current() throws IOException {
    return timeValuePair;
  }
}