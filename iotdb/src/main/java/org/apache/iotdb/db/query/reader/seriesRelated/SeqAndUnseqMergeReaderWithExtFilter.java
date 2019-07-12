package org.apache.iotdb.db.query.reader.seriesRelated;

import java.io.IOException;
import org.apache.iotdb.db.query.reader.IBatchReader;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

public class SeqAndUnseqMergeReaderWithExtFilter extends SeqAndUnseqMergeReader {

  private Filter filter;
  private boolean hasCachedValue;
  private TimeValuePair timeValuePair;

  public SeqAndUnseqMergeReaderWithExtFilter(IBatchReader seqSeriesReader, IPointReader unseqSeriesReader,
      Filter filter) {
    super(seqSeriesReader, unseqSeriesReader);
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