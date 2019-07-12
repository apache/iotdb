package org.apache.iotdb.db.query.reader.chunkRelated;

import java.util.Iterator;
import org.apache.iotdb.db.engine.memtable.TimeValuePairSorter;
import org.apache.iotdb.db.query.reader.IReaderByTimestamp;
import org.apache.iotdb.db.utils.TimeValuePair;

public class MemChunkReaderByTimestamp implements IReaderByTimestamp {

  private Iterator<TimeValuePair> timeValuePairIterator;
  private boolean hasCachedTimeValuePair;
  private TimeValuePair cachedTimeValuePair;

  public MemChunkReaderByTimestamp(TimeValuePairSorter readableChunk) {
    timeValuePairIterator = readableChunk.getIterator();
  }

  @Override
  public boolean hasNext() {
    if (hasCachedTimeValuePair) {
      return true;
    }
    return timeValuePairIterator.hasNext();
  }

  private TimeValuePair next() {
    if (hasCachedTimeValuePair) {
      hasCachedTimeValuePair = false;
      return cachedTimeValuePair;
    } else {
      return timeValuePairIterator.next();
    }
  }

  // TODO consider change timeValuePairIterator to List structure, and use binary search instead of
  // sequential search
  @Override
  public Object getValueInTimestamp(long timestamp) {
    while (hasNext()) {
      TimeValuePair timeValuePair = next();
      long time = timeValuePair.getTimestamp();
      if (time == timestamp) {
        return timeValuePair.getValue().getValue();
      } else if (time > timestamp) {
        hasCachedTimeValuePair = true;
        cachedTimeValuePair = timeValuePair;
        break;
      }
    }
    return null;
  }
}
