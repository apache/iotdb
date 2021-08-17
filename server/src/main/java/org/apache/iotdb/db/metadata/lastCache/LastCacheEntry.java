package org.apache.iotdb.db.metadata.lastCache;

import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

public class LastCacheEntry implements ILastCacheEntry {

  ILastCacheValue lastCacheValue;

  @Override
  public void init(int size) {
    if (size > 1) {
      lastCacheValue = new VectorLastCacheValue(size);
    }
  }

  @Override
  public TimeValuePair getCachedLast() {
    return null;
  }

  @Override
  public TimeValuePair getCachedLast(int index) {
    return null;
  }

  @Override
  public void updateCachedLast(
      TimeValuePair timeValuePair, boolean highPriorityUpdate, Long latestFlushedTime) {
    if (timeValuePair == null || timeValuePair.getValue() == null) {
      return;
    }

    if (lastCacheValue == null) {
      // If no cached last, (1) a last query (2) an unseq insertion or (3) a seq insertion will
      // update cache.
      if (!highPriorityUpdate || latestFlushedTime <= timeValuePair.getTimestamp()) {
        lastCacheValue =
            new MonadLastCacheValue(timeValuePair.getTimestamp(), timeValuePair.getValue());
      }
    } else if (timeValuePair.getTimestamp() > lastCacheValue.getTimestamp()
        || (timeValuePair.getTimestamp() == lastCacheValue.getTimestamp() && highPriorityUpdate)) {
      lastCacheValue.setTimestamp(timeValuePair.getTimestamp());
      lastCacheValue.setValue(timeValuePair.getValue());
    }
  }

  @Override
  public void updateCachedLast(
      int index, TimeValuePair timeValuePair, boolean highPriorityUpdate, Long latestFlushedTime) {
    if (timeValuePair == null || timeValuePair.getValue() == null) {
      return;
    }

    if (lastCacheValue == null) {
      // If no cached last, (1) a last query (2) an unseq insertion or (3) a seq insertion will
      // update cache.
      if (!highPriorityUpdate || latestFlushedTime <= timeValuePair.getTimestamp()) {
        lastCacheValue = new VectorLastCacheValue();
        lastCacheValue.setTimestamp(timeValuePair.getTimestamp());
        lastCacheValue.setValue(index, timeValuePair.getValue());
      }
    } else if (timeValuePair.getTimestamp() > lastCacheValue.getTimestamp()
        || (timeValuePair.getTimestamp() == lastCacheValue.getTimestamp() && highPriorityUpdate)) {
      lastCacheValue.setTimestamp(timeValuePair.getTimestamp());
      lastCacheValue.setValue(index, timeValuePair.getValue());
    }
  }

  @Override
  public void updateCachedLast(
      long timestamp,
      TsPrimitiveType[] values,
      boolean highPriorityUpdate,
      Long latestFlushedTime) {}

  @Override
  public void resetCache() {}

  @Override
  public boolean isEmpty() {
    return lastCacheValue == null;
  }
}
