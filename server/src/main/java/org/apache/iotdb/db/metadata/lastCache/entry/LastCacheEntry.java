package org.apache.iotdb.db.metadata.lastCache.entry;

import org.apache.iotdb.db.metadata.lastCache.entry.value.ILastCacheValue;
import org.apache.iotdb.db.metadata.lastCache.entry.value.MonadLastCacheValue;
import org.apache.iotdb.db.metadata.lastCache.entry.value.VectorLastCacheValue;
import org.apache.iotdb.tsfile.read.TimeValuePair;

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
    return lastCacheValue == null ? null : lastCacheValue.getTimeValuePair();
  }

  @Override
  public TimeValuePair getCachedLast(int index) {
    return lastCacheValue == null ? null : lastCacheValue.getTimeValuePair(index);
  }

  @Override
  public synchronized void updateCachedLast(
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
  public synchronized void updateCachedLast(
      int index, TimeValuePair timeValuePair, boolean highPriorityUpdate, Long latestFlushedTime) {
    if (timeValuePair == null || timeValuePair.getValue() == null) {
      return;
    }

    if (lastCacheValue.getTimeValuePair(index) == null) {
      // If no cached last, (1) a last query (2) an unseq insertion or (3) a seq insertion will
      // update cache.
      if (!highPriorityUpdate || latestFlushedTime <= timeValuePair.getTimestamp()) {
        lastCacheValue.setTimestamp(index, timeValuePair.getTimestamp());
        lastCacheValue.setValue(index, timeValuePair.getValue());
      }
    } else if (timeValuePair.getTimestamp() > lastCacheValue.getTimestamp(index)) {
      lastCacheValue.setTimestamp(index, timeValuePair.getTimestamp());
      lastCacheValue.setValue(index, timeValuePair.getValue());
    } else if (timeValuePair.getTimestamp() == lastCacheValue.getTimestamp(index)) {
      if (highPriorityUpdate || lastCacheValue.getValue(index) == null) {
        lastCacheValue.setTimestamp(index, timeValuePair.getTimestamp());
        lastCacheValue.setValue(index, timeValuePair.getValue());
      }
    }
  }

  @Override
  public synchronized void resetLastCache() {
    lastCacheValue = null;
  }

  @Override
  public void resetLastCache(int index) {
    if (lastCacheValue instanceof VectorLastCacheValue) {
      lastCacheValue.setValue(index, null);
    } else {
      lastCacheValue = null;
    }
  }

  @Override
  public boolean isEmpty() {
    return lastCacheValue == null;
  }
}
