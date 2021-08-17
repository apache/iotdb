package org.apache.iotdb.db.metadata.lastCache;

import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

public interface ILastCacheEntry {

  void init(int size);

  TimeValuePair getCachedLast();

  TimeValuePair getCachedLast(int index);

  /**
   * update last point cache
   *
   * @param timeValuePair last point
   * @param highPriorityUpdate whether it's a high priority update
   * @param latestFlushedTime latest flushed time
   */
  void updateCachedLast(
      TimeValuePair timeValuePair, boolean highPriorityUpdate, Long latestFlushedTime);

  void updateCachedLast(
      int index, TimeValuePair timeValuePair, boolean highPriorityUpdate, Long latestFlushedTime);

  void updateCachedLast(
      long timestamp, TsPrimitiveType[] values, boolean highPriorityUpdate, Long latestFlushedTime);

  void resetCache();

  boolean isEmpty();
}
