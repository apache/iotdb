package org.apache.iotdb.db.metadata.lastCache.entry.value;

import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

public class MonadLastCacheValue implements ILastCacheValue {

  private long timestamp;

  private TsPrimitiveType value;

  public MonadLastCacheValue(long timestamp, TsPrimitiveType value) {
    this.timestamp = timestamp;
    this.value = value;
  }

  @Override
  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  @Override
  public void setValue(TsPrimitiveType value) {
    this.value = value;
  }

  @Override
  public TimeValuePair getTimeValuePair() {
    return new TimeValuePair(timestamp, value);
  }

  @Override
  public int getSize() {
    return 1;
  }

  @Override
  public long getTimestamp(int index) {
    return index == 0 ? timestamp : 0;
  }

  @Override
  public void setTimestamp(int index, long timestamp) {
    if (index == 0) {
      this.timestamp = timestamp;
    }
  }

  @Override
  public TsPrimitiveType getValue(int index) {
    if (index == 0) {
      return value;
    }
    return null;
  }

  @Override
  public void setValue(int index, TsPrimitiveType value) {
    if (index == 0) {
      this.value = value;
    }
  }

  @Override
  public TimeValuePair getTimeValuePair(int index) {
    if (value == null || index != 0) {
      return null;
    }
    return new TimeValuePair(timestamp, value);
  }
}
