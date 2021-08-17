package org.apache.iotdb.db.metadata.lastCache;

import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

public class MonadLastCacheValue extends LastCacheValue {

  private TsPrimitiveType value;

  public MonadLastCacheValue() {}

  public MonadLastCacheValue(long timestamp, TsPrimitiveType value) {
    this.timestamp = timestamp;
    this.value = value;
  }

  @Override
  public TimeValuePair getTimeValuePair() {
    return new TimeValuePair(timestamp, value);
  }

  @Override
  public TimeValuePair getTimeValuePair(int index) {
    return null;
  }

  @Override
  public void setValue(TsPrimitiveType value) {
    this.value = value;
  }

  @Override
  public void setValue(int index, TsPrimitiveType value) {}

  @Override
  public void setValue(TsPrimitiveType[] values) {}
}
