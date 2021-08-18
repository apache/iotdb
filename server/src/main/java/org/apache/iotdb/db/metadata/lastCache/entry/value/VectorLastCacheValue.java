package org.apache.iotdb.db.metadata.lastCache.entry.value;

import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

public class VectorLastCacheValue implements ILastCacheValue {

  private long[] timestamps;

  private TsPrimitiveType[] values;

  public VectorLastCacheValue(int size) {
    timestamps = new long[size];
    values = new TsPrimitiveType[size];
  }

  @Override
  public int getSize() {
    return values.length;
  }

  @Override
  public long getTimestamp(int index) {
    return timestamps[index];
  }

  @Override
  public void setTimestamp(int index, long timestamp) {
    timestamps[index] = timestamp;
  }

  @Override
  public TsPrimitiveType getValue(int index) {
    return values == null ? null : values[index];
  }

  @Override
  public void setValue(int index, TsPrimitiveType value) {
    values[index] = value;
  }

  @Override
  public TimeValuePair getTimeValuePair(int index) {
    if (values == null || index < 0 || index >= values.length || values[index] == null) {
      return null;
    }
    return new TimeValuePair(timestamps[index], values[index]);
  }

  @Override
  public long getTimestamp() {
    return 0;
  }

  @Override
  public void setTimestamp(long timestamp) {}

  @Override
  public void setValue(TsPrimitiveType value) {}

  @Override
  public TimeValuePair getTimeValuePair() {
    return null;
  }
}
