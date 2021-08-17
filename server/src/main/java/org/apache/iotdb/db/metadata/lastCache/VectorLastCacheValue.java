package org.apache.iotdb.db.metadata.lastCache;

import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import java.util.Arrays;

public class VectorLastCacheValue extends LastCacheValue {

  private TsPrimitiveType[] values;

  public VectorLastCacheValue() {}

  public VectorLastCacheValue(int size) {
    values = new TsPrimitiveType[size];
  }

  @Override
  public TimeValuePair getTimeValuePair() {
    return null;
  }

  @Override
  public TimeValuePair getTimeValuePair(int index) {
    return new TimeValuePair(timestamp, values[index]);
  }

  @Override
  public void setValue(TsPrimitiveType value) {}

  @Override
  public void setValue(int index, TsPrimitiveType value) {
    if (values == null) {
      values = new TsPrimitiveType[index + 1];
    }
    if (values.length <= index) {
      values = Arrays.copyOf(values, index + 1);
    }
    values[index] = value;
  }

  @Override
  public void setValue(TsPrimitiveType[] values) {
    this.values = values;
  }
}
