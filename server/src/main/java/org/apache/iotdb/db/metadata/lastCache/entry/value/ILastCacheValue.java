package org.apache.iotdb.db.metadata.lastCache.entry.value;

import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

public interface ILastCacheValue {

  long getTimestamp();

  void setTimestamp(long timestamp);

  void setValue(TsPrimitiveType value);

  TimeValuePair getTimeValuePair();

  int getSize();

  long getTimestamp(int index);

  void setTimestamp(int index, long timestamp);

  TsPrimitiveType getValue(int index);

  void setValue(int index, TsPrimitiveType value);

  TimeValuePair getTimeValuePair(int index);
}
