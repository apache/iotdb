package org.apache.iotdb.db.metadata.lastCache;

import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

public interface ILastCacheValue {

  long getTimestamp();

  TimeValuePair getTimeValuePair();

  TimeValuePair getTimeValuePair(int index);

  void setTimestamp(long timestamp);

  void setValue(TsPrimitiveType value);

  void setValue(int index, TsPrimitiveType value);

  void setValue(TsPrimitiveType[] values);
}
