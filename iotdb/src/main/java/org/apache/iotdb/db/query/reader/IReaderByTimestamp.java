package org.apache.iotdb.db.query.reader;

import java.io.IOException;

public interface IReaderByTimestamp {

  /**
   * Given a timestamp, the reader is supposed to return the corresponding value in the timestamp.
   * If no value in this timestamp, null will be returned.
   *
   * Note that when the higher layer needs to call this function multiple times, it is required that
   * the timestamps be in strictly increasing order.
   */
  Object getValueInTimestamp(long timestamp) throws IOException;

  // TODO hasNext在这个接口里是有用的吗？
  boolean hasNext() throws IOException;
}
