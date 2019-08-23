package org.apache.iotdb.db.tools.logvisual;

import java.io.IOException;

public interface LogParser {

  /**
   * return the next LogEntry or null if there is no more logs.
   */
  LogEntry next() throws IOException;

  void close() throws IOException;

  void reset() throws IOException;
}