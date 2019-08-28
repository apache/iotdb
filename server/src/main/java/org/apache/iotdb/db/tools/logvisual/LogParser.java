package org.apache.iotdb.db.tools.logvisual;

import java.io.IOException;

/**
 * LogParser works as an iterator of logs.
 */
public interface LogParser {

  /**
   * return the next LogEntry or null if there is no more logs.
   */
  LogEntry next() throws IOException;

  /**
   * Release resources such as file streams.
   * @throws IOException
   */
  void close() throws IOException;

  /**
   * Start the parse from the beginning. Must be called before the first call to next().
   * @throws IOException
   */
  void reset() throws IOException;
}