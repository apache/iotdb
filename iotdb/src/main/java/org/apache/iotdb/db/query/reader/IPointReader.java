package org.apache.iotdb.db.query.reader;

import java.io.IOException;
import org.apache.iotdb.db.utils.TimeValuePair;

public interface IPointReader {

  boolean hasNext() throws IOException;

  TimeValuePair next() throws IOException;

  TimeValuePair current() throws IOException;

  void close() throws IOException;
}
