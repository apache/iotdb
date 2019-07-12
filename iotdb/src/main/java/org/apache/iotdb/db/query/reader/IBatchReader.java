package org.apache.iotdb.db.query.reader;

import java.io.IOException;
import org.apache.iotdb.tsfile.read.common.BatchData;

public interface IBatchReader {

  // TODO hasNextBatch & MemChunkReader problem
  boolean hasNext() throws IOException;

  BatchData nextBatch() throws IOException;

  void close() throws IOException;
}
