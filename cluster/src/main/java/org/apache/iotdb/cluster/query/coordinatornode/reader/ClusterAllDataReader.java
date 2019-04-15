package org.apache.iotdb.cluster.query.coordinatornode.reader;

import java.io.IOException;
import org.apache.iotdb.db.query.reader.IBatchReader;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.utils.TimeValuePair;

public class ClusterAllDataReader implements IPointReader {

  private IBatchReader rpcBatchReader;

  @Override
  public TimeValuePair current() throws IOException {
    return null;
  }

  @Override
  public boolean hasNext() throws IOException {
    return false;
  }

  @Override
  public TimeValuePair next() throws IOException {
    return null;
  }

  @Override
  public void close() throws IOException {

  }
}
