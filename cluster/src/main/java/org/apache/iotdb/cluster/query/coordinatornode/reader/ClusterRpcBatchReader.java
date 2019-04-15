package org.apache.iotdb.cluster.query.coordinatornode.reader;

import java.io.IOException;
import org.apache.iotdb.db.query.reader.IBatchReader;
import org.apache.iotdb.tsfile.read.common.BatchData;

public class ClusterRpcBatchReader implements IBatchReader {

  @Override
  public boolean hasNext() throws IOException {
    return false;
  }

  @Override
  public BatchData nextBatch() throws IOException {
    return null;
  }

  @Override
  public void close() throws IOException {

  }
}
