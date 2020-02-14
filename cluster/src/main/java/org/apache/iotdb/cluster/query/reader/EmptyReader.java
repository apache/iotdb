package org.apache.iotdb.cluster.query.reader;

import java.io.IOException;
import org.apache.iotdb.db.query.reader.ManagedSeriesReader;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;

/**
 * A placeholder when the remote node does not contain satisfying data of a series.
 */
public class EmptyReader implements ManagedSeriesReader {

  private volatile boolean managedByPool;
  private volatile boolean hasRemaining;

  @Override
  public boolean isManagedByQueryManager() {
    return managedByPool;
  }

  @Override
  public void setManagedByQueryManager(boolean managedByQueryManager) {
    this.managedByPool = managedByQueryManager;
  }

  @Override
  public boolean hasRemaining() {
    return hasRemaining;
  }

  @Override
  public void setHasRemaining(boolean hasRemaining) {
    this.hasRemaining = hasRemaining;
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
  public TimeValuePair current() throws IOException {
    return null;
  }

  @Override
  public boolean hasNextBatch() throws IOException {
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
