package org.apache.iotdb.cluster.query.reader;

import java.io.IOException;
import org.apache.iotdb.db.query.reader.series.IAggregateReader;
import org.apache.iotdb.db.query.reader.series.ManagedSeriesReader;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.IPointReader;

/**
 * A placeholder when the remote node does not contain satisfying data of a series.
 */
public class EmptyReader implements ManagedSeriesReader, IAggregateReader, IPointReader {

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
  public boolean hasNextBatch() {
    return false;
  }

  @Override
  public BatchData nextBatch() {
    return null;
  }

  @Override
  public boolean hasNextTimeValuePair() throws IOException {
    return false;
  }

  @Override
  public TimeValuePair nextTimeValuePair() throws IOException {
    return null;
  }

  @Override
  public TimeValuePair currentTimeValuePair() throws IOException {
    return null;
  }

  @Override
  public void close() {

  }

  @Override
  public boolean hasNextChunk() {
    return false;
  }

  @Override
  public boolean canUseCurrentChunkStatistics() {
    return false;
  }

  @Override
  public Statistics currentChunkStatistics() {
    return null;
  }

  @Override
  public void skipCurrentChunk() {

  }

  @Override
  public boolean hasNextPage() {
    return false;
  }

  @Override
  public boolean canUseCurrentPageStatistics() {
    return false;
  }

  @Override
  public Statistics currentPageStatistics() {
    return null;
  }

  @Override
  public void skipCurrentPage() {

  }

  @Override
  public BatchData nextPage() throws IOException {
    return null;
  }
}
