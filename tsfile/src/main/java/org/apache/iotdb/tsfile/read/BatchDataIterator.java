package org.apache.iotdb.tsfile.read;

import org.apache.iotdb.tsfile.read.common.BatchData;

import java.io.IOException;

public class BatchDataIterator implements IPointReader {

  private BatchData batchData;

  public BatchDataIterator(BatchData batchData) {
    this.batchData = batchData;
  }

  @Override
  public boolean hasNextTimeValuePair() {
    return batchData.hasCurrent();
  }

  @Override
  public TimeValuePair nextTimeValuePair() {
    TimeValuePair timeValuePair = new TimeValuePair(batchData.currentTime(), batchData.currentTsPrimitiveType());
    batchData.next();
    return timeValuePair;
  }

  @Override
  public TimeValuePair currentTimeValuePair() {
    return new TimeValuePair(batchData.currentTime(), batchData.currentTsPrimitiveType());
  }

  @Override
  public void close() throws IOException {
    batchData = null;
  }
}
