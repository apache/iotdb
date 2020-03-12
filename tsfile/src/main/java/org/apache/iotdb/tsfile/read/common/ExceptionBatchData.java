package org.apache.iotdb.tsfile.read.common;


public class ExceptionBatchData extends BatchData {

  public Exception exception;

  public ExceptionBatchData(Exception exception) {
    this.exception = exception;
  }

  @Override
  public boolean hasCurrent() {
    throw new UnsupportedOperationException("hasCurrent is not supported for ExceptionBatchData");
  }
}
