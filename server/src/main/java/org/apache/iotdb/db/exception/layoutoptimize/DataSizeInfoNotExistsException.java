package org.apache.iotdb.db.exception.layoutoptimize;

public class DataSizeInfoNotExistsException extends LayoutOptimizeException {
  public DataSizeInfoNotExistsException(String message) {
    super(message);
  }

  public DataSizeInfoNotExistsException(String message, int errorCode) {
    super(message, errorCode);
  }

  public DataSizeInfoNotExistsException(String message, Throwable cause, int errorCode) {
    super(message, cause, errorCode);
  }

  public DataSizeInfoNotExistsException(Throwable cause, int errorCode) {
    super(cause, errorCode);
  }
}
