package org.apache.iotdb.db.exception.layoutoptimize;

public class LayoutNotExistException extends LayoutOptimizeException {
  public LayoutNotExistException(String message) {
    super(message);
  }

  public LayoutNotExistException(String message, int errorCode) {
    super(message, errorCode);
  }

  public LayoutNotExistException(String message, Throwable cause, int errorCode) {
    super(message, cause, errorCode);
  }

  public LayoutNotExistException(Throwable cause, int errorCode) {
    super(cause, errorCode);
  }
}
