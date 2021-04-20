package org.apache.iotdb.db.exception.layoutoptimize;

public class LayoutOptimizeException extends Exception {
  protected int errorCode;

  public LayoutOptimizeException(String message) {
    super(message);
  }

  public LayoutOptimizeException(String message, int errorCode) {
    super(message);
    this.errorCode = errorCode;
  }

  public LayoutOptimizeException(String message, Throwable cause, int errorCode) {
    super(message, cause);
    this.errorCode = errorCode;
  }

  public LayoutOptimizeException(Throwable cause, int errorCode) {
    super(cause);
    this.errorCode = errorCode;
  }

  public int getErrorCode() {
    return errorCode;
  }
}
