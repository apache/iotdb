package org.apache.iotdb.db.tools.logvisual.exceptions;

public class VisualizeException extends Exception {

  public VisualizeException() {
  }

  public VisualizeException(String message) {
    super(message);
  }

  public VisualizeException(String message, Throwable cause) {
    super(message, cause);
  }

  public VisualizeException(Throwable cause) {
    super(cause);
  }
}