package org.apache.iotdb.db.tools.logvisual.exceptions;

public class AnalyzeException extends Exception {

  public AnalyzeException() {
  }

  public AnalyzeException(String message) {
    super(message);
  }

  public AnalyzeException(String message, Throwable cause) {
    super(message, cause);
  }

  public AnalyzeException(Throwable cause) {
    super(cause);
  }
}