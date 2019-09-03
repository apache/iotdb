package org.apache.iotdb.db.tools.logvisual.exceptions;

public class VisualizationException extends Exception {

  public VisualizationException() {
  }

  public VisualizationException(String message) {
    super(message);
  }

  public VisualizationException(String message, Throwable cause) {
    super(message, cause);
  }

  public VisualizationException(Throwable cause) {
    super(cause);
  }
}