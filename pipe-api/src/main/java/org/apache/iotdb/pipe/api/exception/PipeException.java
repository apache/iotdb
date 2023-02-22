package org.apache.iotdb.pipe.api.exception;

public class PipeException extends RuntimeException {

  public PipeException(String message) {
    super(message);
  }

  public PipeException(String message, Throwable cause) {
    super(message, cause);
  }
}
