package org.apache.iotdb.pipe.api.exception;

public class PipeManagementException extends PipeException {

  public PipeManagementException(String message, Throwable cause) {
    super(message);
    this.initCause(cause);
  }

  public PipeManagementException(String message) {
    super(message);
  }
}
