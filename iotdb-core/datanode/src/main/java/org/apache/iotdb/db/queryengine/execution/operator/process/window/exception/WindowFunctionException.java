package org.apache.iotdb.db.queryengine.execution.operator.process.window.exception;

public class WindowFunctionException extends RuntimeException {
  public WindowFunctionException(String message) {
    super(message);
  }
}
