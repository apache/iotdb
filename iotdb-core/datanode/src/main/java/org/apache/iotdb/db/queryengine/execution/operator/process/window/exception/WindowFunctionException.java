package org.apache.iotdb.db.queryengine.execution.operator.process.window.exception;

import org.apache.iotdb.db.queryengine.execution.operator.process.window.function.WindowFunction;

public class WindowFunctionException extends RuntimeException {
  public WindowFunctionException(String message) {
    super(message);
  }

  public WindowFunctionException(String message, Throwable cause) {
    super(message, cause);
  }
}
