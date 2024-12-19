package org.apache.iotdb.db.queryengine.execution.operator.process.window.exception;

public class FrameTypeException extends WindowFunctionException {
  public FrameTypeException(boolean isStart) {
    super(
        isStart
            ? "UNBOUND FOLLOWING is not allowed in frame start!"
            : "UNBOUND PRECEDING is not allowed in frame end!");
  }
}
