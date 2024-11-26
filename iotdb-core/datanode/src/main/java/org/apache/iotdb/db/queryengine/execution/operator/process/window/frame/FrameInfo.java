package org.apache.iotdb.db.queryengine.execution.operator.process.window.frame;

public class FrameInfo {
  public enum FrameType {
    RANGE, ROWS, GROUPS
  }

  public enum FrameBoundType {
    UNBOUNDED_PRECEDING,
    PRECEDING,
    CURRENT_ROW,
    FOLLOWING,
    UNBOUNDED_FOLLOWING
  }

  private final FrameType frameType;
  private final FrameBoundType startType;
  private final FrameBoundType endType;

  public FrameInfo(FrameType frameType, FrameBoundType startType, FrameBoundType endType) {
    this.frameType = frameType;
    this.startType = startType;
    this.endType = endType;
  }

  public FrameType getFrameType() {
    return frameType;
  }

  public FrameBoundType getStartType() {
    return startType;
  }

  public FrameBoundType getEndType() {
    return endType;
  }
}
