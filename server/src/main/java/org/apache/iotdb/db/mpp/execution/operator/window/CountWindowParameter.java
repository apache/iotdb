package org.apache.iotdb.db.mpp.execution.operator.window;

public class CountWindowParameter extends WindowParameter {
  private final long countNumber;
  private final int controlColumnIndex;

  public CountWindowParameter(long countNumber, int controlColumnIndex, boolean needOutputEndTime) {
    super(needOutputEndTime);
    this.windowType = WindowType.COUNT_WINDOW;
    this.countNumber = countNumber;
    this.controlColumnIndex = controlColumnIndex;
  }

  public int getControlColumnIndex() {
    return controlColumnIndex;
  }

  public long getCountNumber() {
    return countNumber;
  }
}
