package org.apache.iotdb.db.mpp.execution.operator.window;

import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.Column;

public class CountWindow implements IWindow {

  private final int controlColumnIndex;
  private final boolean needOutputEndTime;
  private final long countNumber;
  private long startTime = Long.MAX_VALUE;
  private long endTime = Long.MIN_VALUE;

  private long leftCount;

  public CountWindow(CountWindowParameter countWindowParameter) {
    this.controlColumnIndex = countWindowParameter.getControlColumnIndex();
    this.needOutputEndTime = countWindowParameter.isNeedOutputEndTime();
    this.countNumber = countWindowParameter.getCountNumber();
    resetCurCount();
  }

  @Override
  public Column getControlColumn(TsBlock tsBlock) {
    return tsBlock.getColumn(controlColumnIndex);
  }

  @Override
  public boolean satisfy(Column column, int index) {
    return leftCount != 0;
  }

  @Override
  public void mergeOnePoint(Column[] timeAndValueColumn, int index) {
    long currentTime = timeAndValueColumn[1].getLong(index);
    startTime = Math.min(startTime, currentTime);
    endTime = Math.max(endTime, currentTime);
    leftCount--;
  }

  @Override
  public boolean contains(Column column) {
    return false;
  }

  public boolean isNeedOutputEndTime() {
    return needOutputEndTime;
  }

  public void resetCurCount() {
    setLeftCount(countNumber);
  }

  public long getStartTime() {
    return startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public long getLeftCount() {
    return leftCount;
  }

  public void setLeftCount(long leftCount) {
    this.leftCount = leftCount;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }
}
