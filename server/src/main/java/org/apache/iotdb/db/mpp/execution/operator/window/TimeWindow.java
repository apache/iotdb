package org.apache.iotdb.db.mpp.execution.operator.window;

import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.column.Column;

public class TimeWindow implements IWindow {

  private TimeRange curTimeRange;

  private long curMinTime;
  private long curMaxTime;

  public TimeWindow() {}

  public TimeWindow(TimeRange curTimeRange) {
    this.curTimeRange = curTimeRange;
    this.curMinTime = curTimeRange.getMin();
    this.curMaxTime = curTimeRange.getMax();
  }

  public TimeRange getCurTimeRange() {
    return curTimeRange;
  }

  public long getCurMinTime() {
    return curMinTime;
  }

  public long getCurMaxTime() {
    return curMaxTime;
  }

  @Override
  public int getControlColumnIndex() {
    return 0;
  }

  @Override
  public boolean satisfy(Column column, int index) {
    long curTime = column.getLong(index);
    return curTime <= this.curMaxTime && curTime >= this.curMinTime;
  }

  @Override
  public boolean isTimeWindow() {
    return true;
  }

  @Override
  public void mergeOnePoint() {}

  @Override
  public void update(TimeRange curTimeRange) {
    this.curTimeRange = curTimeRange;
    this.curMinTime = curTimeRange.getMin();
    this.curMaxTime = curTimeRange.getMax();
  }
}
