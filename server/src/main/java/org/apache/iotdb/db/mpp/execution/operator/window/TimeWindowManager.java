package org.apache.iotdb.db.mpp.execution.operator.window;

import org.apache.iotdb.db.mpp.aggregation.timerangeiterator.ITimeRangeIterator;
import org.apache.iotdb.db.mpp.execution.operator.AggregationUtil;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockUtil;

public class TimeWindowManager implements IWindowManager {

  private TimeWindow curWindow;
  private boolean initialized;

  private boolean ascending;

  private ITimeRangeIterator timeRangeIterator;

  public TimeWindowManager(ITimeRangeIterator timeRangeIterator) {
    this.timeRangeIterator = timeRangeIterator;
    this.initialized = false;
    this.curWindow = new TimeWindow();
    this.ascending = timeRangeIterator.isAscending();
  }

  @Override
  public boolean isCurWindowInit() {
    return this.initialized;
  }

  @Override
  public void initCurWindow(TsBlock tsBlock) {
    this.curWindow.update(this.timeRangeIterator.nextTimeRange());
    this.initialized = true;
  }

  @Override
  public boolean hasNext() {
    return this.timeRangeIterator.hasNextTimeRange();
  }

  @Override
  public void genNextWindow() {
    this.initialized = false;
  }

  @Override
  public long currentOutputTime() {
    return timeRangeIterator.currentOutputTime();
  }

  @Override
  public IWindow getCurWindow() {
    return curWindow;
  }

  @Override
  public TsBlock skipPointsOutOfTimeRange(TsBlock inputTsBlock) {
    if ((ascending && inputTsBlock.getStartTime() < curWindow.getCurMinTime())
        || (!ascending && inputTsBlock.getStartTime() > curWindow.getCurMaxTime())) {
      return TsBlockUtil.skipPointsOutOfTimeRange(
          inputTsBlock, curWindow.getCurTimeRange(), ascending);
    }
    return inputTsBlock;
  }

  @Override
  public boolean satisfiedTimeRange(TsBlock inputTsBlock) {
    return AggregationUtil.satisfiedTimeRange(inputTsBlock, curWindow.getCurTimeRange(), ascending);
  }

  @Override
  public boolean isTsBlockOutOfBound(TsBlock inputTsBlock) {
    return inputTsBlock != null
        && (this.ascending
            ? inputTsBlock.getEndTime() > this.curWindow.getCurMaxTime()
            : inputTsBlock.getEndTime() < this.curWindow.getCurMinTime());
  }
}
