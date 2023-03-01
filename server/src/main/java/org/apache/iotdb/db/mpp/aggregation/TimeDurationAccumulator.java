package org.apache.iotdb.db.mpp.aggregation;

import org.apache.iotdb.db.mpp.execution.operator.window.IWindow;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;

public class TimeDurationAccumulator implements Accumulator {
  protected long minTime = Long.MAX_VALUE;
  protected long maxTime = Long.MIN_VALUE;

  @Override
  public int addInput(Column[] column, IWindow window, boolean ignoringNull) {
    int curPositionCount = column[0].getPositionCount();
    for (int i = 0; i < curPositionCount; i++) {
      // skip null value in control column
      if (ignoringNull && column[0].isNull(i)) {
        continue;
      }
      if (!window.satisfy(column[0], i)) {
        return i;
      }
      window.mergeOnePoint(column, i);
      if (!column[2].isNull(i)) {
        updateMaxTime(column[1].getLong(i));
        updateMinTime(column[1].getLong(i));
      }
    }
    return curPositionCount;
  }

  @Override
  public void addIntermediate(Column[] partialResult) {
    if (partialResult[0].isNull(0)) {
      return;
    }
    updateMaxTime(partialResult[0].getLong(0));
    updateMinTime(partialResult[1].getLong(0));
  }

  @Override
  public void addStatistics(Statistics statistics) {
    updateMaxTime(statistics.getEndTime());
    updateMinTime(statistics.getStartTime());
  }

  @Override
  public void setFinal(Column finalResult) {
    if (finalResult.isNull(0)) {
      return;
    }
    maxTime = finalResult.getLong(0);
    minTime = 0L;
  }

  @Override
  public void outputIntermediate(ColumnBuilder[] tsBlockBuilder) {
    tsBlockBuilder[0].writeLong(maxTime);
    tsBlockBuilder[1].writeLong(minTime);
  }

  @Override
  public void outputFinal(ColumnBuilder tsBlockBuilder) {
    tsBlockBuilder.writeLong(maxTime - minTime);
  }

  @Override
  public void reset() {}

  @Override
  public boolean hasFinalResult() {
    return false;
  }

  @Override
  public TSDataType[] getIntermediateType() {
    return new TSDataType[] {TSDataType.INT64, TSDataType.INT64};
  }

  @Override
  public TSDataType getFinalType() {
    return TSDataType.getTsDataType((byte) 2);
  }

  protected void updateMaxTime(long curTime) {
    maxTime = Math.max(maxTime, curTime);
  }

  protected void updateMinTime(long curTime) {
    minTime = Math.min(minTime, curTime);
  }
}
